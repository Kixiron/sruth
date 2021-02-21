use crate::{
    dataflow::{
        operators::{CollectCastable, CollectDeclarations, CountExt, FilterMap},
        Program,
    },
    repr::{function::FunctionDesc, instruction::Call, terminator::Return, InstructionExt},
};
use differential_dataflow::{
    algorithms::graphs::propagate,
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Iterate, Join, Reduce, Threshold},
    ExchangeData,
};
use std::ops::Mul;
use timely::dataflow::Scope;

pub trait Cleanup {
    fn cleanup(&self) -> Self;

    fn compact_basic_blocks(&self) -> Self;

    fn cull_unreachable_blocks(&self) -> Self;
}

// TODO: Implement this for `ArrangedProgram`, try to squeeze some perf out of it
impl<S, R> Cleanup for Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    fn cleanup(&self) -> Self {
        // TODO: Rewrite as one single `.scoped()` using `SemigroupVariable`s that's mutually
        //       recursive between the set of used instructions, blocks and functions. Maybe
        //       use `Present` as the scope's inner difference type, ask Nami
        // TODO: Start with a set of desired functions
        // TODO: Only include reachable return statements
        // TODO: Update `FunctionMeta`s
        // TODO: Update `BasicBlockMeta`s
        let program = self.instructions.scope().region_named("Cleanup", |region| {
            let program = self.enter_region(region);

            // TODO: Filter for reachable returns
            let returned_vars = program.block_terminators.collect_castable::<Return>();
            let declared_vars = program.instructions.collect_declarations();

            // The instructions required for the program to be valid
            let required_instructions = declared_vars
                .semijoin(&returned_vars.filter_map(|(_, ret)| ret.returned_var()))
                .map(|(_, inst)| inst)
                .iterate(|required| {
                    let instructions = program.instructions.enter(&required.scope());
                    let declared_vars = declared_vars.enter(&required.scope());

                    let used_vars = instructions
                        .semijoin(&required)
                        .flat_map(|(_, inst)| inst.used_vars().into_iter());

                    declared_vars
                        .semijoin(&used_vars)
                        .map(|(_, inst)| inst)
                        .concat(&required)
                        .distinct_core()
                });

            // The blocks required for the program to be valid
            let required_blocks = returned_vars
                .map(|(block, _)| block)
                .concat(
                    &program
                        .block_instructions
                        .semijoin(&required_instructions)
                        .map(|(_, block)| block),
                )
                .iterate(|blocks| {
                    let terminators = program
                        .block_terminators
                        .enter(&blocks.scope())
                        .semijoin(&blocks);

                    terminators
                        .flat_map(|(_, term)| term.jump_targets().into_iter())
                        .concat(&blocks)
                        .distinct_core()
                });

            // The functions required for program execution
            let required_functions = program
                .function_blocks
                .semijoin(&required_blocks)
                .map(|(_, func)| func)
                .iterate(|funcs| {
                    let function_blocks = program
                        .function_blocks
                        .enter(&funcs.scope())
                        .map(|(block, func)| (func, block))
                        .semijoin(&funcs)
                        .map(|(_, block)| block);

                    let inst_ids = program
                        .block_instructions
                        .enter(&funcs.scope())
                        .map(|(inst, block)| (block, inst))
                        .semijoin(&function_blocks)
                        .map(|(_, inst)| inst);

                    program
                        .instructions
                        .enter(&funcs.scope())
                        .semijoin(&inst_ids)
                        .collect_castable::<Call>()
                        .map(|(_, call)| call.func)
                        .concat(&funcs)
                        .distinct_core()
                });

            let block_instructions = program.block_instructions.semijoin(&required_instructions);
            let agg_inst = block_instructions
                .consolidate()
                .map(|(inst, block)| (block, inst))
                .reduce(|_, instructions, output| {
                    let instructions: Vec<_> = instructions.iter().map(|(&id, _)| id).collect();
                    output.push((instructions, R::from(1)));
                });

            let block_descriptors = program
                .block_descriptors
                .semijoin(&required_blocks)
                .join_map(&agg_inst, |&id, desc, instructions| {
                    let mut desc = desc.clone();
                    desc.instructions = instructions.to_owned();

                    (id, desc)
                });

            let function_blocks = program.function_blocks.semijoin(&required_blocks);
            let agg_blocks = function_blocks
                .consolidate()
                .map(|(block, func)| (func, block))
                .reduce(|_, blocks, output| {
                    let blocks: Vec<_> = blocks.iter().map(|(&id, _)| id).collect();
                    output.push((blocks, R::from(1)));
                });

            let function_descriptors = program
                .function_descriptors
                .semijoin(&required_functions)
                .join_map(&agg_blocks, |&id, desc, blocks| {
                    let mut desc = desc.clone();
                    desc.basic_blocks = blocks.to_owned();

                    (id, desc)
                });

            Program {
                instructions: program.instructions.semijoin(&required_instructions),
                block_instructions,
                block_terminators: program.block_terminators.semijoin(&required_blocks),
                block_descriptors,
                function_blocks,
                function_descriptors,
            }
            .leave_region()
        });

        if cfg!(debug_assertions) {
            self.instructions
                .join(&self.block_instructions)
                .antijoin(&program.instructions.map(|(id, _)| id))
                .consolidate()
                .inspect(|((inst_id, (inst, block_id)), _, _)| {
                    tracing::trace!(
                        inst = ?inst,
                        "removed instruction {:?} from {:?}",
                        inst_id,
                        block_id,
                    );
                });

            self.block_terminators
                .antijoin(&program.block_terminators.map(|(id, _)| id))
                .consolidate()
                .inspect(|((block_id, term), _, _)| {
                    tracing::trace!("removed terminator {:?} from {:?}", term, block_id);
                });

            self.function_blocks
                .antijoin(&program.function_blocks.map(|(id, _)| id))
                .consolidate()
                .inspect(|((block_id, func_id), _, _)| {
                    tracing::trace!("removed {:?} from {:?}", block_id, func_id);
                });

            self.function_descriptors
                .antijoin(&self.function_descriptors.map(|(id, _)| id))
                .consolidate()
                .inspect(|((func_id, _desc), _, _)| tracing::trace!("removed func {:?}", func_id));
        }

        program
    }

    fn compact_basic_blocks(&self) -> Self {
        self.instructions
            .scope()
            .region_named("compact basic blocks", |region| {
                let program = self.enter(region);

                // Find all jumps that occur
                // A collection of blocks -> the blocks they target
                let all_jumps = program.block_terminators.flat_map(|(block, term)| {
                    term.jump_targets()
                        .into_iter()
                        .map(move |target| (block, target))
                });

                // Find all blocks that are unconditionally jumped to
                // A collection of blocks -> unconditional jumps
                let unconditional_jumps = program
                    .block_terminators
                    .flat_map(|(from, term)| term.into_jump().map(move |to| (from, to)));

                // Filter blocks that are only jumped to once
                // A collection of jumped-to blocks -> the number of times they're jumped to
                let total_jumps_to = all_jumps.map(|(_, target)| target).count_core::<R>();

                // A collection of jumped-to-blocks who're only jumped to one time
                let only_jumped_to_once = total_jumps_to.filter_map(|(target, count)| {
                    if count == R::from(1) {
                        Some(target)
                    } else {
                        None
                    }
                });

                // A collection of jumped-from-blocks -> jumped-to-blocks who're only jumped to once
                // by an unconditional branch
                let single_unconditional_jumps = unconditional_jumps
                    .map(|(from, to)| (to, from))
                    .semijoin(&only_jumped_to_once)
                    .map(|(to, from)| (from, to));

                // Inline them onto the end of the block they're jumped to from
                // Remove the `from` block's terminators
                let block_terminators = program
                    .block_terminators
                    .antijoin(&single_unconditional_jumps.map(|(from, _)| from));

                // Rewrite the `InstId` -> `BasicBlockId` collection that previously
                // was associated with `to` to point to `from` as their parent block
                let rewritten_basic_blocks = program
                    .block_instructions
                    .map(|(inst, block)| (block, inst))
                    .join_map(&single_unconditional_jumps, |_from, &inst_id, &to| {
                        (inst_id, to)
                    })
                    .inspect(|((inst, new_block), _, _)| {
                        tracing::trace!("moved {:?} to {:?}", inst, new_block);
                    });

                // Remove the old `to` basic block instructions and introduce the rewritten ones
                let block_instructions = program
                    .block_instructions
                    .antijoin(&rewritten_basic_blocks.map(|(inst, _)| inst))
                    .concat(&rewritten_basic_blocks)
                    .consolidate();

                // Remove the now-redundant block
                let function_blocks = program
                    .function_blocks
                    .antijoin(&single_unconditional_jumps.map(|(to, _)| to))
                    .consolidate();

                // Rewrite functions who's entry blocks we just inlined to point to the new blocks
                let rewritten_entries = program
                    .function_descriptors
                    .map(|(func, meta)| (meta.entry, (func, meta)))
                    .join_map(
                        &single_unconditional_jumps,
                        |_dead_entry, &(func, ref meta), &new_entry| {
                            let mut meta = meta.clone();
                            meta.entry = new_entry;

                            (func, meta)
                        },
                    )
                    .inspect(|((func, meta), _, _)| {
                        tracing::trace!("changed {:?}'s entry block to {:?}", func, meta.entry);
                    });

                let function_meta = program
                    .function_descriptors
                    .antijoin(&rewritten_entries.map(|(func, _)| func))
                    .concat(&rewritten_entries);

                // Collect the basic blocks that constitute each function into a vec
                let function_block_lists = function_blocks
                    .map(|(block, func)| (func, block))
                    .reduce(|_func, blocks, output| {
                        let blocks: Vec<_> = blocks.iter().map(|(&block, _)| block).collect();
                        output.push((blocks, R::from(1)))
                    });

                // Update all function meta with the blocks they now contain
                let function_descriptors =
                    function_meta.join_map(&function_block_lists, |&func, meta, blocks| {
                        let meta = FunctionDesc {
                            basic_blocks: blocks.clone(),
                            ..meta.clone()
                        };

                        (func, meta)
                    });

                let instructions = program
                    .instructions
                    .semijoin(&block_instructions.map(|(inst, _)| inst));

                let agg_inst = block_instructions
                    .consolidate()
                    .map(|(inst, block)| (block, inst))
                    .reduce(|_, instructions, output| {
                        let instructions: Vec<_> = instructions.iter().map(|(&id, _)| id).collect();
                        output.push((instructions, R::from(1)));
                    });

                let block_descriptors = program
                    .block_descriptors
                    .semijoin(&function_blocks.map(|(block, _)| block))
                    .join_map(&agg_inst, |&id, desc, instructions| {
                        let mut desc = desc.clone();
                        desc.instructions = instructions.to_owned();

                        (id, desc)
                    });

                Program {
                    instructions,
                    block_instructions,
                    block_terminators,
                    block_descriptors,
                    function_blocks,
                    function_descriptors,
                }
                .leave_region()
            })
    }

    fn cull_unreachable_blocks(&self) -> Self {
        self.instructions
            .scope()
            .region_named("cull unreachable blocks", |region| {
                let program = self.enter(region);

                // The root nodes are the set function of entry blocks
                let value_roots = program
                    .function_descriptors
                    .map(|(id, meta)| (meta.entry, id));

                // The edges are the paths created by jumps and branches between blocks
                let value_edges = program.block_terminators.flat_map(|(block, term)| {
                    term.jump_targets()
                        .into_iter()
                        .map(move |target| (block, target))
                });

                // Propagate function ids along intra-block paths, all remaining blocks are reachable
                let mut reachable_blocks =
                    propagate::propagate(&value_edges, &value_roots).map(|(block, _)| block);

                let control_roots = program
                    .block_terminators
                    .map(|(block, _)| (block, ()))
                    .join_map(&program.function_blocks, |&node, &(), &func| (node, func));
                let control_edges = program.block_terminators.flat_map(|(src, term)| {
                    term.jump_targets().into_iter().map(move |dest| (src, dest))
                });

                reachable_blocks = reachable_blocks
                    .concat(
                        &propagate::propagate(&control_edges, &control_roots)
                            .map(|(block, _)| block),
                    )
                    .distinct_core();

                let block_instructions = program
                    .block_instructions
                    .map(|(inst, block)| (block, inst))
                    .semijoin(&reachable_blocks)
                    .map(|(block, inst)| (inst, block));

                let instructions = program
                    .instructions
                    .semijoin(&block_instructions.map(|(inst, _)| inst));

                let block_terminators = program.block_terminators.semijoin(&reachable_blocks);
                let block_descriptors = program.block_descriptors.semijoin(&reachable_blocks);
                let function_blocks = program.function_blocks.semijoin(&reachable_blocks);

                let agg_blocks = function_blocks
                    .map(|(block, func)| (func, block))
                    .consolidate()
                    .reduce(|_func, blocks, output| {
                        let blocks: Vec<_> = blocks.iter().map(|(&block, _)| block).collect();
                        output.push((blocks, R::from(1)));
                    });

                let function_descriptors =
                    program
                        .function_descriptors
                        .join_map(&agg_blocks, |&func, meta, blocks| {
                            let mut meta = meta.clone();
                            meta.basic_blocks = blocks.clone();

                            (func, meta)
                        });

                if cfg!(debug_assertions) {
                    program
                        .function_blocks
                        .antijoin(&reachable_blocks)
                        .join(&program.function_blocks)
                        .consolidate()
                        .inspect(|((block, func), _, _)| {
                            tracing::trace!("culled unreachable block {:?} from {:?}", block, func);
                        });
                }

                Program {
                    instructions,
                    block_instructions,
                    block_terminators,
                    block_descriptors,
                    function_blocks,
                    function_descriptors,
                }
                .leave_region()
            })
    }
}

// TODO
// fn eliminate_unreachable_blocks<S, R, A1, A2>(
//     scope: &mut S,
//     function_blocks: &Arranged<S, A1>,
//     terminators: &Arranged<S, A2>,
//     basic_block_ids: &Collection<S, BasicBlockId, R>,
// ) -> Collection<S, (FuncId, BasicBlockId), R>
// where
//     S: Scope,
//     S::Timestamp: Lattice + Clone,
//     R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
//     A1: TraceReader<Key = FuncId, Val = FunctionMeta, R = R, Time = S::Timestamp> + Clone + 'static,
//     A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
//         + Clone
//         + 'static,
// {
//     scope.region_named("eliminate unreachable blocks", |region| {
//         let function_blocks = function_blocks.enter(region);
//
//         let roots =
//             function_blocks.flat_map_ref(|&func_id, meta| iter::once((meta.entry, func_id)));
//         let edges = basic_block_ids
//             .enter(region)
//             .map(|block| (block, ()))
//             .join_core(&terminators.enter(region), |&block, (), term| {
//                 term.jump_targets()
//                     .into_iter()
//                     .map(move |succ| (block, succ))
//             });
//
//         let reachable_blocks =
//             propagate(&edges, &roots).map(|(block_id, func_id)| (func_id, block_id));
//
//         function_blocks
//             .flat_map_ref(|&func, meta| {
//                 meta.basic_blocks
//                     .clone()
//                     .into_iter()
//                     .map(move |block| ((func, block), ()))
//             })
//             .semijoin(&reachable_blocks)
//             .map(|(block, ())| block)
//             .leave_region()
//     })
// }
