use crate::{
    dataflow::{
        operators::{CountExt, FilterMap},
        InputManager,
    },
    repr::{
        function::FunctionMeta, BasicBlockId, FuncId, InstId, Instruction, InstructionExt,
        Terminator,
    },
};
use differential_dataflow::{
    algorithms::graphs::propagate::propagate,
    difference::{Abelian, Monoid, Semigroup},
    lattice::Lattice,
    operators::{arrange::Arranged, Consolidate, Join, JoinCore, Reduce},
    trace::TraceReader,
    Collection, ExchangeData,
};
use std::{
    iter,
    ops::{Mul, Neg},
};
use timely::dataflow::Scope;

type ReducedCode<S, R> = (
    Collection<S, (InstId, Instruction), R>,
    Collection<S, (FuncId, BasicBlockId), R>,
    Collection<S, (BasicBlockId, InstId), R>,
    Collection<S, (BasicBlockId, Terminator), R>,
    Collection<S, (FuncId, FunctionMeta), R>,
);

pub fn dead_code<S, R, A1, A2>(
    scope: &mut S,
    inputs: &mut InputManager<S::Timestamp, R>,
    instructions: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
) -> ReducedCode<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = InstId, Val = Instruction, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    let span = tracing::debug_span!("dead code elimination");
    span.in_scope(|| {
        let block_trace = inputs.basic_block_trace.import(scope);
        let basic_blocks = block_trace.flat_map_ref(|&block, meta| {
            meta.instructions
                .clone()
                .into_iter()
                .map(move |inst| (inst, block))
        });
        let basic_block_ids = block_trace.as_collection(|&block, _| block);

        let culled_instructions = eliminate_unused_assigns(
            scope,
            instructions,
            terminators,
            &basic_blocks,
            &basic_block_ids,
        );

        let function_blocks = inputs.function_trace.import(scope);
        let culled_blocks =
            eliminate_unreachable_blocks(scope, &function_blocks, terminators, &basic_block_ids);

        let (compacted_blocks, compacted_instructions, compacted_terminators, compacted_functions) =
            compact_basic_blocks(
                scope,
                &culled_blocks,
                &basic_blocks,
                &terminators,
                &function_blocks,
            );

        (
            culled_instructions,
            compacted_blocks,
            compacted_instructions,
            compacted_terminators,
            compacted_functions,
        )
    })
}

type CompactionOut<S, R> = (
    Collection<S, (FuncId, BasicBlockId), R>,
    Collection<S, (BasicBlockId, InstId), R>,
    Collection<S, (BasicBlockId, Terminator), R>,
    Collection<S, (FuncId, FunctionMeta), R>,
);

/// "Compacts" basic blocks by appending the contents of all basic blocks who's
/// only predecessor is an unconditional jump, removing unneeded blocks & branches
// TODO: Rewrite `FunctionMeta` so that we get block ordering
// TODO: Update `BasicBlockMeta`s
// TODO: Most of this should be in a `.cleanup()` operator
fn compact_basic_blocks<S, R, A1, A2>(
    scope: &mut S,
    culled_blocks: &Collection<S, (FuncId, BasicBlockId), R>,
    basic_blocks: &Collection<S, (InstId, BasicBlockId), R>,
    terminators: &Arranged<S, A1>,
    function_meta: &Arranged<S, A2>,
) -> CompactionOut<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + From<i8>,
    A1: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
    A2: TraceReader<Key = FuncId, Val = FunctionMeta, R = R, Time = S::Timestamp> + Clone + 'static,
{
    scope.region_named("compact basic blocks", |region| {
        let (culled_blocks, basic_blocks, terminators, function_meta) = (
            culled_blocks.enter(region),
            basic_blocks.enter(region),
            terminators.enter(region),
            function_meta.enter(region),
        );

        // Find all jumps that occur
        // A collection of blocks -> the blocks they target
        let all_jumps = terminators.flat_map_ref(|&block, term| {
            term.jump_targets()
                .into_iter()
                .map(move |target| (block, target))
        });

        // Find all blocks that are unconditionally jumped to
        // A collection of blocks -> unconditional jumps
        let unconditional_jumps = terminators
            .flat_map_ref(|&from, term| term.clone().into_jump().map(move |to| (from, to)));

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
        let terminators = terminators.antijoin(&single_unconditional_jumps.map(|(from, _)| from));

        // Rewrite the `InstId` -> `BasicBlockId` collection that previously
        // was associated with `to` to point to `from` as their parent block
        let rewritten_basic_blocks = basic_blocks.map(|(inst, block)| (block, inst)).join_map(
            &single_unconditional_jumps.map(|(from, to)| (to, from)),
            |_to, &inst_id, &from| (inst_id, from),
        );

        // Remove the old `to` basic block instructions and introduce the rewritten ones
        let basic_blocks = basic_blocks
            .antijoin(&rewritten_basic_blocks.map(|(inst, _)| inst))
            .concat(&rewritten_basic_blocks)
            .consolidate()
            .map(|(inst, block)| (block, inst));

        // Remove the now-redundant block
        let culled_blocks = culled_blocks
            .map(|(func, block)| (block, func))
            .antijoin(&single_unconditional_jumps.map(|(to, _)| to))
            .map(|(block, func)| (func, block))
            .consolidate();

        // Rewrite functions who's entry blocks we just inlined to point to the new blocks
        let rewritten_entries = function_meta
            .as_collection(|&func, meta| (meta.entry, (func, meta.clone())))
            .join_map(
                &single_unconditional_jumps,
                |_dead_entry, &(func, ref meta), &new_entry| {
                    let mut meta = meta.clone();
                    meta.entry = new_entry;

                    (func, meta)
                },
            );

        let function_meta = function_meta
            .as_collection(|&func, meta| (func, meta.clone()))
            .antijoin(&rewritten_entries.map(|(func, _)| func))
            .concat(&rewritten_entries);

        // Collect the basic blocks that constitute each function into a vec
        let function_block_lists = culled_blocks.reduce(|_func, blocks, output| {
            let blocks: Vec<_> = blocks.iter().map(|(&block, _)| block).collect();
            output.push((blocks, R::from(1)))
        });

        // Update all function meta with the blocks they now contain
        let function_meta = function_meta.join_map(&function_block_lists, |&func, meta, blocks| {
            let meta = FunctionMeta {
                basic_blocks: blocks.clone(),
                ..meta.clone()
            };

            (func, meta)
        });

        (
            culled_blocks.leave(),
            basic_blocks.leave(),
            terminators.leave(),
            function_meta.leave(),
        )
    })
}

fn eliminate_unused_assigns<S, R, A1, A2>(
    scope: &mut S,
    instructions: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
    basic_blocks: &Collection<S, (InstId, BasicBlockId), R>,
    basic_block_ids: &Collection<S, BasicBlockId, R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = InstId, Val = Instruction, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    scope.region_named("eliminate unused assignments", |region| {
        let (instructions, terminators, basic_blocks, basic_block_ids) = (
            instructions.enter(region),
            terminators.enter(region),
            basic_blocks.enter(region).consolidate(),
            basic_block_ids.enter(region),
        );

        let used_variables = basic_blocks
            .join_core(&instructions, |_inst_id, &block_id, inst| {
                inst.used_vars().map(move |var| (block_id, var))
            })
            .concat(
                &terminators
                    .semijoin(&basic_block_ids)
                    .flat_map(|(block_id, term)| {
                        term.used_vars().into_iter().map(move |var| (block_id, var))
                    }),
            );

        let declared_variables = basic_blocks
            .join_core(&instructions, |&inst_id, &block_id, inst| {
                iter::once(((block_id, inst.dest()), inst_id))
            });

        let unused_vars = declared_variables
            .antijoin(&used_variables)
            .map(|((_block_id, _dest), inst_id)| inst_id)
            .inspect(|(inst, _, _)| tracing::trace!("removing unused assign {:?}", inst));

        instructions
            .as_collection(|&id, inst| (id, inst.clone()))
            .antijoin(&unused_vars)
            .leave_region()
    })
}

fn eliminate_unreachable_blocks<S, R, A1, A2>(
    scope: &mut S,
    function_blocks: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
    basic_block_ids: &Collection<S, BasicBlockId, R>,
) -> Collection<S, (FuncId, BasicBlockId), R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = FuncId, Val = FunctionMeta, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    scope.region_named("eliminate unreachable blocks", |region| {
        let function_blocks = function_blocks.enter(region);

        let roots =
            function_blocks.flat_map_ref(|&func_id, meta| iter::once((meta.entry, func_id)));
        let edges = basic_block_ids
            .enter(region)
            .map(|block| (block, ()))
            .join_core(&terminators.enter(region), |&block, (), term| {
                term.jump_targets()
                    .into_iter()
                    .map(move |succ| (block, succ))
            });

        let reachable_blocks =
            propagate(&edges, &roots).map(|(block_id, func_id)| (func_id, block_id));

        function_blocks
            .flat_map_ref(|&func, meta| {
                meta.basic_blocks
                    .clone()
                    .into_iter()
                    .map(move |block| ((func, block), ()))
            })
            .semijoin(&reachable_blocks)
            .map(|(block, ())| block)
            .leave_region()
    })
}
