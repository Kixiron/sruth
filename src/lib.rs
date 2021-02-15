#![feature(crate_visibility_modifier)]

pub mod builder;
pub mod dataflow;
pub mod optimize;
pub mod repr;
pub mod verify;
pub mod wasm;

#[cfg(test)]
mod tests {
    use crate::{
        builder::Context,
        dataflow::{
            self,
            operators::{Cleanup, CrossbeamExtractor, CrossbeamPusher},
            Diff, ProgramVariable, Time, TraceManager,
        },
        optimize::{constant_folding, inline, peephole},
        repr::{
            function::Metadata,
            utils::{DisplayCtx, IRDisplay},
            BasicBlock, Constant, Function, Type,
        },
        verify::{verify, ValidityError},
    };
    use dataflow::InputManager;
    use differential_dataflow::{
        input::Input,
        operators::{
            arrange::{ArrangeByKey, ArrangeBySelf, TraceAgent},
            iterate::Variable,
            Consolidate, Join, JoinCore, Reduce, Threshold,
        },
        trace::implementations::ord::OrdKeySpine,
    };
    use pretty::{BoxAllocator, RefDoc};
    use std::{io, iter, sync::Arc};
    use timely::{
        dataflow::{
            operators::{capture::Extract, Capture},
            ProbeHandle, Scope,
        },
        order::Product,
        progress::frontier::AntichainRef,
        Config,
    };
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn optimization_test() {
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::TRACE)
            .with(tracing_subscriber::fmt::layer())
            .try_init();

        let context = Arc::new(Context::new());
        let moved_context = context.clone();

        let (sender, receiver) = crossbeam_channel::unbounded();
        timely::execute(Config::thread(), move |worker| {
            let (mut probe, mut trace_manager) = (ProbeHandle::new(), TraceManager::new());

            let mut input_manager = worker.dataflow_named("inputs", |scope| {
                let mut input = InputManager::new(scope);

                let (instructions, basic_blocks, functions) = (
                    input
                        .instruction_trace
                        .import(scope)
                        .as_collection(|&inst_id, inst| (inst_id, inst.clone())),
                    input
                        .basic_block_trace
                        .import(scope)
                        .as_collection(|&block, meta| (block, meta.clone())),
                    input
                        .function_trace
                        .import(scope)
                        .as_collection(|&func, meta| (func, meta.clone())),
                );

                let errors = verify(scope, &instructions, &basic_blocks, &functions)
                    .probe_with(&mut probe)
                    .arrange_by_self();

                trace_manager.insert_trace::<TraceAgent<OrdKeySpine<ValidityError, Time, Diff>>>(
                    moved_context
                        .interner()
                        .get_or_intern_static("input/errors"),
                    errors.trace,
                );

                input
            });

            let (mut program, mut inline_heuristics) =
                worker.dataflow_named::<Time, _, _>("constant propagation", |scope| {
                    let (instruction_trace, basic_block_trace, function_trace) = (
                        input_manager.instruction_trace.import(scope),
                        input_manager.basic_block_trace.import(scope),
                        input_manager.function_trace.import(scope),
                    );

                    let instructions =
                        instruction_trace.as_collection(|&id, inst| (id, inst.clone()));

                    let block_instructions = basic_block_trace.flat_map_ref(|&block, meta| {
                        meta.instructions
                            .clone()
                            .into_iter()
                            .map(move |inst| (inst, block))
                    });

                    let block_terminators = basic_block_trace
                        .as_collection(|&block, meta| (block, meta.terminator.clone()));

                    let block_descriptors =
                        basic_block_trace.as_collection(|&block, desc| (block, desc.clone()));

                    let function_blocks = function_trace.flat_map_ref(|&func, meta| {
                        meta.basic_blocks
                            .clone()
                            .into_iter()
                            .map(move |block| (block, func))
                    });

                    let function_descriptors =
                        function_trace.as_collection(|&id, func| (id, func.clone()));

                    let program = scope
                        .scoped::<Product<_, Time>, _, _>("optimization", |scope| {
                            let variables = {
                                let summary = Product::new(Default::default(), 1);

                                let instructions =
                                    Variable::new_from(instructions.enter(scope), summary);
                                let block_instructions =
                                    Variable::new_from(block_instructions.enter(scope), summary);
                                let block_terminators =
                                    Variable::new_from(block_terminators.enter(scope), summary);
                                let block_descriptors =
                                    Variable::new_from(block_descriptors.enter(scope), summary);
                                let function_blocks =
                                    Variable::new_from(function_blocks.enter(scope), summary);
                                let function_descriptors =
                                    Variable::new_from(function_descriptors.enter(scope), summary);

                                ProgramVariable::new(
                                    instructions,
                                    block_instructions,
                                    block_terminators,
                                    block_descriptors,
                                    function_blocks,
                                    function_descriptors,
                                )
                            };

                            let mut program = variables.program();

                            let (folded_instructions, folded_terminators) =
                                constant_folding::constant_folding::<_, Diff>(
                                    scope,
                                    &program.instructions,
                                    &program.block_terminators,
                                );
                            program.instructions = folded_instructions;
                            program.block_terminators = folded_terminators;

                            program.instructions = peephole::peephole(scope, &program.instructions);

                            program = program
                                .cull_unreachable_blocks()
                                .compact_basic_blocks()
                                .cleanup();

                            let result = program.consolidate();
                            variables.set(&result);

                            result.leave()
                        })
                        .probe_with(&mut probe);

                    let inline_heuristics = inline::harvest_heuristics(&program)
                        .consolidate()
                        .probe_with(&mut probe);

                    (
                        program.arrange_by_key().trace(),
                        inline_heuristics.arrange_by_key().trace,
                    )
                });

            worker.dataflow_named("reconstruct ir", |scope| {
                let (program, inline_heuristics) =
                    (program.import(scope), inline_heuristics.import(scope));

                let mut rebuilt_basic_blocks = program
                    .block_instructions
                    .join_core(&program.instructions, |_inst_id, &block, inst| {
                        iter::once((block, inst.to_owned()))
                    })
                    .reduce(|_, input, output| {
                        // TODO: Ordering of instructions???
                        let instructions: Vec<_> = input
                            .iter()
                            .copied()
                            .map(|(inst, _diff)| inst.clone())
                            .collect();

                        output.push((instructions, 1));
                    })
                    .join_core(
                        &program.block_terminators,
                        |&block_id, instructions, term| {
                            iter::once((
                                block_id,
                                BasicBlock {
                                    // TODO: Retain this info
                                    name: None,
                                    id: block_id,
                                    instructions: instructions.to_owned(),
                                    terminator: term.to_owned(),
                                },
                            ))
                        },
                    );

                // Add back basic blocks with no instructions since they still have terminators
                rebuilt_basic_blocks = rebuilt_basic_blocks.concat(
                    &program
                        .block_terminators
                        .as_collection(|&block, term| (block, term.clone()))
                        .antijoin(&rebuilt_basic_blocks.map(|(block, _)| block))
                        .map(|(block, terminator)| {
                            (
                                block,
                                BasicBlock {
                                    // TODO: Retain this info
                                    name: None,
                                    id: block,
                                    instructions: Vec::new(),
                                    terminator,
                                },
                            )
                        }),
                );

                let basic_blocks = rebuilt_basic_blocks
                    .join_core(&program.function_blocks, |_block_id, block, &func| {
                        iter::once((func, block.clone()))
                    })
                    .consolidate()
                    .reduce(|_func, blocks, output| {
                        let blocks: Vec<_> = blocks
                            .iter()
                            .copied()
                            .map(|(block, _)| block.to_owned())
                            .collect();

                        output.push((blocks, 1));
                    });

                let function_metadata = inline_heuristics.as_collection(|&func, heuristics| {
                    (func, Metadata::new(Some(heuristics.clone())))
                });

                let functions = program
                    .function_descriptors
                    .as_collection(|&func_id, meta| (func_id, meta.clone()))
                    .join(&function_metadata)
                    .join_map(&basic_blocks, |&func_id, (desc, metadata), blocks| {
                        let func = Function {
                            name: desc.name,
                            id: func_id,
                            params: desc.params.clone(),
                            ret_ty: desc.ret_ty.clone(),
                            entry: desc.entry,
                            basic_blocks: blocks.clone(),
                            metadata: metadata.clone(),
                        };

                        (func_id, func)
                    })
                    .probe_with(&mut probe);

                trace_manager.insert_trace(
                    moved_context
                        .interner()
                        .get_or_intern_static("reconstruct/functions"),
                    functions.arrange_by_key().trace,
                );

                let (_, mut errors) = scope.new_collection();
                let error_traces = &[moved_context
                    .interner()
                    .get_or_intern_static("input/errors")];

                for &trace in error_traces {
                    let trace = trace_manager
                        .get_trace::<TraceAgent<OrdKeySpine<ValidityError, Time, Diff>>>(trace)
                        .unwrap()
                        .import(scope)
                        .as_collection(|error, _| Err(error.clone()));

                    errors = errors.concat(&trace);
                }

                functions
                    .map(Ok)
                    .concat(&errors)
                    .distinct_core::<Diff>()
                    .probe_with(&mut probe)
                    .inner
                    .capture_into(CrossbeamPusher::new(sender.clone()));
            });

            if worker.index() == 0 {
                let mut builder = moved_context.builder();
                let add_uint = builder
                    .named_function("add_uint", Type::Uint, |func| {
                        let lhs = func.param(Type::Uint);
                        let rhs = func.param(Type::Uint);

                        func.basic_block(|block| {
                            let sum = block.add(lhs, rhs)?;
                            block.ret(sum)?;

                            Ok(())
                        })?;

                        Ok(())
                    })
                    .unwrap();

                builder
                    .named_function("cross_branch_propagation", Type::Uint, |func| {
                        let input = func.param(Type::Uint);

                        let instant_return = func.basic_block(|block| {
                            block.ret(Constant::Uint(0))?;

                            Ok(())
                        })?;

                        let folded_block = func.basic_block(|block| {
                            let a = block.assign(Constant::Uint(100));
                            let a_times_two = block.mul(a.clone(), Constant::Uint(2))?;
                            let a_div_two = block.div(a.clone(), Constant::Uint(2))?;
                            let summed_ops = block.call(
                                add_uint,
                                vec![a_times_two.clone().into(), a_div_two.into()],
                            )?;
                            let multed = block.mul(summed_ops, a_times_two)?;
                            let subbed = block.sub(multed, a)?;

                            block.ret(subbed)?;

                            Ok(())
                        })?;

                        let branch_block = func.basic_block(|block| {
                            let _sum = block
                                .call(add_uint, vec![input.into(), Constant::Uint(100).into()])?;

                            block.branch(Constant::Bool(true), folded_block, instant_return)?;

                            Ok(())
                        })?;
                        func.set_entry(branch_block);

                        Ok(())
                    })
                    .unwrap();

                let alloc = BoxAllocator;
                for func in builder.materialize() {
                    func.display::<BoxAllocator, RefDoc, _>(DisplayCtx::new(
                        &alloc,
                        &*moved_context.interner(),
                    ))
                    .1
                    .render(70, &mut io::stdout())
                    .unwrap();
                }

                builder.finish(&mut input_manager).unwrap();
            }

            input_manager.advance_to(1);

            let frontier = AntichainRef::new(&[1]);
            trace_manager.advance_by(frontier);
            trace_manager.distinguish_since(AntichainRef::new(&[]));

            worker.step_while(|| probe.less_than(input_manager.time()));
        })
        .unwrap();

        let alloc = BoxAllocator;
        for (time, data) in CrossbeamExtractor::new(receiver).extract() {
            println!("Data from timestamp {}:", time);

            for (data, _time, _diff) in data {
                match data {
                    Ok((_id, func)) => {
                        func.display::<BoxAllocator, RefDoc, _>(DisplayCtx::new(
                            &alloc,
                            &*context.interner(),
                        ))
                        .1
                        .render(70, &mut io::stdout())
                        .unwrap();
                    }

                    Err(err) => println!("Error: {:?}", err),
                }
            }
        }
    }
}
