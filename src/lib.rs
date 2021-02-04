pub mod dataflow;
pub mod optimize;
pub mod repr;

#[cfg(test)]
mod tests {
    use crate::{
        dataflow::{self, Diff, Time},
        optimize,
        repr::{
            instruction::{Add, Assign, Div, Mul, Sub},
            BasicBlock, BasicBlockId, Constant, FuncId, Function, Instruction, Terminator, Value,
            VarId,
        },
    };
    use dataflow::InputManager;
    use differential_dataflow::operators::{
        arrange::ArrangeByKey, Consolidate, Join, JoinCore, Reduce, Threshold,
    };
    use std::{iter, num::NonZeroU64};
    use timely::{dataflow::ProbeHandle, Config};

    #[test]
    fn optimization_test() {
        let ir = vec![Function {
            name: None,
            id: FuncId::new(NonZeroU64::new(1).unwrap()),
            entry: BasicBlockId::new(NonZeroU64::new(1).unwrap()),
            basic_blocks: vec![BasicBlock {
                name: None,
                id: BasicBlockId::new(NonZeroU64::new(1).unwrap()),
                instructions: vec![
                    Instruction::Assign(Assign {
                        value: Value::Const(Constant::Uint(100)),
                        dest: VarId::new(NonZeroU64::new(6).unwrap()),
                    }),
                    Instruction::Add(Add {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(6).unwrap())),
                        lhs: Value::Const(Constant::Uint(300)),
                        dest: VarId::new(NonZeroU64::new(1).unwrap()),
                    }),
                    Instruction::Mul(Mul {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(1).unwrap())),
                        lhs: Value::Const(Constant::Uint(10)),
                        dest: VarId::new(NonZeroU64::new(2).unwrap()),
                    }),
                    Instruction::Div(Div {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(2).unwrap())),
                        lhs: Value::Const(Constant::Uint(10)),
                        dest: VarId::new(NonZeroU64::new(3).unwrap()),
                    }),
                    Instruction::Sub(Sub {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(3).unwrap())),
                        lhs: Value::Const(Constant::Uint(5)),
                        dest: VarId::new(NonZeroU64::new(4).unwrap()),
                    }),
                    Instruction::Div(Div {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(2).unwrap())),
                        lhs: Value::Const(Constant::Uint(10)),
                        dest: VarId::new(NonZeroU64::new(5).unwrap()),
                    }),
                    Instruction::Sub(Sub {
                        rhs: Value::Var(VarId::new(NonZeroU64::new(3).unwrap())),
                        lhs: Value::Var(VarId::new(NonZeroU64::new(9).unwrap())),
                        dest: VarId::new(NonZeroU64::new(7).unwrap()),
                    }),
                ],
                terminator: Terminator::Return(Some(Value::Var(VarId::new(
                    NonZeroU64::new(4).unwrap(),
                )))),
            }],
        }];

        timely::execute(Config::thread(), move |worker| {
            let mut probe = ProbeHandle::new();

            let mut input_manager =
                worker.dataflow_named("inputs", |scope| InputManager::new(scope));

            let (mut instructions, mut terminators) =
                worker.dataflow_named::<Time, _, _>("constant propagation", |scope| {
                    let (propagated_instructions, _constants, propagated_terminators) =
                        optimize::constant_folding::<_, Diff>(scope, &mut input_manager);

                    let instruction_trace = propagated_instructions
                        .probe_with(&mut probe)
                        .arrange_by_key()
                        .trace;

                    let terminator_trace = propagated_terminators
                        .probe_with(&mut probe)
                        .arrange_by_key()
                        .trace;

                    (instruction_trace, terminator_trace)
                });

            let (mut instructions, _) = worker.dataflow_named("cull dead code", |scope| {
                let (instructions, terminators) = (
                    instructions.import_named(scope, "instructions (post constant folding)"),
                    terminators.import_named(scope, "terminators (post constant folding)"),
                );

                let (instructions, basic_blocks) =
                    optimize::dead_code(scope, &mut input_manager, &instructions, &terminators);

                let instructions = instructions.probe_with(&mut probe).arrange_by_key();
                let basic_blocks = basic_blocks.probe_with(&mut probe).arrange_by_key();

                (instructions.trace, basic_blocks.trace)
            });

            let _reconstructed = worker.dataflow_named("reconstruct ir", |scope| {
                let (instructions, terminators, block_trace) = (
                    instructions.import(scope),
                    terminators.import(scope),
                    input_manager.basic_block_trace.import(scope),
                );

                let mut basic_blocks = block_trace
                    .flat_map_ref(|&block_id, meta| {
                        meta.instructions
                            .clone()
                            .into_iter()
                            .enumerate()
                            .map(move |(idx, inst)| (inst, (block_id, idx)))
                    })
                    .join_core(&instructions, |_, &(block_id, inst_idx), inst| {
                        iter::once((block_id, (inst.clone(), inst_idx)))
                    })
                    .consolidate()
                    .reduce(|_, input, output| {
                        let mut instructions: Vec<_> = input
                            .iter()
                            .copied()
                            .map(|((inst, idx), _diff)| (inst.clone(), idx))
                            .collect();
                        instructions.sort_unstable_by_key(|&(_, idx)| idx);

                        output.push((
                            instructions
                                .into_iter()
                                .map(|(inst, _idx)| inst)
                                .collect::<Vec<_>>(),
                            1,
                        ));
                    })
                    .join_core(&terminators, |&block_id, instructions, term| {
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
                    });

                let terminated_blocks = terminators.semijoin(
                    &block_trace
                        .flat_map_ref(|&block_id, _| iter::once(block_id))
                        .threshold(|_, &diff| if diff >= 1 { 1 } else { 0 }),
                );

                // Add back in the blocks which have no instructions
                basic_blocks = basic_blocks.concat(
                    &terminated_blocks
                        .antijoin(&basic_blocks.map(|(block_id, _)| block_id))
                        .map(|(block_id, terminator)| {
                            (
                                block_id,
                                BasicBlock {
                                    // TODO: Retain this info
                                    name: None,
                                    id: block_id,
                                    instructions: Vec::new(),
                                    terminator,
                                },
                            )
                        }),
                );

                let functions = input_manager
                    .function_trace
                    .import(scope)
                    .flat_map_ref(|&func_id, meta| {
                        let meta = meta.clone();
                        meta.basic_blocks
                            .clone()
                            .into_iter()
                            .map(move |block| (block, (func_id, meta.clone())))
                    })
                    .join_core(
                        &basic_blocks.arrange_by_key(),
                        |&block_id, &(func_id, ref meta), instructions| {
                            iter::once(((func_id, meta.clone()), (block_id, instructions.clone())))
                        },
                    )
                    .consolidate()
                    .reduce(|&(func_id, ref meta), input, output| {
                        let basic_blocks: Vec<_> = input
                            .iter()
                            .copied()
                            .map(|((_, instructions), _diff)| instructions.to_owned())
                            .collect();

                        output.push((
                            Function {
                                name: meta.name,
                                id: func_id,
                                entry: meta.entry,
                                basic_blocks,
                            },
                            1,
                        ));
                    })
                    .inspect(|x| println!("Output: {:#?}", x));

                functions.arrange_by_key().trace
            });

            if worker.index() == 0 {
                dataflow::translate(&mut input_manager, ir.clone());
            }

            input_manager.advance_to(1);
            worker.step_while(|| !probe.less_than(&1));
        })
        .unwrap();
    }
}
