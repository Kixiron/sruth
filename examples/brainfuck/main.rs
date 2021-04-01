mod parse;
mod programs;

use parse::BrainfuckAst;
use programs::HELLO_WORLD;
use sruth::{
    builder::{BuildResult, Context, FunctionBuilder},
    dataflow::{Diff, Time},
    vsdg::{
        dot,
        node::{Constant, NodeId},
        optimization_dataflow,
    },
};
use std::{sync::Arc, vec::IntoIter};
use timely::Config;

fn main() {
    let program = parse::stratify(&parse::parse(HELLO_WORLD));
    let context = Arc::new(Context::new());
    let (sender, receiver) = crossbeam_channel::unbounded();

    timely::execute(Config::thread(), move |worker| {
        let (mut inputs, _trace, probe) =
            optimization_dataflow::<_, Time, Diff>(worker, sender.clone());

        if worker.index() == 0 {
            let mut builder = context.builder();

            let _add = builder
                .named_function("main", sruth::repr::Type::Uint, |func| {
                    func.basic_block(|block| {
                        block.ret(sruth::repr::Constant::Uint(0))?;
                        Ok(())
                    })?;

                    let data_tape = func.vsdg_const(Constant::Array(vec![Constant::Uint8(0); 100]));
                    let mut data_ptr = func.vsdg_ptr_to(data_tape);

                    let mut program = program.clone().into_iter();
                    while let Some(node) = program.next() {
                        data_ptr = compile_node(node, &mut program, func, data_ptr)?;
                    }

                    let load = func.vsdg_load(data_ptr);
                    func.vsdg_return(load)
                })
                .unwrap();

            builder.vsdg_finish(&mut inputs, 0).unwrap();
        }

        inputs.advance_to(1);
        inputs.flush();

        while probe.less_than(inputs.time()) {
            worker.step_or_park(None);
        }
    })
    .unwrap();

    dot::render_graphs(receiver);
}

fn compile_node(
    node: BrainfuckAst,
    _program: &mut IntoIter<BrainfuckAst>,
    func: &mut FunctionBuilder,
    data_ptr: NodeId,
) -> BuildResult<NodeId> {
    match node {
        BrainfuckAst::IncPtr => {
            let one = func.vsdg_const(Constant::Uint8(1));
            func.vsdg_add(data_ptr, one)
        }

        BrainfuckAst::DecPtr => {
            let one = func.vsdg_const(Constant::Uint8(1));
            func.vsdg_sub(data_ptr, one)
        }

        BrainfuckAst::IncValue => {
            let one = func.vsdg_const(Constant::Uint8(1));
            let value = func.vsdg_load(data_ptr);

            let incremented = func.vsdg_add(value, one)?;
            func.vsdg_store(incremented, data_ptr);

            Ok(data_ptr)
        }

        BrainfuckAst::DecValue => {
            let one = func.vsdg_const(Constant::Uint8(1));
            let value = func.vsdg_load(data_ptr);

            let incremented = func.vsdg_sub(value, one)?;
            func.vsdg_store(incremented, data_ptr);

            Ok(data_ptr)
        }

        BrainfuckAst::Output => Ok(data_ptr),
        BrainfuckAst::Input => Ok(data_ptr),
        BrainfuckAst::Loop { body } => Ok(data_ptr),
    }
}
