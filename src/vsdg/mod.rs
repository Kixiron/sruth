mod dce;
mod dot;
mod folding;
mod graph;
mod loops;
pub mod node;

pub use graph::{Edge, ProgramGraph, ProgramInputs};

#[test]
#[allow(clippy::many_single_char_names)]
fn vsdg_test() {
    use crate::{
        builder::Context,
        dataflow::{Diff, Time},
    };
    use node::{
        Add, Branch, Constant, Control, End, FuncId, Function, Node, NodeId, Operation, Parameter,
        Return, Start, Type, Value,
    };
    use std::{num::NonZeroU64, sync::Arc};
    use timely::{dataflow::ProbeHandle, Config};

    crate::tests::init_logging();

    let context = Arc::new(Context::new());

    let (sender, receiver) = crossbeam_channel::unbounded();
    timely::execute(Config::thread(), move |worker| {
        if let Ok(addr) = std::env::var("DIFFERENTIAL_LOG_ADDR") {
            if !addr.is_empty() {
                if let Ok(stream) = std::net::TcpStream::connect(&addr) {
                    differential_dataflow::logging::enable(worker, stream);
                } else {
                    panic!("Could not connect to differential log address: {:?}", addr);
                }
            }
        }

        let probe = ProbeHandle::new();
        let mut inputs = worker.dataflow::<Time, _, _>(|scope| {
            let (graph, inputs) = ProgramGraph::<_, Diff>::new(scope);
            graph.render_graph("unprocessed", sender.clone());

            let graph = folding::constant_folding(scope, &graph);
            graph.render_graph("post folding", sender.clone());

            let graph = dce::dce(scope, &graph);
            graph.render_graph("post dce", sender.clone());

            let loops = loops::detect_loops(scope, &graph)
                .inspect(|x| tracing::trace!("looping edge: {:?}", x));

            graph.render_graph("output", sender.clone());
            inputs
        });

        if worker.index() == 0 {
            let mut builder = context.builder();

            let _add = builder
                .named_function("add", crate::repr::Type::Uint, |func| {
                    func.basic_block(|block| {
                        block.ret(crate::repr::Constant::Uint(0))?;
                        Ok(())
                    })?;

                    let a = func.vsdg_param(Type::Uint8);
                    let b = func.vsdg_param(Type::Uint8);

                    let add = func.vsdg_add(a, b)?;

                    let zero = func.vsdg_const(Constant::Uint8(0));
                    let add_zero = func.vsdg_add(add, zero)?;

                    let add_zero_again = func.vsdg_add(zero, add_zero)?;

                    func.vsdg_return(add_zero_again)
                })
                .unwrap();

            builder.vsdg_finish(&mut inputs, 0).unwrap();

            // let func_id = FuncId(NonZeroU64::new(1).unwrap());
            // inputs.functions.insert((func_id, Function {}));
            //
            // let start = NodeId(NonZeroU64::new(1).unwrap());
            // inputs.nodes.insert((start, Node::Start(Start)));
            //
            // let x = NodeId(NonZeroU64::new(2).unwrap());
            // inputs.nodes.insert((
            //     x,
            //     Node::Value(Value::Parameter(Parameter { ty: Type::Uint8 })),
            // ));
            //
            // let y = NodeId(NonZeroU64::new(3).unwrap());
            // inputs
            //     .nodes
            //     .insert((y, Node::Value(Value::Constant(Constant::Uint8(0)))));
            //
            // let z = NodeId(NonZeroU64::new(4).unwrap());
            // inputs
            //     .nodes
            //     .insert((z, Node::Operation(Operation::Add(Add { lhs: x, rhs: y }))));
            // inputs.value_edges.insert((z, x));
            // inputs.value_edges.insert((z, y));
            //
            // let a = NodeId(NonZeroU64::new(7).unwrap());
            // inputs
            //     .nodes
            //     .insert((a, Node::Value(Value::Constant(Constant::Uint8(10)))));
            //
            // let b = NodeId(NonZeroU64::new(8).unwrap());
            // inputs
            //     .nodes
            //     .insert((b, Node::Value(Value::Constant(Constant::Uint8(10)))));
            //
            // let c = NodeId(NonZeroU64::new(9).unwrap());
            // inputs
            //     .nodes
            //     .insert((c, Node::Operation(Operation::Add(Add { lhs: a, rhs: b }))));
            // inputs.value_edges.insert((c, a));
            // inputs.value_edges.insert((c, b));
            //
            // let d = NodeId(NonZeroU64::new(10).unwrap());
            // inputs
            //     .nodes
            //     .insert((d, Node::Operation(Operation::Add(Add { lhs: c, rhs: z }))));
            // inputs.value_edges.insert((d, c));
            // inputs.value_edges.insert((d, z));
            //
            // let ret = NodeId(NonZeroU64::new(5).unwrap());
            // inputs
            //     .nodes
            //     .insert((ret, Node::Control(Control::Return(Return {}))));
            // inputs.value_edges.insert((ret, d));
            //
            // let end = NodeId(NonZeroU64::new(6).unwrap());
            // inputs.nodes.insert((end, Node::End(End)));
            //
            // inputs.control_edges.insert((end, ret));
            // inputs.control_edges.insert((ret, start));
            //
            // let loop_head = NodeId(NonZeroU64::new(12).unwrap());
            // inputs
            //     .nodes
            //     .insert((loop_head, Node::Control(Control::Branch(Branch {}))));
            //
            // let loop_tail = NodeId(NonZeroU64::new(13).unwrap());
            // inputs
            //     .nodes
            //     .insert((loop_tail, Node::Control(Control::Branch(Branch {}))));
            // inputs.control_edges.insert((loop_head, loop_tail));
            // inputs.control_edges.insert((loop_tail, loop_head));
            //
            // inputs.control_edges.insert((end, loop_head));
            // inputs.control_edges.insert((end, loop_tail));
        }
        inputs.advance_to(1);
        inputs.flush();

        while probe.less_than(inputs.time()) {
            worker.step_or_park(None);
        }

        worker.log_register().remove("differential/arrange");
    })
    .unwrap();

    dot::render_graph(receiver);
}
