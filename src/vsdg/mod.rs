mod dce;
mod dot;
mod folding;
mod graph;
mod node;

pub use graph::{Edge, ProgramGraph};

#[test]
#[allow(clippy::many_single_char_names)]
fn vsdg_test() {
    use crate::dataflow::{Diff, Time};
    use node::{
        Add, Constant, Control, End, FuncId, Function, Node, NodeId, Operation, Parameter, Return,
        Start, Type, Value,
    };
    use std::num::NonZeroU64;
    use timely::{dataflow::ProbeHandle, Config};

    crate::tests::init_logging();

    let (sender, receiver) = crossbeam_channel::unbounded();
    timely::execute(Config::thread(), move |worker| {
        let probe = ProbeHandle::new();
        let mut inputs = worker.dataflow::<Time, _, _>(|scope| {
            let (graph, inputs) = ProgramGraph::<_, Diff>::new(scope);

            let graph = folding::constant_folding(scope, &graph);
            let graph = dce::dce(scope, &graph);

            graph.render_graph(sender.clone());
            inputs
        });

        if worker.index() == 0 {
            let func_id = FuncId(NonZeroU64::new(1).unwrap());
            inputs.functions.insert((func_id, Function {}));

            let start = NodeId(NonZeroU64::new(1).unwrap());
            inputs.nodes.insert((start, Node::Start(Start)));

            let x = NodeId(NonZeroU64::new(2).unwrap());
            inputs.nodes.insert((
                x,
                Node::Value(Value::Parameter(Parameter { ty: Type::Uint8 })),
            ));

            let y = NodeId(NonZeroU64::new(3).unwrap());
            inputs.nodes.insert((
                y,
                Node::Value(Value::Parameter(Parameter { ty: Type::Uint8 })),
            ));

            let z = NodeId(NonZeroU64::new(4).unwrap());
            inputs
                .nodes
                .insert((z, Node::Operation(Operation::Add(Add { lhs: x, rhs: y }))));
            inputs.value_edges.insert((z, x));
            inputs.value_edges.insert((z, y));

            let a = NodeId(NonZeroU64::new(7).unwrap());
            inputs
                .nodes
                .insert((a, Node::Value(Value::Constant(Constant::Uint8(10)))));

            let b = NodeId(NonZeroU64::new(8).unwrap());
            inputs
                .nodes
                .insert((b, Node::Value(Value::Constant(Constant::Uint8(10)))));

            let c = NodeId(NonZeroU64::new(9).unwrap());
            inputs
                .nodes
                .insert((c, Node::Operation(Operation::Add(Add { lhs: a, rhs: b }))));
            inputs.value_edges.insert((c, a));
            inputs.value_edges.insert((c, b));

            let d = NodeId(NonZeroU64::new(10).unwrap());
            inputs
                .nodes
                .insert((d, Node::Operation(Operation::Add(Add { lhs: c, rhs: z }))));
            inputs.value_edges.insert((d, c));
            inputs.value_edges.insert((d, z));

            let ret = NodeId(NonZeroU64::new(5).unwrap());
            inputs
                .nodes
                .insert((ret, Node::Control(Control::Return(Return {}))));
            inputs.value_edges.insert((ret, d));

            let end = NodeId(NonZeroU64::new(6).unwrap());
            inputs.nodes.insert((end, Node::End(End)));

            inputs.control_edges.insert((end, ret));
            inputs.control_edges.insert((ret, start));
        }
        inputs.advance_to(1);
        inputs.flush();

        while probe.less_than(inputs.time()) {
            worker.step_or_park(None);
        }
    })
    .unwrap();

    dot::render_graph(receiver);
}
