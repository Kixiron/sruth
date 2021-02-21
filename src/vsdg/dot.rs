use crate::{
    dataflow::operators::{CrossbeamExtractor, CrossbeamPusher},
    vsdg::{
        node::{Constant, Control, FuncId, Function, Node, NodeId, Operation, Value},
        Edge, ProgramGraph,
    },
};
use abomonation_derive::Abomonation;
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{difference::Semigroup, lattice::Lattice, ExchangeData};
use petgraph::{dot::Dot, Graph};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use timely::dataflow::{
    operators::{
        capture::{Event, EventPusher},
        Capture,
    },
    Scope, ScopeParent,
};

type RenderSender<S, R> =
    Sender<Event<<S as ScopeParent>::Timestamp, (GraphNode, <S as ScopeParent>::Timestamp, R)>>;

impl<S, R> ProgramGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    // TODO: Work this out better than returning a join handle and allow naming the graph
    pub fn render_graph(&self, sender: RenderSender<S, R>) -> Self
    where
        S::Timestamp: Lattice,
        R: ExchangeData,
    {
        self.scope().region_named("debug program graph", |region| {
            self.enter_region(region)
                .consolidate()
                .capture_into(CrossbeamPusher::new(sender));
        });

        self.clone()
    }

    pub fn capture_into<P>(&self, pusher: P)
    where
        P: EventPusher<S::Timestamp, (GraphNode, S::Timestamp, R)> + Clone + 'static,
    {
        self.value_edges
            .map(GraphNode::ValueEdge)
            .inner
            .capture_into(pusher.clone());

        self.effect_edges
            .map(GraphNode::EffectEdge)
            .inner
            .capture_into(pusher.clone());

        self.control_edges
            .map(GraphNode::ControlEdge)
            .inner
            .capture_into(pusher.clone());

        self.nodes
            .map(GraphNode::Node)
            .inner
            .capture_into(pusher.clone());

        self.function_nodes
            .map(GraphNode::FunctionNode)
            .inner
            .capture_into(pusher.clone());

        self.functions
            .map(GraphNode::Function)
            .inner
            .capture_into(pusher);
    }
}

pub fn render_graph<T, R>(receiver: Receiver<Event<T, (GraphNode, T, R)>>) {
    let mut graph_data: Vec<_> = CrossbeamExtractor::new(receiver)
        .into_iter()
        .flat_map(|event| {
            if let Event::Messages(_, data) = event {
                data
            } else {
                Vec::new()
            }
        })
        .collect();

    graph_data.sort_by(|(a, _, _), (b, _, _)| {
        match (
            matches!(a, GraphNode::Node(_)),
            matches!(b, GraphNode::Node(_)),
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        }
    });

    let (mut graph, mut node_ids) = (Graph::new(), HashMap::new());
    for (node, _time, _diff) in graph_data {
        match node {
            GraphNode::ValueEdge((src, dest)) => {
                let src = *node_ids.get(&src).unwrap();
                let dest = *node_ids.get(&dest).unwrap();

                graph.add_edge(src, dest, EdgeKind::Value);
            }

            GraphNode::EffectEdge((src, dest)) => {
                let src = *node_ids.get(&src).unwrap();
                let dest = *node_ids.get(&dest).unwrap();

                graph.add_edge(src, dest, EdgeKind::Effect);
            }

            GraphNode::ControlEdge((src, dest)) => {
                let src = *node_ids.get(&src).unwrap();
                let dest = *node_ids.get(&dest).unwrap();

                graph.add_edge(src, dest, EdgeKind::Control);
            }

            GraphNode::Node((node_id, node)) => {
                let graph_id = graph.add_node(node);
                node_ids.insert(node_id, graph_id);
            }

            // TODO
            GraphNode::FunctionNode(_) | GraphNode::Function(_) => {}
        }
    }

    let dot = Dot::with_attr_getters(
        &graph,
        &[
            petgraph::dot::Config::EdgeNoLabel,
            petgraph::dot::Config::NodeNoLabel,
        ],
        &|_graph, edge| {
            match edge.weight() {
                EdgeKind::Control => "color = black",
                EdgeKind::Effect => "color = cornflowerblue",
                EdgeKind::Value => "color = forestgreen",
            }
            .to_owned()
        },
        &|_graph, (_idx, node)| match node {
            Node::Value(value) => match value {
                Value::Constant(constant) => match constant {
                    Constant::Uint8(uint8) => {
                        format!("label = \"{}: u8\", shape = circle", uint8)
                    }
                },
                Value::Parameter(param) => {
                    format!("label = \"param: {}\", shape = doublecircle", param.ty)
                }
            },
            Node::Control(control) => match control {
                Control::Return(_) => "label = \"Return\", shape = diamond".to_owned(),
            },
            Node::Operation(operation) => match operation {
                Operation::Add(_) => "label = \"Add\", shape = box".to_owned(),
            },
            Node::End(_) => "label = \"End\", shape = box, peripheries = 2".to_owned(),
            Node::Start(_) => "label = \"Start\", shape = box, peripheries = 2".to_owned(),
            Node::Merge(_) => "label = \"Merge\", shape = box, peripheries = 2".to_owned(),
        },
    );

    let system_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("target/debug/{}.dot", system_time);

    {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&name)
            .unwrap();

        file.write_all(format!("{:?}", dot).as_bytes()).unwrap();
    }

    Command::new("dot")
        .arg(&name)
        .args(&["-Tpng", "-o"])
        .arg(name.replace(".dot", ".png"))
        .status()
        .unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum GraphNode {
    ValueEdge(Edge),
    EffectEdge(Edge),
    ControlEdge(Edge),
    Node((NodeId, Node)),
    FunctionNode((NodeId, FuncId)),
    Function((FuncId, Function)),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum EdgeKind {
    Value,
    Effect,
    Control,
}

// A hack because `petgraph::Dot` requires it
#[doc(hidden)]
impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}
