use crate::{
    dataflow::operators::CrossbeamExtractor,
    vsdg::{
        logging::GraphReceiver,
        node::{Constant, Error, FuncId, Function, Node, NodeExt, NodeId, Value},
        Edge, ProgramGraph,
    },
};
use abomonation_derive::Abomonation;
use differential_dataflow::difference::{Monoid, Semigroup};
use petgraph::{dot::Dot, Graph};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    fs::{self, OpenOptions},
    hash::Hash,
    io::Write,
    iter::Step,
    process::Command,
};
use timely::dataflow::{
    operators::{
        capture::{Event, EventPusher},
        Capture, Map,
    },
    Scope,
};

impl<S, R> ProgramGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    // TODO: Work this out better than returning a join handle and allow naming the graph
    // pub fn render_graph<N>(&self, name: N, sender: RenderSender<S, R>) -> Self
    // where
    //     N: Into<String>,
    //     S::Timestamp: Lattice,
    //     R: ExchangeData,
    // {
    //     self.scope().region_named("debug program graph", |region| {
    //         self.enter_region(region)
    //             .consolidate()
    //             .capture_into(name.into(), CrossbeamPusher::new(sender));
    //     });
    //
    //     self.clone()
    // }

    pub fn capture_into<P>(&self, name: String, pusher: P)
    where
        P: EventPusher<S::Timestamp, (String, (GraphNode, S::Timestamp, R))> + 'static,
    {
        self.value_edges
            .map(GraphNode::ValueEdge)
            .concatenate(vec![
                self.effect_edges.map(GraphNode::EffectEdge),
                self.control_edges.map(GraphNode::ControlEdge),
                self.nodes.map(GraphNode::Node),
                self.functions.map(GraphNode::Function),
                self.function_nodes.map(GraphNode::FunctionNode),
            ])
            .inner
            .map(move |node| (name.clone(), node))
            .capture_into(pusher);
    }
}

pub fn render_graphs<T, R>(receiver: GraphReceiver<T, R>)
where
    R: Monoid + Step,
{
    let mut graphs = HashMap::new();
    for event in CrossbeamExtractor::new(receiver) {
        if let Event::Messages(_, data) = event {
            for ((name, node), _time, diff) in data {
                let entry = graphs
                    .entry(name)
                    .or_insert_with(|| Vec::with_capacity(1024));

                for _ in R::zero()..diff {
                    entry.push(node.clone());
                }
            }
        }
    }

    for (graph_name, mut graph_data) in graphs {
        graph_data.sort_by(|a, b| {
            match (
                matches!(a, GraphNode::Node(_)),
                matches!(b, GraphNode::Node(_)),
            ) {
                (true, true) | (false, false) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
            }
        });

        let (mut graph, mut node_ids) = (Graph::new(), HashMap::new());
        for node in graph_data {
            match node {
                GraphNode::ValueEdge((src, dest)) => {
                    let src = node_ids.get(&src).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge source");
                        graph.add_node(Node::Error(Error {}))
                    });

                    let dest = node_ids.get(&dest).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge dest");
                        graph.add_node(Node::Error(Error {}))
                    });

                    graph.add_edge(src, dest, EdgeKind::Value);
                }

                GraphNode::EffectEdge((src, dest)) => {
                    let src = node_ids.get(&src).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge source");
                        graph.add_node(Node::Error(Error {}))
                    });

                    let dest = node_ids.get(&dest).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge dest");
                        graph.add_node(Node::Error(Error {}))
                    });

                    graph.add_edge(src, dest, EdgeKind::Effect);
                }

                GraphNode::ControlEdge((src, dest)) => {
                    let src = node_ids.get(&src).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge source");
                        graph.add_node(Node::Error(Error {}))
                    });

                    let dest = node_ids.get(&dest).copied().unwrap_or_else(|| {
                        tracing::error!(src = ?src, dest = ?dest, "missing value edge dest");
                        graph.add_node(Node::Error(Error {}))
                    });

                    graph.add_edge(src, dest, EdgeKind::Control);
                }

                GraphNode::Node((node_id, ref node)) => {
                    let graph_id = graph.add_node(node.clone());

                    if let Some(old_idx) = node_ids.insert(node_id, graph_id) {
                        tracing::error!(
                            node_id = ?node_id,
                            node = ?node,
                            old_idx = ?old_idx,
                            new_idx = ?graph_id,
                            "double inserted a graph node",
                        );
                    }
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
                    EdgeKind::Error => "color = red",
                }
                .to_owned()
            },
            &|_graph, (_idx, node)| match node {
                Node::Value(value) => match value {
                    Value::Constant(constant) => match constant {
                        Constant::Uint8(uint8) => {
                            format!("label = \"{}: u8\", shape = circle", uint8)
                        }
                        Constant::Bool(b) => {
                            format!("label = \"{}: bool\", shape = circle", b)
                        }
                        Constant::Array(arr) => {
                            format!("label = \"{:?}: array\", shape = circle", arr)
                        }
                    },
                    Value::Parameter(param) => {
                        format!("label = \"param: {}\", shape = doublecircle", param.ty)
                    }
                    Value::Pointer(_ptr) => "label = \"pointer\", shape = doublecircle".to_owned(),
                },
                Node::Control(control) => {
                    format!("label = \"{}\", shape = diamond", control.node_name())
                }
                Node::Operation(operation) => {
                    format!("label = \"{}\", shape = box", operation.node_name())
                }
                Node::End(_) | Node::Start(_) | Node::Merge(_) => {
                    format!(
                        "label = \"{}\", shape = box, peripheries = 2",
                        node.node_name(),
                    )
                }
                Node::Place(_) => "shape = point".to_owned(),
                Node::Error(error) => format!("label = \"{}\", shape = diamond", error.node_name()),
            },
        );

        let _ = fs::remove_dir_all(format!("target/debug/{}", graph_name));
        let _ = fs::remove_file(format!("target/debug/{}.png", graph_name));
        fs::create_dir_all(format!("target/debug/{}", graph_name)).unwrap();

        let name = format!("target/debug/{}/graphviz.dot", graph_name);

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
            .arg(name.replace("/graphviz.dot", ".png"))
            .status()
            .unwrap();

        Command::new("dot")
            .arg(&name)
            .args(&["-Tsvg", "-o"])
            .arg(name.replace(".dot", ".svg"))
            .status()
            .unwrap();
    }
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
    Error,
}

// A hack because `petgraph::Dot` requires it
#[doc(hidden)]
impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}
