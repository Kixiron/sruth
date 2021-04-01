use crate::{
    dataflow::operators::CrossbeamPusher,
    vsdg::{dot::GraphNode, ProgramGraph},
};
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{
    difference::Monoid, lattice::Lattice, operators::Consolidate, ExchangeData,
};
use timely::dataflow::{
    operators::{capture::Event, Capture, Map},
    Scope,
};

// TODO: Interned names
pub type GraphSender<T, R> = Sender<Event<T, ((String, GraphNode), T, R)>>;
pub type GraphReceiver<T, R> = Receiver<Event<T, ((String, GraphNode), T, R)>>;

impl<S, R> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Monoid + ExchangeData,
{
    // TODO: Work this out better than returning a join handle and allow naming the graph
    pub fn render_graph<N>(&self, name: N, sender: GraphSender<S::Timestamp, R>) -> Self
    where
        N: Into<String>,
    {
        let name = name.into();

        if cfg!(debug_assertions) {
            tracing::trace!("installing stream for graph debug {}", name);

            self.scope().region_named("debug program graph", |region| {
                let graph = self.enter_region(region);

                graph
                    .value_edges
                    .map(GraphNode::ValueEdge)
                    .concatenate(vec![
                        graph.effect_edges.map(GraphNode::EffectEdge),
                        graph.control_edges.map(GraphNode::ControlEdge),
                        graph.nodes.map(GraphNode::Node),
                        graph.functions.map(GraphNode::Function),
                        graph.function_nodes.map(GraphNode::FunctionNode),
                    ])
                    .consolidate()
                    .inner
                    .map(move |(data, time, diff)| ((name.clone(), data), time, diff))
                    .capture_into(CrossbeamPusher::new(sender));
            });
        }

        self.clone()
    }
}
