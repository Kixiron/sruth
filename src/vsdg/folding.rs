use crate::{
    dataflow::operators::{FilterMap, FlatSplit, Keys},
    vsdg::{
        node::{Constant, NodeExt, NodeId},
        ProgramGraph,
    },
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{consolidate::ConsolidateStream, Consolidate, Join, Reduce},
    ExchangeData,
};
use std::{iter, mem, ops::Mul};
use timely::dataflow::Scope;

pub fn constant_folding<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    // TODO: constant_folding is gonna be a lot more efficient if you use a delta-join for the first two joins,
    //       apply the .flat_split on the delta-stream, conclude with the antijoin (the .concat is the same
    //       in delta-stream-land) and finish up with .integrate()
    scope.region_named("constant folding", |region| {
        let graph = graph.enter_region(region);
        let values_to_consumers = graph
            .value_edges
            .map_in_place(|(src, dest)| mem::swap(src, dest));

        let constant_nodes = graph
            .nodes
            .filter_map(|(id, node)| node.cast::<Constant>().cloned().map(|node| (id, node)));

        let used_constants = constant_nodes
            .join_map(&values_to_consumers, |&value, constant, &consumer| {
                (consumer, (value, constant.clone()))
            })
            .reduce(|_consumer, constants, output| {
                let constants: Vec<(NodeId, Constant)> =
                    constants.iter().map(|(c, _)| *c).cloned().collect();
                output.push((constants, R::from(1)));
            });

        let (evaluated_nodes, value_edges_to_remove) =
            graph.nodes.consolidate().join(&used_constants).flat_split(
                |(node_id, (node, constants))| {
                    let (evaluated_node, removed_edges) = node.evaluate_with_constants(&constants);

                    (
                        iter::once((node_id, evaluated_node)),
                        removed_edges.into_iter().map(move |node| (node_id, node)),
                    )
                },
            );

        let value_edges = graph
            .value_edges
            .concat(&value_edges_to_remove.negate())
            .consolidate_stream();

        let nodes = graph
            .nodes
            .antijoin(&evaluated_nodes.keys())
            .concat(&evaluated_nodes);

        ProgramGraph {
            value_edges,
            nodes,
            ..graph
        }
        .leave_region()
    })
}
