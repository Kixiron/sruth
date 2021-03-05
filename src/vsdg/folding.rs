use crate::{
    dataflow::operators::{FilterMap, FlatSplit, Flatten, InspectExt, Keys, SplitBy},
    vsdg::{
        node::{Add, Constant, NodeExt, NodeId},
        ProgramGraph,
    },
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf},
        consolidate::ConsolidateStream,
        Consolidate, Join, JoinCore, Reduce,
    },
    ExchangeData,
};
use dogsdogsdogs::{
    altneu::AltNeu,
    calculus::{Differentiate, Integrate},
};
use std::{convert::identity, iter, mem, ops::Mul};
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
        let graph = mathematical_simplification(region, &graph);

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

        let (evaluated_nodes, value_edges_to_remove) = graph
            .nodes
            .join(&used_constants)
            .flat_split(|(node_id, (node, constants))| {
                let (evaluated_node, removed_edges) = node.evaluate_with_constants(&constants);

                (
                    iter::once((node_id, evaluated_node)),
                    removed_edges.into_iter().map(move |node| (node_id, node)),
                )
            });

        let value_edges = graph.value_edges.concat(&value_edges_to_remove.negate());

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

fn mathematical_simplification<S, R>(
    _scope: &mut S,
    graph: &ProgramGraph<S, R>,
) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R>,
{
    let addition = graph.nodes.filter(|(_id, node)| node.is::<Add>());
    let zeroes = graph.nodes.filter(|(_id, node)| {
        node.cast::<Constant>()
            .map(|constant| constant.is_zero())
            .unwrap_or_default()
    });
    let non_const = graph.nodes.filter(|(_id, node)| node.isnt::<Constant>());

    let (new_nodes, new_edges, discarded_edges) = addition
        .join_map(&graph.value_edges, |&add_id, _add_node, &producer_id| {
            (producer_id, add_id)
        })
        .join_map(&zeroes, |&zero_id, &add_id, _zero_node| {
            (add_id, (add_id, zero_id))
        })
        .join_map(&graph.value_edges, |&add_id, &zero_edge, &producer_id| {
            (producer_id, (add_id, zero_edge))
        })
        .join_map(&non_const, |&value_id, &(add_id, zero_edge), value| {
            (
                add_id,
                ((value_id, value.to_owned()), zero_edge, (add_id, value_id)),
            )
        })
        .join_map(
            &graph
                .value_edges
                .map_in_place(|(src, dest)| mem::swap(src, dest)),
            |&add_id, &((new_id, ref new_node), zero_edge, val_edge), &consumer_id| {
                (
                    (new_id, new_node.clone()),
                    (consumer_id, new_id),
                    vec![zero_edge, val_edge, (consumer_id, add_id)],
                )
            },
        )
        .split_by(identity);

    let discarded_edges =
        discarded_edges
            .flatten()
            .negate()
            .debug_inspect(|((src, dest), time, diff)| {
                tracing::trace!(
                    "discarding value edge from math simplification: ({}->{}, {:?}, {:?})",
                    src,
                    dest,
                    time,
                    diff,
                );
            });

    let nodes = graph.nodes.concat(&new_nodes);
    let value_edges = graph
        .value_edges
        .concatenate(vec![discarded_edges, new_edges]);

    ProgramGraph {
        value_edges,
        nodes,
        ..graph.clone()
    }
}
