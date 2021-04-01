use crate::{
    dataflow::operators::{FilterSplit, Reverse, SemijoinExt, Split},
    vsdg::{
        node::{Constant, End, NodeExt},
        ProgramGraph,
    },
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    algorithms::graphs::propagate,
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf},
        Join, JoinCore, Threshold,
    },
    ExchangeData,
};
use std::{convert::identity, ops::Mul};
use timely::dataflow::Scope;

pub fn cse<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    scope.region_named("common subexpression elimination", |region| {
        let graph = graph.enter_region(region);

        let edges = graph.all_edges();
        let roots = graph
            .nodes
            .filter(|(_, node)| node.is::<End>())
            .map(|(id, _node)| (id, id));

        // Group nodes by the function they belong to so that we can
        // eliminate expressions on a per-function basis
        //
        // Is a collection of `NodeId`s to their root `End`'s `NodeId`
        let nodes_grouped = propagate::propagate(&edges, &roots);

        // Get all the nodes eligible for elimination
        // TODO: Add more node kinds other than just constants,
        //       binops, pure functions and anything else without
        //       effectual impact should be eligible
        let eligible_for_elimination = graph.nodes.filter(|(_, node)| node.is::<Constant>());

        let eligible_grouped = eligible_for_elimination
            .join_map(&nodes_grouped, |&node_id, node, &root_id| {
                (root_id, (node_id, node.to_owned()))
            })
            .arrange_by_key();

        // All nodes that have an identical node within the same function
        // TODO: Can we shrink this semi-cartesian join or work around doing it?
        let to_be_eliminated = eligible_grouped
            .join_core(
                &eligible_grouped,
                |_root_id, &(node1_id, ref node1), &(node2_id, ref node2)| {
                    if node1_id != node2_id && node1 == node2 {
                        Some(((node1_id, node2_id), node1.to_owned()))
                    } else {
                        None
                    }
                },
            )
            .distinct_core();

        let to_be_eliminated_arranged = to_be_eliminated
            .map(|((node1_id, node2_id), node1)| (node2_id, (node1_id, node1)))
            .arrange_by_key();
        let to_be_rerouted = to_be_eliminated
            .map(|((_, node2_id), _)| node2_id)
            .arrange_by_self();

        let rerouted_edges_forward = SemijoinExt::semijoin(&graph.value_edges, &to_be_rerouted);
        let rerouted_edges_reverse =
            SemijoinExt::semijoin(&graph.value_edges.reverse(), &to_be_rerouted);

        let (rerouted_edges_forward, rerouted_edges_reverse) = rerouted_edges_forward
            .map(|edge| (edge, Direction::Forward))
            .concat(&rerouted_edges_reverse.map(|(src, dest)| ((dest, src), Direction::Reverse)))
            .filter_split(|((src, dest), direction)| match direction {
                Direction::Forward => (Some((src, dest)), None),
                Direction::Reverse => (None, Some((dest, src))),
            });

        // Get the newly rerouted edges and the old edges
        let (new_edges_forward, discarded_edges_forward) = to_be_eliminated_arranged
            .join_map(
                &rerouted_edges_forward,
                |&node2_id, &(node1_id, _), &forward| ((node1_id, forward), (node2_id, forward)),
            )
            .split(identity);

        let (new_edges_reverse, discarded_edges_reverse) = to_be_eliminated_arranged
            .join_map(
                &rerouted_edges_reverse,
                |&node2_id, &(node1_id, _), &reverse| ((reverse, node1_id), (reverse, node2_id)),
            )
            .split(identity);

        let value_edges = graph.value_edges.concatenate(vec![
            new_edges_forward,
            new_edges_reverse,
            discarded_edges_forward.negate(),
            discarded_edges_reverse.negate(),
        ]);

        ProgramGraph {
            value_edges,
            ..graph
        }
        .leave_region()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Direction {
    Forward,
    Reverse,
}
