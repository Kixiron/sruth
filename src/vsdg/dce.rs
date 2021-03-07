use crate::{
    dataflow::{
        algorithms::reachable,
        operators::{FilterMap, Flatten, Split},
    },
    vsdg::{
        node::{End, Node, NodeExt, NodeId, Place},
        Edge, ProgramGraph,
    },
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf},
        Join, JoinCore,
    },
    Collection, ExchangeData,
};
use dogsdogsdogs::{
    altneu::AltNeu,
    calculus::{Differentiate, Integrate},
};
use std::{convert::identity, mem, ops::Mul};
use timely::dataflow::Scope;

pub fn dce<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    scope.region_named("dead code elimination", |scope| {
        let graph = remove_places(scope, &graph.enter_region(scope));

        // TODO: Is a `.distinct_core()` correct here?
        let edges = graph.all_edges();
        let roots = graph
            .nodes
            .filter_map(|(id, node): (NodeId, Node)| node.cast::<End>().map(|_| id));

        let retained = reachable::reachable(&edges, &roots);

        let value_edges = delta_cull_edges(scope, &graph.value_edges, &retained);
        let effect_edges = delta_cull_edges(scope, &graph.effect_edges, &retained);
        let control_edges = delta_cull_edges(scope, &graph.control_edges, &retained);
        let nodes = graph.nodes.semijoin(&retained);

        ProgramGraph {
            value_edges,
            effect_edges,
            control_edges,
            nodes,
            ..graph
        }
        .leave_region()
    })
}

/// Cull unused graph edges using delta joins, see [Worst-case optimal joins, in dataflow][1]
///
/// Takes a collection of edges and a collection of retained nodes and removes all
/// edges containing nodes not mentioned in the `edges` collection.
///
/// [1]: http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
fn delta_cull_edges<S, R>(
    scope: &mut S,
    edges: &Collection<S, Edge, R>,
    retained: &Collection<S, NodeId, R>,
) -> Collection<S, Edge, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R>,
{
    scope.scoped::<AltNeu<_>, _, _>("delta-join culling edges", |inner| {
        let neu = |time: &AltNeu<_>| {
            let mut time = time.clone();
            time.neu = true;
            time
        };

        let retained_ingress = retained.enter(inner);
        let retained_alt = retained_ingress.arrange_by_self();
        let retained_neu = retained_ingress.delay(neu).arrange_by_self();
        let d_retained = retained.differentiate(inner).arrange_by_self();

        let edges_forward = edges.enter(inner);
        let edges_reverse = edges_forward.map(|(src, dst)| (dst, src));

        let edges_forward_alt = edges_forward.arrange_by_key();
        // let edges_forward_neu = edges_forward.delay(neu).arrange_by_key();

        let edges_reverse_alt = edges_reverse.arrange_by_key();
        // let edges_reverse_neu = edges_reverse.delay(neu).arrange_by_key();

        let d_edges = edges.differentiate(inner).arrange_by_key();

        // retained_value_edges(a, b) := value_edges(a, b) ⋈ retained(a) ⋈ retained(b)
        // retained_value_edges(a, b) := value_edges(a, b) ⋈ retained(a) ⋈ retained(b)

        // WCOJ-eligible
        // d/d(value_edges(a, b)) := d(value_edges(a, b)) ⋈ retained(a) ⋈ retained(b)
        let d_value_edges_ab = d_edges
            .join_core(&retained_neu, |&a, &b, _| Some((b, a)))
            .join_core(&retained_neu, |&b, &a, _| Some((a, b)));

        // WCOJ-ineligible: b only introduced in value_edges(a,b)
        // d/d(retained(a)) := d(retained(a)) ⋈ value_edges(a, b) ⋈ retained(b)
        let d_retained_a = d_retained
            .join_core(&edges_forward_alt, |&a, _, &b| Some((b, a)))
            .join_core(&retained_neu, |&b, &a, _| Some((a, b)));

        // WCOJ-ineligible: a only introduced in value_edges(a,b)
        // d/d(retained(b)) := d(retained(b)) ⋈ value_edges(a, b) ⋈ retained(a)
        let d_retained_b = d_retained
            .join_core(&edges_reverse_alt, |&b, _, &a| Some((a, b)))
            .join_core(&retained_alt, |&a, &b, _| Some((a, b)));

        d_value_edges_ab
            .concatenate(vec![d_retained_a, d_retained_b])
            .integrate()
    })
}

fn remove_places<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R>,
{
    scope.region_named("remove place values", |region| {
        let graph = graph.enter_region(region);

        // TODO: Arrange this
        let places = graph.nodes.filter(|(_, node)| node.is::<Place>());

        // TODO: Arrange these
        let edges_forward = graph.value_edges.clone();
        let edges_reverse = edges_forward.map_in_place(|(src, dest)| mem::swap(src, dest));

        let downstream_exclusions = places
            .join_map(&edges_forward, |&place_id, _place, &downstream_id| {
                (downstream_id, place_id)
            })
            .join_map(&places, |_downstream_id, &place_id, _downstream| place_id);

        let upstream_exclusions = places
            .join_map(&edges_reverse, |&place_id, _place, &upstream_id| {
                (upstream_id, place_id)
            })
            .join_map(&places, |_upstream_id, &place_id, _upstream| place_id);

        let exclusions = downstream_exclusions.concat(&upstream_exclusions);

        let eligible_places = places.antijoin(&exclusions);

        let (new_edges, discarded_edges) = eligible_places
            .join_map(&edges_forward, |&place_id, _place, &downstream_id| {
                (place_id, downstream_id)
            })
            .join_map(&edges_reverse, |&place_id, &downstream_id, &upstream_id| {
                (
                    (upstream_id, downstream_id),
                    vec![(place_id, downstream_id), (upstream_id, place_id)],
                )
            })
            .split(identity);

        ProgramGraph {
            value_edges: graph
                .value_edges
                .concatenate(vec![new_edges, discarded_edges.flatten().negate()]),
            ..graph
        }
        .leave_region()
    })
}
