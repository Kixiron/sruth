use crate::{
    dataflow::operators::{CountExt, InspectExt},
    vsdg::{node::NodeExt, ProgramGraph},
};
use differential_dataflow::{
    difference::{Abelian, Multiply},
    lattice::Lattice,
    operators::Join,
    ExchangeData,
};
use std::iter;
use timely::dataflow::Scope;

// TODO: Make this runtime configurable
// TODO: This is 100% a guess tbh
const TRIVIAL_INLINE_THRESHOLD: isize = 100;

pub fn trivial_inline<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
    isize: Multiply<R, Output = isize>,
{
    scope.region_named("trivial inline", |region| {
        let graph = graph.enter_region(region);

        // let function_ends = graph.function_ends();
        // function_ends.join(&)

        let node_costs = graph
            .nodes
            .map(|(id, node)| (id, (node.inline_cost(), node)));

        let node_memberships = graph.node_memberships();

        let function_costs = node_memberships
            .join_map(&node_costs, |_, &end_id, &(cost, _)| (end_id, cost))
            .explode(|(end_id, cost)| iter::once((end_id, cost)))
            .count_core::<R>();

        let _viable_functions = function_costs
            .filter(|&(_, cost)| cost <= TRIVIAL_INLINE_THRESHOLD)
            .debug_inspect(|((end_id, cost), timestamp, diff)| {
                tracing::trace!(
                    end_id = %end_id,
                    cost = cost,
                    inline_threshold=TRIVIAL_INLINE_THRESHOLD,
                    timestamp = ?timestamp,
                    diff = ?diff,
                    "found function valid for trivial inline",
                );
            });

        // TODO: Inlining

        graph.leave_region()
    })
}
