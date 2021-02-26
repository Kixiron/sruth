use crate::vsdg::{Edge, ProgramGraph};
use differential_dataflow::{
    algorithms::graphs::scc,
    difference::Abelian,
    lattice::Lattice,
    operators::{Iterate, Join, Threshold},
    Collection, ExchangeData,
};
use std::ops::Mul;
use timely::dataflow::Scope;

pub fn detect_loops<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> Collection<S, Edge, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    scope.region_named("detect loops", |region| {
        // Reduce the control graph to strongly connected components
        let control_edges = scc::strongly_connected(&graph.control_edges.enter_region(region));

        control_edges
            .iterate(|edges| {
                // keep edges from active edge destinations.
                let active = edges.map(|(_src, dst)| dst).threshold(|_, c| {
                    if c.is_zero() {
                        R::from(0)
                    } else {
                        R::from(1)
                    }
                });

                // TODO: We can arrange `control_edges` and keep it around
                control_edges.enter(&edges.scope()).semijoin(&active)
            })
            .leave_region()
    })
}
