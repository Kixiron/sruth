use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::ArrangeBySelf,
        arrange::{ArrangeByKey, Arranged},
        iterate::SemigroupVariable,
        reduce::ReduceCore,
        JoinCore,
    },
    trace::{implementations::ord::OrdKeySpine, TraceReader},
    Collection, ExchangeData,
};
use std::{hash::Hash, ops::Mul};
use timely::{dataflow::Scope, order::Product};

/// Propagates the reachability of nodes forward from the roots, returning
/// a collection of all nodes that could be reached
pub fn reachable<S, N, R>(
    edges: &Collection<S, (N, N), R>,
    roots: &Collection<S, N, R>,
) -> Collection<S, N, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    N: ExchangeData + Hash,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    reachable_core("Reachable", &edges.arrange_by_key(), roots)
}

/// Propagates the reachability of nodes forward from the roots, returning
/// a collection of all nodes that could be reached
// This is `differential_dataflow::algorithms::graphs::propagate::propagate_core()` specialized for cases
// where `nodes` doesn't carry any data along any data and only cares about node reachability
pub fn reachable_core<S, N, Trace, R>(
    name: &str,
    edges: &Arranged<S, Trace>,
    roots: &Collection<S, N, R>,
) -> Collection<S, N, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    N: ExchangeData + Hash,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
    Trace: TraceReader<Key = N, Val = N, Time = S::Timestamp, R = R> + Clone + 'static,
{
    roots.scope().iterative::<usize, _, _>(|scope| {
        let (edges, roots) = (edges.enter(scope), roots.enter(scope));
        let proposals = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

        let labels = proposals
            .concat(&roots)
            .arrange_by_self()
            .reduce_abelian::<_, OrdKeySpine<_, _, _>>(name, |_key, _input, output| {
                output.push(((), R::from(1)));
            });

        let propagate: Collection<_, N, R> =
            labels.join_core(&edges, |_, &(), node| Some(node.clone()));
        proposals.set(&propagate);

        labels.as_collection(|k, &()| k.clone()).leave()
    })
}
