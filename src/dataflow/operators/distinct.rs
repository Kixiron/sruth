use crate::dataflow::operators::ArrangeBySelfExt;
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{arrange::Arranged, Threshold},
    trace::{BatchReader, Cursor, TraceReader},
    Collection, ExchangeData, Hashable,
};
use timely::dataflow::Scope;

pub trait DistinctExt<D, R1, R2 = R1> {
    type Output;

    fn distinct_exchange<F>(&self, route: F) -> Self::Output
    where
        F: Fn(&D) -> u64 + 'static,
    {
        self.distinct_exchange_named("DistinctExchange", route)
    }

    fn distinct_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&D) -> u64 + 'static;

    fn distinct_pipelined(&self) -> Self::Output {
        self.distinct_pipelined_named("DistinctPipelined")
    }

    fn distinct_pipelined_named(&self, name: &str) -> Self::Output;
}

impl<S, D, R1, R2> DistinctExt<D, R1, R2> for Collection<S, D, R1>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R1: Semigroup + ExchangeData,
    R2: Semigroup + Abelian + From<i8>,
{
    type Output = Collection<S, D, R2>;

    fn distinct_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&D) -> u64 + 'static,
    {
        self.arrange_by_self_exchange(route)
            .threshold_named(name, |_, _| R2::from(1))
    }

    fn distinct_pipelined_named(&self, name: &str) -> Self::Output {
        self.arrange_by_self_pipelined()
            .threshold_named(name, |_, _| R2::from(1))
    }
}

impl<S, K, R1, R2, A> DistinctExt<K, R1, R2> for Arranged<S, A>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    R1: Semigroup + ExchangeData,
    R2: Semigroup + Abelian + From<i8>,
    A: TraceReader<Key = K, Val = (), Time = S::Timestamp, R = R1> + Clone + 'static,
    A::Batch: BatchReader<K, (), S::Timestamp, R1>,
    A::Cursor: Cursor<K, (), S::Timestamp, R1>,
{
    type Output = Collection<S, K, R2>;

    // TODO: It's funky that this is a noop
    fn distinct_exchange_named<F>(&self, name: &str, _route: F) -> Self::Output
    where
        F: Fn(&K) -> u64 + 'static,
    {
        self.threshold_named(name, |_, _| R2::from(1))
    }

    fn distinct_pipelined_named(&self, name: &str) -> Self::Output {
        self.threshold_named(name, |_, _| R2::from(1))
    }
}
