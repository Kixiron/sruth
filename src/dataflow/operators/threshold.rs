use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeBySelf, Arranged, TraceAgent},
        reduce::ReduceCore,
    },
    trace::{implementations::ord::OrdKeySpine, BatchReader, Cursor, TraceReader},
    Collection, Data, ExchangeData, Hashable,
};
use timely::dataflow::Scope;

pub trait ThresholdExt<S, K, R1>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: Data + Ord,
{
    fn threshold_arranged<R2, F>(
        &self,
        name: &str,
        thresh: F,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<K, S::Timestamp, R2>>>
    where
        R2: Abelian,
        F: FnMut(&K, &R1) -> R2 + 'static;

    fn distinct_arranged<R2: Abelian + From<i8>>(
        &self,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<K, S::Timestamp, R2>>> {
        self.threshold_arranged("Distinct", |_, _| R2::from(1i8))
    }
}

impl<S, K, R1> ThresholdExt<S, K, R1> for Collection<S, K, R1>
where
    S::Timestamp: Lattice + Ord,
    S: Scope,
    K: ExchangeData + Hashable,
    R1: ExchangeData + Semigroup,
{
    fn threshold_arranged<R2, F>(
        &self,
        name: &str,
        thresh: F,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<K, S::Timestamp, R2>>>
    where
        R2: Abelian,
        F: FnMut(&K, &R1) -> R2 + 'static,
    {
        self.arrange_by_self_named(&format!("Arrange: {}", name))
            .threshold_arranged(name, thresh)
    }
}

impl<S, K, T1, R1> ThresholdExt<S, K, R1> for Arranged<S, T1>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    K: Data,
    R1: Semigroup,
    T1: TraceReader<Key = K, Val = (), Time = S::Timestamp, R = R1> + Clone + 'static,
    T1::Batch: BatchReader<K, (), S::Timestamp, R1>,
    T1::Cursor: Cursor<K, (), S::Timestamp, R1>,
{
    fn threshold_arranged<R2, F>(
        &self,
        name: &str,
        mut thresh: F,
    ) -> Arranged<S, TraceAgent<OrdKeySpine<K, S::Timestamp, R2>>>
    where
        R2: Abelian,
        F: FnMut(&K, &R1) -> R2 + 'static,
    {
        self.reduce_abelian::<_, OrdKeySpine<_, _, _>>(name, move |k, s, t| {
            t.push(((), thresh(k, &s[0].1)))
        })
    }
}
