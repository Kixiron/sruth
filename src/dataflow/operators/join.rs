use std::ops::Mul;

use differential_dataflow::{
    difference::Semigroup,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf, Arranged},
        JoinCore,
    },
    trace::TraceReader,
    Collection, ExchangeData, Hashable,
};
use timely::dataflow::Scope;

pub trait SemijoinExt<S, K, V, R, R2, Other> {
    fn semijoin(&self, other: &Other) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S: Scope,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData + Semigroup,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup;
}

impl<S, K, V, R, R2, Trace1, Trace2> SemijoinExt<S, K, V, R, R2, Arranged<S, Trace2>>
    for Arranged<S, Trace1>
where
    S: Scope,
    S::Timestamp: Lattice,
    R2: Semigroup,
    Trace1: TraceReader<Time = S::Timestamp, Key = K, Val = V, R = R> + Clone + 'static,
    Trace2: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
{
    fn semijoin(&self, other: &Arranged<S, Trace2>) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S: Scope,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData + Semigroup,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
    {
        self.join_core(&other, |k, v, _| Some((k.clone(), v.clone())))
    }
}

impl<S, K, V, R, R2, Trace> SemijoinExt<S, K, V, R, R2, Collection<S, K, R2>> for Arranged<S, Trace>
where
    S: Scope,
    S::Timestamp: Lattice,
    R2: Semigroup,
    Trace: TraceReader<Time = S::Timestamp, Key = K, Val = V, R = R> + Clone + 'static,
{
    fn semijoin(
        &self,
        other: &Collection<S, K, R2>,
    ) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S: Scope,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData + Semigroup,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
    {
        let other = other.arrange_by_self();
        self.join_core(&other, |k, v, _| Some((k.clone(), v.clone())))
    }
}

impl<S, K, V, R, R2, Trace> SemijoinExt<S, K, V, R, R2, Arranged<S, Trace>>
    for Collection<S, (K, V), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
    Trace: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
{
    fn semijoin(&self, other: &Arranged<S, Trace>) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S: Scope,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData + Semigroup,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
    {
        self.arrange_by_key()
            .join_core(&other, |k, v, _| Some((k.clone(), v.clone())))
    }
}
