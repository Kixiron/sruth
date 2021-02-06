use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeBySelf, Arranged},
        reduce::ReduceCore,
    },
    trace::{implementations::ord::OrdValSpine, BatchReader, Cursor, TraceReader},
    Collection, ExchangeData, Hashable,
};
use timely::dataflow::Scope;

pub trait CountExt<S, K, R1>
where
    S: Scope,
    R1: Semigroup,
{
    fn count_core<R2>(&self) -> Collection<S, (K, R1), R2>
    where
        R2: Semigroup + Abelian + From<i8>;
}

impl<S, K, R1> CountExt<S, K, R1> for Collection<S, K, R1>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    K: ExchangeData + Hashable,
    R1: Semigroup + ExchangeData,
{
    fn count_core<R2>(&self) -> Collection<S, (K, R1), R2>
    where
        R2: Semigroup + Abelian + From<i8>,
    {
        self.arrange_by_self_named("Arrange: Count").count_core()
    }
}

impl<S, K, R1, A1> CountExt<S, K, R1> for Arranged<S, A1>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    K: ExchangeData + Hashable,
    R1: Semigroup + ExchangeData,
    A1: TraceReader<Key = K, Val = (), Time = S::Timestamp, R = R1> + Clone + 'static,
    A1::Batch: BatchReader<K, (), S::Timestamp, R1>,
    A1::Cursor: Cursor<K, (), S::Timestamp, R1>,
{
    fn count_core<R2>(&self) -> Collection<S, (K, R1), R2>
    where
        R2: Semigroup + Abelian + From<i8>,
    {
        self.reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Count", |_key, input, output| {
            output.push((input[0].1.clone(), R2::from(1)))
        })
        .as_collection(|key, count| (key.clone(), count.clone()))
    }
}
