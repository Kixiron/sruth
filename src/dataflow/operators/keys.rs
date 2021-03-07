use super::MapExt;
use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{Scope, Stream},
    Data,
};

pub trait Keys {
    type Output;

    fn keys(&self) -> Self::Output;
}

impl<S, K, V> Keys for Stream<S, (K, V)>
where
    S: Scope,
    K: Data,
    V: Data,
{
    type Output = Stream<S, K>;

    fn keys(&self) -> Self::Output {
        self.map_named("Keys", |(key, _value)| key)
    }
}

impl<S, K, V, R> Keys for Collection<S, (K, V), R>
where
    S: Scope,
    K: Data,
    V: Data,
    R: Semigroup,
{
    type Output = Collection<S, K, R>;

    fn keys(&self) -> Self::Output {
        self.inner
            .map_named("Keys", |((key, _value), time, diff)| (key, time, diff))
            .as_collection()
    }
}
