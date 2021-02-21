use differential_dataflow::{difference::Semigroup, Collection};
use timely::{
    dataflow::{operators::Map, Scope, Stream},
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
        // TODO: Map named
        self.map(|(key, _value)| key)
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
        // TODO: Map named
        self.map(|(key, _value)| key)
    }
}
