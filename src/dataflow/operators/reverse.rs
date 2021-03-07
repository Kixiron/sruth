use super::MapExt;
use differential_dataflow::{difference::Semigroup, Collection};
use timely::{
    dataflow::{Scope, Stream},
    Data,
};

pub trait Reverse {
    type Output;

    fn reverse(&self) -> Self::Output;
}

// TODO: Specialized impl where `A == B`
impl<S, A, B> Reverse for Stream<S, (A, B)>
where
    S: Scope,
    A: Data,
    B: Data,
{
    type Output = Stream<S, (B, A)>;

    fn reverse(&self) -> Self::Output {
        self.map_named("Reverse", |(a, b)| (b, a))
    }
}

impl<S, A, B, R> Reverse for Collection<S, (A, B), R>
where
    S: Scope,
    A: Data,
    B: Data,
    R: Semigroup,
{
    type Output = Collection<S, (B, A), R>;

    fn reverse(&self) -> Self::Output {
        self.map_named("Reverse", |(a, b)| (b, a))
    }
}
