use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait BufferedFlatMap<D, D2> {
    type Output;

    fn buffered_flat_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(D, &mut AppendOnlyVec<D2>) + 'static,
    {
        self.buffered_flat_map_named("BufferedFlatMap", logic)
    }

    fn buffered_flat_map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(D, &mut AppendOnlyVec<D2>) + 'static;
}

impl<S, D, D2> BufferedFlatMap<D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    fn buffered_flat_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D, &mut AppendOnlyVec<D2>) + 'static,
    {
        let mut buffer = Vec::new();
        let mut user_buffer = AppendOnlyVec::new();

        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    for data in buffer.drain(..) {
                        logic(data, &mut user_buffer);
                    }

                    output
                        .session(&capability)
                        .give_iterator(user_buffer.0.drain(..))
                });
            }
        })
    }
}

impl<S, D, D2, R> BufferedFlatMap<D, D2> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    D2: Data,
    R: Semigroup + Clone,
{
    type Output = Collection<S, D2, R>;

    fn buffered_flat_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D, &mut AppendOnlyVec<D2>) + 'static,
    {
        let mut user_buffer = AppendOnlyVec::new();

        self.inner
            .buffered_flat_map_named(name, move |(data, time, diff), buffer| {
                logic(data, &mut user_buffer);

                buffer.extend(
                    user_buffer
                        .0
                        .drain(..)
                        .map(|data| (data, time.clone(), diff.clone())),
                );
            })
            .as_collection()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppendOnlyVec<T>(Vec<T>);

impl<T> AppendOnlyVec<T> {
    #[inline]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.0.push(value)
    }

    #[inline]
    pub fn append(&mut self, other: &mut Vec<T>) {
        self.0.append(other)
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional)
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.0.reserve_exact(additional)
    }
}

impl<'a, T: 'a + Copy> Extend<&'a T> for AppendOnlyVec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<T> Extend<T> for AppendOnlyVec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}
