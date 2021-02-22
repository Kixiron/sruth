use differential_dataflow::{collection::AsCollection, difference::Semigroup, Collection};
use timely::{
    dataflow::{
        channels::pact::Pipeline, operators::generic::builder_rc::OperatorBuilder, Scope, Stream,
    },
    Data,
};

pub trait Split<D, Left, Right> {
    type LeftStream;
    type RightStream;

    fn split<L>(&self, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
    {
        self.split_named("Split", logic)
    }

    fn split_named<L>(&self, name: &str, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static;
}

impl<S, D, Left, Right> Split<D, Left, Right> for Stream<S, D>
where
    S: Scope,
    D: Data,
    Left: Data,
    Right: Data,
{
    type LeftStream = Stream<S, Left>;
    type RightStream = Stream<S, Right>;

    fn split_named<L>(&self, name: &str, mut logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
    {
        let mut buffer = Vec::new();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let (mut left_out, left_stream) = builder.new_output();
        let (mut right_out, right_stream) = builder.new_output();

        builder.build(move |_capabilities| {
            move |_frontiers| {
                let (mut left_out, mut right_out) = (left_out.activate(), right_out.activate());

                input.for_each(|capability, data| {
                    let capability = capability.retain();
                    data.swap(&mut buffer);

                    let (mut left_session, mut right_session) = (
                        left_out.session(&capability),
                        right_out.session(&capability),
                    );

                    for data in buffer.drain(..) {
                        let (left, right) = logic(data);

                        left_session.give(left);
                        right_session.give(right);
                    }
                });
            }
        });

        (left_stream, right_stream)
    }
}

impl<S, D, R, Left, Right> Split<D, Left, Right> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    R: Semigroup + Clone,
    Left: Data,
    Right: Data,
{
    type LeftStream = Collection<S, Left, R>;
    type RightStream = Collection<S, Right, R>;

    fn split_named<L>(&self, name: &str, mut logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
    {
        let (left, right) = self.inner.split_named(name, move |(data, time, diff)| {
            let (left, right) = logic(data);
            ((left, time.clone(), diff.clone()), (right, time, diff))
        });

        (left.as_collection(), right.as_collection())
    }
}

pub trait FilterSplit<D, Left, Right> {
    type LeftStream;
    type RightStream;

    fn filter_split<L>(&self, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Option<Left>, Option<Right>) + 'static,
    {
        self.filter_split_named("FilterSplit", logic)
    }

    fn filter_split_named<L>(&self, name: &str, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Option<Left>, Option<Right>) + 'static;
}

impl<S, D, Left, Right> FilterSplit<D, Left, Right> for Stream<S, D>
where
    S: Scope,
    D: Data,
    Left: Data,
    Right: Data,
{
    type LeftStream = Stream<S, Left>;
    type RightStream = Stream<S, Right>;

    fn filter_split_named<L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Option<Left>, Option<Right>) + 'static,
    {
        let mut buffer = Vec::new();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let (mut left_out, left_stream) = builder.new_output();
        let (mut right_out, right_stream) = builder.new_output();

        builder.build(move |_capabilities| {
            move |_frontiers| {
                let (mut left_out, mut right_out) = (left_out.activate(), right_out.activate());

                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    let (mut left_session, mut right_session) = (
                        left_out.session(&capability),
                        right_out.session(&capability),
                    );

                    for data in buffer.drain(..) {
                        let (left, right) = logic(data);

                        if let Some(left) = left {
                            left_session.give(left);
                        }

                        if let Some(right) = right {
                            right_session.give(right);
                        }
                    }
                });
            }
        });

        (left_stream, right_stream)
    }
}

impl<S, D, R, Left, Right> FilterSplit<D, Left, Right> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    R: Semigroup + Clone,
    Left: Data,
    Right: Data,
{
    type LeftStream = Collection<S, Left, R>;
    type RightStream = Collection<S, Right, R>;

    fn filter_split_named<L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Option<Left>, Option<Right>) + 'static,
    {
        let (left, right) = self
            .inner
            .filter_split_named(name, move |(data, time, diff)| {
                let (left, right) = logic(data);

                (
                    left.map(|left| (left, time.clone(), diff.clone())),
                    right.map(|right| (right, time, diff)),
                )
            });

        (left.as_collection(), right.as_collection())
    }
}

pub trait FlatSplit<D, Left, Right> {
    type LeftStream;
    type RightStream;

    fn flat_split<L, LeftIter, RightIter>(&self, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        self.flat_split_named("FlatSplit", logic)
    }

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>;
}

impl<S, D, Left, Right> FlatSplit<D, Left, Right> for Stream<S, D>
where
    S: Scope,
    D: Data,
    Left: Data,
    Right: Data,
{
    type LeftStream = Stream<S, Left>;
    type RightStream = Stream<S, Right>;

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        let mut buffer = Vec::new();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let (mut left_out, left_stream) = builder.new_output();
        let (mut right_out, right_stream) = builder.new_output();

        builder.build(move |_capabilities| {
            move |_frontiers| {
                let (mut left_out, mut right_out) = (left_out.activate(), right_out.activate());

                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    let (mut left_session, mut right_session) = (
                        left_out.session(&capability),
                        right_out.session(&capability),
                    );

                    for data in buffer.drain(..) {
                        let (left, right) = logic(data);

                        left_session.give_iterator(left.into_iter());
                        right_session.give_iterator(right.into_iter());
                    }
                });
            }
        });

        (left_stream, right_stream)
    }
}

impl<S, D, R, Left, Right> FlatSplit<D, Left, Right> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    R: Semigroup + Clone,
    Left: Data,
    Right: Data,
{
    type LeftStream = Collection<S, Left, R>;
    type RightStream = Collection<S, Right, R>;

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        let (left, right) = self
            .inner
            .flat_split_named(name, move |(data, time, diff)| {
                let (left, right) = logic(data);

                let (left_time, left_diff) = (time.clone(), diff.clone());
                let left = left
                    .into_iter()
                    .map(move |left| (left, left_time.clone(), left_diff.clone()));

                let right = right
                    .into_iter()
                    .map(move |right| (right, time.clone(), diff.clone()));

                (left, right)
            });

        (left.as_collection(), right.as_collection())
    }
}

// pub trait SplitBy<S, D, R>
// where
//     S: Scope,
//     R: Semigroup,
// {
//     fn split_by<Split, L>(&self, logic: L) -> Split::Collection<S, R>
//     where
//         Split: Splittable,
//         L: FnMut(D) -> Split + 'static,
//     {
//         self.split_by_named("SplitBy", logic)
//     }
//
//     fn split_by_named<Split, L>(&self, name: &str, logic: L) -> Split::Collection<S, R>
//     where
//         Split: Splittable,
//         L: FnMut(D) -> Split + 'static;
// }
//
// impl<S, D, R> SplitBy<S, D, R> for Collection<S, D, R>
// where
//     S: Scope,
//     R: Semigroup,
// {
//     fn split_by_named<Split, L>(&self, name: &str, logic: L) -> Split::Collection<S, R>
//     where
//         Split: Splittable,
//         L: FnMut(D) -> Split + 'static {
//         todo!()
//     }
// }
//
// pub trait Splittable {
//     type Collection<S, R>
//     where
//         S: Scope,
//         R: Semigroup;
// }
//
// macro_rules! impl_splittable {
//     (
//         $(
//             ($($elem:ident),* $(,)?)
//         ),* $(,)?
//     ) => {
//         $(
//             impl<$($elem,)*> Splittable for ($($elem,)*) {
//                 type Collection<S, R>
//                 where
//                     S: Scope,
//                     R: Semigroup,
//                 = ($(Collection<S, $elem, R>,)*);
//             }
//         )*
//     };
// }
//
// impl_splittable! {
//     (A,),
//     (A, B),
//     (A, B, C),
//     (A, B, C, D),
//     (A, B, C, D, E),
//     (A, B, C, D, E, F),
//     (A, B, C, D, E, F, G),
//     (A, B, C, D, E, F, G, H),
//     (A, B, C, D, E, F, G, H, I),
//     (A, B, C, D, E, F, G, H, I, J),
//     (A, B, C, D, E, F, G, H, I, J, K),
//     (A, B, C, D, E, F, G, H, I, J, K, L),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W, X),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W, X, Y),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W, X, Y, Z),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W, X, Y, Z, AA),
//     (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, T, U, V, W, X, Y, Z, AA, BB),
// }
