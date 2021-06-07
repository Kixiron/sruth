use differential_dataflow::{
    difference::{Abelian, Multiply, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Reduce},
    Collection, Data, ExchangeData, Hashable,
};
use std::{fmt::Debug, panic::Location};
use timely::dataflow::{operators::Inspect, Scope, Stream};

pub trait InspectExt {
    type Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static;

    #[track_caller]
    fn debug(&self) -> Self
    where
        Self: Sized,
        Self::Value: Debug,
    {
        let location = Location::caller();
        self.debug_inspect(move |value| {
            eprintln!(
                "[{}:{}:{}] {:?}",
                location.file(),
                location.line(),
                location.column(),
                value,
            );
        })
    }
}

impl<S, D, R> InspectExt for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    type Value = (D, S::Timestamp, R);

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}

impl<S, D> InspectExt for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    type Value = D;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}

pub trait AggregatedDebug {
    type Data;
    type Time;
    type Diff;

    fn inspect_aggregate<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Time, &[(Self::Data, Self::Diff)]) + 'static;

    #[track_caller]
    fn debug_aggregate(&self) -> Self
    where
        Self: Sized,
        Self::Data: Debug,
        Self::Time: Debug,
        Self::Diff: Debug,
    {
        let location = Location::caller();
        self.inspect_aggregate(move |time, value| {
            eprintln!(
                "[{}:{}:{}] {:?} @ {:?}",
                location.file(),
                location.line(),
                location.column(),
                value,
                time,
            );
        })
    }
}

impl<S, D, R> AggregatedDebug for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: ExchangeData + Abelian + Multiply<Output = R> + From<i8>,
    ((), D): Hashable,
{
    type Data = D;
    type Time = S::Timestamp;
    type Diff = R;

    fn inspect_aggregate<F>(&self, mut inspect: F) -> Self
    where
        F: FnMut(&Self::Time, &[(Self::Data, Self::Diff)]) + 'static,
    {
        if cfg!(debug_assertions) {
            self.map(|data| ((), data))
                .consolidate()
                .reduce(|&(), input, output| {
                    let aggregate: Vec<_> = input
                        .iter()
                        .map(|(data, diff)| ((*data).clone(), diff.clone()))
                        .collect();

                    output.push((aggregate, R::from(1)));
                })
                .inspect(move |(((), aggregate), time, _)| inspect(time, aggregate))
                .explode(|((), data)| data)
        } else {
            self.clone()
        }
    }
}
