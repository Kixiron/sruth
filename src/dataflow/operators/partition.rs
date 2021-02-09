use differential_dataflow::{difference::Semigroup, Collection, Data};
use std::{convert::TryInto, mem};
use timely::dataflow::{
    channels::{
        pact::Pipeline,
        pushers::{buffer::Session, Tee},
    },
    operators::generic::{builder_rc::OperatorBuilder, OutputHandle},
    Scope, ScopeParent,
};

pub trait PartitionExt<D1, D2 = D1> {
    type Output;

    fn partition<F>(&self, parts: usize, route: F) -> Vec<Self::Output>
    where
        F: Fn(D1) -> (usize, D2) + 'static,
    {
        self.partition_named("Partition", parts, route)
    }

    fn partition_named<F>(&self, name: &str, parts: usize, route: F) -> Vec<Self::Output>
    where
        F: Fn(D1) -> (usize, D2) + 'static;

    fn partition_array<F, const N: usize>(&self, route: F) -> [Self::Output; N]
    where
        F: Fn(D1) -> (usize, D2) + 'static,
    {
        self.partition_array_named("PartitionArray", route)
    }

    fn partition_array_named<F, const N: usize>(&self, name: &str, route: F) -> [Self::Output; N]
    where
        F: Fn(D1) -> (usize, D2) + 'static;
}

type Bundle<S, D, R> = (D, <S as ScopeParent>::Timestamp, R);
type ActivatedOut<'a, S, D, R> = OutputHandle<
    'a,
    <S as ScopeParent>::Timestamp,
    Bundle<S, D, R>,
    Tee<<S as ScopeParent>::Timestamp, Bundle<S, D, R>>,
>;
type SessionOut<'a, S, D, R> = Session<
    'a,
    <S as ScopeParent>::Timestamp,
    Bundle<S, D, R>,
    Tee<<S as ScopeParent>::Timestamp, Bundle<S, D, R>>,
>;

impl<S, D1, D2, R> PartitionExt<D1, D2> for Collection<S, D1, R>
where
    S: Scope,
    D1: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn partition_named<F>(&self, name: &str, parts: usize, route: F) -> Vec<Self::Output>
    where
        F: Fn(D1) -> (usize, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let mut input = builder.new_input(&self.inner, Pipeline);

        let (mut outputs, mut streams) = (Vec::new(), Vec::new());

        for _ in 0..parts {
            let (output, stream) = builder.new_output();
            outputs.push(output);
            streams.push(Collection::new(stream));
        }

        builder.build(move |_| {
            let mut vector = Vec::new();
            move |_frontiers| {
                let (mut handles, mut sessions) = (
                    Vec::<ActivatedOut<S, D2, R>>::with_capacity(outputs.len()),
                    Vec::<SessionOut<S, D2, R>>::with_capacity(outputs.len()),
                );

                for handle in outputs.iter_mut() {
                    handles.push(handle.activate());
                }

                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    sessions.extend(
                        handles
                            .iter_mut()
                            // Safety: This allows us to reuse the `sessions` vector for each input batch,
                            //         it's alright because we clear the sessions buffer at the end of each
                            //         input batch
                            .map(|handle| unsafe { mem::transmute(handle.session(&time)) }),
                    );

                    for (data, time, diff) in vector.drain(..) {
                        let (part, data) = route(data);
                        sessions[part as usize].give((data, time, diff));
                    }

                    sessions.clear();
                });
            }
        });

        streams
    }

    fn partition_array_named<F, const N: usize>(&self, name: &str, route: F) -> [Self::Output; N]
    where
        F: Fn(D1) -> (usize, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let mut input = builder.new_input(&self.inner, Pipeline);

        let (mut outputs, mut streams) = (Vec::with_capacity(N), Vec::with_capacity(N));

        for _ in 0..N {
            let (output, stream) = builder.new_output();
            outputs.push(output);
            streams.push(Collection::new(stream));
        }

        builder.build(move |_| {
            let mut vector = Vec::with_capacity(N);
            move |_frontiers| {
                let (mut handles, mut sessions) = (
                    Vec::<ActivatedOut<S, D2, R>>::with_capacity(outputs.len()),
                    Vec::<SessionOut<S, D2, R>>::with_capacity(outputs.len()),
                );

                for handle in outputs.iter_mut() {
                    handles.push(handle.activate());
                }

                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    sessions.extend(
                        handles
                            .iter_mut()
                            // Safety: This allows us to reuse the `sessions` vector for each input batch,
                            //         it's alright because we clear the sessions buffer at the end of each
                            //         input batch
                            .map(|handle| unsafe { mem::transmute(handle.session(&time)) }),
                    );

                    for (data, time, diff) in vector.drain(..) {
                        let (part, data) = route(data);
                        sessions[part as usize].give((data, time, diff));
                    }

                    sessions.clear();
                });
            }
        });

        streams
            .try_into()
            .unwrap_or_else(|_| unreachable!("vector is the correct size"))
    }
}
