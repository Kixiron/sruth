use differential_dataflow::{difference::Semigroup, AsCollection, Collection, Data};
use timely::dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream};

pub trait Flatten<D1, D2> {
    type Output;

    fn flatten(&self) -> Self::Output
    where
        D1: IntoIterator<Item = D2>,
    {
        self.flatten_named("Flatten")
    }

    fn flatten_named(&self, name: &str) -> Self::Output
    where
        D1: IntoIterator<Item = D2>;
}

impl<S, D1, D2> Flatten<D1, D2> for Stream<S, D1>
where
    S: Scope,
    D1: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    fn flatten_named(&self, name: &str) -> Self::Output
    where
        D1: IntoIterator<Item = D2>,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&capability)
                        .give_iterator(buffer.drain(..).flatten());
                });
            }
        })
    }
}

impl<S, D1, D2, R> Flatten<D1, D2> for Collection<S, D1, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D1: Data,
    D2: Data,
    R: Semigroup + Clone,
{
    type Output = Collection<S, D2, R>;

    fn flatten_named(&self, name: &str) -> Self::Output
    where
        D1: IntoIterator<Item = D2>,
    {
        let mut buffer = Vec::new();

        self.inner
            .unary(Pipeline, name, move |_capability, _info| {
                move |input, output| {
                    input.for_each(|capability, data| {
                        data.swap(&mut buffer);

                        let mut session = output.session(&capability);
                        for (data, time, diff) in buffer.drain(..) {
                            session.give_iterator(
                                data.into_iter()
                                    .map(|data| (data, time.clone(), diff.clone())),
                            );
                        }
                    });
                }
            })
            .as_collection()
    }
}
