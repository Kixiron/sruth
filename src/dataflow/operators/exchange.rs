use differential_dataflow::{difference::Semigroup, AsCollection, Collection, ExchangeData};
use timely::dataflow::{channels::pact::Exchange, operators::Operator, Scope, Stream};

pub trait ExchangeExt<D> {
    fn exchange<F>(&self, route: F) -> Self
    where
        F: Fn(&D) -> u64 + 'static,
        Self: Sized,
    {
        self.exchange_named("Exchange", route)
    }

    fn exchange_named<F>(&self, name: &str, route: F) -> Self
    where
        F: Fn(&D) -> u64 + 'static,
        Self: Sized;
}

impl<S, D> ExchangeExt<D> for Stream<S, D>
where
    S: Scope,
    D: ExchangeData,
{
    fn exchange_named<F>(&self, name: &str, route: F) -> Self
    where
        F: Fn(&D) -> u64 + 'static,
        Self: Sized,
    {
        self.unary(Exchange::new(route), name, move |_capability, _info| {
            let mut buffer = Vec::new();

            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    output.session(&time).give_vec(&mut buffer);
                });
            }
        })
    }
}

impl<S, D, R> ExchangeExt<D> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: ExchangeData,
    R: Semigroup + ExchangeData,
    D: ExchangeData,
{
    fn exchange_named<F>(&self, name: &str, route: F) -> Self
    where
        F: Fn(&D) -> u64 + 'static,
        Self: Sized,
    {
        self.inner
            .exchange_named(name, move |(data, _time, _diff)| route(data))
            .as_collection()
    }
}
