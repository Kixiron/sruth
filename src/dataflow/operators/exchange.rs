use differential_dataflow::{
    difference::Semigroup,
    lattice::Lattice,
    operators::arrange::{Arrange, Arranged, TraceAgent},
    trace::implementations::ord::{OrdKeySpine, OrdValSpine},
    AsCollection, Collection, ExchangeData, Hashable,
};
use timely::dataflow::{
    channels::pact::{Exchange, Pipeline},
    operators::Operator,
    Scope, Stream,
};

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

pub trait ArrangeByKeyExt<K, V> {
    type Output;

    fn arrange_by_key_exchange<F>(&self, route: F) -> Self::Output
    where
        F: Fn(&K, &V) -> u64 + 'static,
    {
        self.arrange_by_key_exchange_named("ArrangeByKeyExchange", route)
    }

    fn arrange_by_key_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&K, &V) -> u64 + 'static;

    fn arrange_by_key_pipelined(&self) -> Self::Output {
        self.arrange_by_key_pipelined_named("ArrangeByKeyPipelined")
    }

    fn arrange_by_key_pipelined_named(&self, name: &str) -> Self::Output;
}

impl<S, K, V, R> ArrangeByKeyExt<K, V> for Collection<S, (K, V), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    #[allow(clippy::type_complexity)]
    type Output = Arranged<S, TraceAgent<OrdValSpine<K, V, S::Timestamp, R>>>;

    fn arrange_by_key_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&K, &V) -> u64 + 'static,
    {
        let exchange = Exchange::new(move |((key, value), _time, _diff)| route(key, value));
        self.arrange_core(exchange, name)
    }

    fn arrange_by_key_pipelined_named(&self, name: &str) -> Self::Output {
        self.arrange_core(Pipeline, name)
    }
}

pub trait ArrangeBySelfExt<K> {
    type Output;

    fn arrange_by_self_exchange<F>(&self, route: F) -> Self::Output
    where
        F: Fn(&K) -> u64 + 'static,
    {
        self.arrange_by_self_exchange_named("ArrangeBySelfExchange", route)
    }

    fn arrange_by_self_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&K) -> u64 + 'static;

    fn arrange_by_self_pipelined(&self) -> Self::Output {
        self.arrange_by_self_pipelined_named("ArrangeBySelfPipelined")
    }

    fn arrange_by_self_pipelined_named(&self, name: &str) -> Self::Output;
}

impl<S, K, R> ArrangeBySelfExt<K> for Collection<S, K, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    R: Semigroup + ExchangeData,
{
    type Output = Arranged<S, TraceAgent<OrdKeySpine<K, S::Timestamp, R>>>;

    fn arrange_by_self_exchange_named<F>(&self, name: &str, route: F) -> Self::Output
    where
        F: Fn(&K) -> u64 + 'static,
    {
        let exchange = Exchange::new(move |((key, ()), _time, _diff)| route(key));
        self.map(|key| (key, ())).arrange_core(exchange, name)
    }

    fn arrange_by_self_pipelined_named(&self, name: &str) -> Self::Output {
        self.map(|key| (key, ())).arrange_core(Pipeline, name)
    }
}
