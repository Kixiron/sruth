use differential_dataflow::{collection::AsCollection, difference::Semigroup, Collection};
use std::{collections::VecDeque, iter};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait FilterMap<D, D2> {
    type Output;

    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.filter_map_named("FilterMap", logic)
    }

    fn filter_map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static;
}

impl<S, D, D2> FilterMap<D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        let mut buffer = Vec::new();
        let mut work_queue = VecDeque::new();

        self.unary(Pipeline, name, move |_capability, info| {
            let activator = self.scope().activator_for(&info.address);

            move |input, output| {
                input.for_each(|capability, data| {
                    let capability = capability.retain();
                    data.swap(&mut buffer);

                    work_queue.extend(buffer.drain(..).zip(iter::repeat(capability)));
                });

                let mut fuel = 1_000_000;
                while let Some((element, capability)) = work_queue.pop_front() {
                    if let Some(element) = logic(element) {
                        output.session(&capability).give(element);
                    }

                    fuel -= 1;
                    if fuel == 0 {
                        break;
                    }
                }

                if !work_queue.is_empty() {
                    activator.activate();
                }
            }
        })
    }
}

impl<S, D, D2, R> FilterMap<D, D2> for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_named(name, move |(data, time, diff)| {
                logic(data).map(|data| (data, time, diff))
            })
            .as_collection()
    }
}
