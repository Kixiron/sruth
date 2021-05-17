use differential_dataflow::{collection::AsCollection, difference::Monoid, Collection};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope},
    Data,
};

pub trait FilterDiff<R> {
    type Output;

    fn filter_diff<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(&R) -> bool + 'static,
    {
        self.filter_diff_named("FilterDiff", logic)
    }

    fn filter_diff_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(&R) -> bool + 'static;
}

impl<S, D, R> FilterDiff<R> for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Monoid,
{
    type Output = Collection<S, D, R>;

    fn filter_diff_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&R) -> bool + 'static,
    {
        let mut buffer = Vec::new();
        self.inner
            .unary(Pipeline, name, move |_capability, _info| {
                move |input, output| {
                    input.for_each(|capability, data| {
                        data.swap(&mut buffer);

                        output.session(&capability).give_iterator(
                            buffer.drain(..).filter(|(_data, _time, diff)| logic(diff)),
                        );
                    });
                }
            })
            .as_collection()
    }
}
