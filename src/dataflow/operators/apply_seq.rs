use differential_dataflow::{
    difference::Abelian, lattice::Lattice, operators::iterate::Variable, AsCollection, Collection,
    Data,
};
use timely::{
    dataflow::{operators::Map, Scope},
    order::Product,
};

pub trait ApplySequenced<S, D> {
    type Output;

    fn apply_sequenced(&self, sequenced: Vec<Box<dyn Fn(D) -> D>>) -> Self::Output {
        self.apply_sequenced_named("ApplySequenced", sequenced)
    }

    fn apply_sequenced_named(
        &self,
        name: &str,
        sequenced: Vec<Box<dyn Fn(D) -> D>>,
    ) -> Self::Output;
}

impl<S, D, R> ApplySequenced<S, D> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Data,
    R: Abelian,
{
    type Output = Collection<S, D, R>;

    fn apply_sequenced_named(
        &self,
        name: &str,
        sequenced: Vec<Box<dyn Fn(D) -> D>>,
    ) -> Self::Output {
        self.inner
            .scope()
            .scoped::<Product<S::Timestamp, u32>, _, _>(name, move |scope| {
                let variable =
                    Variable::new_from(self.enter(scope), Product::new(Default::default(), 1));

                let applied = variable
                    .inner
                    .map(move |(data, time, diff)| {
                        let apply = &*sequenced[time.inner as usize % sequenced.len()];

                        (apply(data), time, diff)
                    })
                    .as_collection();

                variable.set(&applied).leave()
            })
    }
}
