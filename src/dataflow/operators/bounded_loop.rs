use differential_dataflow::{difference::Semigroup, AsCollection, Collection, Data};
use timely::{
    dataflow::{
        operators::{feedback::LoopVariable, BranchWhen, ConnectLoop},
        scopes::child::Iterative,
        Scope,
    },
    progress::Timestamp,
};

pub trait BoundedLoop<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    fn bounded_loop<T, F>(&self, max_iterations: T, looped: F) -> Self
    where
        T: Semigroup + Timestamp<Summary = T> + From<i8>,
        for<'a> F:
            FnOnce(&Collection<Iterative<'a, S, T>, D, R>) -> Collection<Iterative<'a, S, T>, D, R>;
}

impl<S, D, R> BoundedLoop<S, D, R> for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    fn bounded_loop<T, F>(&self, max_iterations: T, looped: F) -> Self
    where
        T: Semigroup + Timestamp<Summary = T> + From<i8>,
        for<'a> F:
            FnOnce(&Collection<Iterative<'a, S, T>, D, R>) -> Collection<Iterative<'a, S, T>, D, R>,
    {
        self.scope().iterative(move |scope| {
            let (handle, cycle) = scope.loop_variable(T::from(1));

            let collection = looped(&self.enter(scope).concat(&cycle.as_collection()));
            let (connected, _discarded) = collection
                .inner
                .branch_when(move |time| time.inner >= max_iterations);

            connected.connect_loop(handle);
            collection.leave()
        })
    }
}
