use crate::repr::{InstId, Instruction, InstructionExt, Type, Value, VarId};
use differential_dataflow::{difference::Semigroup, Collection};
use timely::dataflow::Scope;

pub trait CollectUsages {
    type Output;

    fn collect_usages(&self) -> Self::Output {
        self.collect_usages_named("CollectUsages")
    }

    fn collect_usages_named(&self, name: &str) -> Self::Output;
}

impl<S, R> CollectUsages for Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    R: Semigroup,
{
    type Output = Collection<S, (VarId, InstId), R>;

    fn collect_usages_named(&self, name: &str) -> Self::Output {
        self.scope().region_named(name, |region| {
            let stream = self.enter_region(region);

            stream
                .flat_map(|(id, inst)| inst.used_vars().map(move |var| (var, id)))
                .leave_region()
        })
    }
}

pub trait CollectDeclarations {
    type Output;

    fn collect_declarations(&self) -> Self::Output {
        self.collect_declarations_named("CollectDeclarations")
    }

    fn collect_declarations_named(&self, name: &str) -> Self::Output;
}

impl<S, R> CollectDeclarations for Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    R: Semigroup,
{
    type Output = Collection<S, (VarId, InstId), R>;

    fn collect_declarations_named(&self, name: &str) -> Self::Output {
        self.scope().region_named(name, |region| {
            let stream = self.enter_region(region);

            stream.map(|(id, inst)| (inst.dest(), id)).leave_region()
        })
    }
}

pub trait CollectVariableTypes {
    type Output;

    fn collect_var_types(&self) -> Self::Output {
        self.collect_var_types_named("CollectVariableTypes")
    }

    fn collect_var_types_named(&self, name: &str) -> Self::Output;
}

impl<S, R> CollectVariableTypes for Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    R: Semigroup,
{
    type Output = Collection<S, (VarId, Type), R>;

    fn collect_var_types_named(&self, name: &str) -> Self::Output {
        self.scope().region_named(name, |region| {
            let stream = self.enter_region(region);

            stream
                .map(|(_id, inst)| (inst.dest(), inst.dest_type()))
                .leave_region()
        })
    }
}

pub trait CollectValues {
    type Output;

    fn collect_values(&self) -> Self::Output {
        self.collect_values_named("CollectValues")
    }

    fn collect_values_named(&self, name: &str) -> Self::Output;
}

impl<S, R> CollectValues for Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    R: Semigroup,
{
    type Output = Collection<S, (InstId, Value), R>;

    fn collect_values_named(&self, name: &str) -> Self::Output {
        self.scope().region_named(name, |region| {
            let stream = self.enter_region(region);

            stream
                .flat_map(|(id, inst)| inst.used_values().map(move |val| (id, val)))
                .leave_region()
        })
    }
}
