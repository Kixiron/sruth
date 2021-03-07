use std::mem;

use crate::{
    dataflow::operators::{BufferedFlatMap, FilterMap, MapExt},
    repr::{Cast, InstId, Instruction, InstructionExt, RawCast, Type, TypedVar, Value, VarId},
};
use differential_dataflow::{difference::Semigroup, Collection, Data};
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
    type Output = Collection<S, (TypedVar, InstId), R>;

    fn collect_usages_named(&self, name: &str) -> Self::Output {
        self.buffered_flat_map_named(name, |(id, inst), buf| {
            buf.extend(inst.used_vars().into_iter().map(move |var| (var, id)));
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
    type Output = Collection<S, (TypedVar, InstId), R>;

    fn collect_declarations_named(&self, name: &str) -> Self::Output {
        self.map_named(name, |(id, inst)| {
            (TypedVar::new(inst.dest(), inst.dest_type()), id)
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
        self.map_named(name, |(_id, inst)| (inst.dest(), inst.dest_type()))
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
        let mut buffer = Vec::new();

        self.buffered_flat_map_named(name, move |(id, inst), buf| {
            // Safety: `buffer` is cleared before the values go out of scope
            inst.used_values_into(unsafe {
                mem::transmute::<&mut Vec<&Value>, &mut Vec<&Value>>(&mut buffer)
            });

            buf.extend(buffer.drain(..).cloned().map(move |val| (id, val)));
        })
    }
}

pub trait CollectCastable<S, I, T, R>
where
    S: Scope,
    R: Semigroup,
{
    fn collect_castable<U>(&self) -> Collection<S, (I, U), R>
    where
        T: RawCast<U>,
        U: Data,
    {
        self.collect_castable_named("CollectCastable")
    }

    fn collect_castable_named<U>(&self, name: &str) -> Collection<S, (I, U), R>
    where
        T: RawCast<U>,
        U: Data;
}

impl<S, I, T, R> CollectCastable<S, I, T, R> for Collection<S, (I, T), R>
where
    S: Scope,
    T: Data,
    I: Data,
    R: Semigroup,
{
    fn collect_castable_named<U>(&self, name: &str) -> Collection<S, (I, U), R>
    where
        T: RawCast<U>,
        U: Data,
    {
        self.filter_map_named(name, |(id, value)| {
            value.cast::<U>().map(move |val| (id, val))
        })
    }
}
