use crate::repr::{
    utils::{DisplayCtx, EstimateAsm, IRDisplay, InstructionExt, InstructionPurity},
    Ident, Type, TypedVar, Value,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Assign {
    pub value: Value,
    pub dest: VarId,
    pub name: Option<Ident>,
}

impl Assign {
    pub const fn new(dest: VarId, value: Value, name: Option<Ident>) -> Self {
        Self { value, dest, name }
    }

    pub const fn is_const(&self) -> bool {
        self.value.is_const()
    }
}

impl InstructionExt for Assign {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        self.value.ty.clone()
    }

    fn purity(&self) -> InstructionPurity {
        InstructionPurity::Pure
    }

    fn replace_uses(&mut self, from: VarId, to: &Value) -> bool {
        if let Some(var) = self.value.as_var() {
            if var == from {
                self.value = to.clone();
                return true;
            }
        }

        false
    }

    fn used_vars(&self) -> Vec<TypedVar> {
        self.value.as_typed_var().into_iter().collect()
    }

    fn used_values_into<'a>(&'a self, buf: &mut Vec<&'a Value>) {
        buf.push(&self.value);
    }

    fn used_values_mut(&mut self) -> Vec<&mut Value> {
        vec![&mut self.value]
    }
}

impl EstimateAsm for Assign {
    fn estimated_instructions(&self) -> usize {
        1
    }
}

impl IRDisplay for Assign {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.nil()
            .append(self.dest.display(ctx))
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(self.value.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct VarId(NonZeroU64);

impl VarId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }
}

impl IRDisplay for VarId {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.text(format!("_{}", self.0))
    }
}
