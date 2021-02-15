use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt, InstructionPurity},
    Ident, Type, Value,
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

    fn estimated_instructions(&self) -> usize {
        0
    }

    fn replace_uses(&mut self, from: VarId, to: Value) -> bool {
        if let Some(var) = self.value.as_var() {
            if var == from {
                self.value = to;
                return true;
            }
        }

        false
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct TypedVar {
    pub var: VarId,
    pub ty: Type,
}

impl TypedVar {
    pub const fn new(var: VarId, ty: Type) -> Self {
        Self { var, ty }
    }
}
