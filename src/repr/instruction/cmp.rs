use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt},
    Type, Value, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Cmp {
    pub lhs: Value,
    pub rhs: Value,
    pub dest: VarId,
}

impl Cmp {
    pub const fn new(lhs: Value, rhs: Value, dest: VarId) -> Self {
        Self { lhs, rhs, dest }
    }

    pub const fn is_const(&self) -> bool {
        self.rhs.is_const() && self.lhs.is_const()
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        let mut replaced = false;

        if let Some(var) = self.lhs.as_var_mut() {
            if *var == from {
                *var = to;
                replaced = true
            }
        }

        if let Some(var) = self.rhs.as_var_mut() {
            if *var == from {
                *var = to;
                replaced = true
            }
        }

        replaced
    }
}

impl InstructionExt for Cmp {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        Type::Bool
    }
}

impl IRDisplay for Cmp {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("cmp"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}
