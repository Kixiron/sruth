use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt},
    Type, Value, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Neg {
    pub value: Value,
    pub dest: VarId,
}

impl Neg {
    pub const fn new(dest: VarId, value: Value) -> Self {
        Self { value, dest }
    }

    // TODO: Make this a method on InstructionExt
    pub const fn is_const(&self) -> bool {
        self.value.is_const()
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        if let Some(var) = self.value.as_var_mut() {
            if *var == from {
                *var = to;
                return true;
            }
        }

        false
    }
}

impl InstructionExt for Neg {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        self.value.ty.clone()
    }
}

impl IRDisplay for Neg {
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
            .append(ctx.text("neg"))
            .append(ctx.space())
            .append(self.value.display(ctx))
            .group()
    }
}
