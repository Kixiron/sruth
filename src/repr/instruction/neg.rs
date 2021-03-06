use crate::repr::{
    utils::{DisplayCtx, EstimateAsm, IRDisplay, InstructionExt, InstructionPurity},
    Type, TypedVar, Value, VarId,
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
}

impl InstructionExt for Neg {
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

impl EstimateAsm for Neg {
    fn estimated_instructions(&self) -> usize {
        1
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
