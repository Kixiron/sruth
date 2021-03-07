use crate::repr::{
    utils::{DisplayCtx, EstimateAsm, IRDisplay, InstructionExt, InstructionPurity},
    Type, TypedVar, Value, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

// TODO: Comparison kind
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
}

impl InstructionExt for Cmp {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        Type::Bool
    }

    fn purity(&self) -> InstructionPurity {
        InstructionPurity::Pure
    }

    fn replace_uses(&mut self, from: VarId, to: &Value) -> bool {
        let mut replaced = false;

        if let Some(var) = self.lhs.as_var() {
            if var == from {
                self.lhs = to.clone();
                replaced = true
            }
        }

        if let Some(var) = self.rhs.as_var() {
            if var == from {
                self.rhs = to.clone();
                replaced = true
            }
        }

        replaced
    }

    fn used_vars(&self) -> Vec<TypedVar> {
        self.lhs
            .as_typed_var()
            .into_iter()
            .chain(self.rhs.as_typed_var())
            .collect()
    }

    fn used_values_into<'a>(&'a self, buf: &mut Vec<&'a Value>) {
        buf.push(&self.lhs);
        buf.push(&self.rhs);
    }

    fn used_values_mut(&mut self) -> Vec<&mut Value> {
        vec![&mut self.lhs, &mut self.rhs]
    }
}

impl EstimateAsm for Cmp {
    fn estimated_instructions(&self) -> usize {
        1
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
