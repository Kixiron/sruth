use crate::repr::{
    utils::{DisplayCtx, EstimateAsm, IRDisplay, InstructionPurity},
    InstructionExt, Type, TypedVar, Value, ValueKind, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Bitcast {
    pub dest: TypedVar,
    pub source: Value,
}

impl Bitcast {
    pub const fn new(dest: TypedVar, source: TypedVar) -> Self {
        Self {
            dest,
            source: Value {
                value: ValueKind::Var(source.var),
                ty: source.ty,
            },
        }
    }

    pub const fn dest_ty(&self) -> &Type {
        &self.dest.ty
    }

    pub const fn source_ty(&self) -> &Type {
        &self.source.ty
    }

    pub fn types(&self) -> (&Type, &Type) {
        (&self.source.ty, &self.dest.ty)
    }

    pub fn is_valid(&self) -> bool {
        matches!(
            (&self.dest.ty, &self.source.ty),
            (Type::Int, Type::Int)
                | (Type::Int, Type::Uint)
                | (Type::Uint, Type::Int)
                | (Type::Uint, Type::Uint)
                | (Type::Int, Type::Bool)
                | (Type::Uint, Type::Bool)
                | (Type::Bool, Type::Int)
                | (Type::Bool, Type::Uint)
                | (Type::Bool, Type::Bool)
        )
    }

    pub fn is_redundant(&self) -> bool {
        self.dest.ty == self.source.ty
    }
}

impl InstructionExt for Bitcast {
    fn dest(&self) -> VarId {
        self.dest.var
    }

    fn dest_type(&self) -> Type {
        self.dest.ty.clone()
    }

    fn purity(&self) -> InstructionPurity {
        InstructionPurity::Pure
    }

    fn replace_uses(&mut self, from: VarId, to: &Value) -> bool {
        if let Some(to) = to.as_typed_var() {
            if self.source.as_var() == Some(from) {
                self.source = to.into();
                return true;
            }
        }

        false
    }

    fn used_vars(&self) -> Vec<TypedVar> {
        self.source.as_typed_var().into_iter().collect()
    }

    fn used_values_into<'a>(&'a self, buf: &mut Vec<&'a Value>) {
        buf.push(&self.source);
    }

    fn used_values_mut(&mut self) -> Vec<&mut Value> {
        vec![&mut self.source]
    }
}

impl EstimateAsm for Bitcast {
    fn estimated_instructions(&self) -> usize {
        0
    }
}

impl IRDisplay for Bitcast {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .var
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("bitcast"))
            .append(ctx.space())
            .append(self.source.display(ctx))
            .append(ctx.space())
            .append(ctx.text("as"))
            .append(ctx.space())
            .append(self.dest.ty.display(ctx))
            .group()
    }
}
