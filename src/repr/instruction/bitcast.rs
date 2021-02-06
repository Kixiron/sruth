use crate::repr::{
    utils::{DisplayCtx, IRDisplay},
    InstructionExt, Type, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Bitcast {
    pub dest: VarId,
    pub dest_ty: Type,
    pub source: VarId,
    pub source_ty: Type,
}

impl Bitcast {
    pub const fn dest_ty(&self) -> &Type {
        &self.dest_ty
    }

    pub const fn source_ty(&self) -> &Type {
        &self.source_ty
    }

    pub fn types(self) -> (Type, Type) {
        (self.source_ty, self.dest_ty)
    }

    pub fn is_valid(&self) -> bool {
        match (&self.dest_ty, &self.source_ty) {
            (Type::Int, Type::Int)
            | (Type::Int, Type::Uint)
            | (Type::Uint, Type::Int)
            | (Type::Uint, Type::Uint)
            | (Type::Int, Type::Bool)
            | (Type::Uint, Type::Bool)
            | (Type::Bool, Type::Int)
            | (Type::Bool, Type::Uint)
            | (Type::Bool, Type::Bool) => true,
        }
    }

    pub fn is_redundant(&self) -> bool {
        self.dest_ty == self.source_ty
    }
}

impl InstructionExt for Bitcast {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        self.dest_ty.clone()
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
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("bitcast"))
            .append(ctx.space())
            .append(self.source.display(ctx))
            .append(ctx.space())
            .append(self.source_ty.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.dest_ty.display(ctx))
            .group()
    }
}
