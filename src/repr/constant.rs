use crate::repr::{
    utils::{DisplayCtx, IRDisplay},
    Type,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Constant {
    Bool(bool),
    Int(i64),
    Uint(u64),
}

impl Constant {
    pub fn ty(&self) -> Type {
        match self {
            Self::Bool(_) => Type::Bool,
            Self::Int(_) => Type::Int,
            Self::Uint(_) => Type::Uint,
        }
    }
}

impl IRDisplay for Constant {
    fn display<'a, D, A, R>(&self, alloc: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        match self {
            Self::Bool(boolean) => alloc.text(format!("{}", boolean)),
            Self::Int(int) => alloc.text(format!("{}", int)),
            Self::Uint(uint) => alloc.text(format!("{}", uint)),
        }
    }
}
