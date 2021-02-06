use crate::repr::utils::{DisplayCtx, IRDisplay};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Type {
    Int,
    Uint,
    Bool,
}

impl IRDisplay for Type {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        match self {
            Self::Int => ctx.text("int"),
            Self::Uint => ctx.text("uint"),
            Self::Bool => ctx.text("bool"),
        }
    }
}
