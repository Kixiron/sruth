use crate::repr::{
    instruction::VarId,
    utils::{DisplayCtx, IRDisplay},
    value::Value,
    BasicBlockId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Terminator {
    Jump(BasicBlockId),
    Return(Option<Value>),
    Unreachable,
}

impl Terminator {
    // TODO: Make a `Return` struct
    pub const fn into_return(self) -> Option<Option<Value>> {
        if let Self::Return(ret) = self {
            Some(ret)
        } else {
            None
        }
    }

    pub fn used_vars(&self) -> impl Iterator<Item = VarId> {
        match self {
            Self::Jump(_) => None.into_iter(),
            Self::Return(ret) => ret.as_ref().and_then(Value::as_var).into_iter(),
            Self::Unreachable => None.into_iter(),
        }
    }

    pub fn succ(&self) -> impl Iterator<Item = BasicBlockId> {
        match *self {
            Self::Jump(block) => Some(block).into_iter(),
            Self::Return(_) | Self::Unreachable => None.into_iter(),
        }
    }
}

impl IRDisplay for Terminator {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        match self {
            Self::Jump(addr) => ctx
                .text("jump")
                .append(ctx.space())
                .append(addr.display(ctx))
                .group(),

            Self::Return(ret) => ctx
                .text("ret")
                .append(
                    ret.as_ref()
                        .map(|ret| ctx.space().append(ret.display(ctx)).append(ctx.space()))
                        .unwrap_or_else(|| ctx.nil()),
                )
                .group(),

            Self::Unreachable => ctx.text("unreachable"),
        }
    }
}
