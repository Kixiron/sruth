use crate::repr::{
    utils::{DisplayCtx, IRDisplay},
    Ident, InstId, Instruction, Terminator,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct BasicBlock {
    pub name: Option<Ident>,
    pub id: BasicBlockId,
    pub instructions: Vec<Instruction>,
    pub terminator: Terminator,
}

impl IRDisplay for BasicBlock {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        let name = self
            .name
            .map(|name| {
                ctx.text(";")
                    .append(ctx.space())
                    .append(name.display(ctx))
                    .group()
                    .append(ctx.hardline())
            })
            .unwrap_or_else(|| ctx.nil());

        name.append(self.id.display(ctx))
            .append(ctx.text(":"))
            .group()
            .append(ctx.hardline())
            .append(
                ctx.intersperse(
                    self.instructions.iter().map(|inst| inst.display(ctx)),
                    ctx.hardline(),
                )
                .append(if self.instructions.is_empty() {
                    ctx.nil()
                } else {
                    ctx.hardline()
                })
                .append(self.terminator.display(ctx)),
            )
            .nest(4)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct BasicBlockId(NonZeroU64);

impl BasicBlockId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0.get() - 1
    }
}

impl IRDisplay for BasicBlockId {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.text(format!("block.{}", self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct BasicBlockMeta {
    pub name: Option<Ident>,
    pub id: BasicBlockId,
    pub instructions: Vec<InstId>,
    pub terminator: Terminator,
}

impl BasicBlockMeta {
    pub const fn new(
        name: Option<Ident>,
        id: BasicBlockId,
        instructions: Vec<InstId>,
        terminator: Terminator,
    ) -> Self {
        Self {
            name,
            id,
            instructions,
            terminator,
        }
    }
}
