use super::{utils::IRDisplay, BasicBlockId};
use crate::repr::{utils::DisplayCtx, BasicBlock, Ident};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Function {
    pub name: Option<Ident>,
    pub id: FuncId,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlock>,
}

impl IRDisplay for Function {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        let name = self
            .name
            .map(|name| ctx.text("@").append(name.display(ctx)))
            .unwrap_or_else(|| self.id.display(ctx));

        ctx.text("def")
            .append(ctx.space())
            .append(name)
            .append(ctx.nil().parens())
            .append(ctx.space())
            .append(ctx.text("{"))
            .group()
            .append(ctx.hardline())
            .append(
                self.basic_blocks
                    .iter()
                    .find(|b| b.id == self.entry)
                    .unwrap()
                    .display(ctx)
                    .append(
                        if self
                            .basic_blocks
                            .iter()
                            .filter(|b| b.id != self.entry)
                            .count()
                            == 0
                        {
                            ctx.nil()
                        } else {
                            ctx.hardline()
                        },
                    )
                    .append(
                        ctx.intersperse(
                            self.basic_blocks
                                .iter()
                                .filter(|b| b.id != self.entry)
                                .map(|b| b.display(ctx)),
                            ctx.hardline().append(ctx.hardline()),
                        ),
                    ),
            )
            .nest(4)
            .append(ctx.hardline())
            .append(ctx.text("}"))
            .append(ctx.hardline())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct FuncId(NonZeroU64);

impl FuncId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0.get() - 1
    }
}

impl IRDisplay for FuncId {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.text(format!("@function.{}", self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct FunctionMeta {
    pub name: Option<Ident>,
    pub id: FuncId,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlockId>,
}

impl FunctionMeta {
    pub const fn new(
        name: Option<Ident>,
        id: FuncId,
        entry: BasicBlockId,
        basic_blocks: Vec<BasicBlockId>,
    ) -> Self {
        Self {
            name,
            id,
            entry,
            basic_blocks,
        }
    }
}