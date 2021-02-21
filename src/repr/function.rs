use super::{utils::IRDisplay, BasicBlockId, TypedVar};
use crate::{
    optimize::inline::InlineHeuristics,
    repr::{utils::DisplayCtx, BasicBlock, Ident, Type},
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Function {
    pub name: Option<Ident>,
    pub id: FuncId,
    // TODO: Struct param
    pub params: Vec<TypedVar>,
    pub ret_ty: Type,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlock>,
    pub metadata: Metadata,
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
            .map(|name| name.display(ctx))
            .unwrap_or_else(|| self.id.display(ctx));

        ctx.text("; entry:")
            .append(ctx.space())
            .append(self.entry.display(ctx))
            .append(ctx.hardline())
            .append(self.metadata.display(ctx))
            .append(ctx.text("def"))
            .append(ctx.space())
            .append(name)
            .append(
                ctx.intersperse(
                    self.params.iter().map(|var| {
                        var.var
                            .display(ctx)
                            .append(ctx.text(":"))
                            .append(ctx.space())
                            .append(var.ty.display(ctx))
                            .group()
                    }),
                    ctx.text(",").append(ctx.space()),
                )
                .parens(),
            )
            .append(ctx.space())
            .append(if self.ret_ty == Type::Unit {
                ctx.nil()
            } else {
                ctx.text("->")
                    .append(ctx.space())
                    .append(self.ret_ty.display(ctx))
                    .append(ctx.space())
            })
            .append(ctx.text("{"))
            .group()
            .append(
                ctx.hardline()
                    .append(
                        self.basic_blocks
                            .iter()
                            .find(|b| b.id == self.entry)
                            .map_or_else(
                                || ctx.nil(),
                                |entry| {
                                    entry.display(ctx).append(
                                        if self
                                            .basic_blocks
                                            .iter()
                                            .filter(|b| b.id != self.entry)
                                            .count()
                                            == 0
                                        {
                                            ctx.nil()
                                        } else {
                                            ctx.hardline().append(ctx.hardline())
                                        },
                                    )
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
                    .nest(4),
            )
            .append(ctx.hardline())
            .append(ctx.text("}"))
            .append(ctx.hardline())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Default)]
pub struct Metadata {
    pub inline_heuristics: Option<InlineHeuristics>,
}

impl Metadata {
    pub const fn new(inline_heuristics: Option<InlineHeuristics>) -> Self {
        Self { inline_heuristics }
    }
}

impl IRDisplay for Metadata {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        if let Some(heuristics) = self.inline_heuristics.as_ref() {
            ctx.text(";")
                .append(ctx.space())
                .append(ctx.text(format!(
                    "stats: {} branches, {} invocations, {} blocks, {} est. asm, {} instructions, {} function calls, {}, {}",
                    heuristics.branches, heuristics.invocations,
                    heuristics.block_length, heuristics.estimated_asm,
                    heuristics.ssa_inst_length, heuristics.function_calls,
                    if heuristics.is_pure { "pure" } else { "impure" },
                    if heuristics.is_recursive { "recursive" } else { "not recursive" },
                )))
                .group()
                .append(ctx.hardline())
                .append(
                    ctx.text(";")
                        .append(ctx.space())
                        .append(ctx.text(format!("inline cost: {:.2}", heuristics.inline_cost())))
                        .group()
                )
                .append(ctx.hardline())
        } else {
            ctx.nil()
        }
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
        ctx.text(format!("function.{}", self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct FunctionDesc {
    pub name: Option<Ident>,
    pub id: FuncId,
    pub params: Vec<TypedVar>,
    pub ret_ty: Type,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlockId>,
}

impl FunctionDesc {
    pub const fn new(
        name: Option<Ident>,
        id: FuncId,
        params: Vec<TypedVar>,
        ret_ty: Type,
        entry: BasicBlockId,
        basic_blocks: Vec<BasicBlockId>,
    ) -> Self {
        Self {
            name,
            id,
            params,
            ret_ty,
            entry,
            basic_blocks,
        }
    }
}
