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
    // TODO: Make this hold a label & a dedicated `Jump` struct
    Jump(BasicBlockId),
    // TODO: Make a `Return` struct
    Return(Option<Value>),
    Branch(Branch),
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

    pub const fn into_branch(self) -> Option<Branch> {
        if let Self::Branch(branch) = self {
            Some(branch)
        } else {
            None
        }
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        match self {
            Self::Jump(_) => Vec::new(),
            Self::Branch(branch) => branch.used_vars(),
            Self::Return(ret) => {
                if let Some(val) = ret.as_ref().and_then(Value::as_var) {
                    vec![val]
                } else {
                    Vec::new()
                }
            }
            Self::Unreachable => Vec::new(),
        }
    }

    pub fn succ(&self) -> Vec<BasicBlockId> {
        match self {
            &Self::Jump(block) => vec![block],
            Self::Branch(branch) => branch.succ(),
            Self::Return(_) | Self::Unreachable => Vec::new(),
        }
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        if let Self::Return(Some(value)) = self {
            if let Some(var) = value.as_var_mut() {
                if *var == from {
                    *var = to;
                    return true;
                }
            }
        }

        false
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

            Self::Branch(branch) => branch.display(ctx),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Branch {
    pub cond: Value,
    pub if_true: Label,
    pub if_false: Label,
}

impl Branch {
    pub const fn new(cond: Value, if_true: Label, if_false: Label) -> Self {
        Self {
            cond,
            if_true,
            if_false,
        }
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        // TODO: Make this a method on value
        if let Some(val) = self.cond.as_var() {
            vec![val]
        } else {
            Vec::new()
        }
    }

    pub fn succ(&self) -> Vec<BasicBlockId> {
        vec![self.if_true.block, self.if_false.block]
    }
}

impl IRDisplay for Branch {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.text("branch")
            .append(self.cond.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.if_true.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.if_false.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Label {
    pub block: BasicBlockId,
}

impl Label {
    pub const fn new(block: BasicBlockId) -> Self {
        Self { block }
    }
}

impl IRDisplay for Label {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.block.display(ctx)
    }
}
