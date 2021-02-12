use crate::repr::{
    instruction::VarId,
    utils::{DisplayCtx, IRDisplay, RawCast},
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
    Return(Return),
    Branch(Branch),
    Unreachable,
}

impl Terminator {
    // TODO: Make a `Return` struct
    pub const fn into_return(self) -> Option<Return> {
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

    pub const fn into_jump(self) -> Option<BasicBlockId> {
        if let Self::Jump(jump) = self {
            Some(jump)
        } else {
            None
        }
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        match self {
            Self::Jump(_) => Vec::new(),
            Self::Branch(branch) => branch.used_vars(),
            Self::Return(ret) => ret.used_vars(),
            Self::Unreachable => Vec::new(),
        }
    }

    pub fn jump_targets(&self) -> Vec<BasicBlockId> {
        match self {
            &Self::Jump(block) => vec![block],
            Self::Branch(branch) => branch.jump_targets(),
            Self::Return(_) | Self::Unreachable => Vec::new(),
        }
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        match self {
            Self::Return(ret) => ret.replace_uses(from, to),
            Self::Branch(branch) => branch.replace_uses(from, to),
            Self::Jump(_) | Self::Unreachable => false,
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

            Self::Return(ret) => ret.display(ctx),

            Self::Unreachable => ctx.text("unreachable"),

            Self::Branch(branch) => branch.display(ctx),
        }
    }
}

macro_rules! impl_terminator {
    ($($type:ident),* $(,)?) => {
        $(
            impl From<$type> for Terminator {
                fn from(term: $type) -> Self {
                    Self::$type(term)
                }
            }

            impl RawCast<$type> for Terminator {
                fn is_raw(&self) -> bool {
                    matches!(self, Self::$type(_))
                }

                fn cast_raw(self) -> Option<$type> {
                    if let Self::$type(value) = self {
                        Some(value)
                    } else {
                        None
                    }
                }
            }
        )*

        // TODO
        // impl Terminator {
        //     pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        //         match self {
        //             $(Self::$type(value) => value.replace_uses(from, to),)*
        //         }
        //     }
        // }

        // TODO
        // impl IRDisplay for Terminator {
        //     fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
        //     where
        //         D: DocAllocator<'a, A>,
        //         D::Doc: Clone,
        //         A: Clone + 'a,
        //         R: Resolver,
        //     {
        //         match self {
        //             $(Self::$type(value) => value.display(ctx),)*
        //         }
        //     }
        // }
    };
}

impl_terminator! {
    Branch,
    Return,
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

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        if let Some(var) = self.cond.as_var_mut() {
            if *var == from {
                *var = to;
                return true;
            }
        }

        false
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        // TODO: Make this a method on value
        if let Some(val) = self.cond.as_var() {
            vec![val]
        } else {
            Vec::new()
        }
    }

    pub fn jump_targets(&self) -> Vec<BasicBlockId> {
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
            .append(ctx.space())
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Return {
    pub value: Option<Value>,
}

impl Return {
    pub fn returned_var(&self) -> Option<VarId> {
        self.value.as_ref().and_then(Value::as_var)
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        if let Some(var) = self.value.as_mut().and_then(Value::as_var_mut) {
            if *var == from {
                *var = to;
                return true;
            }
        }

        false
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        if let Some(val) = self.value.as_ref().and_then(Value::as_var) {
            vec![val]
        } else {
            Vec::new()
        }
    }
}

impl IRDisplay for Return {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        ctx.text("return")
            .append(
                self.value
                    .as_ref()
                    .map(|ret| ctx.space().append(ret.display(ctx)).append(ctx.space()))
                    .unwrap_or_else(|| ctx.nil()),
            )
            .group()
    }
}
