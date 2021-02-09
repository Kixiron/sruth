mod assign;
mod binary_ops;
mod bitcast;

pub use assign::{Assign, VarId};
pub use binary_ops::{Add, BinaryOp, BinopExt, Div, Mul, Sub};
pub use bitcast::Bitcast;

use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt, RawCast},
    Type, Value,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Instruction {
    Assign(Assign),
    Add(Add),
    Sub(Sub),
    Mul(Mul),
    Div(Div),
    Bitcast(Bitcast),
    Neg(Neg),
    Cmp(Cmp),
}

impl Instruction {
    // TODO: Make this a method on InstructionExt
    pub const fn is_binop(&self) -> bool {
        matches!(
            self,
            Self::Add(_) | Self::Sub(_) | Self::Mul(_) | Self::Div(_)
        )
    }

    // TODO: Make this a method on InstructionExt
    pub fn used_vars(&self) -> impl Iterator<Item = VarId> {
        match self {
            Self::Assign(assign) => assign.value.as_var().into_iter().chain(None),
            Self::Add(add) => add.lhs.as_var().into_iter().chain(add.rhs.as_var()),
            Self::Sub(sub) => sub.lhs.as_var().into_iter().chain(sub.rhs.as_var()),
            Self::Mul(mul) => mul.lhs.as_var().into_iter().chain(mul.rhs.as_var()),
            Self::Div(div) => div.lhs.as_var().into_iter().chain(div.rhs.as_var()),
            Self::Bitcast(bitcast) => Some(bitcast.source).into_iter().chain(None),
            Self::Neg(neg) => neg.value.as_var().into_iter().chain(None),
            Self::Cmp(cmp) => cmp.lhs.as_var().into_iter().chain(cmp.rhs.as_var()),
        }
    }

    // TODO: Make this a method on InstructionExt
    pub fn used_values(&self) -> impl Iterator<Item = Value> {
        // TODO: Less clones, less allocation
        match self {
            Self::Assign(assign) => vec![assign.value.clone()],
            Self::Add(add) => vec![add.lhs.clone(), add.rhs.clone()],
            Self::Sub(sub) => vec![sub.lhs.clone(), sub.rhs.clone()],
            Self::Mul(mul) => vec![mul.lhs.clone(), mul.rhs.clone()],
            Self::Div(div) => vec![div.lhs.clone(), div.rhs.clone()],
            Self::Bitcast(_) => Vec::new(),
            Self::Neg(neg) => vec![neg.value.clone()],
            Self::Cmp(cmp) => vec![cmp.lhs.clone(), cmp.rhs.clone()],
        }
        .into_iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct InstId(NonZeroU64);

impl InstId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0.get() - 1
    }
}

macro_rules! impl_instruction {
    ($($type:ident),* $(,)?) => {
        $(
            impl From<$type> for Instruction {
                fn from(inst: $type) -> Self {
                    Self::$type(inst)
                }
            }

            impl RawCast<$type> for Instruction {
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

        impl Instruction {
            // TODO: Make this a method on InstructionExt
            pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
                match self {
                    $(Self::$type(value) => value.replace_uses(from, to),)*
                }
            }
        }

        impl InstructionExt for Instruction {
            fn dest(&self) -> VarId {
                match self {
                    $(Self::$type(value) => value.dest(),)*
                }
            }

            fn dest_type(&self) -> Type {
                match self {
                    $(Self::$type(value) => value.dest_type(),)*
                }
            }
        }

        impl IRDisplay for Instruction {
            fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
            where
                D: DocAllocator<'a, A>,
                D::Doc: Clone,
                A: Clone + 'a,
                R: Resolver,
            {
                match self {
                    $(Self::$type(value) => value.display(ctx),)*
                }
            }
        }
    };
}

impl_instruction! {
    Add,
    Sub,
    Mul,
    Div,
    Assign,
    Bitcast,
    Neg,
    Cmp,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Neg {
    pub value: Value,
    pub dest: VarId,
}

impl Neg {
    pub const fn new(dest: VarId, value: Value) -> Self {
        Self { value, dest }
    }

    // TODO: Make this a method on InstructionExt
    pub const fn is_const(&self) -> bool {
        self.value.is_const()
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        if let Some(var) = self.value.as_var_mut() {
            if *var == from {
                *var = to;
                return true;
            }
        }

        false
    }
}

impl InstructionExt for Neg {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        self.value.ty.clone()
    }
}

impl IRDisplay for Neg {
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
            .append(ctx.text("neg"))
            .append(ctx.space())
            .append(self.value.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Cmp {
    pub lhs: Value,
    pub rhs: Value,
    pub dest: VarId,
}

impl Cmp {
    pub const fn new(lhs: Value, rhs: Value, dest: VarId) -> Self {
        Self { lhs, rhs, dest }
    }

    pub const fn is_const(&self) -> bool {
        self.rhs.is_const() && self.lhs.is_const()
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        let mut replaced = false;

        if let Some(var) = self.lhs.as_var_mut() {
            if *var == from {
                *var = to;
                replaced = true
            }
        }

        if let Some(var) = self.rhs.as_var_mut() {
            if *var == from {
                *var = to;
                replaced = true
            }
        }

        replaced
    }
}

impl InstructionExt for Cmp {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        Type::Bool
    }
}

impl IRDisplay for Cmp {
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
            .append(ctx.text("cmp"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}
