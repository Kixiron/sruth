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
}

impl Instruction {
    pub const fn is_binop(&self) -> bool {
        matches!(
            self,
            Self::Add(_) | Self::Sub(_) | Self::Mul(_) | Self::Div(_)
        )
    }

    pub fn used_vars(&self) -> impl Iterator<Item = VarId> {
        match self {
            Self::Assign(assign) => assign.value.as_var().into_iter().chain(None),
            Self::Add(add) => add.lhs.as_var().into_iter().chain(add.rhs.as_var()),
            Self::Sub(sub) => sub.lhs.as_var().into_iter().chain(sub.rhs.as_var()),
            Self::Mul(mul) => mul.lhs.as_var().into_iter().chain(mul.rhs.as_var()),
            Self::Div(div) => div.lhs.as_var().into_iter().chain(div.rhs.as_var()),
            Self::Bitcast(bitcast) => Some(bitcast.source).into_iter().chain(None),
        }
    }

    pub fn used_values(&self) -> impl Iterator<Item = Value> {
        // TODO: Less clones, less allocation
        match self {
            Self::Assign(assign) => vec![assign.value.clone()],
            Self::Add(add) => vec![add.lhs.clone(), add.rhs.clone()],
            Self::Sub(sub) => vec![sub.lhs.clone(), sub.rhs.clone()],
            Self::Mul(mul) => vec![mul.lhs.clone(), mul.rhs.clone()],
            Self::Div(div) => vec![div.lhs.clone(), div.rhs.clone()],
            Self::Bitcast(_) => Vec::new(),
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
}
