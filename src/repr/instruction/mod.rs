mod assign;
mod binary_ops;

pub use assign::{Assign, VarId};
pub use binary_ops::{Add, BinOp, Div, Mul, Sub};

use crate::repr::utils::{Cast, InstructionExt};
use abomonation_derive::Abomonation;
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Instruction {
    Assign(Assign),
    Add(Add),
    Sub(Sub),
    Mul(Mul),
    Div(Div),
}

impl Instruction {
    pub const fn is_binop(&self) -> bool {
        matches!(
            self,
            Self::Add(_) | Self::Sub(_) | Self::Mul(_) | Self::Div(_)
        )
    }

    pub const fn into_add(self) -> Option<Add> {
        if let Self::Add(add) = self {
            Some(add)
        } else {
            None
        }
    }

    pub const fn into_assign(self) -> Option<Assign> {
        if let Self::Assign(assign) = self {
            Some(assign)
        } else {
            None
        }
    }

    pub fn used_vars(&self) -> impl Iterator<Item = VarId> {
        match self {
            Self::Assign(assign) => assign.value.as_var().into_iter().chain(None),
            Self::Add(add) => add.lhs.as_var().into_iter().chain(add.rhs.as_var()),
            Self::Sub(sub) => sub.lhs.as_var().into_iter().chain(sub.rhs.as_var()),
            Self::Mul(mul) => mul.lhs.as_var().into_iter().chain(mul.rhs.as_var()),
            Self::Div(div) => div.lhs.as_var().into_iter().chain(div.rhs.as_var()),
        }
    }
}

impl InstructionExt for Instruction {
    fn destination(&self) -> VarId {
        match self {
            Self::Assign(assign) => assign.destination(),
            Self::Add(add) => add.destination(),
            Self::Sub(sub) => sub.destination(),
            Self::Mul(mul) => mul.destination(),
            Self::Div(div) => div.destination(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct InstId(NonZeroU64);

impl InstId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
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

            impl Cast<$type> for Instruction {
                fn is(&self) -> bool {
                    matches!(self, Self::$type(_))
                }

                fn cast(self) -> Option<$type> {
                    if let Self::$type(value) = self {
                        Some(value)
                    } else {
                        None
                    }
                }
            }
        )*
    };
}

impl_instruction! {
    Add,
    Sub,
    Mul,
    Div,
    Assign,
}
