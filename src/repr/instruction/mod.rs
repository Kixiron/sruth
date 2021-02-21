mod assign;
mod binary_ops;
mod bitcast;
mod call;
mod cmp;
mod neg;

pub use assign::{Assign, VarId};
pub use binary_ops::{Add, BinaryOp, BinopExt, Div, Mul, Sub};
pub use bitcast::Bitcast;
pub use call::Call;
pub use cmp::Cmp;
pub use neg::Neg;

use crate::repr::{
    utils::{
        DisplayCtx, EstimateAsm, IRDisplay, InstructionExt, InstructionPurity, RawCast, RawRefCast,
    },
    Type, TypedVar, Value,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};
use std::num::NonZeroU64;

// TODO: Make instructions self-describing (Let them carry their own id around somehow)
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
    Call(Call),
}

impl Instruction {
    // TODO: Make this a method on InstructionExt
    pub const fn is_binop(&self) -> bool {
        matches!(
            self,
            Self::Add(_) | Self::Sub(_) | Self::Mul(_) | Self::Div(_)
        )
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

            impl RawRefCast<$type> for Instruction {
                fn cast_raw_ref(&self) -> Option<&$type> {
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

            fn purity(&self) -> InstructionPurity {
                match self {
                    $(Self::$type(value) => value.purity(),)*
                }
            }

            fn replace_uses(&mut self, from: VarId, to: &Value) -> bool {
                match self {
                    $(Self::$type(value) => value.replace_uses(from, to),)*
                }
            }

            fn used_vars(&self) -> Vec<TypedVar> {
                match self {
                    $(Self::$type(value) => value.used_vars(),)*
                }
            }

            fn used_values(&self) -> Vec<&Value> {
                match self {
                    $(Self::$type(value) => value.used_values(),)*
                }
            }

            fn used_values_mut(&mut self) -> Vec<&mut Value> {
                match self {
                    $(Self::$type(value) => value.used_values_mut(),)*
                }
            }
        }

        impl EstimateAsm for Instruction {
            fn estimated_instructions(&self) -> usize {
                match self {
                    $(Self::$type(value) => value.estimated_instructions(),)*
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
    Call,
}
