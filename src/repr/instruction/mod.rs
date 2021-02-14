mod assign;
mod binary_ops;
mod bitcast;
mod call;
mod cmp;
mod neg;

pub use assign::{Assign, TypedVar, VarId};
pub use binary_ops::{Add, BinaryOp, BinopExt, Div, Mul, Sub};
pub use bitcast::Bitcast;
pub use call::Call;
pub use cmp::Cmp;
pub use neg::Neg;

use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt, RawCast},
    Type, Value,
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

    // TODO: Make this a method on InstructionExt
    pub fn used_vars(&self) -> Vec<VarId> {
        match self {
            Self::Assign(assign) => {
                if let Some(val) = assign.value.as_var() {
                    vec![val]
                } else {
                    Vec::new()
                }
            }

            Self::Add(add) => add
                .lhs
                .as_var()
                .into_iter()
                .chain(add.rhs.as_var())
                .collect(),

            Self::Sub(sub) => sub
                .lhs
                .as_var()
                .into_iter()
                .chain(sub.rhs.as_var())
                .collect(),

            Self::Mul(mul) => mul
                .lhs
                .as_var()
                .into_iter()
                .chain(mul.rhs.as_var())
                .collect(),

            Self::Div(div) => div
                .lhs
                .as_var()
                .into_iter()
                .chain(div.rhs.as_var())
                .collect(),

            Self::Bitcast(bitcast) => vec![bitcast.source],

            Self::Neg(neg) => {
                if let Some(val) = neg.value.as_var() {
                    vec![val]
                } else {
                    Vec::new()
                }
            }

            Self::Cmp(cmp) => cmp
                .lhs
                .as_var()
                .into_iter()
                .chain(cmp.rhs.as_var())
                .collect(),

            Self::Call(call) => call.used_vars(),
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
            Self::Call(call) => call.used_values(),
        }
        .into_iter()
    }

    pub fn used_values_mut(&mut self) -> impl Iterator<Item = &mut Value> {
        // TODO: Less clones, less allocation
        match self {
            Self::Assign(assign) => vec![&mut assign.value],
            Self::Add(add) => vec![&mut add.lhs, &mut add.rhs],
            Self::Sub(sub) => vec![&mut sub.lhs, &mut sub.rhs],
            Self::Mul(mul) => vec![&mut mul.lhs, &mut mul.rhs],
            Self::Div(div) => vec![&mut div.lhs, &mut div.rhs],
            Self::Bitcast(_) => Vec::new(),
            Self::Neg(neg) => vec![&mut neg.value],
            Self::Cmp(cmp) => vec![&mut cmp.lhs, &mut cmp.rhs],
            Self::Call(call) => call.used_values_mut(),
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
    Call,
}
