use crate::repr::{
    instruction::{Assign, VarId},
    utils::InstructionExt,
    Constant, Instruction, Value,
};
use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Add {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Add {
    pub const fn is_const(&self) -> bool {
        self.rhs.is_const() && self.lhs.is_const()
    }

    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Int(lhs + rhs)),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Uint(lhs + rhs)),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Sub {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Sub {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Int(lhs - rhs)),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Uint(lhs - rhs)),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Mul {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Mul {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Int(lhs * rhs)),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Uint(lhs * rhs)),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Div {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Div {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Int(lhs / rhs)),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::Const(Constant::Uint(lhs / rhs)),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

pub trait BinOp {
    fn lhs(&self) -> Value;

    fn rhs(&self) -> Value;

    fn dest(&self) -> VarId;

    fn from_parts(lhs: Value, rhs: Value, dest: VarId) -> Self;
}

macro_rules! impl_binop {
    ($($type:ident),* $(,)?) => {
        $(
            impl BinOp for $type {
                fn lhs(&self) -> Value {
                    self.lhs.clone()
                }

                fn rhs(&self) -> Value {
                    self.rhs.clone()
                }

                fn dest(&self) -> VarId {
                    self.dest
                }

                fn from_parts(lhs: Value, rhs: Value, dest: VarId) -> Self {
                    Self { lhs, rhs, dest }
                }
            }

            impl InstructionExt for $type {
                fn destination(&self) -> VarId {
                    self.dest
                }
            }
        )*
    };
}

impl_binop! {
    Add,
    Sub,
    Mul,
    Div,
}
