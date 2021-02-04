use crate::repr::{constant::Constant, instruction::VarId};
use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Value {
    Const(Constant),
    Var(VarId),
}

impl Value {
    pub const fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    pub const fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    pub const fn as_const(&self) -> Option<&Constant> {
        if let Self::Const(constant) = self {
            Some(constant)
        } else {
            None
        }
    }

    pub const fn into_const(self) -> Option<Constant> {
        if let Self::Const(constant) = self {
            Some(constant)
        } else {
            None
        }
    }

    pub const fn as_var(&self) -> Option<VarId> {
        if let Self::Var(var) = *self {
            Some(var)
        } else {
            None
        }
    }
}
