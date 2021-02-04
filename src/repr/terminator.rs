use crate::repr::{instruction::VarId, value::Value, BasicBlockId};
use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Terminator {
    Jump(BasicBlockId),
    Return(Option<Value>),
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

    pub fn used_vars(&self) -> impl Iterator<Item = VarId> {
        match self {
            Self::Jump(_) => None.into_iter(),
            Self::Return(ret) => ret.as_ref().and_then(Value::as_var).into_iter(),
            Self::Unreachable => None.into_iter(),
        }
    }

    pub fn succ(&self) -> impl Iterator<Item = BasicBlockId> {
        match *self {
            Self::Jump(block) => Some(block).into_iter(),
            Self::Return(_) | Self::Unreachable => None.into_iter(),
        }
    }
}
