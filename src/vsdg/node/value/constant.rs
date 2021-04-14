use super::{
    super::node_ext::{Castable, NodeExt},
    Node, NodeId, Value,
};
use abomonation_derive::Abomonation;
use std::{
    hint,
    ops::{Add, Mul, Sub},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Constant {
    Uint8(u8),
    Bool(bool),
    Array(Vec<Constant>),
}

impl Constant {
    pub const fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(b) = *self {
            Some(b)
        } else {
            None
        }
    }

    pub const fn is_zero(&self) -> bool {
        matches!(self, Self::Uint8(0))
    }

    /// Returns `true` if the constant is [`Uint8`]
    pub const fn is_uint8(&self) -> bool {
        matches!(self, Self::Uint8(..))
    }

    pub const fn as_uint8(&self) -> Option<u8> {
        if let Self::Uint8(int) = *self {
            Some(int)
        } else {
            None
        }
    }

    /// Returns `true` if the constant is [`Bool`]
    pub const fn is_bool(&self) -> bool {
        matches!(self, Self::Bool(..))
    }

    /// Returns `true` if the constant is [`Array`]
    pub const fn is_array(&self) -> bool {
        matches!(self, Self::Array(..))
    }

    pub const fn as_array(&self) -> Option<&Vec<Constant>> {
        if let Self::Array(array) = self {
            Some(array)
        } else {
            None
        }
    }
}

impl NodeExt for Constant {
    fn node_name(&self) -> &'static str {
        "Constant"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    fn inline_cost(&self) -> isize {
        0
    }
}

impl Mul<Constant> for Constant {
    // TODO: Should be result
    type Output = Constant;

    fn mul(self, rhs: Constant) -> Self::Output {
        &self * &rhs
    }
}

impl Mul<&Constant> for &Constant {
    // TODO: Should be result
    type Output = Constant;

    fn mul(self, rhs: &Constant) -> Self::Output {
        match (self, rhs) {
            (&Constant::Uint8(left), &Constant::Uint8(right)) => {
                Constant::Uint8(left.wrapping_mul(right))
            }
            _ => panic!(),
        }
    }
}

impl Add<Constant> for Constant {
    // TODO: Should be result
    type Output = Constant;

    fn add(self, rhs: Constant) -> Self::Output {
        &self + &rhs
    }
}

impl Add<&Constant> for &Constant {
    // TODO: Should be result
    type Output = Constant;

    fn add(self, rhs: &Constant) -> Self::Output {
        match (self, rhs) {
            (&Constant::Uint8(left), &Constant::Uint8(right)) => {
                Constant::Uint8(left.wrapping_add(right))
            }
            _ => panic!(),
        }
    }
}

impl Sub<Constant> for Constant {
    // TODO: Should be result
    type Output = Constant;

    fn sub(self, rhs: Constant) -> Self::Output {
        &self - &rhs
    }
}

impl Sub<&Constant> for &Constant {
    // TODO: Should be result
    type Output = Constant;

    fn sub(self, rhs: &Constant) -> Self::Output {
        match (self, rhs) {
            (&Constant::Uint8(left), &Constant::Uint8(right)) => {
                Constant::Uint8(left.wrapping_sub(right))
            }
            _ => panic!(),
        }
    }
}

impl Castable<Constant> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Value(Value::Constant(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Constant {
        if let Self::Value(Value::Constant(constant)) = self {
            constant
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Constant> for Node {
    fn from(constant: Constant) -> Self {
        Self::Value(Value::Constant(constant))
    }
}
