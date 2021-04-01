mod constant;
mod parameter;

pub use constant::Constant;
pub use parameter::Parameter;

use super::{
    node_ext::{Castable, NodeExt},
    Node, NodeId,
};
use abomonation_derive::Abomonation;
use derive_more::From;
use sruth_derive::{Castable, NodeExt};
use std::{
    fmt::{self, Display},
    hint,
};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, NodeExt, Castable, From,
)]
pub enum Value {
    Constant(Constant),
    // TODO: More info on function parameters
    Parameter(Parameter),
    Pointer(Pointer),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Type {
    Uint8,
    Bool,
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uint8 => f.write_str("u8"),
            Self::Bool => f.write_str("bool"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Pointer {}

impl NodeExt for Pointer {
    fn node_name(&self) -> &'static str {
        "Pointer"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    fn inline_cost(&self) -> isize {
        0
    }
}

impl Castable<Pointer> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Value(Value::Pointer(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Pointer {
        if let Self::Value(Value::Pointer(ptr)) = self {
            ptr
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Pointer> for Node {
    fn from(ptr: Pointer) -> Self {
        Self::Value(Value::Pointer(ptr))
    }
}
