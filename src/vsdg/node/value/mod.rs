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
use std::fmt::{self, Display};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, NodeExt, Castable, From,
)]
pub enum Value {
    Constant(Constant),
    // TODO: More info on function parameters
    Parameter(Parameter),
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
