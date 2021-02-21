use std::hint;

use super::{
    node_ext::{Castable, NodeExt},
    Constant, Node, NodeId,
};
use abomonation_derive::Abomonation;
use derive_more::From;
use sruth_derive::{Castable, NodeExt};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, NodeExt, Castable, From,
)]
pub enum Control {
    Return(Return),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Return {}

impl NodeExt for Return {
    fn node_name(&self) -> &'static str {
        "Return"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

impl Castable<Return> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Control(Control::Return(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Return {
        if let Self::Control(Control::Return(ret)) = self {
            ret
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Return> for Node {
    fn from(ret: Return) -> Self {
        Self::Control(Control::Return(ret))
    }
}
