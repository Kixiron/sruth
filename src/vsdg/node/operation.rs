use super::{
    node_ext::{Castable, NodeExt},
    Constant, Node, NodeId,
};
use abomonation_derive::Abomonation;
use derive_more::From;
use sruth_derive::{Castable, NodeExt};
use std::hint;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, NodeExt, Castable, From,
)]
pub enum Operation {
    Add(Add),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Add {
    pub lhs: NodeId,
    pub rhs: NodeId,
}

impl NodeExt for Add {
    fn node_name(&self) -> &'static str {
        "Add"
    }

    // TODO: Maybe start using error nodes for failures
    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        match constants {
            [] => {
                tracing::warn!("tried to evaluate an `Add` node with no operands");
                (self.into(), Vec::new())
            }

            [_] => {
                tracing::trace!("tried to evaluate an `Add` node with one operand, skipping");
                (self.into(), Vec::new())
            }

            &[(left_id, ref left), (right_id, ref right)]
                if (left_id == self.lhs && right_id == self.rhs)
                    || (left_id == self.rhs && right_id == self.lhs) =>
            {
                let sum = left + right;
                tracing::trace!(
                    "evaluating an `Add` node: {:?} + {:?} = {:?}",
                    left,
                    right,
                    sum,
                );

                // Note: If something with add is buggy, it's because it doesn't actually respect
                //       the declared left hand and right hand side nodes. This *shouldn't* matter,
                //       but if weird shit starts happening it's probably this
                (sum.into(), vec![left_id, right_id])
            }

            [rest @ ..] => {
                tracing::error!(
                    "tried to evaluate an `Add` node with {} operands: {:?}, expected [{:?}, {:?}]",
                    rest.len(),
                    rest,
                    self.lhs,
                    self.rhs,
                );
                debug_assert_eq!(
                    rest.len(),
                    2,
                    "incorrect number of operands passed to an `Add` node",
                );

                (self.into(), Vec::new())
            }
        }
    }
}

impl Castable<Add> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Operation(Operation::Add(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Add {
        if let Self::Operation(Operation::Add(add)) = self {
            add
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Add> for Node {
    fn from(add: Add) -> Self {
        Self::Operation(Operation::Add(add))
    }
}
