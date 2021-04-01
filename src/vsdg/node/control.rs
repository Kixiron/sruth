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
    Branch(Branch),
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

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        1
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Branch {}

impl NodeExt for Branch {
    fn node_name(&self) -> &'static str {
        "Branch"
    }

    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        match constants {
            [] => {
                tracing::warn!("tried to evaluate a `Branch` node with no condition");
                (self.into(), Vec::new())
            }

            &[(_, ref cond)] => {
                tracing::trace!("evaluating a `Branch` node with condition {:?}", cond);

                // TODO: Return error
                let _cond = cond.as_bool().expect("branch without a boolean");

                // TODO: More complex api
                (self.into(), vec![])
            }

            [rest @ ..] => {
                tracing::error!(
                    "tried to evaluate a `Branch` node with {} conditions: {:?}, expected one bool constant",
                    rest.len(),
                    rest,
                );
                debug_assert_eq!(
                    rest.len(),
                    2,
                    "incorrect number of operands passed to a `Branch` node",
                );

                (self.into(), Vec::new())
            }
        }
    }

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        1
    }
}

impl Castable<Branch> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Control(Control::Branch(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Branch {
        if let Self::Control(Control::Branch(br)) = self {
            br
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Branch> for Node {
    fn from(br: Branch) -> Self {
        Self::Control(Control::Branch(br))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Error {}

impl NodeExt for Error {
    fn node_name(&self) -> &'static str {
        "Error"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    fn inline_cost(&self) -> isize {
        0
    }
}
