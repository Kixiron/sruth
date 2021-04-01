use super::{
    node_ext::{Castable, NodeExt},
    Constant, Node, NodeId,
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
pub enum Operation {
    Add(Add),
    Sub(Sub),
    Cmp(Cmp),
    Load(Load),
    Store(Store),
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
            [] | [_] => (self.into(), Vec::new()),

            &[(left_id, ref left), (right_id, ref right)] =>
                // FIXME: Update rhs and lhs when folding
                // if (left_id == self.lhs && right_id == self.rhs)
                //     || (left_id == self.rhs && right_id == self.lhs) =>
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

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        1
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Sub {
    pub lhs: NodeId,
    pub rhs: NodeId,
}

impl NodeExt for Sub {
    fn node_name(&self) -> &'static str {
        "Sub"
    }

    // TODO: Maybe start using error nodes for failures
    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        match constants {
            [] | [_] => (self.into(), Vec::new()),

            &[(left_id, ref left), (right_id, ref right)]
                if (left_id == self.lhs && right_id == self.rhs)
                    || (left_id == self.rhs && right_id == self.lhs) =>
            {
                let (left, right) = if left_id == self.lhs && right_id == self.rhs {
                    (left, right)
                } else {
                    (right, left)
                };
                let sum = left - right;

                tracing::trace!(
                    "evaluating a `Sub` node: {:?} + {:?} = {:?}",
                    left,
                    right,
                    sum,
                );

                (sum.into(), vec![left_id, right_id])
            }

            [rest @ ..] => {
                tracing::error!(
                    "tried to evaluate a `Sub` node with {} operands: {:?}, expected [{:?}, {:?}]",
                    rest.len(),
                    rest,
                    self.lhs,
                    self.rhs,
                );
                debug_assert_eq!(
                    rest.len(),
                    2,
                    "incorrect number of operands passed to a `Sub` node",
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Cmp {
    pub lhs: NodeId,
    pub rhs: NodeId,
    pub kind: CmpKind,
}

impl NodeExt for Cmp {
    fn node_name(&self) -> &'static str {
        "Cmp"
    }

    // TODO: Maybe start using error nodes for failures
    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        match constants {
            [] | [_] => (self.into(), Vec::new()),

            &[(left_id, ref left), (right_id, ref right)]
                if (left_id == self.lhs && right_id == self.rhs)
                    || (left_id == self.rhs && right_id == self.lhs) =>
            {
                let (left, right) = if left_id == self.lhs && right_id == self.rhs {
                    (left, right)
                } else if left_id == self.rhs && right_id == self.lhs {
                    (right, left)
                } else {
                    panic!("invalid node ids")
                };

                // TODO: Make sure ord/eq are implemented properly
                let result = Constant::Bool(match self.kind {
                    CmpKind::Eq => left == right,
                    CmpKind::NotEq => left != right,
                    CmpKind::Less => left < right,
                    CmpKind::Greater => left > right,
                    CmpKind::LessEq => left <= right,
                    CmpKind::GreaterEq => left >= right,
                });

                tracing::trace!(
                    "evaluating a `Cmp` node: {:?} {} {:?} = {:?}",
                    left,
                    self.kind,
                    right,
                    result,
                );

                (result.into(), vec![left_id, right_id])
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

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        2
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum CmpKind {
    Eq,
    NotEq,
    Less,
    Greater,
    LessEq,
    GreaterEq,
}

impl Display for CmpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => f.write_str("=="),
            Self::NotEq => f.write_str("!="),
            Self::Less => f.write_str("<"),
            Self::Greater => f.write_str(">"),
            Self::LessEq => f.write_str("<="),
            Self::GreaterEq => f.write_str(">="),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Load {}

impl NodeExt for Load {
    fn node_name(&self) -> &'static str {
        "Load"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        2
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Store {}

impl NodeExt for Store {
    fn node_name(&self) -> &'static str {
        "Store"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    // TODO: Take into account the const-ness of inputs and the possibility of
    //       const promotion
    fn inline_cost(&self) -> isize {
        2
    }
}

macro_rules! util_traits {
    ($($type:ident),* $(,)?) => {
        $(
            impl Castable<$type> for Node {
                fn is(&self) -> bool {
                    matches!(self, Self::Operation(Operation::$type(_)))
                }

                unsafe fn cast_unchecked(&self) -> &$type {
                    if let Self::Operation(Operation::$type(value)) = self {
                        value
                    } else {
                        hint::unreachable_unchecked()
                    }
                }
            }

            impl From<$type> for Node {
                fn from(value: $type) -> Self {
                    Self::Operation(Operation::$type(value))
                }
            }
        )*
    };
}

util_traits! {
    Add,
    Sub,
    Cmp,
    Load,
    Store,
}
