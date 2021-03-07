use super::{Constant, Node, NodeExt, NodeId};
use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Start;

impl NodeExt for Start {
    fn node_name(&self) -> &'static str {
        "Start"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct End;

impl NodeExt for End {
    fn node_name(&self) -> &'static str {
        "End"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Merge;

impl NodeExt for Merge {
    fn node_name(&self) -> &'static str {
        "Merge"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Place;

impl NodeExt for Place {
    fn node_name(&self) -> &'static str {
        "Place"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}
