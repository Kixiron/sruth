use super::{
    super::node_ext::{Castable, NodeExt},
    Constant, Node, NodeId, Type, Value,
};
use abomonation_derive::Abomonation;
use std::hint;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Parameter {
    pub ty: Type,
}

impl NodeExt for Parameter {
    fn node_name(&self) -> &'static str {
        "Parameter"
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }

    fn inline_cost(&self) -> isize {
        0
    }
}

impl Castable<Parameter> for Node {
    fn is(&self) -> bool {
        matches!(self, Self::Value(Value::Parameter(_)))
    }

    unsafe fn cast_unchecked(&self) -> &Parameter {
        if let Self::Value(Value::Parameter(param)) = self {
            param
        } else {
            hint::unreachable_unchecked()
        }
    }
}

impl From<Parameter> for Node {
    fn from(param: Parameter) -> Self {
        Self::Value(Value::Parameter(param))
    }
}
