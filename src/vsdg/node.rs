use abomonation_derive::Abomonation;
use std::{
    any::{self, Any, TypeId},
    fmt::{self, Display},
    hint,
    num::NonZeroU64,
    ops::Add,
};

pub trait NodeExt: Any {
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn type_name(&self) -> &'static str {
        any::type_name::<Self>()
    }

    fn node_name(&self) -> &'static str;

    fn is<T>(&self) -> bool
    where
        Self: Castable<T>,
    {
        Castable::is(self)
    }

    fn cast<T>(&self) -> Option<&T>
    where
        Self: Castable<T>,
    {
        if Castable::is(self) {
            // Safety: `self` is an instance of `T`
            Some(unsafe { self.cast_unchecked() })
        } else {
            None
        }
    }

    // TODO: Should probably return a result
    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>);
}

pub trait Castable<T> {
    fn is(&self) -> bool;

    unsafe fn cast_unchecked(&self) -> &T;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Node {
    Value(Value),
    Control(Control),
    Operation(Operation),
    Start(Start),
    End(End),
    Merge(Merge),
}

macro_rules! impl_node {
    ($($type:ident),* $(,)?) => {
        impl NodeExt for Node {
            fn node_name(&self) -> &'static str {
                match self {
                    $(Self::$type(inner) => inner.node_name(),)*
                }
            }

            fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId> ){
                match self {
                    $(Self::$type(inner) => inner.evaluate_with_constants(constants),)*
                }
            }
        }

        $(
            impl Castable<$type> for Node {
                fn is(&self) -> bool {
                    matches!(self, Self::$type(_))
                }

                unsafe fn cast_unchecked(&self) -> &$type {
                    if let Self::$type(casted) = self {
                        casted
                    } else {
                        hint::unreachable_unchecked()
                    }
                }
            }

            impl From<$type> for Node {
                fn from(value: $type) -> Self {
                    Self::$type(value)
                }
            }
        )*
    };
}

impl_node! {
    Value,
    Control,
    Operation,
    Start,
    End,
    Merge,
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
pub enum Control {
    Return,
}

impl NodeExt for Control {
    fn node_name(&self) -> &'static str {
        match self {
            Self::Return => "Return",
        }
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Value {
    Constant(Constant),
    // TODO: More info on function parameters
    Parameter(Type),
}

impl NodeExt for Value {
    fn node_name(&self) -> &'static str {
        match self {
            Self::Constant(_) => "Constant",
            Self::Parameter(_) => "Parameter",
        }
    }

    fn evaluate_with_constants(self, _constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        (self.into(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Type {
    Uint8,
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uint8 => f.write_str("u8"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Constant {
    Uint8(u8),
}

impl Add<Constant> for Constant {
    // TODO: Should be result
    type Output = Constant;

    fn add(self, rhs: Constant) -> Self::Output {
        match (self, rhs) {
            (Self::Uint8(left), Self::Uint8(right)) => Self::Uint8(left + right),
        }
    }
}

impl Add<&Constant> for &Constant {
    // TODO: Should be result
    type Output = Constant;

    fn add(self, rhs: &Constant) -> Self::Output {
        match (self, rhs) {
            (&Constant::Uint8(left), &Constant::Uint8(right)) => Constant::Uint8(left + right),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Operation {
    // TODO: Param ordering
    Add,
}

impl NodeExt for Operation {
    fn node_name(&self) -> &'static str {
        match self {
            Self::Add => "Add",
        }
    }

    fn evaluate_with_constants(self, constants: &[(NodeId, Constant)]) -> (Node, Vec<NodeId>) {
        match self {
            Self::Add => match constants {
                [] => {
                    tracing::warn!("tried to evaluate an `Add` node with no operands");
                    (self.into(), Vec::new())
                }

                [_] => {
                    tracing::trace!("tried to evaluate an `Add` node with one operand, skipping");
                    (self.into(), Vec::new())
                }

                [(left_id, left), (right_id, right)] => {
                    tracing::trace!("evaluating an `Add` node: {:?} + {:?}", left, right);

                    ((left + right).into(), vec![*left_id, *right_id])
                }

                [rest @ ..] => {
                    tracing::error!(
                        "tried to evaluate an `Add` node with {} operands: {:?}",
                        rest.len(),
                        rest,
                    );
                    debug_assert_eq!(
                        rest.len(),
                        2,
                        "incorrect number of operands passed to an `Add` node",
                    );

                    (self.into(), Vec::new())
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Function {}

macro_rules! create_id {
    ($($id:ident),* $(,)?) => {
        $(
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
            #[repr(transparent)]
            pub struct $id(pub NonZeroU64);
        )*
    };
}

create_id! {
    NodeId,
    FuncId,
}
