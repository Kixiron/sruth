mod control;
mod node_ext;
mod operation;
mod structure;
mod value;

pub use control::{Control, Return};
pub use node_ext::{Castable, NodeExt};
pub use operation::{Add, Operation};
pub use structure::{End, Merge, Start};
pub use value::{Constant, Parameter, Type, Value};

use abomonation_derive::Abomonation;
use derive_more::From;
use sruth_derive::{Castable, NodeExt};
use std::num::NonZeroU64;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, NodeExt, Castable, From,
)]
pub enum Node {
    Value(Value),
    Control(Control),
    Operation(Operation),
    Start(Start),
    End(End),
    Merge(Merge),
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
