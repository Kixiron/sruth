mod control;
mod node_ext;
mod operation;
mod structure;
mod value;

pub use control::{Branch, Control, Return};
pub use node_ext::{Castable, NodeExt};
pub use operation::{Add, Cmp, CmpKind, Operation, Sub};
pub use structure::{End, Merge, Place, Start};
pub use value::{Constant, Parameter, Type, Value};

use abomonation_derive::Abomonation;
use derive_more::From;
use sruth_derive::{Castable, NodeExt};
use std::{
    fmt::{self, Display},
    num::NonZeroU64,
};

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
    Place(Place),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Function {}

macro_rules! create_id {
    ($($id:ident),* $(,)?) => {
        $(
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
            #[repr(transparent)]
            pub struct $id(crate NonZeroU64);

            #[allow(dead_code)]
            impl $id {
                crate const fn new(id: NonZeroU64) -> Self {
                    Self(id)
                }
            }

            impl Display for $id {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    Display::fmt(&self.0, f)
                }
            }
        )*
    };
}

create_id! {
    NodeId,
    FuncId,
}
