use super::{Constant, Node, NodeId};
use std::any::{self, Any, TypeId};

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

    fn isnt<T>(&self) -> bool
    where
        Self: Castable<T>,
    {
        !Castable::is(self)
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

    /// # Safety
    ///
    /// `T` must be the same type as the underlying item
    ///
    unsafe fn cast_unchecked(&self) -> &T;
}
