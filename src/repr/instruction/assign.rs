use crate::repr::{utils::InstructionExt, Value};
use abomonation_derive::Abomonation;
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Assign {
    pub value: Value,
    pub dest: VarId,
}

impl Assign {
    pub const fn is_const(&self) -> bool {
        self.value.is_const()
    }
}

impl InstructionExt for Assign {
    fn destination(&self) -> VarId {
        self.dest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct VarId(NonZeroU64);

impl VarId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }
}
