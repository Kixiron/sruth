use crate::repr::{Ident, InstId, Instruction, Terminator};
use abomonation_derive::Abomonation;
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct BasicBlock {
    pub name: Option<Ident>,
    pub id: BasicBlockId,
    pub instructions: Vec<Instruction>,
    pub terminator: Terminator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct BasicBlockId(NonZeroU64);

impl BasicBlockId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct BasicBlockMeta {
    pub name: Option<Ident>,
    pub id: BasicBlockId,
    pub instructions: Vec<InstId>,
    pub terminator: Terminator,
}

impl BasicBlockMeta {
    pub const fn new(
        name: Option<Ident>,
        id: BasicBlockId,
        instructions: Vec<InstId>,
        terminator: Terminator,
    ) -> Self {
        Self {
            name,
            id,
            instructions,
            terminator,
        }
    }
}
