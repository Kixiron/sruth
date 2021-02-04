use super::BasicBlockId;
use crate::repr::{BasicBlock, Ident};
use abomonation_derive::Abomonation;
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Function {
    pub name: Option<Ident>,
    pub id: FuncId,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
#[repr(transparent)]
pub struct FuncId(NonZeroU64);

impl FuncId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn as_u64(self) -> u64 {
        self.0.get() - 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct FunctionMeta {
    pub name: Option<Ident>,
    pub id: FuncId,
    pub entry: BasicBlockId,
    pub basic_blocks: Vec<BasicBlockId>,
}

impl FunctionMeta {
    pub const fn new(
        name: Option<Ident>,
        id: FuncId,
        entry: BasicBlockId,
        basic_blocks: Vec<BasicBlockId>,
    ) -> Self {
        Self {
            name,
            id,
            entry,
            basic_blocks,
        }
    }
}
