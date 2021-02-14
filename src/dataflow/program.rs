use crate::repr::{
    basic_block::BasicBlockMeta, function::FunctionMeta, BasicBlockId, FuncId, InstId, Instruction,
    Terminator,
};
use differential_dataflow::{difference::Semigroup, Collection};
use timely::{
    dataflow::{scopes::Child, Scope},
    progress::{timestamp::Refines, Timestamp},
};

pub struct Program<S, R>
where
    S: Scope,
    R: Semigroup,
{
    pub instructions: Collection<S, (InstId, Instruction), R>,
    pub block_instructions: Collection<S, (InstId, BasicBlockId), R>,
    pub block_terminators: Collection<S, (BasicBlockId, Terminator), R>,
    pub block_descriptors: Collection<S, (BasicBlockId, BasicBlockMeta), R>,
    pub function_blocks: Collection<S, (BasicBlockId, FuncId), R>,
    pub function_descriptors: Collection<S, (FuncId, FunctionMeta), R>,
}

impl<S, R> Program<S, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn new(
        instructions: Collection<S, (InstId, Instruction), R>,
        block_instructions: Collection<S, (InstId, BasicBlockId), R>,
        block_terminators: Collection<S, (BasicBlockId, Terminator), R>,
        block_descriptors: Collection<S, (BasicBlockId, BasicBlockMeta), R>,
        function_blocks: Collection<S, (BasicBlockId, FuncId), R>,
        function_descriptors: Collection<S, (FuncId, FunctionMeta), R>,
    ) -> Self {
        Self {
            instructions,
            block_instructions,
            block_terminators,
            block_descriptors,
            function_blocks,
            function_descriptors,
        }
    }

    pub fn enter<'a, T>(&self, scope: &Child<'a, S, T>) -> Program<Child<'a, S, T>, R>
    where
        T: Timestamp + Refines<S::Timestamp>,
    {
        Program {
            instructions: self.instructions.enter(scope),
            block_instructions: self.block_instructions.enter(scope),
            block_terminators: self.block_terminators.enter(scope),
            block_descriptors: self.block_descriptors.enter(scope),
            function_blocks: self.function_blocks.enter(scope),
            function_descriptors: self.function_descriptors.enter(scope),
        }
    }

    pub fn enter_region<'a>(
        &self,
        scope: &Child<'a, S, S::Timestamp>,
    ) -> Program<Child<'a, S, S::Timestamp>, R> {
        Program {
            instructions: self.instructions.enter_region(scope),
            block_instructions: self.block_instructions.enter_region(scope),
            block_terminators: self.block_terminators.enter_region(scope),
            block_descriptors: self.block_descriptors.enter_region(scope),
            function_blocks: self.function_blocks.enter_region(scope),
            function_descriptors: self.function_descriptors.enter_region(scope),
        }
    }
}

impl<'a, S, T, R> Program<Child<'a, S, T>, R>
where
    S: Scope,
    R: Semigroup,
    T: Timestamp + Refines<S::Timestamp>,
{
    pub fn leave(&self) -> Program<S, R> {
        Program {
            instructions: self.instructions.leave(),
            block_instructions: self.block_instructions.leave(),
            block_terminators: self.block_terminators.leave(),
            block_descriptors: self.block_descriptors.leave(),
            function_blocks: self.function_blocks.leave(),
            function_descriptors: self.function_descriptors.leave(),
        }
    }
}

impl<'a, S, R> Program<Child<'a, S, S::Timestamp>, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn leave_region(&self) -> Program<S, R> {
        Program {
            instructions: self.instructions.leave_region(),
            block_instructions: self.block_instructions.leave_region(),
            block_terminators: self.block_terminators.leave_region(),
            block_descriptors: self.block_descriptors.leave_region(),
            function_blocks: self.function_blocks.leave_region(),
            function_descriptors: self.function_descriptors.leave_region(),
        }
    }
}
