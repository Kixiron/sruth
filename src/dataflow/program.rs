use std::panic::Location;

use crate::repr::{
    basic_block::BasicBlockMeta, function::FunctionDesc, BasicBlockId, FuncId, InstId, Instruction,
    Terminator,
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, Arranged, TraceAgent},
        iterate::Variable,
        Consolidate,
    },
    trace::implementations::ord::OrdValSpine,
    Collection, ExchangeData, Hashable,
};
use timely::{
    dataflow::{operators::probe::Handle, scopes::Child, Scope},
    progress::{timestamp::Refines, Timestamp},
};

#[derive(Clone)]
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
    pub function_descriptors: Collection<S, (FuncId, FunctionDesc), R>,
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
        function_descriptors: Collection<S, (FuncId, FunctionDesc), R>,
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

    pub fn consolidate(&self) -> Self
    where
        S::Timestamp: Lattice,
        R: ExchangeData,
    {
        Self {
            instructions: self.instructions.consolidate(),
            block_instructions: self.block_instructions.consolidate(),
            block_terminators: self.block_terminators.consolidate(),
            block_descriptors: self.block_descriptors.consolidate(),
            function_blocks: self.function_blocks.consolidate(),
            function_descriptors: self.function_descriptors.consolidate(),
        }
    }

    pub fn probe(&self) -> Handle<S::Timestamp> {
        let mut handle = Handle::new();
        self.probe_with(&mut handle);

        handle
    }

    pub fn probe_with(&self, handle: &mut Handle<S::Timestamp>) -> Self {
        Self {
            instructions: self.instructions.probe_with(handle),
            block_instructions: self.block_instructions.probe_with(handle),
            block_terminators: self.block_terminators.probe_with(handle),
            block_descriptors: self.block_descriptors.probe_with(handle),
            function_blocks: self.function_blocks.probe_with(handle),
            function_descriptors: self.function_descriptors.probe_with(handle),
        }
    }

    pub fn assert_eq(&self, other: &Self)
    where
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Hashable,
    {
        self.instructions.assert_eq(&other.instructions);
        self.block_instructions.assert_eq(&other.block_instructions);
        self.block_terminators.assert_eq(&other.block_terminators);
        self.block_descriptors.assert_eq(&other.block_descriptors);
        self.function_blocks.assert_eq(&other.function_blocks);
        self.function_descriptors
            .assert_eq(&other.function_descriptors);
    }

    #[track_caller]
    pub fn debug(&self) -> Self {
        let location = Location::caller();

        Self {
            instructions: self.instructions.inspect(move |x| {
                println!(
                    "[{}:{}] instructions: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
            block_instructions: self.block_instructions.inspect(move |x| {
                println!(
                    "[{}:{}] block instructions: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
            block_terminators: self.block_terminators.inspect(move |x| {
                println!(
                    "[{}:{}] block terminators: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
            block_descriptors: self.block_descriptors.inspect(move |x| {
                println!(
                    "[{}:{}] block descriptors: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
            function_blocks: self.function_blocks.inspect(move |x| {
                println!(
                    "[{}:{}] function blocks: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
            function_descriptors: self.function_descriptors.inspect(move |x| {
                println!(
                    "[{}:{}] function descriptors: {:?}",
                    location.file(),
                    location.line(),
                    x,
                );
            }),
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

impl<S, R> Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + ExchangeData,
{
    pub fn arrange_by_key(&self) -> ArrangedProgram<S, R> {
        ArrangedProgram {
            instructions: self.instructions.arrange_by_key(),
            block_instructions: self.block_instructions.arrange_by_key(),
            block_terminators: self.block_terminators.arrange_by_key(),
            block_descriptors: self.block_descriptors.arrange_by_key(),
            function_blocks: self.function_blocks.arrange_by_key(),
            function_descriptors: self.function_descriptors.arrange_by_key(),
        }
    }
}

impl<S, R> From<&ProgramVariable<S, R>> for Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian,
{
    fn from(program: &ProgramVariable<S, R>) -> Self {
        Self {
            instructions: program.instructions.clone(),
            block_instructions: program.block_instructions.clone(),
            block_terminators: program.block_terminators.clone(),
            block_descriptors: program.block_descriptors.clone(),
            function_blocks: program.function_blocks.clone(),
            function_descriptors: program.function_descriptors.clone(),
        }
    }
}

pub struct ProgramVariable<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian,
{
    pub instructions: Variable<S, (InstId, Instruction), R>,
    pub block_instructions: Variable<S, (InstId, BasicBlockId), R>,
    pub block_terminators: Variable<S, (BasicBlockId, Terminator), R>,
    pub block_descriptors: Variable<S, (BasicBlockId, BasicBlockMeta), R>,
    pub function_blocks: Variable<S, (BasicBlockId, FuncId), R>,
    pub function_descriptors: Variable<S, (FuncId, FunctionDesc), R>,
}

impl<S, R> ProgramVariable<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian,
{
    pub fn new(
        instructions: Variable<S, (InstId, Instruction), R>,
        block_instructions: Variable<S, (InstId, BasicBlockId), R>,
        block_terminators: Variable<S, (BasicBlockId, Terminator), R>,
        block_descriptors: Variable<S, (BasicBlockId, BasicBlockMeta), R>,
        function_blocks: Variable<S, (BasicBlockId, FuncId), R>,
        function_descriptors: Variable<S, (FuncId, FunctionDesc), R>,
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

    pub fn set(self, result: &Program<S, R>) -> Program<S, R> {
        Program {
            instructions: self.instructions.set(&result.instructions),
            block_instructions: self.block_instructions.set(&result.block_instructions),
            block_terminators: self.block_terminators.set(&result.block_terminators),
            block_descriptors: self.block_descriptors.set(&result.block_descriptors),
            function_blocks: self.function_blocks.set(&result.function_blocks),
            function_descriptors: self.function_descriptors.set(&result.function_descriptors),
        }
    }

    pub fn set_concat(self, result: &Program<S, R>) -> Program<S, R> {
        Program {
            instructions: self.instructions.set_concat(&result.instructions),
            block_instructions: self
                .block_instructions
                .set_concat(&result.block_instructions),
            block_terminators: self.block_terminators.set_concat(&result.block_terminators),
            block_descriptors: self.block_descriptors.set_concat(&result.block_descriptors),
            function_blocks: self.function_blocks.set_concat(&result.function_blocks),
            function_descriptors: self
                .function_descriptors
                .set_concat(&result.function_descriptors),
        }
    }

    pub fn program(&self) -> Program<S, R> {
        Program {
            instructions: self.instructions.clone(),
            block_instructions: self.block_instructions.clone(),
            block_terminators: self.block_terminators.clone(),
            block_descriptors: self.block_descriptors.clone(),
            function_blocks: self.function_blocks.clone(),
            function_descriptors: self.function_descriptors.clone(),
        }
    }
}

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct ArrangedProgram<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
{
    pub instructions: Arranged<S, TraceAgent<OrdValSpine<InstId, Instruction, S::Timestamp, R>>>,
    pub block_instructions:
        Arranged<S, TraceAgent<OrdValSpine<InstId, BasicBlockId, S::Timestamp, R>>>,
    pub block_terminators:
        Arranged<S, TraceAgent<OrdValSpine<BasicBlockId, Terminator, S::Timestamp, R>>>,
    pub block_descriptors:
        Arranged<S, TraceAgent<OrdValSpine<BasicBlockId, BasicBlockMeta, S::Timestamp, R>>>,
    pub function_blocks:
        Arranged<S, TraceAgent<OrdValSpine<BasicBlockId, FuncId, S::Timestamp, R>>>,
    pub function_descriptors:
        Arranged<S, TraceAgent<OrdValSpine<FuncId, FunctionDesc, S::Timestamp, R>>>,
}

impl<S, R> ArrangedProgram<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
{
    pub fn trace(self) -> ProgramTrace<S::Timestamp, R> {
        ProgramTrace {
            instructions: self.instructions.trace,
            block_instructions: self.block_instructions.trace,
            block_terminators: self.block_terminators.trace,
            block_descriptors: self.block_descriptors.trace,
            function_blocks: self.function_blocks.trace,
            function_descriptors: self.function_descriptors.trace,
        }
    }

    pub fn as_collection(&self) -> Program<S, R> {
        Program {
            instructions: self
                .instructions
                .as_collection(|&inst_id, inst| (inst_id, inst.clone())),
            block_instructions: self
                .block_instructions
                .as_collection(|&inst, &block| (inst, block)),
            block_terminators: self
                .block_terminators
                .as_collection(|&block, term| (block, term.clone())),
            block_descriptors: self
                .block_descriptors
                .as_collection(|&block, desc| (block, desc.clone())),
            function_blocks: self
                .function_blocks
                .as_collection(|&block, &func| (block, func)),
            function_descriptors: self
                .function_descriptors
                .as_collection(|&func, desc| (func, desc.clone())),
        }
    }
}

#[derive(Clone)]
pub struct ProgramTrace<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup,
{
    pub instructions: TraceAgent<OrdValSpine<InstId, Instruction, T, R>>,
    pub block_instructions: TraceAgent<OrdValSpine<InstId, BasicBlockId, T, R>>,
    pub block_terminators: TraceAgent<OrdValSpine<BasicBlockId, Terminator, T, R>>,
    pub block_descriptors: TraceAgent<OrdValSpine<BasicBlockId, BasicBlockMeta, T, R>>,
    pub function_blocks: TraceAgent<OrdValSpine<BasicBlockId, FuncId, T, R>>,
    pub function_descriptors: TraceAgent<OrdValSpine<FuncId, FunctionDesc, T, R>>,
}

impl<T, R> ProgramTrace<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup,
{
    pub fn import<S>(&mut self, scope: &S) -> ArrangedProgram<S, R>
    where
        S: Scope<Timestamp = T>,
    {
        ArrangedProgram {
            instructions: self.instructions.import(scope),
            block_instructions: self.block_instructions.import(scope),
            block_terminators: self.block_terminators.import(scope),
            block_descriptors: self.block_descriptors.import(scope),
            function_blocks: self.function_blocks.import(scope),
            function_descriptors: self.function_descriptors.import(scope),
        }
    }
}
