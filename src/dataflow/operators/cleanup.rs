use super::{CollectCastable, CollectDeclarations, FilterMap};
use crate::repr::{
    basic_block::BasicBlockMeta, function::FunctionMeta, instruction::Call, terminator::Return,
    BasicBlockId, FuncId, InstId, Instruction, Terminator,
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Iterate, Join, Reduce, Threshold},
    Collection, ExchangeData,
};
use std::ops::Mul;
use timely::{
    dataflow::{scopes::Child, Scope},
    progress::{timestamp::Refines, Timestamp},
};

pub struct ProgramContents<S, R>
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

impl<S, R> ProgramContents<S, R>
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

    pub fn enter<'a, T>(&self, scope: &Child<'a, S, T>) -> ProgramContents<Child<'a, S, T>, R>
    where
        T: Timestamp + Refines<S::Timestamp>,
    {
        ProgramContents {
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
    ) -> ProgramContents<Child<'a, S, S::Timestamp>, R> {
        ProgramContents {
            instructions: self.instructions.enter_region(scope),
            block_instructions: self.block_instructions.enter_region(scope),
            block_terminators: self.block_terminators.enter_region(scope),
            block_descriptors: self.block_descriptors.enter_region(scope),
            function_blocks: self.function_blocks.enter_region(scope),
            function_descriptors: self.function_descriptors.enter_region(scope),
        }
    }
}

impl<'a, S, T, R> ProgramContents<Child<'a, S, T>, R>
where
    S: Scope,
    R: Semigroup,
    T: Timestamp + Refines<S::Timestamp>,
{
    pub fn leave(&self) -> ProgramContents<S, R> {
        ProgramContents {
            instructions: self.instructions.leave(),
            block_instructions: self.block_instructions.leave(),
            block_terminators: self.block_terminators.leave(),
            block_descriptors: self.block_descriptors.leave(),
            function_blocks: self.function_blocks.leave(),
            function_descriptors: self.function_descriptors.leave(),
        }
    }
}

impl<'a, S, R> ProgramContents<Child<'a, S, S::Timestamp>, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn leave_region(&self) -> ProgramContents<S, R> {
        ProgramContents {
            instructions: self.instructions.leave_region(),
            block_instructions: self.block_instructions.leave_region(),
            block_terminators: self.block_terminators.leave_region(),
            block_descriptors: self.block_descriptors.leave_region(),
            function_blocks: self.function_blocks.leave_region(),
            function_descriptors: self.function_descriptors.leave_region(),
        }
    }
}

pub trait Cleanup {
    fn cleanup(&self) -> Self;
}

impl<S, R> Cleanup for ProgramContents<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    fn cleanup(&self) -> Self {
        // TODO: Rewrite as one single `.scoped()` using `SemigroupVariable`s that's mutually
        //       recursive between the set of used instructions, blocks and functions. Maybe
        //       use `Present` as the scope's inner difference type, ask Nami
        // TODO: Start with a set of desired functions
        // TODO: Only include reachable return statements
        // TODO: Update `FunctionMeta`s
        // TODO: Update `BasicBlockMeta`s
        let program = self.instructions.scope().region_named("Cleanup", |region| {
            let program = self.enter(region);

            // TODO: Filter for reachable returns
            let returned_vars = program.block_terminators.collect_castable::<Return>();
            let declared_vars = program.instructions.collect_declarations();

            // The instructions required for the program to be valid
            let required_instructions = declared_vars
                .semijoin(&returned_vars.filter_map(|(_, ret)| ret.returned_var()))
                .iterate(|vars| {
                    let instructions = program.instructions.enter(&vars.scope());

                    instructions
                        .semijoin(&vars.map(|(_, inst)| inst))
                        .flat_map(|(id, inst)| {
                            inst.used_vars().into_iter().map(move |var| (var, id))
                        })
                        .concat(&vars)
                        .distinct_core()
                })
                .map(|(_, inst)| inst);

            // The blocks required for the program to be valid
            let required_blocks = returned_vars
                .map(|(block, _)| block)
                .concat(
                    &program
                        .block_instructions
                        .semijoin(&required_instructions)
                        .map(|(_, block)| block),
                )
                .iterate(|blocks| {
                    let terminators = program
                        .block_terminators
                        .enter(&blocks.scope())
                        .semijoin(&blocks);

                    terminators
                        .flat_map(|(_, term)| term.jump_targets().into_iter())
                        .concat(&blocks)
                        .distinct_core()
                });

            // The functions required for program execution
            let required_functions = program
                .function_blocks
                .semijoin(&required_blocks)
                .map(|(_, func)| func)
                .iterate(|funcs| {
                    let function_blocks = program
                        .function_blocks
                        .enter(&funcs.scope())
                        .map(|(block, func)| (func, block))
                        .semijoin(&funcs)
                        .map(|(_, block)| block);

                    let inst_ids = program
                        .block_instructions
                        .enter(&funcs.scope())
                        .map(|(inst, block)| (block, inst))
                        .semijoin(&function_blocks)
                        .map(|(_, inst)| inst);

                    program
                        .instructions
                        .enter(&funcs.scope())
                        .semijoin(&inst_ids)
                        .collect_castable::<Call>()
                        .map(|(_, call)| call.func)
                        .concat(&funcs)
                        .distinct_core()
                });

            let block_instructions = program.block_instructions.semijoin(&required_instructions);
            let agg_inst = block_instructions
                .consolidate()
                .map(|(inst, block)| (block, inst))
                .reduce(|_, instructions, output| {
                    let instructions: Vec<_> = instructions.iter().map(|(&id, _)| id).collect();
                    output.push((instructions, R::from(1)));
                });

            let block_descriptors = program
                .block_descriptors
                .semijoin(&required_blocks)
                .join_map(&agg_inst, |&id, desc, instructions| {
                    let mut desc = desc.clone();
                    desc.instructions = instructions.to_owned();

                    (id, desc)
                });

            let function_blocks = program.function_blocks.semijoin(&required_blocks);
            let agg_blocks = function_blocks
                .consolidate()
                .map(|(block, func)| (func, block))
                .reduce(|_, blocks, output| {
                    let blocks: Vec<_> = blocks.iter().map(|(&id, _)| id).collect();
                    output.push((blocks, R::from(1)));
                });

            let function_descriptors = program
                .function_descriptors
                .semijoin(&required_functions)
                .join_map(&agg_blocks, |&id, desc, blocks| {
                    let mut desc = desc.clone();
                    desc.basic_blocks = blocks.to_owned();

                    (id, desc)
                });

            ProgramContents {
                instructions: program.instructions.semijoin(&required_instructions),
                block_instructions,
                block_terminators: program.block_terminators.semijoin(&required_blocks),
                block_descriptors,
                function_blocks,
                function_descriptors,
            }
            .leave_region()
        });

        if cfg!(debug_assertions) {
            self.instructions
                .join(&self.block_instructions)
                .antijoin(&program.instructions.map(|(id, _)| id))
                .inspect(|((inst_id, (inst, block_id)), _, _)| {
                    tracing::trace!(
                        inst = ?inst,
                        "removed instruction {:?} from {:?}",
                        inst_id,
                        block_id,
                    );
                });

            self.block_terminators
                .antijoin(&program.block_terminators.map(|(id, _)| id))
                .inspect(|((block_id, term), _, _)| {
                    tracing::trace!("removed terminator {:?} from {:?}", term, block_id);
                });

            self.function_blocks
                .antijoin(&program.function_blocks.map(|(id, _)| id))
                .inspect(|((block_id, func_id), _, _)| {
                    tracing::trace!("removed {:?} from {:?}", block_id, func_id);
                });

            self.function_descriptors
                .antijoin(&self.function_descriptors.map(|(id, _)| id))
                .inspect(|((func_id, _desc), _, _)| tracing::trace!("removed func {:?}", func_id));
        }

        program
    }
}
