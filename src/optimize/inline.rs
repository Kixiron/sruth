use crate::{
    dataflow::{
        operators::{CountExt, FilterMap},
        Program,
    },
    repr::{instruction::Call, Cast, FuncId},
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::Join,
    Collection, ExchangeData,
};
use num_traits::AsPrimitive;
use std::ops::Mul;
use timely::dataflow::Scope;

pub fn harvest_heuristics<S, R>(
    program: &Program<S, R>,
) -> Collection<S, (FuncId, InlineHeuristics), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + AsPrimitive<usize> + From<i8> + Clone,
{
    let mut block_lengths = program
        .function_blocks
        .map(|(_, func)| func)
        .count_core::<R>();

    block_lengths = block_lengths.concat(
        &program
            .function_descriptors
            .map(|(id, _)| (id, R::from(0)))
            .antijoin(&block_lengths.map(|(func, _)| func)),
    );

    let mut inst_lengths = program
        .block_instructions
        .map(|(inst, block)| (block, inst))
        .join_map(&program.function_blocks, |_block, _inst, &func| func)
        .count_core();

    inst_lengths = inst_lengths.concat(
        &program
            .function_descriptors
            .map(|(id, _)| (id, R::from(0)))
            .antijoin(&inst_lengths.map(|(func, _)| func)),
    );

    let mut invocations = program
        .instructions
        .filter_map(|(_, inst)| inst.cast::<Call>().map(|call| call.func))
        .count_core();

    invocations = invocations.concat(
        &program
            .function_descriptors
            .map(|(id, _)| (id, R::from(0)))
            .antijoin(&invocations.map(|(func, _)| func)),
    );

    let mut branches = program
        .block_terminators
        .filter(|(_, term)| term.is_branching())
        .join_map(&program.function_blocks, |_block, _term, &func| func)
        .count_core();

    branches = branches.concat(
        &program
            .function_descriptors
            .map(|(id, _)| (id, R::from(0)))
            .antijoin(&branches.map(|(func, _)| func)),
    );

    block_lengths
        .join(&inst_lengths)
        .join(&invocations)
        .join_map(
            &branches,
            |&func, ((block_length, inst_length), invocations), branches| {
                (
                    func,
                    InlineHeuristics::new(
                        branches.clone().as_(),
                        invocations.clone().as_(),
                        block_length.clone().as_(),
                        inst_length.clone().as_(),
                    ),
                )
            },
        )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct InlineHeuristics {
    pub branches: usize,
    pub invocations: usize,
    pub block_length: usize,
    pub inst_length: usize,
}

impl InlineHeuristics {
    pub const fn new(
        branches: usize,
        invocations: usize,
        block_length: usize,
        inst_length: usize,
    ) -> Self {
        Self {
            branches,
            invocations,
            block_length,
            inst_length,
        }
    }
}
