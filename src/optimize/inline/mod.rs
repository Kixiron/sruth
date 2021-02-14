mod early_inline;
mod heuristics;

pub use early_inline::early_inline;
pub use heuristics::{harvest_heuristics, InlineHeuristics};

use crate::{
    dataflow::{operators::CollectCastable, Program},
    repr::{instruction::Call, FuncId},
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::Join,
    Collection, ExchangeData,
};
use std::ops::Mul;
use timely::dataflow::Scope;

impl<S, R> Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R>,
{
    // TODO: Finish this
    pub fn inline_functions(&self, functions: &Collection<S, FuncId, R>) -> Self {
        self.instructions
            .scope()
            .region_named("inline functions", |region| {
                let (program, functions) = (self.enter(region), functions.enter(region));

                let call_sites = program
                    .instructions
                    .collect_castable::<Call>()
                    .map(|(inst, call)| (call.func, (inst, call)))
                    .semijoin(&functions);

                let _instructions = program
                    .instructions
                    .antijoin(&call_sites.map(|(_, (inst, _))| inst));

                program.leave_region()
            })
    }
}
