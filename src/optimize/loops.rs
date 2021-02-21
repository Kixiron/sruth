use crate::{dataflow::Program, repr::BasicBlockId};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Iterate, Join, Threshold},
    Collection, ExchangeData,
};
use std::ops::Mul;
use timely::dataflow::Scope;

impl<S, R> Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    pub fn loops(&self) -> Collection<S, (BasicBlockId, BasicBlockId), R> {
        let graph = self.block_terminators.flat_map(|(block, term)| {
            term.jump_targets()
                .into_iter()
                .map(move |target| (block, target))
        });

        graph
            .iterate(|edges| {
                // Keep edges from active edge destinations
                let active = edges.map(|(_src, dest)| dest).distinct_core();

                graph.enter(&edges.scope()).semijoin(&active)
            })
            .consolidate()
            .inspect(|x| println!("{:?}", x))
    }
}
