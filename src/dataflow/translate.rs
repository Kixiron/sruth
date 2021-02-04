use crate::{
    dataflow::InputManager,
    repr::{basic_block::BasicBlockMeta, function::FunctionMeta, Function, InstId},
};
use differential_dataflow::{difference::Semigroup, lattice::Lattice};
use std::num::NonZeroU64;
use timely::progress::Timestamp;

pub fn translate<T, R, I>(input: &mut InputManager<T, R>, functions: I)
where
    T: Timestamp + Lattice + Clone,
    R: Semigroup + From<i8>,
    I: IntoIterator<Item = Function>,
{
    for function in functions {
        let meta = FunctionMeta::new(
            function.name,
            function.id,
            function.entry,
            function.basic_blocks.iter().map(|block| block.id).collect(),
        );
        input.functions.update((function.id, meta), R::from(1));

        for basic_block in function.basic_blocks {
            let mut instructions = Vec::with_capacity(basic_block.instructions.len());

            for (idx, instruction) in basic_block.instructions.into_iter().enumerate() {
                let inst_id = InstId::new(NonZeroU64::new(idx as u64 + 1).unwrap());

                instructions.push(inst_id);
                input
                    .instructions
                    .update((inst_id, instruction), R::from(1));
            }

            let meta = BasicBlockMeta::new(
                basic_block.name,
                basic_block.id,
                instructions,
                basic_block.terminator,
            );
            input
                .basic_blocks
                .update((basic_block.id, meta), R::from(1));
        }
    }
}
