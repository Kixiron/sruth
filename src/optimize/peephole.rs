use crate::{
    dataflow::operators::{CollectCastable, FilterMap},
    repr::{
        instruction::{Assign, BinopExt, Mul},
        Constant, InstId, Instruction, InstructionExt,
    },
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Join},
    Collection, ExchangeData,
};
use std::ops::Mul as Mult;
use timely::dataflow::Scope;

pub fn peephole<S, R>(
    scope: &mut S,
    instructions: &Collection<S, (InstId, Instruction), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mult<Output = R>,
    // A1: TraceReader<Key = InstId, Val = Instruction, R = R, Time = S::Timestamp> + Clone + 'static,
{
    let span = tracing::debug_span!("peephole optimization");
    span.in_scope(|| {
        scope.region_named("peephole optimization", |region| {
            let seed_instructions = instructions.enter(region);
            let mul_by_zero = apply::<_, _, MulByZero>(&seed_instructions);

            seed_instructions
                .antijoin(&mul_by_zero.map(|(id, _)| id))
                .concat(&mul_by_zero)
                .consolidate()
                .leave_region()
        })
    })
}

fn apply<S, R, P>(
    instructions: &Collection<S, (InstId, Instruction), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    R: Semigroup,
    P: PeepholePass,
{
    let span = tracing::debug_span!("peephole pass", name = P::name());
    span.in_scope(|| P::stream_instructions(instructions))
}

pub trait PeepholePass {
    fn name() -> &'static str;

    fn stream_instructions<S, R>(
        instructions: &Collection<S, (InstId, Instruction), R>,
    ) -> Collection<S, (InstId, Instruction), R>
    where
        S: Scope,
        R: Semigroup,
    {
        instructions.clone()
    }
}

struct MulByZero;

impl PeepholePass for MulByZero {
    fn name() -> &'static str {
        "multiply by zero"
    }

    fn stream_instructions<S, R>(
        instructions: &Collection<S, (InstId, Instruction), R>,
    ) -> Collection<S, (InstId, Instruction), R>
    where
        S: Scope,
        R: Semigroup,
    {
        // TODO: This could be a `.map_in_place()`
        instructions
            .collect_castable::<Mul>()
            .filter_map(|(id, mul)| {
                let constant = {
                    let lhs = mul.lhs();
                    lhs.as_const().cloned().map(|c| ((lhs, c), true))
                }
                .or_else(|| {
                    let rhs = mul.rhs();
                    rhs.as_const().cloned().map(|c| ((rhs, c), false))
                });

                constant
                    .and_then(|((val, constant), is_lhs)| match constant {
                        Constant::Int(int) if int == 0 => Some(((val, Constant::Int(0)), is_lhs)),
                        Constant::Uint(uint) if uint == 0 => {
                            Some(((val, Constant::Uint(0)), is_lhs))
                        }
                        _ => None,
                    })
                    .map(|((value, zero), is_lhs)| {
                        tracing::trace!(
                            inst = ?id,
                            "reduced `{}` to `0`",
                            if is_lhs { "0 * x" } else { "x * 0" },
                        );

                        let assign = Assign::new(mul.dest(), value.map(|_| zero.into()));
                        Instruction::Assign(assign)
                    })
                    .map(|inst| (id, inst))
            })
    }
}
