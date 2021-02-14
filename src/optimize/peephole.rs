use crate::{
    dataflow::operators::{CollectCastable, FilterMap},
    repr::{
        instruction::{Assign, BinopExt, Mul, Neg, Sub},
        Constant, InstId, Instruction, InstructionExt,
    },
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{consolidate::ConsolidateStream, Consolidate, Join},
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
{
    let span = tracing::debug_span!("peephole optimization");
    span.in_scope(|| {
        scope.region_named("peephole optimization", |region| {
            let instructions = instructions.enter(region).consolidate_stream();
            let instructions = apply::<_, _, MulByZero>(&instructions).consolidate_stream();
            let instructions = apply::<_, _, SubtractZero>(&instructions);

            instructions.consolidate().leave_region()
        })
    })
}

fn apply<S, R, P>(
    instructions: &Collection<S, (InstId, Instruction), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Mult<Output = R>,
    P: PeepholePass,
{
    let span = tracing::debug_span!("peephole pass", name = P::name());
    let reduced = span.in_scope(|| P::stream_instructions(instructions));

    instructions
        .antijoin(&reduced.map(|(inst, _)| inst))
        .concat(&reduced)
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

                        let assign = Assign::new(mul.dest(), value.map(|_| zero.into()), None);
                        Instruction::Assign(assign)
                    })
                    .map(|inst| (id, inst))
            })
    }
}

struct SubtractZero;

impl PeepholePass for SubtractZero {
    fn name() -> &'static str {
        "subtract zero"
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
            .collect_castable::<Sub>()
            .filter_map(|(id, sub)| {
                if let Some(constant) = sub.lhs().as_const() {
                    // TODO: figure out unsigned interactions
                    if constant.is_zero() && constant.is_signed_int() {
                        tracing::trace!(
                            inst = ?id,
                            "reduced `0 - x` to `-x`",
                        );

                        return Some((id, Instruction::Neg(Neg::new(sub.dest(), sub.rhs()))));
                    }
                } else if let Some(constant) = sub.rhs().as_const() {
                    if constant.is_zero() {
                        tracing::trace!(
                            inst = ?id,
                            "reduced `x - 0` to `x`",
                        );

                        return Some((
                            id,
                            Instruction::Assign(Assign::new(sub.dest(), sub.lhs(), None)),
                        ));
                    }
                }

                None
            })
    }
}
