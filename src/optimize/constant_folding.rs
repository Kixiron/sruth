use crate::{
    dataflow::{FilterMap, InputManager, Time},
    repr::{
        basic_block::BasicBlockMeta,
        instruction::{Add, Assign, BinOp, Div, Mul, Sub},
        BasicBlockId, Cast, Constant, InstId, Instruction, Terminator, Value, VarId,
    },
};
use differential_dataflow::{
    difference::{Monoid, Semigroup},
    input::Input,
    lattice::Lattice,
    operators::{arrange::Arranged, iterate::Variable, Consolidate, Join},
    trace::TraceReader,
    AsCollection, Collection, Data, ExchangeData,
};
use std::{
    convert::TryInto,
    ops::{Mul as OpsMul, Neg},
};
use timely::{
    dataflow::{operators::Partition, Scope, Stream},
    order::Product,
};

type ConstProp<S, R> = (
    Collection<S, (InstId, Instruction), R>,
    Collection<S, (VarId, Constant), R>,
    Collection<S, (BasicBlockId, Terminator), R>,
);

pub fn constant_folding<S, R>(
    scope: &mut S,
    inputs: &mut InputManager<S::Timestamp, R>,
) -> ConstProp<S, R>
where
    S: Scope + Input,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R> + From<i8>,
{
    let instructions = inputs
        .instruction_trace
        .import(scope)
        .as_collection(|&id, inst| (id, inst.clone()));

    let seed_constants = instructions
        .inspect(|x| println!("Before: {:?}", x))
        .filter_map(|(_, inst)| {
            inst.into_assign().and_then(|Assign { value, dest }| {
                value.into_const().map(|constant| (dest, constant))
            })
        })
        .inspect(|x| println!("After: {:?}", x));

    let (propagated_instructions, constants) =
        scope.scoped::<Product<_, Time>, _, _>("iterative constant propagation", |nested| {
            let instructions = Variable::new_from(
                instructions.enter(nested),
                Product::new(Default::default(), 1),
            );
            let constants = Variable::new_from(
                seed_constants.enter(nested),
                Product::new(Default::default(), 1),
            );

            // Evaluate binary operations
            let evaluated_binops = evalutate_binary_op::<_, Add, _>(&instructions, &constants)
                .concat(&evalutate_binary_op::<_, Sub, _>(&instructions, &constants))
                .concat(&evalutate_binary_op::<_, Mul, _>(&instructions, &constants))
                .concat(&evalutate_binary_op::<_, Div, _>(&instructions, &constants));

            // Add the newly derived constants to the stream of constants
            let new_constants = evaluated_binops
                .map(|(_, assign)| {
                    let assign = assign.into_assign().unwrap();
                    (assign.dest, assign.value.into_const().unwrap())
                })
                .concat(&constants)
                .consolidate();

            // Replace the instructions we've modified
            let new_instructions = instructions
                .antijoin(&evaluated_binops.map(|(id, _)| id))
                .concat(&evaluated_binops)
                .consolidate();

            (
                instructions.set(&new_instructions).leave(),
                constants.set(&new_constants).leave(),
            )
        });

    let instructions = promote_constants(&propagated_instructions, &constants);
    let terminators = propagate_to_terminators(&inputs.basic_block_trace.import(scope), &constants);

    (instructions, constants, terminators)
}

fn propagate_to_terminators<S, R, A>(
    blocks: &Arranged<S, A>,
    constants: &Collection<S, (VarId, Constant), R>,
) -> Collection<S, (BasicBlockId, Terminator), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + ExchangeData + OpsMul<Output = R>,
    A: TraceReader<Key = BasicBlockId, Val = BasicBlockMeta, Time = S::Timestamp, R = R> + Clone,
{
    let returns = blocks
        .as_collection(|&id, meta| (id, meta.terminator.clone()))
        .filter_map(|(id, term)| {
            term.into_return()
                .flatten()
                .and_then(|term| term.as_var())
                .map(|var| (var, id))
        });

    returns.join_map(constants, |_, &id, constant| {
        (id, Terminator::Return(Some(Value::Const(constant.clone()))))
    })
}

fn promote_constants<S, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    constants: &Collection<S, (VarId, Constant), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R>,
{
    let promoted_binops = promote_binop::<_, Add, _>(instructions, constants)
        .concat(&promote_binop::<_, Sub, _>(instructions, constants))
        .concat(&promote_binop::<_, Mul, _>(instructions, constants))
        .concat(&promote_binop::<_, Div, _>(instructions, constants));

    instructions
        .antijoin(&promoted_binops.map(|(id, _)| id))
        .concat(&promoted_binops)
}

fn promote_binop<S, T, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    constants: &Collection<S, (VarId, Constant), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    T: ExchangeData + BinOp + Into<Instruction>,
    Instruction: Cast<T>,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R>,
{
    let [lhs_var, rhs_var, both_var, _]: [Stream<_, _>; 4] = instructions
        .filter_map(|(id, binop)| binop.cast().map(|add| (id, add)))
        .inner
        .partition(4, |((id, binop), time, diff)| {
            let stream = match (binop.lhs().is_var(), binop.rhs().is_var()) {
                (true, false) => 0,
                (false, true) => 1,
                (true, true) => 2,
                (false, false) => 3,
            };

            (stream, ((id, binop), time, diff))
        })
        .try_into()
        .unwrap_or_else(|_| unreachable!());

    let both_promoted = {
        let both_var = both_var.as_collection();

        let both_promoted = both_var
            .map(|(id, binop)| {
                (
                    binop.lhs().as_var().unwrap(),
                    (id, binop.dest(), binop.rhs()),
                )
            })
            .join_map(&constants, |_, &(id, dest, ref rhs), lhs| {
                (
                    rhs.clone().as_var().unwrap(),
                    (id, dest, Value::Const(lhs.clone())),
                )
            })
            .join_map(&constants, |_, &(id, dest, ref lhs), rhs| {
                (
                    id,
                    T::from_parts(lhs.clone(), Value::Const(rhs.clone()), dest).into(),
                )
            });

        let lhs_promoted = both_var
            .antijoin(&both_promoted.map(|(id, _)| id))
            .map(|(id, binop)| {
                (
                    binop.lhs().as_var().unwrap(),
                    (id, binop.dest(), binop.rhs()),
                )
            })
            .join_map(&constants, |_, &(id, dest, ref rhs), lhs| {
                (
                    id,
                    T::from_parts(Value::Const(lhs.clone()), rhs.clone(), dest).into(),
                )
            });

        let rhs_promoted = both_var
            .antijoin(&both_promoted.map(|(id, _)| id))
            .map(|(id, binop)| {
                (
                    binop.rhs().as_var().unwrap(),
                    (id, binop.dest(), binop.lhs()),
                )
            })
            .join_map(&constants, |_, &(id, dest, ref lhs), rhs| {
                (
                    id,
                    T::from_parts(lhs.clone(), Value::Const(rhs.clone()), dest).into(),
                )
            });

        both_promoted.concat(&lhs_promoted).concat(&rhs_promoted)
    };

    let lhs_promoted = lhs_var
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.lhs().as_var().unwrap(),
                (id, binop.dest(), binop.rhs()),
            )
        })
        .join_map(&constants, |_, &(id, dest, ref rhs), lhs| {
            (
                id,
                T::from_parts(Value::Const(lhs.clone()), rhs.clone(), dest).into(),
            )
        });

    let rhs_promoted = rhs_var
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.rhs().as_var().unwrap(),
                (id, binop.dest(), binop.lhs()),
            )
        })
        .join_map(&constants, |_, &(id, dest, ref lhs), rhs| {
            (
                id,
                T::from_parts(lhs.clone(), Value::Const(rhs.clone()), dest).into(),
            )
        });

    both_promoted.concat(&lhs_promoted).concat(&rhs_promoted)
}

trait Evaluate {
    type Output;

    fn eval(self) -> Self::Output;
}

impl Evaluate for Add {
    type Output = Instruction;

    fn eval(self) -> Self::Output {
        self.evaluate().unwrap()
    }
}

impl Evaluate for Sub {
    type Output = Instruction;

    fn eval(self) -> Self::Output {
        self.evaluate().unwrap()
    }
}

impl Evaluate for Mul {
    type Output = Instruction;

    fn eval(self) -> Self::Output {
        self.evaluate().unwrap()
    }
}

impl Evaluate for Div {
    type Output = Instruction;

    fn eval(self) -> Self::Output {
        self.evaluate().unwrap()
    }
}

fn evalutate_binary_op<S, T, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    constants: &Collection<S, (VarId, Constant), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    T: Evaluate<Output = Instruction> + BinOp + Data,
    Instruction: Cast<T>,
    R: Semigroup + ExchangeData + OpsMul<Output = R>,
{
    let [both_const, lhs_const, rhs_const, no_const]: [Stream<_, _>; 4] = instructions
        .filter_map(|(id, binop)| binop.cast().map(|binop| (id, binop)))
        .inner
        .partition(4, |((id, binop), time, diff)| {
            let stream = match (binop.lhs().is_const(), binop.rhs().is_const()) {
                (true, true) => 0,
                (true, false) => 1,
                (false, true) => 2,
                (false, false) => 3,
            };

            (stream, ((id, binop), time, diff))
        })
        .try_into()
        .unwrap_or_else(|_| unreachable!());

    let both_const = both_const
        .as_collection()
        .map(|(id, binop)| (id, binop.eval()));

    let lhs_const = lhs_const
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.rhs().as_var().unwrap(),
                (id, binop.dest(), binop.lhs()),
            )
        })
        .join_map(&constants, |_, &(id, dest, ref lhs), rhs| {
            let evaluated = T::from_parts(lhs.clone(), Value::Const(rhs.clone()), dest).eval();

            (id, evaluated)
        });

    let rhs_const = rhs_const
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.lhs().as_var().unwrap(),
                (id, binop.dest(), binop.rhs()),
            )
        })
        .join_map(&constants, |_, &(id, dest, ref rhs), lhs| {
            let evaluated = T::from_parts(Value::Const(lhs.clone()), rhs.clone(), dest).eval();

            (id, evaluated)
        });

    let no_const = no_const
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.lhs().as_var().unwrap(),
                (id, binop.dest(), binop.rhs().as_var().unwrap()),
            )
        })
        .join_map(&constants, |_, &(id, dest, rhs), lhs| {
            (rhs, (id, dest, lhs.clone()))
        })
        .join_map(&constants, |_, &(id, dest, ref lhs), rhs| {
            let evaluated =
                T::from_parts(Value::Const(lhs.clone()), Value::Const(rhs.clone()), dest).eval();

            (id, evaluated)
        });

    both_const
        .concat(&lhs_const)
        .concat(&rhs_const)
        .concat(&no_const)
}
