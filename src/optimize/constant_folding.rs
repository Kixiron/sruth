use crate::{
    dataflow::{
        operators::{CollectCastable, FilterMap},
        Time,
    },
    repr::{
        instruction::{Add, Assign, BinopExt, Div, Mul, Sub},
        BasicBlockId, Cast, Constant, InstId, Instruction, RawCast, Terminator, Type, Value,
        ValueKind, VarId,
    },
};
use differential_dataflow::{
    difference::{Monoid, Semigroup},
    lattice::Lattice,
    operators::{iterate::Variable, Consolidate, Join},
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
    Collection<S, (BasicBlockId, Terminator), R>,
);

pub fn constant_folding<S, R>(
    scope: &mut S,
    instructions: &Collection<S, (InstId, Instruction), R>,
    terminators: &Collection<S, (BasicBlockId, Terminator), R>,
) -> ConstProp<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R> + From<i8>,
{
    let span = tracing::debug_span!("constant folding");
    span.in_scope(|| {
        let seed_constants = instructions.filter_map(|(_, inst)| {
            inst.cast().and_then(|Assign { value, dest }| {
                let (value, ty) = value.split();
                value.into_const().map(|constant| (dest, (constant, ty)))
            })
        });

        let (instructions, terminators) =
            scope.scoped::<Product<_, Time>, _, _>("iterative constant propagation", |nested| {
                let instructions = Variable::new_from(
                    instructions.enter(nested),
                    Product::new(Default::default(), 1),
                );
                let constants = Variable::new_from(
                    seed_constants.enter(nested),
                    Product::new(Default::default(), 1),
                );
                let terminators = Variable::new_from(
                    terminators.enter(nested),
                    Product::new(Default::default(), 1),
                );

                // Evaluate binary operations
                let evaluated_binops = evalutate_binary_op::<_, Add, _>(&instructions, &constants)
                    .concat(&evalutate_binary_op::<_, Sub, _>(&instructions, &constants))
                    .concat(&evalutate_binary_op::<_, Mul, _>(&instructions, &constants))
                    .concat(&evalutate_binary_op::<_, Div, _>(&instructions, &constants));

                // Replace the instructions we've modified
                let new_instructions = instructions
                    .antijoin(&evaluated_binops.map(|(id, _)| id))
                    .concat(&evaluated_binops);

                // Add the newly derived constants to the stream of constants
                let new_constants = new_instructions
                    .collect_castable::<Assign>()
                    .map(|(_, assign)| {
                        let (value, ty) = assign.value.split();
                        (assign.dest, (value.into_const().unwrap(), ty))
                    })
                    .consolidate();

                let promoted_instructions =
                    promote_constants(&new_instructions, &new_constants).consolidate();

                let folded_terminators =
                    propagate_to_terminators(&terminators, &new_constants).consolidate();

                constants.set(&new_constants);

                (
                    instructions.set(&promoted_instructions).leave(),
                    terminators.set(&folded_terminators).leave(),
                )
            });

        (instructions, terminators)
    })
}

fn propagate_to_terminators<S, R>(
    terminators: &Collection<S, (BasicBlockId, Terminator), R>,
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
) -> Collection<S, (BasicBlockId, Terminator), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R>,
{
    let returns = terminators.filter_map(|(id, term)| {
        term.into_return()
            .flatten()
            .and_then(|term| term.as_var().map(|var| (var, term.ty)))
            .map(|(var, ty)| (var, (id, ty)))
    });

    let folded_terminators =
        returns.join_map(constants, |_, &(id, ref ret_ty), (constant, const_ty)| {
            assert_eq!(ret_ty, const_ty);

            (
                id,
                Terminator::Return(Some(Value::new(
                    ValueKind::Const(constant.clone()),
                    const_ty.clone(),
                ))),
            )
        });

    terminators
        .map(|term| (term, ()))
        .antijoin(&folded_terminators)
        .map(|(term, ())| term)
        .concat(&folded_terminators)
}

fn promote_constants<S, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
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
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    T: ExchangeData + BinopExt + Into<Instruction>,
    Instruction: RawCast<T>,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R>,
{
    // TODO: Partition for collections
    let [lhs_var, rhs_var, both_var, _]: [Stream<_, _>; 4] = instructions
        .collect_castable::<T>()
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
            .join_map(&constants, |_, &(id, dest, ref rhs), (lhs, lhs_ty)| {
                (
                    rhs.clone().as_var().unwrap(),
                    (
                        id,
                        dest,
                        Value::new(ValueKind::Const(lhs.clone()), lhs_ty.clone()),
                    ),
                )
            })
            .join_map(&constants, |_, &(id, dest, ref lhs), (rhs, rhs_ty)| {
                (
                    id,
                    T::from_parts(
                        lhs.clone(),
                        Value::new(ValueKind::Const(rhs.clone()), rhs_ty.clone()),
                        dest,
                    )
                    .into(),
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
            .join_map(&constants, |_, &(id, dest, ref rhs), (lhs, lhs_ty)| {
                (
                    id,
                    T::from_parts(
                        Value::new(ValueKind::Const(lhs.clone()), lhs_ty.clone()),
                        rhs.clone(),
                        dest,
                    )
                    .into(),
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
            .join_map(&constants, |_, &(id, dest, ref lhs), (rhs, rhs_ty)| {
                (
                    id,
                    T::from_parts(
                        lhs.clone(),
                        Value::new(ValueKind::Const(rhs.clone()), rhs_ty.clone()),
                        dest,
                    )
                    .into(),
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
        .join_map(&constants, |_, &(id, dest, ref rhs), (lhs, lhs_ty)| {
            (
                id,
                T::from_parts(
                    Value::new(ValueKind::Const(lhs.clone()), lhs_ty.clone()),
                    rhs.clone(),
                    dest,
                )
                .into(),
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
        .join_map(&constants, |_, &(id, dest, ref lhs), (rhs, rhs_ty)| {
            (
                id,
                T::from_parts(
                    lhs.clone(),
                    Value::new(ValueKind::Const(rhs.clone()), rhs_ty.clone()),
                    dest,
                )
                .into(),
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
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    T: Evaluate<Output = Instruction> + BinopExt + Data,
    Instruction: RawCast<T>,
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
        .map(|(id, binop)| (id, binop.eval()))
        .inspect(|((id, inst), _, _)| tracing::trace!(inst = ?inst, "folded {:?}", id));

    let lhs_const = lhs_const
        .as_collection()
        .map(|(id, binop)| {
            (
                binop.rhs().as_var().unwrap(),
                (id, binop.dest(), binop.lhs()),
            )
        })
        .join_map(&constants, |_, &(id, dest, ref lhs), (rhs, rhs_ty)| {
            let evaluated = T::from_parts(
                lhs.clone(),
                Value::new(ValueKind::Const(rhs.clone()), rhs_ty.clone()),
                dest,
            )
            .eval();

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
        .join_map(&constants, |_, &(id, dest, ref rhs), (lhs, lhs_ty)| {
            let evaluated = T::from_parts(
                Value::new(ValueKind::Const(lhs.clone()), lhs_ty.clone()),
                rhs.clone(),
                dest,
            )
            .eval();

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
        .join_map(
            &constants,
            |_, &(id, dest, (ref lhs, ref lhs_ty)), (rhs, rhs_ty)| {
                let evaluated = T::from_parts(
                    Value::new(ValueKind::Const(lhs.clone()), lhs_ty.clone()),
                    Value::new(ValueKind::Const(rhs.clone()), rhs_ty.clone()),
                    dest,
                )
                .eval();

                (id, evaluated)
            },
        );

    both_const
        .concat(&lhs_const)
        .concat(&rhs_const)
        .concat(&no_const)
}
