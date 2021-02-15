use crate::{
    dataflow::operators::{FilterMap, InspectExt},
    repr::{
        instruction::{Add, BinopExt, Div, Mul, Sub},
        Cast, Constant, InstId, Instruction, RawCast, Type, Value, ValueKind, VarId,
    },
};
use differential_dataflow::{
    difference::Semigroup, lattice::Lattice, operators::Join, AsCollection, Collection, Data,
    ExchangeData,
};
use std::{convert::TryInto, ops::Mul as OpsMul};
use timely::dataflow::{operators::Partition, Scope, Stream};

// TODO: This sucks
pub(super) fn evaluate_binary_op<S, T, R>(
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
        .debug_inspect(|((id, inst), _, _)| tracing::trace!(inst = ?inst, "folded {:?}", id));

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

// TODO: These suck
pub(super) trait Evaluate {
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
