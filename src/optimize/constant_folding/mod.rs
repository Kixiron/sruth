mod evaluation;
mod promotion;

use crate::{
    dataflow::operators::{CollectCastable, CollectUsages, FilterMap, FilterSplit, InspectExt},
    repr::{
        instruction::{Add, Assign, Div, Mul, Sub},
        terminator::Return,
        BasicBlockId, Cast, Constant, InstId, Instruction, InstructionExt, Terminator, Type, Value,
        ValueKind, VarId,
    },
};
use differential_dataflow::{
    difference::{Abelian, Multiply},
    lattice::Lattice,
    operators::{consolidate::ConsolidateStream, Consolidate, Join},
    Collection, ExchangeData,
};
use timely::dataflow::Scope;

type ConstProp<S, R> = (
    Collection<S, (InstId, Instruction), R>,
    Collection<S, (BasicBlockId, Terminator), R>,
);

pub fn constant_folding<S, R>(
    _scope: &mut S,
    instructions: &Collection<S, (InstId, Instruction), R>,
    terminators: &Collection<S, (BasicBlockId, Terminator), R>,
) -> ConstProp<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    let span = tracing::debug_span!("constant folding");
    span.in_scope(|| {
        let constants = instructions.filter_map(|(_, inst)| {
            inst.cast().and_then(|Assign { value, dest, .. }| {
                let (value, ty) = value.split();
                value.into_const().map(|constant| (dest, (constant, ty)))
            })
        });

        // Evaluate binary operations
        let evaluated_binops =
            evaluation::evaluate_binary_op::<_, Add, _>(&instructions, &constants)
                .concat(&evaluation::evaluate_binary_op::<_, Sub, _>(
                    &instructions,
                    &constants,
                ))
                .concat(&evaluation::evaluate_binary_op::<_, Mul, _>(
                    &instructions,
                    &constants,
                ))
                .concat(&evaluation::evaluate_binary_op::<_, Div, _>(
                    &instructions,
                    &constants,
                ));

        // Replace the instructions we've modified
        let new_instructions = instructions
            .antijoin(&evaluated_binops.map(|(id, _)| id))
            .concat(&evaluated_binops);

        // Add the newly derived constants to the stream of constants
        let new_constants = new_instructions
            .collect_castable::<Assign>()
            .filter_map(|(_, assign)| {
                let (value, ty) = assign.value.split();
                let dest = assign.dest;

                value
                    .into_const()
                    .map(move |constant| (dest, (constant, ty)))
            })
            .consolidate();

        let promoted_instructions = promotion::promote_constants(&new_instructions, &new_constants);

        let folded_terminators =
            propagate_to_terminators(&terminators, &new_constants).consolidate();

        let (eliminated_instructions, eliminated_terminators) = eliminate_redundant_assigns(
            &promoted_instructions.consolidate(),
            &folded_terminators.consolidate(),
        );

        (eliminated_instructions, eliminated_terminators)
    })
}

fn propagate_to_terminators<S, R>(
    terminators: &Collection<S, (BasicBlockId, Terminator), R>,
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
) -> Collection<S, (BasicBlockId, Terminator), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R>,
{
    let returns = terminators.filter_map(|(id, term)| {
        term.into_return()
            .and_then(|ret| ret.value)
            .and_then(|val| val.as_var().map(|var| (var, val.ty)))
            .map(|(var, ty)| (var, (id, ty)))
    });

    let folded_returns =
        returns.join_map(constants, |_, &(id, ref ret_ty), (constant, const_ty)| {
            debug_assert_eq!(ret_ty, const_ty);

            (
                id,
                Terminator::Return(Return {
                    value: Some(Value::new(
                        ValueKind::Const(constant.clone()),
                        const_ty.clone(),
                    )),
                }),
            )
        });

    let branches = terminators
        .filter_map(|(id, term)| term.into_branch().map(move |br| (id, br)))
        .consolidate_stream();
    let (branch_const, branch_vars) =
        branches.filter_split(|(id, br)| match br.cond.value.clone() {
            ValueKind::Const(constant) => (Some((id, (constant, br))), None),
            ValueKind::Var(var) => (None, Some((var, (id, br)))),
        });

    let const_branches = branch_const
        .map(|(id, (constant, br))| {
            if constant.as_bool().unwrap() {
                (id, Terminator::Jump(br.if_true.block))
            } else {
                (id, Terminator::Jump(br.if_false.block))
            }
        })
        .debug_inspect(|((id, term), _, _)| {
            tracing::trace!("folded constant branch at {:?} into {:?}", id, term);
        });

    let var_branches = branch_vars
        .join_map(&constants, |_var, &(id, ref br), (constant, _ty)| {
            if constant.as_bool().unwrap() {
                (id, Terminator::Jump(br.if_true.block))
            } else {
                (id, Terminator::Jump(br.if_false.block))
            }
        })
        .debug_inspect(|((id, term), _, _)| {
            tracing::trace!("folded constant var branch at {:?} into {:?}", id, term);
        });

    let identical_branches = branches
        .antijoin(&const_branches.concat(&var_branches).map(|(id, _)| id))
        .filter(|(_, br)| br.if_true == br.if_false)
        .map(|(id, br)| (id, Terminator::Jump(br.if_true.block)))
        .debug_inspect(|((id, term), _, _)| {
            tracing::trace!("folded identical branch at {:?} into {:?}", id, term);
        });

    let folded_terminators = folded_returns
        .concat(&const_branches)
        .concat(&var_branches)
        .concat(&identical_branches);

    terminators
        .antijoin(&folded_terminators.map(|(id, _)| id))
        .concat(&folded_terminators)
}

// TODO: This should be a general function on `Program`
fn eliminate_redundant_assigns<S, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    terminators: &Collection<S, (BasicBlockId, Terminator), R>,
) -> ConstProp<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R>,
{
    let redundant_assignments = instructions
        .filter_map(|(id, inst)| {
            let var = inst
                .cast::<Assign>()
                .and_then(|assign| assign.value.as_var().map(|var| (assign.dest, var)));
            var.map(|(dest, var)| (id, (dest, var)))
        })
        .debug_inspect(|((id, (dest, var)), _, _)| {
            tracing::trace!(
                "eliminating redundant assignment at {:?}, rewriting refs from {:?} to {:?}",
                id,
                dest,
                var,
            );
        });

    let use_sites = instructions
        .collect_usages()
        .map(|(var, inst)| (var.var, (var.ty, inst)))
        .join_map(
            &redundant_assignments.map(|(_, (from, to))| (from, to)),
            |&from, &(ref ty, inst), &to| (inst, (from, Value::new(to.into(), ty.clone()))),
        );

    let rewritten_instructions = instructions
        .join_map(&use_sites, |&id, inst, &(from, ref to)| {
            let mut inst = inst.clone();
            inst.replace_uses(from, to);

            (id, inst)
        })
        .debug_inspect(|((inst, _), _, _)| tracing::trace!("rewrote variables for {:?}", inst));

    let instructions = instructions
        .antijoin(&redundant_assignments.map(|(id, _)| id))
        .antijoin(&rewritten_instructions.map(|(id, _)| id))
        .concat(&rewritten_instructions);

    let terminator_usages = terminators
        .flat_map(|(block, term)| term.used_vars().into_iter().map(move |var| (var, block)))
        .join_map(
            &redundant_assignments.map(|(_, (from, to))| (from, to)),
            |&from, &block, &to| (block, (from, to)),
        );

    let rewritten_terminators = terminators
        .join_map(&terminator_usages, |&block, term, &(from, to)| {
            let mut term = term.clone();
            term.replace_uses(from, to);

            (block, term)
        })
        .debug_inspect(|((block, _), _, _)| tracing::trace!("rewrote variables for {:?}", block));

    let terminators = terminators
        .antijoin(&rewritten_terminators.map(|(id, _)| id))
        .concat(&rewritten_terminators);

    (instructions, terminators)
}
