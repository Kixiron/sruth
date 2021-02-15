use crate::{
    dataflow::operators::CollectUsages,
    repr::{Constant, InstId, Instruction, InstructionExt, Type, Value, VarId},
};
use differential_dataflow::{
    difference::{Monoid, Semigroup},
    lattice::Lattice,
    operators::{consolidate::ConsolidateStream, Consolidate, Join, Reduce},
    Collection, ExchangeData,
};
use std::ops::{Mul as OpsMul, Neg};
use timely::dataflow::Scope;

/// Promote constant values to an inline position, turing operations that depend on
/// statically known values via variables into operations with constant immediates
///
/// ```
/// foo := uint 3
/// add uint foo, uint 10
///
/// ; transformed to
///
/// foo := uint 3 ; Note that this transformation does
///               ; not remove the now-redundant assignments
/// add uint 3, uint 10
/// ```
///
pub(super) fn promote_constants<S, R>(
    instructions: &Collection<S, (InstId, Instruction), R>,
    constants: &Collection<S, (VarId, (Constant, Type)), R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Monoid + ExchangeData + OpsMul<Output = R> + Neg<Output = R> + From<i8>,
{
    // Collect all variables that are used and the instructions that use them,
    // this allows us to ignore instructions which don't actually have any
    // variable args
    let used_vars = instructions
        .collect_usages()
        .map(|(var, inst)| (var.var, inst));

    // Collect all used constant values and aggregate them into
    // `(InstId, Vec<(VarId, Value)>)` with the target instruction and a
    // vec of variables to-be-replaced and their replacement values
    let replacements = used_vars
        .join_map(&constants, |&var, &inst, (constant, ty)| {
            (inst, (var, Value::new(constant.clone().into(), ty.clone())))
        })
        .consolidate()
        .reduce(|_, replacements, output| {
            let replacements: Vec<_> = replacements.iter().map(|&(var, _)| var.clone()).collect();
            output.push((replacements, R::from(1)));
        });

    // Promote instructions using the collected constants
    let promoted_instructions =
        instructions.join_map(&replacements, |&inst_id, inst, replacements| {
            let mut inst = inst.clone();
            inst.replace_all_uses(replacements);

            (inst_id, inst)
        });

    // Debug the pre and post promotion states of all the instructions we just promoted
    if cfg!(debug_assertions) {
        instructions
            .join(&promoted_instructions)
            .join(&replacements)
            .consolidate()
            .inspect(|((inst, ((before, after), replacements)), _, _)| {
                tracing::trace!(
                    before = ?before,
                    after = ?after,
                    "promoted {} used values for {:?} to constants",
                    replacements.len(),
                    inst,
                );
            });
    }

    // Return the whole set of instructions, replacing the newly promoted ones
    instructions
        .antijoin(&promoted_instructions.map(|(id, _)| id))
        .concat(&promoted_instructions)
        .consolidate_stream()
}
