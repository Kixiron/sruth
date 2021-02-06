//! Tools for verifying the well-formedness of IR

use crate::{
    dataflow::operators::{
        CollectDeclarations, CollectUsages, CollectValues, CollectVariableTypes, CountExt,
        FilterMap, FilterSplit,
    },
    repr::{
        basic_block::BasicBlockMeta,
        function::FunctionMeta,
        instruction::{BinaryOp, Bitcast},
        BasicBlockId, Cast, Constant, FuncId, InstId, Instruction, InstructionExt, Type, ValueKind,
        VarId,
    },
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Join, JoinCore, Threshold},
    Collection, ExchangeData,
};
use std::{iter, ops::Mul};
use timely::dataflow::Scope;

pub fn verify<S, R>(
    scope: &mut S,
    instructions: &Collection<S, (InstId, Instruction), R>,
    basic_blocks: &Collection<S, (BasicBlockId, BasicBlockMeta), R>,
    functions: &Collection<S, (FuncId, FunctionMeta), R>,
) -> Collection<S, ValidityError, R>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    R: Semigroup + Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    let used_variables = instructions.collect_usages();
    let declared_variables = instructions.collect_declarations();

    let undeclared_variables = used_variables.antijoin(&declared_variables.map(|(var, _)| var));

    #[allow(clippy::suspicious_map)]
    let redeclarations = declared_variables
        .map(|(var, _)| var)
        .count_core::<R>()
        .filter(|(_, count)| count > &R::from(1))
        .join_map(&declared_variables, |&var, _count, &inst| (inst, var));

    let block_ids = basic_blocks.map(|(block, _)| block);
    let targeted_blocks = basic_blocks
        .flat_map(|(block, meta)| meta.terminator.succ().map(move |target| (target, block)));

    let undeclared_blocks = targeted_blocks
        .antijoin(&block_ids)
        .map(|(target, source)| (source, target));

    let blocks_for_functions = functions
        .flat_map(|(func, meta)| {
            meta.basic_blocks
                .into_iter()
                .map(move |block| (block, func))
        })
        .arrange_by_key();

    let cross_function_jumps = targeted_blocks
        .join_core(
            &blocks_for_functions,
            |&target_block, &source_block, &target_func| {
                iter::once((source_block, (target_block, target_func)))
            },
        )
        .join_core(
            &blocks_for_functions,
            |&source_block, &(target_block, target_func), &source_func| {
                if target_func != source_func {
                    Some((source_block, source_func, target_block, target_func))
                } else {
                    None
                }
            },
        );

    let variable_types = instructions.collect_var_types();
    let malformed_variables = undeclared_variables
        .map(|(var, _)| var)
        .concat(&redeclarations.map(|(_, var)| var));

    // Correct the types of malformed variables by giving them an error type (`None`)
    let mut variables = variable_types
        .semijoin(&malformed_variables)
        .map(|(var, _)| (var, None));

    variables = variables.concat(
        &variable_types
            .antijoin(&malformed_variables)
            .map(|(var, ty)| (var, Some(ty))),
    );

    let (mut inferred_types, mut incorrect_variable_types) =
        instructions.filter_split(|(_id, inst)| {
            if let Some(op) = inst.cast::<BinaryOp>() {
                let (lhs, rhs) = op.operands();
                let dest = op.dest();

                if lhs.ty() == rhs.ty() {
                    (Some((dest, lhs.ty)), None)
                } else {
                    (None, Some((dest, lhs.ty, rhs.ty)))
                }
            } else {
                (None, None)
            }
        });

    incorrect_variable_types =
        incorrect_variable_types.concat(&variables.join(&inferred_types).filter_map(
            |(var, (expected, real))| {
                // Ignore error types
                expected
                    .map(|expected| {
                        if expected != real {
                            Some((var, expected, real))
                        } else {
                            None
                        }
                    })
                    .flatten()
            },
        ));

    let (bitcast_infers, invalid_bitcast) = instructions.filter_split(|(id, inst)| {
        if let Some(bitcast) = inst.cast::<Bitcast>() {
            if bitcast.is_valid() {
                (Some((bitcast.dest(), bitcast.dest_ty)), None)
            } else {
                (None, Some((id, bitcast.types())))
            }
        } else {
            (None, None)
        }
    });
    inferred_types = inferred_types.concat(&bitcast_infers);

    incorrect_variable_types =
        incorrect_variable_types.concat(&variable_types.join(&inferred_types).filter_map(
            |(var, (expected, real))| {
                if expected != real {
                    Some((var, expected, real))
                } else {
                    None
                }
            },
        ));

    let (constants, variables) =
        instructions
            .collect_values()
            .filter_split(|(id, value)| match value.value {
                ValueKind::Const(constant) => (Some((id, constant, value.ty)), None),
                ValueKind::Var(var) => (None, Some((var, value.ty))),
            });

    let invalid_constant_types = constants.filter(|(_, constant, ty)| &constant.ty() != ty);

    incorrect_variable_types =
        incorrect_variable_types.concat(&variables.join(&variable_types).filter_map(
            |(var, (expected, got))| {
                if expected != got {
                    Some((var, expected, got))
                } else {
                    None
                }
            },
        ));

    concat_validity_errors(
        scope,
        &undeclared_variables,
        &redeclarations,
        &undeclared_blocks,
        &cross_function_jumps,
        &incorrect_variable_types,
        &invalid_bitcast,
        &invalid_constant_types,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum ValidityError {
    UndeclaredVariable {
        inst: InstId,
        var: VarId,
    },
    Redeclaration {
        inst: InstId,
        var: VarId,
    },
    UndeclaredBlock {
        source: BasicBlockId,
        target: BasicBlockId,
    },
    CrossFunctionJump {
        source_block: BasicBlockId,
        source_func: FuncId,
        target_block: BasicBlockId,
        target_func: FuncId,
    },
    VariableTypeMismatch {
        var: VarId,
        expected: Type,
        got: Type,
    },
    InvalidBitcast {
        inst: InstId,
        source: Type,
        dest: Type,
    },
    ConstantTypeMismatch {
        inst: InstId,
        constant_ty: Type,
        declared_as: Type,
    },
}

#[allow(clippy::too_many_arguments)]
fn concat_validity_errors<S, R>(
    scope: &mut S,
    undeclared_variables: &Collection<S, (VarId, InstId), R>,
    redeclarations: &Collection<S, (InstId, VarId), R>,
    undeclared_blocks: &Collection<S, (BasicBlockId, BasicBlockId), R>,
    cross_function_jumps: &Collection<S, (BasicBlockId, FuncId, BasicBlockId, FuncId), R>,
    incorrect_variable_types: &Collection<S, (VarId, Type, Type), R>,
    invalid_bitcast: &Collection<S, (InstId, (Type, Type)), R>,
    invalid_constant_types: &Collection<S, (InstId, Constant, Type), R>,
) -> Collection<S, ValidityError, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + From<i8>,
{
    scope.region_named("concat validity errors", |region| {
        undeclared_variables
            .enter_region(region)
            .map(|(var, inst)| ValidityError::UndeclaredVariable { var, inst })
            .concat(
                &redeclarations
                    .enter_region(region)
                    .map(|(inst, var)| ValidityError::Redeclaration { inst, var }),
            )
            .concat(
                &undeclared_blocks
                    .enter_region(region)
                    .map(|(source, target)| ValidityError::UndeclaredBlock { source, target }),
            )
            .concat(&cross_function_jumps.enter_region(region).map(
                |(source_block, source_func, target_block, target_func)| {
                    ValidityError::CrossFunctionJump {
                        source_block,
                        source_func,
                        target_block,
                        target_func,
                    }
                },
            ))
            .concat(
                &incorrect_variable_types
                    .enter_region(region)
                    .map(|(var, expected, got)| ValidityError::VariableTypeMismatch {
                        var,
                        expected,
                        got,
                    }),
            )
            .concat(
                &invalid_bitcast
                    .enter_region(region)
                    .map(|(inst, (source, dest))| ValidityError::InvalidBitcast {
                        inst,
                        source,
                        dest,
                    }),
            )
            .concat(&invalid_constant_types.enter_region(region).map(
                |(inst, constant, declared_as)| ValidityError::ConstantTypeMismatch {
                    inst,
                    constant_ty: constant.ty(),
                    declared_as,
                },
            ))
            .distinct_core()
            .leave_region()
    })
}
