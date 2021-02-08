use crate::{
    dataflow::InputManager,
    repr::{
        function::FunctionMeta, BasicBlockId, FuncId, InstId, Instruction, InstructionExt,
        Terminator,
    },
};
use differential_dataflow::{
    algorithms::graphs::propagate::propagate,
    difference::{Monoid, Semigroup},
    lattice::Lattice,
    operators::{arrange::Arranged, Consolidate, Join, JoinCore},
    trace::TraceReader,
    Collection, ExchangeData,
};
use std::{
    iter,
    ops::{Mul, Neg},
};
use timely::dataflow::Scope;

type ReducedCode<S, R> = (
    Collection<S, (InstId, Instruction), R>,
    Collection<S, (FuncId, BasicBlockId), R>,
);

pub fn dead_code<S, R, A1, A2>(
    scope: &mut S,
    inputs: &mut InputManager<S::Timestamp, R>,
    instructions: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
) -> ReducedCode<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = InstId, Val = Instruction, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    let span = tracing::debug_span!("dead code elimination");
    span.in_scope(|| {
        let block_trace = inputs.basic_block_trace.import(scope);
        let basic_blocks = block_trace.flat_map_ref(|&block, meta| {
            meta.instructions
                .clone()
                .into_iter()
                .map(move |inst| (inst, block))
        });
        let basic_block_ids = block_trace.as_collection(|&block, _| block);

        let culled_instructions = eliminate_unused_assigns(
            scope,
            instructions,
            terminators,
            &basic_blocks,
            &basic_block_ids,
        );

        let function_blocks = inputs.function_trace.import(scope);
        let culled_blocks =
            eliminate_unreachable_blocks(scope, &function_blocks, terminators, &basic_block_ids);

        (culled_instructions, culled_blocks)
    })
}

fn eliminate_unused_assigns<S, R, A1, A2>(
    scope: &mut S,
    instructions: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
    basic_blocks: &Collection<S, (InstId, BasicBlockId), R>,
    basic_block_ids: &Collection<S, BasicBlockId, R>,
) -> Collection<S, (InstId, Instruction), R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = InstId, Val = Instruction, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    scope.region_named("eliminate unused assignments", |region| {
        let (instructions, terminators, basic_blocks, basic_block_ids) = (
            instructions.enter(region),
            terminators.enter(region),
            basic_blocks.enter(region).consolidate(),
            basic_block_ids.enter(region),
        );

        let used_variables = basic_blocks
            .join_core(&instructions, |_inst_id, &block_id, inst| {
                inst.used_vars().map(move |var| (block_id, var))
            })
            .concat(
                &terminators
                    .semijoin(&basic_block_ids)
                    .flat_map(|(block_id, term)| term.used_vars().map(move |var| (block_id, var))),
            );

        let declared_variables = basic_blocks
            .join_core(&instructions, |&inst_id, &block_id, inst| {
                iter::once(((block_id, inst.dest()), inst_id))
            });

        let unused_vars = declared_variables
            .antijoin(&used_variables)
            .map(|((_block_id, _dest), inst_id)| inst_id)
            .inspect(|(inst, _, _)| tracing::trace!("removing unused assign {:?}", inst));

        instructions
            .as_collection(|&id, inst| (id, inst.clone()))
            .antijoin(&unused_vars)
            .leave_region()
    })
}

fn eliminate_unreachable_blocks<S, R, A1, A2>(
    scope: &mut S,
    function_blocks: &Arranged<S, A1>,
    terminators: &Arranged<S, A2>,
    basic_block_ids: &Collection<S, BasicBlockId, R>,
) -> Collection<S, (FuncId, BasicBlockId), R>
where
    S: Scope,
    S::Timestamp: Lattice + Clone,
    R: Semigroup + Monoid + ExchangeData + Mul<Output = R> + Neg<Output = R> + From<i8>,
    A1: TraceReader<Key = FuncId, Val = FunctionMeta, R = R, Time = S::Timestamp> + Clone + 'static,
    A2: TraceReader<Key = BasicBlockId, Val = Terminator, R = R, Time = S::Timestamp>
        + Clone
        + 'static,
{
    scope.region_named("eliminate unreachable blocks", |region| {
        let function_blocks = function_blocks.enter(region);

        let roots =
            function_blocks.flat_map_ref(|&func_id, meta| iter::once((meta.entry, func_id)));
        let edges = basic_block_ids
            .enter(region)
            .map(|block| (block, ()))
            .join_core(&terminators.enter(region), |&block, (), term| {
                term.succ().map(move |succ| (block, succ))
            });

        let reachable_blocks =
            propagate(&edges, &roots).map(|(block_id, func_id)| (func_id, block_id));

        function_blocks
            .flat_map_ref(|&func, meta| {
                meta.basic_blocks
                    .clone()
                    .into_iter()
                    .map(move |block| ((func, block), ()))
            })
            .semijoin(&reachable_blocks)
            .map(|(block, ())| block)
            .leave_region()
    })
}
