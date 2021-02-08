use crate::repr::{
    basic_block::BasicBlockMeta, function::FunctionMeta, BasicBlockId, FuncId, InstId, Instruction,
};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    input::{Input, InputSession},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, TraceAgent},
        Threshold,
    },
    trace::implementations::ord::OrdValSpine,
    ExchangeData,
};
use std::fmt::Debug;
use timely::{dataflow::Scope, progress::Timestamp};

pub struct InputManager<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup,
{
    pub instructions: InputSession<T, (InstId, Instruction), R>,
    pub instruction_trace: TraceAgent<OrdValSpine<InstId, Instruction, T, R>>,

    pub basic_blocks: InputSession<T, (BasicBlockId, BasicBlockMeta), R>,
    pub basic_block_trace: TraceAgent<OrdValSpine<BasicBlockId, BasicBlockMeta, T, R>>,

    pub functions: InputSession<T, (FuncId, FunctionMeta), R>,
    pub function_trace: TraceAgent<OrdValSpine<FuncId, FunctionMeta, T, R>>,
}

impl<T, R> InputManager<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + ExchangeData,
{
    pub fn new<S>(scope: &mut S) -> Self
    where
        S: Scope<Timestamp = T> + Input,
        R: Abelian + From<i8>,
    {
        tracing::info!("created a new input manager");

        let (instructions, instruction_trace) = scope.new_collection::<(InstId, Instruction), R>();
        let (basic_blocks, basic_block_trace) =
            scope.new_collection::<(BasicBlockId, BasicBlockMeta), R>();
        let (functions, function_trace) = scope.new_collection::<(FuncId, FunctionMeta), R>();

        // TODO: Exchange more intelligently to put all blocks & instructions for
        //       a given function onto the same worker
        let instruction_trace = instruction_trace.distinct_core().arrange_by_key().trace;
        let basic_block_trace = basic_block_trace.distinct_core().arrange_by_key().trace;
        let function_trace = function_trace.distinct_core().arrange_by_key().trace;

        Self {
            instructions,
            instruction_trace,
            basic_blocks,
            basic_block_trace,
            functions,
            function_trace,
        }
    }

    pub fn advance_to(&mut self, time: T)
    where
        T: Debug + Clone,
    {
        tracing::info!("advancing to timestamp {:?}", time);

        self.instructions.advance_to(time.clone());
        self.instructions.flush();

        self.basic_blocks.advance_to(time.clone());
        self.basic_blocks.flush();

        self.functions.advance_to(time);
        self.functions.flush();
    }

    pub fn time(&self) -> &T {
        debug_assert_eq!(self.instructions.time(), self.basic_blocks.time());
        debug_assert_eq!(self.instructions.time(), self.functions.time());

        self.instructions.time()
    }
}
