use crate::repr::{
    basic_block::BasicBlockMeta, function::FunctionMeta, BasicBlockId, FuncId, InstId, Instruction,
};
use differential_dataflow::{
    difference::Semigroup,
    input::{Input, InputSession},
    lattice::Lattice,
    operators::arrange::{ArrangeByKey, TraceAgent},
    trace::implementations::ord::OrdValSpine,
    ExchangeData,
};
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
    {
        let (instructions, instruction_trace) = scope.new_collection();
        let (basic_blocks, basic_block_trace) = scope.new_collection();
        let (functions, function_trace) = scope.new_collection();

        Self {
            instructions,
            instruction_trace: instruction_trace
                .inspect(|x| println!("Input: {:?}", x))
                .arrange_by_key()
                .trace,
            basic_blocks,
            basic_block_trace: basic_block_trace
                .inspect(|x| println!("Input: {:?}", x))
                .arrange_by_key()
                .trace,
            functions,
            function_trace: function_trace
                .inspect(|x| println!("Input: {:?}", x))
                .arrange_by_key()
                .trace,
        }
    }

    pub fn advance_to(&mut self, time: T)
    where
        T: Clone,
    {
        self.instructions.advance_to(time.clone());
        self.instructions.flush();

        self.basic_blocks.advance_to(time.clone());
        self.basic_blocks.flush();

        self.functions.advance_to(time);
        self.functions.flush();
    }
}
