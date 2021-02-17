mod block;
mod context;
mod error;
mod function;

pub use block::BasicBlockBuilder;
pub use context::Context;
pub use error::{BuildResult, BuilderError};
pub use function::FunctionBuilder;

use crate::{
    builder::function::{DeferredFunction, IncompleteFunction},
    dataflow::InputManager,
    repr::{
        basic_block::BasicBlockDesc,
        function::{FunctionDesc, Metadata},
        instruction::Call,
        BasicBlock, FuncId, Function, Ident, InstId, Instruction, InstructionExt, Type,
    },
};
use abomonation_derive::Abomonation;
use differential_dataflow::{difference::Semigroup, lattice::Lattice};
use std::{mem, sync::Arc, thread};
use timely::progress::Timestamp;

pub struct Builder {
    blocks: Vec<BasicBlockDesc>,
    functions: Vec<FunctionDesc>,
    instructions: Vec<(InstId, Instruction)>,
    effect_edges: Vec<EffectEdge>,
    context: Arc<Context>,
    finished: bool,
}

// Public API
// TODO: Allocate function
impl Builder {
    pub fn function<T, F>(&mut self, return_ty: T, build: F) -> BuildResult<FuncId>
    where
        T: Into<Type>,
        F: FnOnce(&mut FunctionBuilder<'_>) -> BuildResult<()>,
    {
        self.build_function(None, None, return_ty.into(), build)
    }

    pub fn named_function<N, T, F>(
        &mut self,
        name: N,
        return_ty: T,
        build: F,
    ) -> BuildResult<FuncId>
    where
        N: AsRef<str>,
        T: Into<Type>,
        F: FnOnce(&mut FunctionBuilder<'_>) -> BuildResult<()>,
    {
        let name = Ident::new(self.context.interner.get_or_intern(name));
        self.build_function(None, Some(name), return_ty.into(), build)
    }

    pub fn allocate_function<T>(&mut self, return_ty: T) -> DeferredFunction
    where
        T: Into<Type>,
    {
        DeferredFunction::new(self.context.function_id(), None, return_ty.into())
    }

    pub fn allocate_named_function<N, T>(&mut self, name: N, return_ty: T) -> DeferredFunction
    where
        N: AsRef<str>,
        T: Into<Type>,
    {
        let name = Ident::new(self.context.interner.get_or_intern(name));
        DeferredFunction::new(self.context.function_id(), Some(name), return_ty.into())
    }

    pub fn resume_building<F>(
        &mut self,
        mut function: DeferredFunction,
        build: F,
    ) -> BuildResult<FuncId>
    where
        F: FnOnce(&mut FunctionBuilder<'_>) -> BuildResult<()>,
    {
        debug_assert!(
            !function.finished,
            "a deferred function was somehow finished twice",
        );
        function.finished = true;

        self.build_function(
            Some(function.id),
            function.name,
            mem::replace(&mut function.return_ty, Type::Unit),
            build,
        )
    }

    pub fn materialize(&self) -> impl Iterator<Item = Function> + '_ {
        self.functions.iter().map(move |func| Function {
            name: func.name,
            id: func.id,
            params: func.params.clone(),
            ret_ty: func.ret_ty.clone(),
            entry: func.entry,
            basic_blocks: func
                .basic_blocks
                .iter()
                .map(|&id| {
                    let block = self.blocks.iter().find(|block| block.id == id).unwrap();

                    BasicBlock {
                        name: block.name,
                        id: block.id,
                        instructions: block
                            .instructions
                            .iter()
                            .map(|&id| {
                                self.instructions
                                    .iter()
                                    .find(|&&(inst, _)| inst == id)
                                    .unwrap()
                                    .1
                                    .clone()
                            })
                            .collect(),
                        terminator: block.terminator.clone(),
                    }
                })
                .collect(),
            metadata: Metadata::default(),
        })
    }

    /// Discards a [`Builder`] without adding its contents
    pub fn discard(mut self) {
        if cfg!(debug_assertions) && self.finished {
            self.finished = true;
            panic!("discarded a builder twice??");
        }

        self.finished = true;
        tracing::trace!("discarded a builder");
    }

    pub fn finish<T, R>(mut self, input: &mut InputManager<T, R>, time: T) -> BuildResult<()>
    where
        T: Timestamp + Lattice + Clone,
        R: Semigroup + From<i8>,
    {
        if cfg!(debug_assertions) && self.finished {
            self.finished = true;
            panic!("finished a builder twice??");
        }

        self.finished = true;
        tracing::trace!("finished a builder, giving all data to the dataflow");

        let mut needs_fixup = Vec::new();
        for (_id, inst) in self.instructions.iter_mut() {
            if let Instruction::Call(Call {
                dest, func, ret_ty, ..
            }) = inst
            {
                if ret_ty.is_infer() {
                    let ty = self
                        .functions
                        .iter()
                        .find(|meta| meta.id == *func)
                        .expect("missing function")
                        .ret_ty
                        .clone();

                    *ret_ty = ty.clone();
                    needs_fixup.push((*dest, ty));
                }
            }
        }

        for (id, ty) in needs_fixup {
            for value in self
                .instructions
                .iter_mut()
                .flat_map(|(_, inst)| inst.used_values_mut())
                .filter(|val| val.is_var())
            {
                if value.as_var().unwrap() == id && value.ty == Type::Infer {
                    value.ty = ty.clone();
                }
            }
        }

        for function in self.functions.drain(..) {
            input
                .functions
                .update_at((function.id, function), time.clone(), R::from(1));
        }

        for block in self.blocks.drain(..) {
            input
                .basic_blocks
                .update_at((block.id, block), time.clone(), R::from(1));
        }

        for instruction in self.instructions.drain(..) {
            input
                .instructions
                .update_at(instruction, time.clone(), R::from(1));
        }

        // TODO: Effect edges

        Ok(())
    }
}

// Private API
impl Builder {
    fn new(context: Arc<Context>) -> Self {
        Self {
            blocks: Vec::with_capacity(1024),
            functions: Vec::with_capacity(512),
            instructions: Vec::with_capacity(2048),
            effect_edges: Vec::with_capacity(1024),
            context,
            finished: false,
        }
    }

    fn build_function<F>(
        &mut self,
        id: Option<FuncId>,
        name: Option<Ident>,
        return_ty: Type,
        build: F,
    ) -> BuildResult<FuncId>
    where
        F: FnOnce(&mut FunctionBuilder<'_>) -> BuildResult<()>,
    {
        let span = tracing::trace_span!("building function");
        span.in_scope(|| {
            let id = id.unwrap_or_else(|| self.context.function_id());

            if let Some(name) = name {
                tracing::trace!(
                    "started building function {} ({:?})",
                    self.context.interner.resolve(&name.0),
                    id,
                );
            } else {
                tracing::trace!("started building nameless function {:?}", id);
            }

            let meta = IncompleteFunction::new(name, id, Vec::new(), return_ty, None, Vec::new());
            let mut builder = FunctionBuilder::new(
                meta,
                &mut self.blocks,
                &mut self.functions,
                &mut self.instructions,
                &mut self.effect_edges,
                &*self.context,
            );

            build(&mut builder)?;
            builder.finish()
        })
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        if !thread::panicking() && !self.finished {
            panic!("dropped a builder without finishing construction or discarding it");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct EffectEdge {
    pub from: InstId,
    pub to: InstId,
}
