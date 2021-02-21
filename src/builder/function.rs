use crate::{
    builder::{
        block::{DeferredBasicBlock, IncompleteBasicBlock},
        BasicBlockBuilder, BuildResult, BuilderError, Context, EffectEdge,
    },
    repr::{
        basic_block::BasicBlockDesc, function::FunctionDesc, BasicBlockId, FuncId, Ident, InstId,
        Instruction, Type, TypedVar,
    },
};
use std::{convert::TryInto, mem, ops::Deref, thread};

#[derive(Debug)]
pub struct FunctionBuilder<'a> {
    pub(super) meta: IncompleteFunction,
    pub(super) blocks: &'a mut Vec<BasicBlockDesc>,
    pub(super) functions: &'a mut Vec<FunctionDesc>,
    pub(super) instructions: &'a mut Vec<(InstId, Instruction)>,
    pub(super) effect_edges: &'a mut Vec<EffectEdge>,
    pub(super) context: &'a Context,
    pub(super) finished: bool,
}

// TODO: Allocate block
impl<'a> FunctionBuilder<'a> {
    pub const fn has_entry(&self) -> bool {
        self.meta.entry.is_some()
    }

    pub fn basic_block<F>(&mut self, build: F) -> BuildResult<BasicBlockId>
    where
        F: FnOnce(&mut BasicBlockBuilder<'_, '_>) -> BuildResult<()>,
    {
        self.build_basic_block(None, None, build)
    }

    pub fn named_basic_block<N, F>(&mut self, name: N, build: F) -> BuildResult<BasicBlockId>
    where
        N: AsRef<str>,
        F: FnOnce(&mut BasicBlockBuilder<'_, '_>) -> BuildResult<()>,
    {
        let name = Ident::new(self.context.interner.get_or_intern(name));

        self.build_basic_block(None, Some(name), build)
    }

    pub fn allocate_basic_block(&mut self) -> DeferredBasicBlock {
        DeferredBasicBlock::new(self.context.block_id(), None)
    }

    pub fn allocate_named_basic_block<N>(&mut self, name: N) -> DeferredBasicBlock
    where
        N: AsRef<str>,
    {
        let name = Ident::new(self.context.interner.get_or_intern(name));
        DeferredBasicBlock::new(self.context.block_id(), Some(name))
    }

    pub fn resume_building<F>(
        &mut self,
        mut block: DeferredBasicBlock,
        build: F,
    ) -> BuildResult<BasicBlockId>
    where
        F: FnOnce(&mut BasicBlockBuilder<'_, '_>) -> BuildResult<()>,
    {
        debug_assert!(
            !block.finished,
            "a deferred block was somehow finished twice",
        );
        block.finished = true;

        self.build_basic_block(Some(block.id), block.name, build)
    }

    pub const fn name(&self) -> Option<Ident> {
        self.meta.name
    }

    pub fn set_name<N>(&mut self, name: N) -> Option<Ident>
    where
        N: AsRef<str>,
    {
        self.meta
            .name
            .replace(Ident::new(self.context.interner.get_or_intern(name)))
    }

    pub const fn entry(&self) -> Option<BasicBlockId> {
        self.meta.entry
    }

    pub fn set_entry(&mut self, entry: BasicBlockId) -> Option<BasicBlockId> {
        self.meta.entry.replace(entry)
    }

    pub fn param<T>(&mut self, ty: T) -> TypedVar
    where
        T: Into<Type>,
    {
        let (id, ty) = (self.context.var_id(), ty.into());
        self.meta.params.push(TypedVar::new(id, ty.clone()));

        TypedVar::new(id, ty)
    }

    pub fn params<I>(&mut self, types: I) -> Vec<TypedVar>
    where
        I: IntoIterator<Item = Type>,
    {
        let mut ids = Vec::with_capacity(10);
        for ty in types {
            let id = self.context.var_id();
            self.meta.params.push(TypedVar::new(id, ty.clone()));
            ids.push(TypedVar::new(id, ty));
        }

        ids
    }

    pub const fn func_id(&self) -> FuncId {
        self.meta.id
    }
}

impl<'a> FunctionBuilder<'a> {
    pub(super) fn new(
        meta: IncompleteFunction,
        blocks: &'a mut Vec<BasicBlockDesc>,
        functions: &'a mut Vec<FunctionDesc>,
        instructions: &'a mut Vec<(InstId, Instruction)>,
        effect_edges: &'a mut Vec<EffectEdge>,
        context: &'a Context,
    ) -> Self {
        Self {
            meta,
            blocks,
            functions,
            instructions,
            effect_edges,
            context,
            finished: false,
        }
    }

    fn build_basic_block<F>(
        &mut self,
        id: Option<BasicBlockId>,
        name: Option<Ident>,
        build: F,
    ) -> BuildResult<BasicBlockId>
    where
        F: FnOnce(&mut BasicBlockBuilder<'_, '_>) -> BuildResult<()>,
    {
        let span = tracing::trace_span!("building basic block");
        span.in_scope(|| {
            let id = id.unwrap_or_else(|| self.context.block_id());

            if let Some(name) = name {
                tracing::trace!(
                    "started building basic block {} ({:?}) for {:?}",
                    self.context.interner.resolve(&name.0),
                    id,
                    self.func_id(),
                );
            } else {
                tracing::trace!(
                    "started building nameless basic block {:?} for {:?}",
                    id,
                    self.func_id(),
                );
            }

            let meta = IncompleteBasicBlock::new(name, id, Vec::new(), None);
            let mut builder = BasicBlockBuilder::new(meta, self);

            if let Err(err) = build(&mut builder) {
                // Prevent a drop panic when the user closure errors
                builder.finished = true;
                return Err(err);
            }
            let block_id = builder.finish()?;

            if self.meta.entry.is_none() {
                tracing::trace!(
                    "created a block while an entry was not yet set for {:?}, \
                    automatically set the entry point to {:?}",
                    self.func_id(),
                    block_id,
                );

                self.meta.entry = Some(block_id);
            }

            Ok(block_id)
        })
    }

    #[track_caller]
    pub(super) fn finish(mut self) -> BuildResult<FuncId> {
        self.finish_inner()
    }

    #[track_caller]
    fn finish_inner(&mut self) -> BuildResult<FuncId> {
        let id = self.func_id();
        tracing::trace!("finished building function {:?}", id);

        if cfg!(debug_assertions) && self.finished {
            self.finished = true;
            panic!("finished building a function twice");
        }

        self.finished = true;
        let function = self.meta.take().try_into()?;
        self.functions.push(function);

        Ok(id)
    }
}

impl Drop for FunctionBuilder<'_> {
    fn drop(&mut self) {
        if !thread::panicking() && !self.finished {
            tracing::warn!("finished building a function via drop, use `.finish()` instead");

            self.finish_inner()
                .expect("failed to finish function construction");
        }
    }
}

#[derive(Debug)]
#[must_use = "Dropping a deferred function without completing it will panic"]
pub struct DeferredFunction {
    pub(super) name: Option<Ident>,
    pub(super) id: FuncId,
    pub(super) return_ty: Type,
    pub(super) finished: bool,
}

impl DeferredFunction {
    pub(super) const fn new(id: FuncId, name: Option<Ident>, return_ty: Type) -> Self {
        Self {
            name,
            id,
            return_ty,
            finished: false,
        }
    }
}

impl Deref for DeferredFunction {
    type Target = FuncId;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl Drop for DeferredFunction {
    fn drop(&mut self) {
        if !thread::panicking() && !self.finished {
            panic!("dropped an allocated function without completing it");
        }
    }
}

#[derive(Debug)]
pub(super) struct IncompleteFunction {
    name: Option<Ident>,
    id: FuncId,
    params: Vec<TypedVar>,
    pub(super) ret_ty: Type,
    entry: Option<BasicBlockId>,
    pub(super) basic_blocks: Vec<BasicBlockId>,
}

impl IncompleteFunction {
    pub(super) const fn new(
        name: Option<Ident>,
        id: FuncId,
        params: Vec<TypedVar>,
        ret_ty: Type,
        entry: Option<BasicBlockId>,
        basic_blocks: Vec<BasicBlockId>,
    ) -> Self {
        Self {
            name,
            id,
            params,
            ret_ty,
            entry,
            basic_blocks,
        }
    }

    pub(super) fn take(&mut self) -> Self {
        Self {
            name: self.name,
            id: self.id,
            params: mem::take(&mut self.params),
            ret_ty: mem::replace(&mut self.ret_ty, Type::Unit),
            entry: self.entry,
            basic_blocks: mem::take(&mut self.basic_blocks),
        }
    }
}

impl TryInto<FunctionDesc> for IncompleteFunction {
    type Error = BuilderError;

    fn try_into(self) -> Result<FunctionDesc, Self::Error> {
        let entry = self.entry.ok_or(BuilderError::MissingEntryBlock)?;

        if self.basic_blocks.is_empty() {
            return Err(BuilderError::EmptyFunctionBody);
        }

        Ok(FunctionDesc {
            name: self.name,
            id: self.id,
            params: self.params,
            ret_ty: self.ret_ty,
            entry,
            basic_blocks: self.basic_blocks,
        })
    }
}
