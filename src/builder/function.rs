use crate::{
    builder::{
        block::{DeferredBasicBlock, IncompleteBasicBlock},
        BasicBlockBuilder, BuildResult, BuilderError, Context,
    },
    dataflow::operators::Uuid,
    repr::{
        basic_block::BasicBlockDesc, function::FunctionDesc, BasicBlockId, FuncId, Ident, InstId,
        Instruction, Type, TypedVar,
    },
    vsdg::{
        node::{
            Add as NodeAdd, Cmp, CmpKind, Constant, End, FuncId as VFuncId, Load, LoopHead,
            LoopTail, Node, NodeId, Operation, Parameter, Pointer, Return, Start, Store,
            Sub as NodeSub, Type as NodeType, Value,
        },
        Edge,
    },
};
use std::{convert::TryInto, mem, ops::Deref, thread};

#[derive(Debug)]
pub struct FunctionBuilder<'a> {
    pub(super) meta: IncompleteFunction,
    pub(super) blocks: &'a mut Vec<BasicBlockDesc>,
    pub(super) functions: &'a mut Vec<FunctionDesc>,
    pub(super) instructions: &'a mut Vec<(InstId, Instruction)>,
    pub(super) nodes: &'a mut Vec<(NodeId, Node)>,
    pub(super) function_nodes: &'a mut Vec<(NodeId, VFuncId)>,
    pub(super) effect_edges: &'a mut Vec<Edge>,
    pub(super) control_edges: &'a mut Vec<Edge>,
    pub(super) value_edges: &'a mut Vec<Edge>,
    pub(super) context: &'a Context,
    pub(super) finished: bool,
    last_control: NodeId,
    last_effect: Option<NodeId>,
    end_node: NodeId,
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

    pub fn vsdg_param<T>(&mut self, ty: T) -> NodeId
    where
        T: Into<NodeType>,
    {
        let node_id = self.context.node_id();
        let node: Node = Parameter { ty: ty.into() }.into();
        self.nodes.push((node_id, node));
        self.function_nodes.push((
            node_id,
            VFuncId::new(Uuid::new(
                self.context.ident_generation,
                self.func_id().0.get(),
            )),
        ));

        node_id
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

    pub fn vsdg_params<I>(&mut self, types: I) -> Vec<NodeId>
    where
        I: IntoIterator<Item = NodeType>,
    {
        let mut ids = Vec::with_capacity(10);
        for ty in types {
            let node_id = self.context.node_id();
            let node: Node = Parameter { ty }.into();

            self.nodes.push((node_id, node));
            self.function_nodes.push((
                node_id,
                VFuncId::new(Uuid::new(
                    self.context.ident_generation,
                    self.func_id().0.get(),
                )),
            ));

            ids.push(node_id);
        }

        ids
    }

    pub fn vsdg_add(&mut self, lhs: NodeId, rhs: NodeId) -> BuildResult<NodeId> {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, NodeAdd { lhs, rhs }.into()));

        self.value_edges.push((node_id, lhs));
        self.value_edges.push((node_id, rhs));

        Ok(node_id)
    }

    pub fn vsdg_sub(&mut self, lhs: NodeId, rhs: NodeId) -> BuildResult<NodeId> {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, NodeSub { lhs, rhs }.into()));

        self.value_edges.push((node_id, lhs));
        self.value_edges.push((node_id, rhs));

        Ok(node_id)
    }

    pub fn vsdg_return(&mut self, value: NodeId) -> BuildResult<()> {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, Return {}.into()));

        self.value_edges.push((node_id, value));
        self.control_edges.push((node_id, self.last_control));
        self.control_edges.push((self.end_node, node_id));

        Ok(())
    }

    pub fn vsdg_const(&mut self, value: Constant) -> NodeId {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, value.into()));

        node_id
    }

    pub fn vsdg_ptr_to(&mut self, value: NodeId) -> NodeId {
        let node_id = self.context.node_id();
        self.nodes
            .push((node_id, Value::Pointer(Pointer {}).into()));

        self.value_edges.push((node_id, value));

        node_id
    }

    pub fn vsdg_load(&mut self, address: NodeId) -> NodeId {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, Operation::Load(Load {}).into()));

        self.value_edges.push((node_id, address));

        if let Some(last_effect) = self.last_effect {
            self.effect_edges.push((node_id, last_effect));
        }
        self.last_effect = Some(node_id);

        node_id
    }

    pub fn vsdg_store(&mut self, address: NodeId, value: NodeId) -> NodeId {
        let node_id = self.context.node_id();
        self.nodes
            .push((node_id, Operation::Store(Store {}).into()));

        self.value_edges.push((node_id, address));
        self.value_edges.push((node_id, value));

        if let Some(last_effect) = self.last_effect {
            self.effect_edges.push((node_id, last_effect));
        }
        self.last_effect = Some(node_id);

        node_id
    }

    pub fn vsdg_cmp(&mut self, kind: CmpKind, lhs: NodeId, rhs: NodeId) -> NodeId {
        let node_id = self.context.node_id();
        self.nodes.push((node_id, Cmp { lhs, rhs, kind }.into()));

        self.value_edges.push((node_id, lhs));
        self.value_edges.push((node_id, rhs));

        node_id
    }

    pub fn vsdg_loop<F>(&mut self, body: F) -> BuildResult<NodeId>
    where
        F: FnOnce(&mut Self) -> BuildResult<Option<NodeId>>,
    {
        let loop_head = self.context.node_id();
        self.nodes.push((loop_head, LoopHead {}.into()));

        self.control_edges.push((loop_head, self.last_control));
        self.effect_edges
            .push((loop_head, self.last_effect.unwrap()));
        self.last_control = loop_head;
        self.last_effect = Some(loop_head);

        let condition = body(self)?;

        let loop_tail = self.context.node_id();
        self.nodes.push((loop_head, LoopTail {}.into()));

        self.control_edges.push((loop_tail, self.last_control));
        self.effect_edges
            .push((loop_tail, self.last_effect.unwrap()));
        self.last_control = loop_tail;
        self.last_effect = Some(loop_tail);

        if let Some(condition) = condition {
            self.value_edges.push((loop_tail, condition));
        }

        Ok(loop_tail)
    }

    pub const fn func_id(&self) -> FuncId {
        self.meta.id
    }
}

impl<'a> FunctionBuilder<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        meta: IncompleteFunction,
        blocks: &'a mut Vec<BasicBlockDesc>,
        functions: &'a mut Vec<FunctionDesc>,
        instructions: &'a mut Vec<(InstId, Instruction)>,
        nodes: &'a mut Vec<(NodeId, Node)>,
        function_nodes: &'a mut Vec<(NodeId, VFuncId)>,
        value_edges: &'a mut Vec<Edge>,
        control_edges: &'a mut Vec<Edge>,
        effect_edges: &'a mut Vec<Edge>,
        context: &'a Context,
    ) -> Self {
        let last_control = context.node_id();
        nodes.push((last_control, Start.into()));

        let end_node = context.node_id();
        nodes.push((end_node, End.into()));
        function_nodes.push((
            end_node,
            VFuncId::new(Uuid::new(context.ident_generation, meta.id.0.get())),
        ));

        Self {
            meta,
            blocks,
            functions,
            instructions,
            nodes,
            function_nodes,
            value_edges,
            control_edges,
            effect_edges,
            context,
            finished: false,
            last_control,
            last_effect: None,
            end_node,
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
