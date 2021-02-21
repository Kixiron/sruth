use crate::{
    builder::{BuildResult, BuilderError, FunctionBuilder},
    repr::{
        basic_block::BasicBlockDesc,
        instruction::{Add, Assign, Call, Cmp, Div, Mul, Sub},
        terminator::{Branch, Label, Return},
        BasicBlockId, FuncId, Ident, InstId, Terminator, Type, TypedVar, Value, VarId,
    },
};
use std::{convert::TryInto, mem, ops::Deref, thread};

#[derive(Debug)]
pub struct BasicBlockBuilder<'a, 'b: 'a> {
    meta: IncompleteBasicBlock,
    function: &'a mut FunctionBuilder<'b>,
    pub(super) finished: bool,
}

// Public API
impl<'a, 'b> BasicBlockBuilder<'a, 'b> {
    pub const fn block_id(&self) -> BasicBlockId {
        self.meta.id
    }

    pub const fn is_terminated(&self) -> bool {
        self.meta.terminator.is_some()
    }

    pub fn assign<V>(&mut self, value: V) -> TypedVar
    where
        V: Into<Value>,
    {
        let (id, dest) = self.inst_and_dest();
        let value = value.into();
        let var = TypedVar::new(dest, value.ty().clone());

        self.function
            .instructions
            .push((id, Assign::new(dest, value, None).into()));

        var
    }

    pub fn named_assign<V, N>(&mut self, value: V, name: N) -> TypedVar
    where
        V: Into<Value>,
        N: AsRef<str>,
    {
        let (id, dest) = self.inst_and_dest();
        let name = Ident::new(self.function.context.interner.get_or_intern(name));
        let value = value.into();
        let var = TypedVar::new(dest, value.ty().clone());

        self.function
            .instructions
            .push((id, Assign::new(dest, value, Some(name)).into()));

        var
    }

    // TODO: Verify param types & get return type
    pub fn call(&mut self, function: FuncId, args: Vec<Value>) -> BuildResult<TypedVar> {
        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, Type::Infer);

        self.function
            .instructions
            .push((id, Call::new(function, args, dest, Type::Infer).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn add<L, R>(&mut self, lhs: L, rhs: R) -> BuildResult<TypedVar>
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        let (lhs, rhs) = (lhs.into(), rhs.into());
        let (lhs, rhs) = match (lhs.ty().is_infer(), rhs.ty().is_infer()) {
            (true, false) => (
                Value {
                    ty: rhs.ty().clone(),
                    ..lhs
                },
                rhs,
            ),
            (false, true) => {
                let ty = lhs.ty().clone();
                (lhs, Value { ty, ..rhs })
            }
            (true, true) => (lhs, rhs),
            (false, false) if lhs.ty() == rhs.ty() => (lhs, rhs),
            (false, false) => {
                tracing::error!(
                    "created an add with a left hand side type of {:?} and a right hand side type of {:?} in {:?}",
                    lhs.ty(), rhs.ty(), self.block_id(),
                );

                return Err(BuilderError::MismatchedOperandTypes);
            }
        };

        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, lhs.ty().clone());

        self.function
            .instructions
            .push((id, Add::new(lhs, rhs, dest).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn sub<L, R>(&mut self, lhs: L, rhs: R) -> BuildResult<TypedVar>
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        let (lhs, rhs) = (lhs.into(), rhs.into());
        let (lhs, rhs) = match (lhs.ty().is_infer(), rhs.ty().is_infer()) {
            (true, false) => (
                Value {
                    ty: rhs.ty().clone(),
                    ..lhs
                },
                rhs,
            ),
            (false, true) => {
                let ty = lhs.ty().clone();
                (lhs, Value { ty, ..rhs })
            }
            (true, true) => (lhs, rhs),
            (false, false) if lhs.ty() == rhs.ty() => (lhs, rhs),
            (false, false) => {
                tracing::error!(
                    "created an sub with a left hand side type of {:?} and a right hand side type of {:?} in {:?}",
                    lhs.ty(), rhs.ty(), self.block_id(),
                );

                return Err(BuilderError::MismatchedOperandTypes);
            }
        };

        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, lhs.ty().clone());

        self.function
            .instructions
            .push((id, Sub::new(lhs, rhs, dest).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn mul<L, R>(&mut self, lhs: L, rhs: R) -> BuildResult<TypedVar>
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        let (lhs, rhs) = (lhs.into(), rhs.into());
        let (lhs, rhs) = match (lhs.ty().is_infer(), rhs.ty().is_infer()) {
            (true, false) => (
                Value {
                    ty: rhs.ty().clone(),
                    ..lhs
                },
                rhs,
            ),
            (false, true) => {
                let ty = lhs.ty().clone();
                (lhs, Value { ty, ..rhs })
            }
            (true, true) => (lhs, rhs),
            (false, false) if lhs.ty() == rhs.ty() => (lhs, rhs),
            (false, false) => {
                tracing::error!(
                    "created an mul with a left hand side type of {:?} and a right hand side type of {:?} in {:?}",
                    lhs.ty(), rhs.ty(), self.block_id(),
                );

                return Err(BuilderError::MismatchedOperandTypes);
            }
        };

        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, lhs.ty().clone());

        self.function
            .instructions
            .push((id, Mul::new(lhs, rhs, dest).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn div<L, R>(&mut self, lhs: L, rhs: R) -> BuildResult<TypedVar>
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        let (lhs, rhs) = (lhs.into(), rhs.into());
        let (lhs, rhs) = match (lhs.ty().is_infer(), rhs.ty().is_infer()) {
            (true, false) => (
                Value {
                    ty: rhs.ty().clone(),
                    ..lhs
                },
                rhs,
            ),
            (false, true) => {
                let ty = lhs.ty().clone();
                (lhs, Value { ty, ..rhs })
            }
            (true, true) => (lhs, rhs),
            (false, false) if lhs.ty() == rhs.ty() => (lhs, rhs),
            (false, false) => {
                tracing::error!(
                    "created an div with a left hand side type of {:?} and a right hand side type of {:?} in {:?}",
                    lhs.ty(), rhs.ty(), self.block_id(),
                );

                return Err(BuilderError::MismatchedOperandTypes);
            }
        };

        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, lhs.ty().clone());

        self.function
            .instructions
            .push((id, Div::new(lhs, rhs, dest).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn branch<C>(
        &mut self,
        cond: C,
        if_true: BasicBlockId,
        if_false: BasicBlockId,
    ) -> BuildResult<Option<Terminator>>
    where
        C: Into<Value>,
    {
        let mut cond = cond.into();
        if cond.is_var() && cond.ty().is_infer() {
            cond.ty = Type::Bool;
        } else if cond.ty() != &Type::Bool {
            tracing::error!(
                "created a branch with a condition type of {:?} for {:?} in {:?}",
                cond.ty(),
                self.block_id(),
                self.function.func_id(),
            );

            return Err(BuilderError::IncorrectConditionType);
        }

        let old_terminator = self
            .meta
            .terminator
            .replace(Branch::new(cond, Label::new(if_true), Label::new(if_false)).into());

        Ok(old_terminator)
    }

    pub fn cmp<L, R>(&mut self, lhs: L, rhs: R) -> BuildResult<TypedVar>
    where
        L: Into<Value>,
        R: Into<Value>,
    {
        let (lhs, rhs) = (lhs.into(), rhs.into());
        let (lhs, rhs) = match (lhs.ty().is_infer(), rhs.ty().is_infer()) {
            (true, false) => (
                Value {
                    ty: rhs.ty().clone(),
                    ..lhs
                },
                rhs,
            ),
            (false, true) => {
                let ty = lhs.ty().clone();
                (lhs, Value { ty, ..rhs })
            }
            (true, true) => (lhs, rhs),
            (false, false) if lhs.ty() == rhs.ty() => (lhs, rhs),
            (false, false) => {
                tracing::error!(
                    "created a cmp with a left hand side type of {:?} and a right hand side type of {:?} in {:?}",
                    lhs.ty(), rhs.ty(), self.block_id(),
                );

                return Err(BuilderError::MismatchedOperandTypes);
            }
        };

        let (id, dest) = self.inst_and_dest();
        let var = TypedVar::new(dest, Type::Bool);

        self.function
            .instructions
            .push((id, Cmp::new(lhs, rhs, dest).into()));
        self.meta.instructions.push(id);

        Ok(var)
    }

    pub fn jump(&mut self, block: BasicBlockId) -> Option<Terminator> {
        self.meta.terminator.replace(Terminator::Jump(block))
    }

    pub fn ret<V>(&mut self, value: V) -> BuildResult<Option<Terminator>>
    where
        V: Into<Value>,
    {
        let mut value = value.into();
        if value.is_var() && value.ty().is_infer() {
            value.ty = self.function.meta.ret_ty.clone();
        } else if value.ty() != &self.function.meta.ret_ty {
            tracing::error!(
                "created a return with a type of {:?} for {:?} when {:?} returns {:?}",
                value.ty(),
                self.block_id(),
                self.function.func_id(),
                self.function.meta.ret_ty,
            );

            return Err(BuilderError::MismatchedReturnTypes);
        }

        let old_terminator = self
            .meta
            .terminator
            .replace(Return::new(Some(value)).into());

        Ok(old_terminator)
    }

    pub fn ret_unit(&mut self) -> Option<Terminator> {
        self.meta.terminator.replace(Return::new(None).into())
    }
}

// Private API
impl<'a, 'b> BasicBlockBuilder<'a, 'b> {
    pub(super) fn new(meta: IncompleteBasicBlock, function: &'a mut FunctionBuilder<'b>) -> Self {
        Self {
            meta,
            function,
            finished: false,
        }
    }

    fn inst_and_dest(&self) -> (InstId, VarId) {
        (
            self.function.context.inst_id(),
            self.function.context.var_id(),
        )
    }

    #[track_caller]
    pub(super) fn finish(mut self) -> BuildResult<BasicBlockId> {
        self.finish_inner()
    }

    #[track_caller]
    fn finish_inner(&mut self) -> BuildResult<BasicBlockId> {
        let id = self.block_id();
        tracing::trace!("finished building basic block {:?}", id);

        if cfg!(debug_assertions) && self.finished {
            self.finished = true;
            panic!("finished building a basic block twice");
        }

        self.finished = true;
        let block = self.meta.take().try_into()?;
        self.function.blocks.push(block);
        self.function.meta.basic_blocks.push(id);

        Ok(id)
    }
}

impl Drop for BasicBlockBuilder<'_, '_> {
    fn drop(&mut self) {
        if !thread::panicking() && !self.finished {
            tracing::warn!("finished building a basic block via drop, use `.finish()` instead");

            self.finish_inner()
                .expect("failed to finish basic block construction");
        }
    }
}

#[derive(Debug)]
#[must_use = "Dropping a deferred basic block without completing it will panic"]
pub struct DeferredBasicBlock {
    pub(super) name: Option<Ident>,
    pub(super) id: BasicBlockId,
    pub(super) finished: bool,
}

impl DeferredBasicBlock {
    pub(super) const fn new(id: BasicBlockId, name: Option<Ident>) -> Self {
        Self {
            name,
            id,
            finished: false,
        }
    }
}

impl AsRef<BasicBlockId> for DeferredBasicBlock {
    fn as_ref(&self) -> &BasicBlockId {
        &self.id
    }
}

impl Deref for DeferredBasicBlock {
    type Target = BasicBlockId;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl Drop for DeferredBasicBlock {
    fn drop(&mut self) {
        if !thread::panicking() && !self.finished {
            panic!("dropped an allocated basic block without completing it");
        }
    }
}

#[derive(Debug)]
pub(super) struct IncompleteBasicBlock {
    name: Option<Ident>,
    id: BasicBlockId,
    instructions: Vec<InstId>,
    terminator: Option<Terminator>,
}

impl IncompleteBasicBlock {
    pub(super) const fn new(
        name: Option<Ident>,
        id: BasicBlockId,
        instructions: Vec<InstId>,
        terminator: Option<Terminator>,
    ) -> Self {
        Self {
            name,
            id,
            instructions,
            terminator,
        }
    }

    pub(super) fn take(&mut self) -> Self {
        Self {
            name: self.name,
            id: self.id,
            instructions: mem::take(&mut self.instructions),
            terminator: mem::take(&mut self.terminator),
        }
    }
}

impl TryInto<BasicBlockDesc> for IncompleteBasicBlock {
    type Error = BuilderError;

    fn try_into(self) -> Result<BasicBlockDesc, Self::Error> {
        if let Some(terminator) = self.terminator {
            Ok(BasicBlockDesc {
                name: self.name,
                id: self.id,
                instructions: self.instructions,
                terminator,
            })
        } else {
            tracing::error!(
                "failed to turn {:?} into block meta as it has no terminator",
                self.id,
            );

            Err(BuilderError::MissingTerminator)
        }
    }
}
