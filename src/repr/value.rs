use super::Type;
use crate::repr::{
    constant::Constant,
    instruction::VarId,
    utils::{DisplayCtx, IRDisplay},
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Value {
    pub value: ValueKind,
    pub ty: Type,
}

impl Value {
    pub const fn new(value: ValueKind, ty: Type) -> Self {
        Self { value, ty }
    }

    pub fn map<F>(self, map: F) -> Self
    where
        F: FnOnce(ValueKind) -> ValueKind,
    {
        let value = map(self.value);
        Self { value, ty: self.ty }
    }

    pub fn and_then<F>(self, map: F) -> Self
    where
        F: FnOnce(ValueKind) -> Option<ValueKind>,
    {
        if let Some(value) = map(self.value.clone()) {
            Self { value, ty: self.ty }
        } else {
            self
        }
    }

    pub const fn ty(&self) -> &Type {
        &self.ty
    }

    pub const fn is_const(&self) -> bool {
        self.value.is_const()
    }

    pub const fn is_var(&self) -> bool {
        self.value.is_var()
    }

    pub const fn as_const(&self) -> Option<&Constant> {
        self.value.as_const()
    }

    pub const fn into_const(self) -> Option<Constant> {
        self.value.into_const()
    }

    pub const fn as_var(&self) -> Option<VarId> {
        self.value.as_var()
    }

    pub fn as_var_mut(&mut self) -> Option<&mut VarId> {
        self.value.as_var_mut()
    }

    pub const fn split(self) -> (ValueKind, Type) {
        (self.value, self.ty)
    }

    pub fn as_typed_var(&self) -> Option<TypedVar> {
        self.as_var().map(|var| TypedVar::new(var, self.ty.clone()))
    }
}

impl From<Constant> for Value {
    fn from(constant: Constant) -> Self {
        let ty = constant.ty();
        let value = ValueKind::from(constant);

        Self { value, ty }
    }
}

impl From<TypedVar> for Value {
    fn from(TypedVar { var, ty }: TypedVar) -> Self {
        Self {
            value: ValueKind::from(var),
            ty,
        }
    }
}

impl IRDisplay for Value {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.ty
            .display(ctx)
            .append(ctx.space())
            .append(self.value.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum ValueKind {
    Const(Constant),
    Var(VarId),
}

impl ValueKind {
    pub const fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    pub const fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    pub const fn as_const(&self) -> Option<&Constant> {
        if let Self::Const(constant) = self {
            Some(constant)
        } else {
            None
        }
    }

    pub const fn into_const(self) -> Option<Constant> {
        if let Self::Const(constant) = self {
            Some(constant)
        } else {
            None
        }
    }

    pub const fn as_var(&self) -> Option<VarId> {
        if let Self::Var(var) = *self {
            Some(var)
        } else {
            None
        }
    }

    pub fn as_var_mut(&mut self) -> Option<&mut VarId> {
        if let Self::Var(var) = self {
            Some(var)
        } else {
            None
        }
    }
}

impl From<Constant> for ValueKind {
    fn from(constant: Constant) -> Self {
        Self::Const(constant)
    }
}

impl From<VarId> for ValueKind {
    fn from(var: VarId) -> Self {
        Self::Var(var)
    }
}

impl IRDisplay for ValueKind {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        match self {
            Self::Const(constant) => constant.display(ctx),
            Self::Var(var) => var.display(ctx),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct TypedVar {
    pub var: VarId,
    pub ty: Type,
}

impl TypedVar {
    pub const fn new(var: VarId, ty: Type) -> Self {
        Self { var, ty }
    }
}

impl IRDisplay for TypedVar {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.ty
            .display(ctx)
            .append(ctx.space())
            .append(self.ty.display(ctx))
    }
}
