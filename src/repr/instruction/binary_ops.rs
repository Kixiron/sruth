use crate::repr::{
    instruction::{Assign, VarId},
    utils::{DisplayCtx, IRDisplay, InstructionExt, RawCast},
    Constant, Instruction, Type, Value, ValueKind,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Add {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Add {
    pub const fn is_const(&self) -> bool {
        self.rhs.is_const() && self.lhs.is_const()
    }

    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Int(lhs + rhs)), Type::Int),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Uint(lhs + rhs)), Type::Uint),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

impl IRDisplay for Add {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("add"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Sub {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Sub {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Int(lhs - rhs)), Type::Int),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Uint(lhs - rhs)), Type::Uint),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

impl IRDisplay for Sub {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("sub"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Mul {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Mul {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Int(lhs * rhs)), Type::Int),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Uint(lhs * rhs)), Type::Uint),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

impl IRDisplay for Mul {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("mul"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Div {
    pub rhs: Value,
    pub lhs: Value,
    pub dest: VarId,
}

impl Div {
    pub fn evaluate(self) -> Option<Instruction> {
        let (rhs, lhs) = (self.rhs.into_const()?, self.lhs.into_const()?);

        match (rhs, lhs) {
            (Constant::Int(lhs), Constant::Int(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Int(lhs / rhs)), Type::Int),
                dest: self.dest,
            })),
            (Constant::Uint(lhs), Constant::Uint(rhs)) => Some(Instruction::Assign(Assign {
                value: Value::new(ValueKind::Const(Constant::Uint(lhs / rhs)), Type::Uint),
                dest: self.dest,
            })),

            (Constant::Bool(_), Constant::Bool(_))
            | (Constant::Bool(_), Constant::Int(_))
            | (Constant::Bool(_), Constant::Uint(_))
            | (Constant::Int(_), Constant::Bool(_))
            | (Constant::Int(_), Constant::Uint(_))
            | (Constant::Uint(_), Constant::Bool(_))
            | (Constant::Uint(_), Constant::Int(_)) => None,
        }
    }
}

impl IRDisplay for Div {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        self.dest
            .display(ctx)
            .append(ctx.space())
            .append(ctx.text(":="))
            .append(ctx.space())
            .append(ctx.text("div"))
            .append(ctx.space())
            .append(self.lhs.display(ctx))
            .append(ctx.text(","))
            .append(ctx.space())
            .append(self.rhs.display(ctx))
            .group()
    }
}

pub trait BinopExt: InstructionExt {
    fn lhs(&self) -> Value;

    fn rhs(&self) -> Value;

    fn operands(&self) -> (Value, Value) {
        (self.lhs(), self.rhs())
    }

    fn from_parts(lhs: Value, rhs: Value, dest: VarId) -> Self;
}

macro_rules! impl_binop {
    ($($type:ident),* $(,)?) => {
        $(
            impl BinopExt for $type {
                fn lhs(&self) -> Value {
                    self.lhs.clone()
                }

                fn rhs(&self) -> Value {
                    self.rhs.clone()
                }

                fn from_parts(lhs: Value, rhs: Value, dest: VarId) -> Self {
                    Self { lhs, rhs, dest }
                }
            }

            impl InstructionExt for $type {
                fn dest(&self) -> VarId {
                    self.dest
                }

                fn dest_type(&self) -> Type {
                    self.lhs().ty.clone()
                }
            }
        )*

        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
        pub enum BinaryOp {
            $($type($type),)*
        }

        impl BinaryOp {
            pub fn lhs(&self) -> Value {
                match self {
                    $(Self::$type(op) => op.lhs(),)*
                }
            }

            pub fn rhs(&self) -> Value {
                match self {
                    $(Self::$type(op) => op.rhs(),)*
                }
            }

            pub fn operands(&self) -> (Value, Value) {
                match self {
                    $(Self::$type(op) => op.operands(),)*
                }
            }
        }

        $(
            impl From<$type> for BinaryOp {
                fn from(op: $type) -> Self {
                    Self::$type(op)
                }
            }

            impl RawCast<$type> for BinaryOp {
                fn is_raw(&self) -> bool {
                    matches!(self, Self::$type(_))
                }

                fn cast_raw(self) -> Option<$type> {
                    if let Self::$type(value) = self {
                        Some(value)
                    } else {
                        None
                    }
                }
            }
        )*

        impl InstructionExt for BinaryOp {
            fn dest(&self) -> VarId {
                match self {
                    $(Self::$type(op) => op.dest(),)*
                }
            }

            fn dest_type(&self) -> Type {
                match self {
                    $(Self::$type(op) => op.dest_type(),)*
                }
            }
        }

        impl IRDisplay for BinaryOp {
            fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
            where
                D: DocAllocator<'a, A>,
                D::Doc: Clone,
                A: Clone + 'a,
                R: Resolver,
            {
                match self {
                    $(Self::$type(op) => op.display(ctx),)*
                }
            }
        }

        impl RawCast<BinaryOp> for Instruction {
            fn is_raw(&self) -> bool {
                matches!(self, $(Self::$type(_))|*)
            }

            fn cast_raw(self) -> Option<BinaryOp> {
                $(if let Self::$type(op) = self {
                    Some(BinaryOp::$type(op))
                })else*
                else {
                    None
                }
            }
        }
    };
}

impl_binop! {
    Add,
    Sub,
    Mul,
    Div,
}
