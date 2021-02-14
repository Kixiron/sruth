use crate::repr::{
    utils::{DisplayCtx, IRDisplay, InstructionExt, InstructionPurity},
    FuncId, Type, Value, VarId,
};
use abomonation_derive::Abomonation;
use lasso::Resolver;
use pretty::{DocAllocator, DocBuilder};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Call {
    pub func: FuncId,
    pub args: Vec<Value>,
    pub dest: VarId,
    pub ret_ty: Type,
}

impl Call {
    pub const fn new(func: FuncId, args: Vec<Value>, dest: VarId, ret_ty: Type) -> Self {
        Self {
            func,
            args,
            dest,
            ret_ty,
        }
    }

    pub fn replace_uses(&mut self, from: VarId, to: VarId) -> bool {
        let mut replaced = false;

        for var in self.args.iter_mut().filter_map(|var| var.as_var_mut()) {
            if *var == from {
                *var = to;
                replaced = true;
            }
        }

        replaced
    }

    pub fn used_vars(&self) -> Vec<VarId> {
        self.args.iter().filter_map(|arg| arg.as_var()).collect()
    }

    pub fn used_values(&self) -> Vec<Value> {
        self.args.clone()
    }

    pub fn used_values_mut(&mut self) -> Vec<&mut Value> {
        self.args.iter_mut().collect()
    }
}

impl InstructionExt for Call {
    fn dest(&self) -> VarId {
        self.dest
    }

    fn dest_type(&self) -> Type {
        self.ret_ty.clone()
    }

    // TODO
    fn purity(&self) -> InstructionPurity {
        InstructionPurity::Maybe
    }
}

impl IRDisplay for Call {
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
            .append(ctx.text("call"))
            .append(ctx.space())
            .append(self.func.display(ctx))
            .append(
                ctx.intersperse(
                    self.args.iter().map(|arg| arg.display(ctx)),
                    ctx.text(",").append(ctx.space()),
                )
                .parens(),
            )
            .group()
    }
}
