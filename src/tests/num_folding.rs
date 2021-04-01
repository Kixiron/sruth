use crate::{
    builder::Context,
    repr::{Constant, Type},
    tests::run_dataflow,
};
use std::sync::Arc;

#[test]
fn iadd_fold() {
    let context = Arc::new(Context::new(0));
    let mut builder = context.builder();

    builder
        .named_function("iadd_fold", Type::Int, |func| {
            func.named_basic_block("entry", |block| {
                let v0 = block.assign(Constant::Int(32));
                let v1 = block.assign(Constant::Int(5));
                let v2 = block.add(v0, v1)?;

                let v3 = block.assign(Constant::Int(8));
                let v4 = block.add(v2, v3)?;

                block.ret(v4)?;

                Ok(())
            })?;

            Ok(())
        })
        .unwrap();

    run_dataflow(1, builder, context);
}

#[test]
fn isub_fold() {
    let context = Arc::new(Context::new(0));
    let mut builder = context.builder();

    builder
        .named_function("isub_fold", Type::Int, |func| {
            func.named_basic_block("entry", |block| {
                let v0 = block.named_assign(Constant::Int(42), "v1");
                let v1 = block.assign(Constant::Int(1));
                let v2 = block.sub(v0, v1)?;

                block.ret(v2)?;

                Ok(())
            })?;

            Ok(())
        })
        .unwrap();

    run_dataflow(1, builder, context);
}

#[test]
fn looping() {
    let context = Arc::new(Context::new(0));
    let mut builder = context.builder();

    builder
        .named_function("looping", Type::Unit, |func| {
            let param = func.param(Type::Uint);

            let tail = func.allocate_basic_block();
            let ret = func.allocate_basic_block();

            let head = func.basic_block(|block| {
                let cond = block.cmp(param, Constant::Uint(10))?;
                block.branch(cond, *tail, *ret)?;

                Ok(())
            })?;

            func.resume_building(tail, |block| {
                block.jump(head);

                Ok(())
            })?;

            func.resume_building(ret, |block| {
                block.ret_unit();
                Ok(())
            })?;

            Ok(())
        })
        .unwrap();

    run_dataflow(1, builder, context);
}
