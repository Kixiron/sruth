mod cse;
mod dce;
pub mod dot;
mod folding;
mod graph;
mod inline;
mod logging;
mod loops;
pub mod node;
pub mod tests;

pub use graph::{
    Edge, ProgramArranged, ProgramGraph, ProgramInputs, ProgramTrace, ProgramVariable,
};

use crate::{equisat, vsdg::logging::GraphSender};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::{Abelian, Multiply},
    lattice::Lattice,
    ExchangeData,
};
use std::{
    collections::HashMap,
    iter::Step,
    rc::Rc,
    sync::{atomic::AtomicU8, Arc},
    time::Duration,
};
use timely::{
    communication::Allocate,
    dataflow::{
        channels::pact::Pipeline,
        operators::{
            capture::{EventLink, Replay},
            Inspect, Operator,
        },
        ProbeHandle,
    },
    logging::{BatchLogger, TimelyProgressEvent},
    order::TotalOrder,
    progress::{timestamp::Refines, Timestamp},
    worker::Worker,
};

pub fn optimization_dataflow<A, T, R>(
    worker: &mut Worker<A>,
    sender: GraphSender<T, R>,
    ident_generation: Arc<AtomicU8>,
) -> (ProgramInputs<T, R>, ProgramTrace<T, R>, ProbeHandle<T>)
where
    A: Allocate,
    T: Timestamp + Lattice + TotalOrder + Refines<()>,
    R: Abelian + Multiply<Output = R> + From<i8> + ExchangeData + Step,
    isize: Multiply<R, Output = isize>,
{
    worker.dataflow::<T, _, _>(|scope| {
        let (graph, inputs) = ProgramGraph::<_, R>::new(scope);
        graph.render_graph("input", sender.clone());

        let graph = dce::dce(scope, &graph);
        graph.render_graph("dce over input", sender.clone());

        // let graph = scope.scoped::<Product<T, Time>, _, _>("main loop", |scope| {
        //     let variable =
        //         ProgramVariable::new(graph.enter(scope), Product::new(Default::default(), 1));
        //     let graph = ProgramGraph::from(&variable);
        //
        //     let graph = folding::constant_folding(scope, &graph);
        //     let graph = cse::cse(scope, &graph);
        //     let graph = dce::dce(scope, &graph);
        //
        //     let _loops = loops::detect_loops(scope, &graph)
        //         .inspect(|x| tracing::trace!("looping edge: {:?}", x));
        //
        //     let result = graph.consolidate();
        //     variable.set(&result);
        //
        //     result.leave()
        // });

        // let equisat = equisat::saturate(scope, &*ident_generation, &graph);
        // equisat.render_graph("equisat", sender.clone());

        let graph = folding::constant_folding(scope, &graph, ident_generation);
        graph.render_graph("constant folding", sender.clone());

        // let graph = cse::cse(scope, &graph);
        // graph.render_graph("cse", sender.clone());

        let graph = dce::dce(scope, &graph);
        graph.render_graph("dce", sender.clone());

        // let trivial_inline = inline::trivial_inline(scope, &graph);
        // trivial_inline.render_graph("trivial inline", sender.clone());

        // let _loops = loops::detect_loops(scope, &graph)
        //     .inspect(|x| tracing::trace!("looping edge: {:?}", x));

        graph.render_graph("output", sender.clone());
        (inputs, graph.arrange().trace(), graph.probe())
    })
}

#[test]
fn vsdg_test() {
    use crate::builder::{BuildResult, Builder};
    use node::{Constant, Type};

    /*
    crate::tests::init_logging();

    let context = Arc::new(Context::new());

    let (sender, receiver) = crossbeam_channel::unbounded();
    // let (inner_sender, inner_receiver) = crossbeam_channel::unbounded();

    let mut config = Config::thread();

    // TODO: Automated & fine-grained graph debugging
    config
        .worker
        .set("sruth/render_debug_graphs".to_owned(), true);

    timely::execute(config, move |worker| {
        if let Ok(addr) = std::env::var("DIFFERENTIAL_LOG_ADDR") {
            if !addr.is_empty() {
                if let Ok(stream) = std::net::TcpStream::connect(&addr) {
                    differential_dataflow::logging::enable(worker, stream);
                } else {
                    panic!("Could not connect to differential log address: {:?}", addr);
                }
            }
        }

        let (mut inputs, _trace, probe) =
            optimization_dataflow::<_, Time, Diff>(worker, sender.clone());

        if worker.index() == 0 {
            let mut builder = context.builder();

            let _add = builder
                .named_function("add", crate::repr::Type::Uint, |func| {
                    func.basic_block(|block| {
                        block.ret(crate::repr::Constant::Uint(0))?;
                        Ok(())
                    })?;

                    let a = func.vsdg_param(Type::Uint8);
                    let b = func.vsdg_param(Type::Uint8);

                    let add = func.vsdg_add(a, b)?;

                    let zero = func.vsdg_const(Constant::Uint8(0));
                    let add_zero = func.vsdg_add(add, zero)?;

                    let zero = func.vsdg_const(Constant::Uint8(0));
                    let add_zero_again = func.vsdg_add(zero, add_zero)?;

                    let sub = func.vsdg_sub(add_zero_again, add_zero_again)?;

                    // let mut last = add_zero_again;
                    // for i in 0..u8::max_value() {
                    //     let lhs = func.vsdg_const(Constant::Uint8(i));
                    //
                    //     last = func.vsdg_add(lhs, last)?;
                    // }

                    func.vsdg_return(sub)
                })
                .unwrap();

            builder.vsdg_finish(&mut inputs, 0).unwrap();
        }

        inputs.advance_to(1);
        inputs.flush();

        while probe.less_than(inputs.time()) {
            worker.step_or_park(None);
        }

        worker.log_register().remove("differential/arrange");
    })
    .unwrap();

    dot::render_graphs(receiver);
    */

    tests::harness::<_, fn(&mut Builder) -> BuildResult<()>, _, ()>(
        |builder| {
            builder.named_function("add", crate::repr::Type::Uint, |func| {
                func.basic_block(|block| {
                    block.ret(crate::repr::Constant::Uint(0))?;
                    Ok(())
                })?;

                let a = func.vsdg_param(Type::Uint8);
                let b = func.vsdg_param(Type::Uint8);

                let add = func.vsdg_add(a, b)?;

                let zero = func.vsdg_const(Constant::Uint8(0));
                let add_zero = func.vsdg_add(add, zero)?;

                let zero = func.vsdg_const(Constant::Uint8(0));
                let add_zero_again = func.vsdg_add(zero, add_zero)?;

                let sub = func.vsdg_sub(add_zero_again, add_zero_again)?;

                // let mut last = add_zero_again;
                // for i in 0..u8::max_value() {
                //     let lhs = func.vsdg_const(Constant::Uint8(i));
                //
                //     last = func.vsdg_add(lhs, last)?;
                // }

                func.vsdg_return(sub)
            })
        },
        // Some(|builder: &mut Builder| {
        //     builder.named_function("add", crate::repr::Type::Uint, |func| {
        //         func.basic_block(|block| {
        //             block.ret(crate::repr::Constant::Uint(0))?;
        //             Ok(())
        //         })?;
        //
        //         let zero = func.vsdg_const(Constant::Uint8(0));
        //         func.vsdg_return(zero)
        //     })
        // }),
        None,
    );
}
