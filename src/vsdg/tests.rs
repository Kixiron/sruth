#![cfg(test)]

use crate::{
    builder::{BuildResult, Builder, Context},
    dataflow::{Diff, Time},
    vsdg::{optimization_dataflow, ProgramGraph},
};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use timely::Config;

pub fn harness<I, E, R1, R2>(input: I, expected: Option<E>)
where
    I: FnOnce(&mut Builder) -> BuildResult<R1> + Clone + Send + Sync + 'static,
    E: FnOnce(&mut Builder) -> BuildResult<R2> + Clone + Send + Sync + 'static,
{
    crate::tests::init_logging();

    let config = Config::thread();
    let ident_generation = Arc::new(AtomicU8::new(0));

    let (sender, receiver) = crossbeam_channel::unbounded();
    let context = Arc::new(Context::new(
        ident_generation.fetch_add(1, Ordering::Relaxed),
    ));

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

        let (mut inputs, mut trace, mut probe) = optimization_dataflow::<_, Time, Diff>(
            worker,
            sender.clone(),
            ident_generation.clone(),
        );

        let mut expected_input = if expected.is_some() {
            Some(worker.dataflow_named("assertion dataflow", |scope| {
                let (graph, inputs) = ProgramGraph::new(scope);

                graph
                    .probe_with(&mut probe)
                    .assert_eq(&trace.import(scope).as_collection());

                inputs
            }))
        } else {
            None
        };

        if worker.index() == 0 {
            let mut builder = context.builder();

            input.clone()(&mut builder).unwrap();
            builder.vsdg_finish(&mut inputs, 0).unwrap();

            if let (Some(expected), Some(expected_input)) =
                (expected.clone(), expected_input.as_mut())
            {
                let mut builder = context.builder();
                expected(&mut builder).unwrap();
                builder.vsdg_finish(expected_input, 0).unwrap();
            }
        }
        inputs.advance_to(1);
        inputs.flush();

        while probe.less_than(inputs.time()) {
            worker.step_or_park(None);
        }

        worker.log_register().remove("differential/arrange");
    })
    .unwrap();

    super::dot::render_graphs(receiver);
}
