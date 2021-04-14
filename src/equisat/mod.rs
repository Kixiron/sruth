use crate::{
    dataflow::{
        operators::{DiscriminatedIdents, FlatSplit, InspectExt, Keys, Reverse, Uuid},
        Time,
    },
    vsdg::{
        node::{Add, Constant, Mul, Node, NodeExt, NodeId},
        Edge, ProgramGraph,
    },
};
use differential_dataflow::{
    difference::{Abelian, Multiply},
    lattice::Lattice,
    operators::{iterate::Variable, Consolidate, Join},
    Collection, ExchangeData,
};
use std::{
    iter,
    sync::atomic::{AtomicU8, Ordering},
};
use timely::{dataflow::Scope, order::Product};

pub fn saturate<S, R>(
    scope: &mut S,
    ident_generator: &AtomicU8,
    graph: &ProgramGraph<S, R>,
) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R>,
{
    let multiply_by_two = MultiplyByTwo::new(ident_generator.fetch_add(1, Ordering::Relaxed));

    let (new_nodes, new_edges) = scope.iterative::<Time, _, _>(|scope| {
        let value_edges = Variable::new_from(
            graph.value_edges.enter(scope),
            Product::new(Default::default(), 1),
        );
        let nodes = Variable::new_from(
            graph.nodes.enter(scope),
            Product::new(Default::default(), 1),
        );

        let (new_nodes, new_edges) = multiply_by_two.rewrite(&graph.enter(scope));

        // TODO: Choose the optimal optimization

        (
            nodes.set_concat(&new_nodes.consolidate()).leave(),
            value_edges.set_concat(&new_edges.consolidate()).leave(),
        )
    });

    ProgramGraph {
        nodes: new_nodes,
        // Note: This is blatantly wrong for debug purposes
        value_edges: new_edges,
        ..graph.clone()
    }
}

trait Rewrite {
    fn rewrite<S, R>(
        &self,
        graph: &ProgramGraph<S, R>,
    ) -> (Collection<S, (NodeId, Node), R>, Collection<S, Edge, R>)
    where
        S: Scope,
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Multiply<Output = R>;
}

#[derive(Debug)]
struct MultiplyByTwo {
    discriminant: u8,
}

impl MultiplyByTwo {
    pub fn new(discriminant: u8) -> Self {
        Self { discriminant }
    }
}

impl Rewrite for MultiplyByTwo {
    fn rewrite<S, R>(
        &self,
        graph: &ProgramGraph<S, R>,
    ) -> (Collection<S, (NodeId, Node), R>, Collection<S, Edge, R>)
    where
        S: Scope,
        S::Timestamp: Lattice,
        R: Abelian + ExchangeData + Multiply<Output = R>,
    {
        let twos = graph.nodes.filter(|(_, node)| {
            node.cast::<Constant>()
                .and_then(|constant| constant.as_uint8())
                .filter(|&int| int == 2)
                .is_some()
        });

        let mults = graph.nodes.filter(|(_, node)| node.is::<Mul>());

        // Edges going from an add node to a data dependency
        let outgoing_from_mul = graph.value_edges.semijoin(&mults.keys());

        // (node_id, (two_id, var_id))
        let candidates = outgoing_from_mul
            .reverse()
            .semijoin(&twos.keys())
            .reverse()
            .join(&outgoing_from_mul)
            .filter(|(_, (two, rhs))| two != rhs);

        let (new_nodes, new_edges) = candidates
            .map(|(mul_id, (two_id, var_id))| {
                (
                    mul_id,
                    two_id,
                    var_id,
                    Add {
                        lhs: var_id,
                        rhs: var_id,
                    }
                    .into(),
                )
            })
            .discriminated_idents(self.discriminant)
            .flat_split(
                |((_mul_id, _two_id, var_id, add_node), add_id): ((_, _, _, Node), Uuid)| {
                    let add_id = NodeId::new(add_id);

                    (
                        iter::once((add_id, add_node)),
                        // TODO: add->dependent
                        vec![(add_id, var_id), (add_id, var_id)],
                    )
                },
            );

        (new_nodes, new_edges)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        builder::{BuildResult, Builder},
        vsdg::{
            node::{Constant, Type},
            tests,
        },
    };

    #[test]
    fn x_times_two() {
        tests::harness::<_, fn(&mut Builder) -> BuildResult<()>, _, ()>(
            |builder| {
                builder.named_function("x_times_two", crate::repr::Type::Uint, |func| {
                    func.basic_block(|block| {
                        block.ret(crate::repr::Constant::Uint(0))?;
                        Ok(())
                    })?;

                    let a = func.vsdg_param(Type::Uint8);
                    let two = func.vsdg_const(Constant::Uint8(2));
                    let self_mul = func.vsdg_mul(a, two)?;

                    func.vsdg_return(self_mul)
                })
            },
            None,
        );
    }
}
