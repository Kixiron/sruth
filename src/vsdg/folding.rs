use crate::{
    dataflow::operators::{
        FilterMap, FlatSplit, Flatten, InspectExt, Keys, Reverse, SemijoinExt, SplitBy,
    },
    vsdg::{
        node::{Add, Constant, Node, NodeExt, NodeId, Place, Sub},
        ProgramGraph,
    },
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf},
        consolidate::ConsolidateStream,
        Join, JoinCore, Reduce,
    },
    ExchangeData,
};
use std::{
    convert::identity,
    iter::{self, Step},
    mem,
    ops::Mul,
};
use timely::dataflow::Scope;

pub fn constant_folding<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8> + Step,
{
    // TODO: constant_folding is gonna be a lot more efficient if you use a delta-join for the first two joins,
    //       apply the .flat_split on the delta-stream, conclude with the antijoin (the .concat is the same
    //       in delta-stream-land) and finish up with .integrate()
    scope.region_named("constant folding", |region| {
        let graph = graph.enter_region(region);
        let graph = algebraic_simplification(region, &graph);

        let values_to_consumers = graph
            .value_edges
            .map_in_place(|(src, dest)| mem::swap(src, dest));

        let constant_nodes = graph
            .nodes
            .filter_map(|(id, node)| node.cast::<Constant>().cloned().map(|node| (id, node)));

        let used_constants = constant_nodes
            .join_map(&values_to_consumers, |&value, constant, &consumer| {
                (consumer, (value, constant.clone()))
            })
            .reduce(|_consumer, constants, output| {
                let constants: Vec<(NodeId, Constant)> =
                    constants.iter().map(|(c, _)| *c).cloned().collect();
                output.push((constants, R::from(1)));
            });

        let (evaluated_nodes, value_edges_to_remove) = graph
            .nodes
            .join(&used_constants)
            .flat_split(|(node_id, (node, constants))| {
                let (evaluated_node, removed_edges) = node.evaluate_with_constants(&constants);

                (
                    iter::once((node_id, evaluated_node)),
                    removed_edges.into_iter().map(move |node| (node_id, node)),
                )
            });

        let value_edges = graph.value_edges.concat(&value_edges_to_remove.negate());

        let nodes = graph
            .nodes
            .antijoin(&evaluated_nodes.keys())
            .concat(&evaluated_nodes);

        ProgramGraph {
            value_edges,
            nodes,
            ..graph
        }
        .leave_region()
    })
}

fn algebraic_simplification<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8> + Step,
{
    scope.region_named("Algebraic simplification", |region| {
        let graph = graph.enter_region(region);
        let graph = zero_addition(region, &graph);
        let graph = self_subtract(region, &graph);

        graph.leave_region()
    })
}

// TODO: Generalize to chains of `Add(Add(x, 0), 0)`
//       TODO: That may be covered by chained op fusing and be automatically picked up
fn zero_addition<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R>,
{
    scope.region_named("Add(x, 0) | Add(0, x) => x", |region| {
        let graph = graph.enter_region(region);

        let (value_edges_forward, value_edges_reverse) = (
            graph.value_edges.arrange_by_key(),
            graph.value_edges.reverse().arrange_by_key(),
        );

        let addition = graph.nodes.filter(|(_id, node)| node.is::<Add>());
        let zeroes = graph.nodes.filter(|(_id, node)| {
            node.cast::<Constant>()
                .map(|constant| constant.is_zero())
                .unwrap_or_default()
        });
        let non_const = graph.nodes.filter(|(_id, node)| node.isnt::<Constant>());

        let (new_nodes, discarded_nodes, new_edges, discarded_edges) = addition
            .join_core(&value_edges_forward, |&add_id, add_node, &producer_id| {
                iter::once((producer_id, (add_id, add_node.to_owned())))
            })
            .join_map(&zeroes, |&zero_id, &(add_id, ref add_node), _zero_node| {
                (add_id, (add_node.to_owned(), (add_id, zero_id)))
            })
            .join_core(
                &value_edges_forward,
                |&add_id, &(ref add_node, zero_edge), &producer_id| {
                    iter::once((producer_id, ((add_id, add_node.to_owned()), zero_edge)))
                },
            )
            .join_map(
                &non_const,
                |&value_id, &((add_id, ref add_node), zero_edge), _value| {
                    (add_id, (add_node.to_owned(), value_id, zero_edge))
                },
            )
            .join_core(
                &value_edges_reverse,
                |&add_id, &(ref add_node, value_id, zero_edge), &consumer_id| {
                    iter::once((
                        (add_id, Place.into()),
                        (add_id, add_node.to_owned()),
                        (consumer_id, value_id),
                        vec![zero_edge, (consumer_id, add_id)],
                    ))
                },
            )
            .split_by(identity);

        let discarded_edges =
            discarded_edges
                .flatten()
                .negate()
                .debug_inspect(|((src, dest), time, diff)| {
                    tracing::trace!(
                        "discarding value edge from Add(x, 0) | Add(0, x): ({}->{}, {:?}, {:?})",
                        src,
                        dest,
                        time,
                        diff,
                    );
                });

        let new_edges = new_edges.debug_inspect(|((src, dest), time, diff)| {
            tracing::trace!(
                "adding new value edge from Add(x, 0) | Add(0, x): ({}->{}, {:?}, {:?})",
                src,
                dest,
                time,
                diff,
            );
        });

        // TODO: Don't remove nodes, mint them
        let nodes = graph
            .nodes
            .concatenate(vec![new_nodes, discarded_nodes.negate()]);
        let value_edges = graph
            .value_edges
            .concatenate(vec![discarded_edges, new_edges])
            .consolidate_stream();

        ProgramGraph {
            value_edges,
            nodes,
            ..graph.clone()
        }
        .leave_region()
    })
}

fn self_subtract<S, R>(scope: &mut S, graph: &ProgramGraph<S, R>) -> ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8> + Step,
{
    scope.region_named("Sub(x, x) => 0", |region| {
        let graph = graph.enter_region(region);

        let (arranged_nodes, value_edges_forward, value_edges_reverse) = (
            graph.nodes.arrange_by_key(),
            graph.value_edges.arrange_by_key(),
            graph.value_edges.reverse().arrange_by_key(),
        );

        let subtraction = graph.nodes.filter(|(_, node)| node.is::<Sub>());
        let subtraction_nodes = subtraction.keys().arrange_by_self();

        // Reduce value inputs for subtract nodes down to where the operands are identical
        let identical_operands = SemijoinExt::semijoin(&value_edges_forward, &subtraction_nodes)
            .join_core(&arranged_nodes, |&sub_id, &value_id, sub_node| {
                iter::once((sub_id, (value_id, sub_node.to_owned())))
            })
            .debug()
            .reduce(|_, values, output| {
                let all_operands_eq = values.iter().eq_by(
                    values.iter(),
                    |((node1_id, node1), _), ((node2_id, node2), _)| {
                        node1_id == node2_id || node1 == node2
                    },
                );

                if all_operands_eq {
                    let value_ids: Vec<NodeId> = values
                        .iter()
                        .flat_map(|(&(id, _), diff)| {
                            (R::zero()..diff.clone()).into_iter().map(move |_| id)
                        })
                        .collect();

                    output.push((value_ids, R::from(1)));
                }
            });

        let (new_nodes, discarded_nodes, discarded_edges) =
            SemijoinExt::semijoin(&value_edges_reverse, &subtraction_nodes)
                .join_core(&arranged_nodes, |&sub_id, &value_id, sub_node| {
                    iter::once((sub_id, (value_id, sub_node.to_owned())))
                })
                .join_map(
                    &identical_operands,
                    |&sub_id, &(_consumer_id, ref sub_node), value_ids| {
                        let new_node: (NodeId, Node) = (sub_id, Constant::Uint8(0).into());
                        let discarded_node = (sub_id, sub_node.to_owned());

                        let discarded_edges: Vec<(NodeId, NodeId)> = value_ids
                            .iter()
                            .map(|&value_id| (sub_id, value_id))
                            .collect();

                        (new_node, discarded_node, discarded_edges)
                    },
                )
                .split_by(identity);

        let discarded_edges =
            discarded_edges
                .flatten()
                .negate()
                .debug_inspect(|((src, dest), time, diff)| {
                    tracing::trace!(
                        "discarding value edge from Sub(x, x): ({}->{}, {:?}, {:?})",
                        src,
                        dest,
                        time,
                        diff,
                    );
                });

        // TODO: Don't remove nodes, mint them
        let nodes = graph
            .nodes
            .concatenate(vec![new_nodes, discarded_nodes.negate()]);
        let value_edges = graph.value_edges.concat(&discarded_edges);

        ProgramGraph {
            value_edges,
            nodes,
            ..graph
        }
        .leave_region()
    })
}
