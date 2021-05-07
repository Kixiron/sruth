use crate::dataflow::{
    operators::{FilterSplit, InspectExt, Reverse},
    Time,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    algorithms::graphs::propagate,
    difference::{Abelian, Multiply, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::ArrangeByKey, iterate::SemigroupVariable, Join, JoinCore, Reduce, Threshold,
    },
    Collection, ExchangeData,
};
use std::iter;
use timely::{
    dataflow::{operators::probe::Handle, scopes::Child, Scope},
    order::Product,
    progress::timestamp::Refines,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct EClassId(u64);

impl EClassId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ENodeId(u64);

impl ENodeId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum ENode {
    Add(Add),
    Constant,
}

impl ENode {
    pub const fn as_add(self) -> Option<Add> {
        if let Self::Add(add) = self {
            Some(add)
        } else {
            None
        }
    }
}

impl ENode {
    /// Returns `true` if the e_node is [`Add`].
    pub const fn is_add(&self) -> bool {
        matches!(self, Self::Add(..))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Add {
    lhs: EClassId,
    rhs: EClassId,
}

impl Add {
    pub const fn new(lhs: EClassId, rhs: EClassId) -> Self {
        Self { lhs, rhs }
    }

    /// Get the [`Add`]'s left hand side
    pub const fn lhs(&self) -> EClassId {
        self.lhs
    }

    /// Get the [`Add`]'s right hand side
    pub const fn rhs(&self) -> EClassId {
        self.rhs
    }
}

#[derive(Clone)]
pub struct EGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
{
    /// Collection of enodes and their ids
    enodes: Collection<S, (ENodeId, ENode), R>,
    /// Collection of eclass ids to their child nodes
    eclass_nodes: Collection<S, (EClassId, ENodeId), R>,
    /// Collection of eclass ids to their parent eclasses
    eclass_parents: Collection<S, (EClassId, EClassId), R>,
    canon_enode_ids: Collection<S, ENodeId, R>,
}

impl<S, R> EGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    pub fn new(
        enodes: Collection<S, (ENodeId, ENode), R>,
        eclass_nodes: Collection<S, (EClassId, ENodeId), R>,
        eclass_parents: Collection<S, (EClassId, EClassId), R>,
        canon_enode_ids: Collection<S, ENodeId, R>,
    ) -> Self {
        Self {
            enodes,
            eclass_nodes,
            eclass_parents,
            canon_enode_ids,
        }
    }

    pub fn scope(&self) -> S {
        self.enodes.scope()
    }

    pub fn probe_with(&self, probe: &mut Handle<S::Timestamp>) -> Self {
        Self {
            enodes: self.enodes.probe_with(probe),
            eclass_nodes: self.eclass_nodes.probe_with(probe),
            eclass_parents: self.eclass_parents.probe_with(probe),
            canon_enode_ids: self.canon_enode_ids.probe_with(probe),
        }
    }

    pub fn enter<'a, T>(&self, scope: &mut Child<'a, S, T>) -> EGraph<Child<'a, S, T>, R>
    where
        T: Refines<S::Timestamp> + Lattice,
    {
        EGraph {
            enodes: self.enodes.enter(scope),
            eclass_nodes: self.eclass_nodes.enter(scope),
            eclass_parents: self.eclass_parents.enter(scope),
            canon_enode_ids: self.canon_enode_ids.enter(scope),
        }
    }

    #[track_caller]
    pub fn debug(&self) -> Self {
        Self {
            enodes: self.enodes.debug(),
            eclass_nodes: self.eclass_nodes.debug(),
            eclass_parents: self.eclass_parents.debug(),
            canon_enode_ids: self.canon_enode_ids.debug(),
        }
    }

    pub fn run(&self) -> Self {
        let rewrites = self.scope().iterative(|scope| {
            let edges = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

            edges.set(new_edges).leave()
        });

        self.union().leave()
    }

    pub fn union(&self) -> Self {
        let (canon_enode_ids, eclass_parents) = self.scope().iterative::<Time, _, _>(|scope| {
            let product = Product::new(Default::default(), 1);
            let enodes = self.enodes.enter(scope);
            let eclass_parents = SemigroupVariable::new(scope, product);

            let union_find = derive_canonical_eclass_ids(
                &eclass_parents
                    .concat(&self.eclass_parents.enter(scope))
                    // This distinct could be unnecessary, but it's here to make sure that the
                    // multiplicities from the variable don't overflow within the `propagate_core()`
                    // call inside of canon id derivation
                    .distinct_core(),
                &enodes,
            );
            let eclass_union_find = union_find
                .map(|(enode, eclass)| (EClassId(enode.0), eclass))
                .arrange_by_key();

            let (add_lhs, add_rhs) = enodes.filter_split(|(enode_id, enode)| {
                if let Some(add) = enode.as_add() {
                    (Some((add.lhs(), enode_id)), Some((add.rhs(), enode_id)))
                } else {
                    (None, None)
                }
            });

            let canon_lhs = add_lhs.join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                iter::once((parent_enode, eclass))
            });
            let canon_rhs = add_rhs.join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                iter::once((parent_enode, eclass))
            });

            let canon_enodes = canon_lhs
                .join_map(&canon_rhs, |&enode, &lhs, &rhs| {
                    (enode, ENode::Add(Add::new(lhs, rhs)))
                })
                .reverse()
                .arrange_by_key();

            let canon_edges = canon_enodes.reduce(|_enode, enodes, edges| {
                let (&first_enode, _) = enodes[0].clone();

                edges.reserve(enodes.len() - 1);
                edges.extend(
                    enodes
                        .iter()
                        .skip(1)
                        .map(|&(&enode, _)| ((first_enode, enode), R::from(1))),
                );
            });

            let canon_enode_ids = canon_edges
                .map(|(_, (canonical_enode_id, _))| canonical_enode_id)
                .leave()
                .distinct_core::<R>();

            let canon_enode_edges =
                canon_edges.map(|(_enode, (src, dest))| (EClassId(src.0), EClassId(dest.0)));
            let canon_enode_edges = canon_enode_edges.concat(&canon_enode_edges.reverse());

            (
                canon_enode_ids,
                eclass_parents.set(&canon_enode_edges).leave(),
            )
        });

        Self {
            eclass_parents,
            canon_enode_ids,
            ..self.clone()
        }
    }

    fn derive_canonical_eclass_ids(&self) -> Collection<S, (ENodeId, EClassId), R> {
        derive_canonical_eclass_ids(&self.eclass_parents, &self.enodes)
    }
}

fn derive_canonical_eclass_ids<S, R>(
    eclass_parents: &Collection<S, (EClassId, EClassId), R>,
    enodes: &Collection<S, (ENodeId, ENode), R>,
) -> Collection<S, (ENodeId, EClassId), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    let canonicalized_edges = eclass_parents.flat_map(|(src, dest)| {
        vec![
            (ENodeId(src.0), ENodeId(dest.0)),
            (ENodeId(dest.0), ENodeId(src.0)),
        ]
    });

    let implicit_eclass_assignment = enodes
        .map(|(enode, _)| (enode, EClassId(enode.0)))
        .distinct_core();

    propagate::propagate_at(
        &canonicalized_edges,
        &implicit_eclass_assignment,
        |&eclass| eclass.0,
    )
}

#[cfg(test)]
mod tests {
    use crate::equisat::{Add, EClassId, EGraph, ENode, ENodeId};
    use differential_dataflow::{input::Input, lattice::Lattice};
    use timely::dataflow::operators::probe::Handle;

    #[test]
    fn union_find() {
        timely::execute_directly(|worker| {
            let mut probe = Handle::new();

            let (mut enodes, mut eclass_nodes, mut eclass_parents, mut canon_enode_ids) =
                worker.dataflow::<usize, _, _>(|scope| {
                    let (enode_input, enodes) = scope.new_collection();
                    let (eclass_nodes_input, eclass_nodes) = scope.new_collection();
                    let (eclass_parents_input, eclass_parents) = scope.new_collection();
                    let (canon_enode_ids_input, canon_enode_ids) = scope.new_collection();

                    let graph = EGraph::new(enodes, eclass_nodes, eclass_parents, canon_enode_ids);
                    graph.debug().union().debug().probe_with(&mut probe);

                    (
                        enode_input,
                        eclass_nodes_input,
                        eclass_parents_input,
                        canon_enode_ids_input,
                    )
                });

            enodes.insert((
                ENodeId::new(0),
                ENode::Add(Add::new(EClassId::new(3), EClassId::new(4))),
            ));
            enodes.insert((
                ENodeId::new(1),
                ENode::Add(Add::new(EClassId::new(3), EClassId::new(4))),
            ));
            enodes.insert((
                ENodeId::new(2),
                ENode::Add(Add::new(EClassId::new(3), EClassId::new(4))),
            ));
            eclass_nodes.insert((EClassId::new(0), ENodeId::new(0)));
            eclass_nodes.insert((EClassId::new(0), ENodeId::new(1)));
            eclass_nodes.insert((EClassId::new(0), ENodeId::new(2)));
            eclass_parents.insert((EClassId::new(0), EClassId::new(3)));
            eclass_parents.insert((EClassId::new(0), EClassId::new(4)));

            enodes.advance_to(1);
            eclass_nodes.advance_to(1);
            eclass_parents.advance_to(1);
            canon_enode_ids.advance_to(1);
            enodes.flush();
            eclass_nodes.flush();
            eclass_parents.flush();
            canon_enode_ids.flush();

            worker.step_while(|| {
                probe.less_than(
                    &enodes
                        .time()
                        .join(eclass_nodes.time())
                        .join(eclass_parents.time())
                        .join(canon_enode_ids.time()),
                )
            });
        });
    }
}
