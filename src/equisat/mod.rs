use crate::dataflow::{
    operators::{FilterDiff, FilterMap, FilterSplit, InspectExt, Reverse},
    Time,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    algorithms::graphs::propagate,
    collection::concatenate,
    difference::{Abelian, Multiply, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf, Arranged, TraceAgent},
        iterate::SemigroupVariable,
        Join, JoinCore, Reduce, Threshold,
    },
    trace::implementations::ord::{OrdKeySpine, OrdValSpine},
    Collection, ExchangeData,
};
use dogsdogsdogs::{
    altneu::AltNeu,
    calculus::{Differentiate, Integrate},
};
use std::iter;
use timely::{
    dataflow::{operators::probe::Handle, Scope, ScopeParent},
    order::Product,
    progress::Timestamp,
};

mod sexpr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct EClassId(u64);

impl EClassId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_enode(self) -> ENodeId {
        ENodeId(self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ENodeId(u64);

impl ENodeId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_eclass(self) -> EClassId {
        EClassId(self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum ENode {
    Add(Add),
    Sub(Sub),
    Constant,
    Param,
}

impl ENode {
    /// Returns `true` if the enode is [`Add`].
    pub const fn is_add(&self) -> bool {
        matches!(self, Self::Add(..))
    }

    /// Returns `true` if the enode is [`Sub`].
    pub const fn is_sub(&self) -> bool {
        matches!(self, Self::Sub(..))
    }

    /// Returns `true` if the e_node is [`Constant`].
    pub const fn is_constant(&self) -> bool {
        matches!(self, Self::Constant)
    }

    /// Returns `true` if the e_node is [`Param`].
    pub const fn is_param(&self) -> bool {
        matches!(self, Self::Param)
    }

    pub fn as_add(&self) -> Option<Add> {
        if let Self::Add(add) = self {
            Some(add.clone())
        } else {
            None
        }
    }

    pub fn as_sub(&self) -> Option<Sub> {
        if let Self::Sub(sub) = self {
            Some(sub.clone())
        } else {
            None
        }
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Sub {
    lhs: EClassId,
    rhs: EClassId,
}

impl Sub {
    pub const fn new(lhs: EClassId, rhs: EClassId) -> Self {
        Self { lhs, rhs }
    }

    /// Get the [`Sub`]'s left hand side
    pub const fn lhs(&self) -> EClassId {
        self.lhs
    }

    /// Get the [`Sub`]'s right hand side
    pub const fn rhs(&self) -> EClassId {
        self.rhs
    }
}

type ENodeEClassLookup<S, R> =
    Arranged<S, TraceAgent<OrdValSpine<ENodeId, EClassId, <S as ScopeParent>::Timestamp, R>>>;
type EClassENodeLookup<S, R> =
    Arranged<S, TraceAgent<OrdValSpine<EClassId, ENodeId, <S as ScopeParent>::Timestamp, R>>>;
type ENodeLookup<S, R> =
    Arranged<S, TraceAgent<OrdValSpine<ENodeId, ENode, <S as ScopeParent>::Timestamp, R>>>;
type ENodeIds<S, R> =
    Arranged<S, TraceAgent<OrdKeySpine<ENodeId, <S as ScopeParent>::Timestamp, R>>>;
type EClassMerger<S, R> = Collection<S, (EClassId, EClassId), R>;
type ENodeCollection<S, R> = Collection<S, (ENodeId, ENode), R>;

pub struct EGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
{
    enodes: Vec<ENodeCollection<S, R>>,
    eclass_mergers: Vec<EClassMerger<S, R>>,
    eclass_mergers_feedback: SemigroupVariable<S, (EClassId, EClassId), R>,
    enodes_feedback: SemigroupVariable<S, (ENodeId, ENode), R>,
    enode_eclass_lookup: ENodeEClassLookup<S, R>,
    eclass_enode_lookup: EClassENodeLookup<S, R>,
    canon_enodes: ENodeLookup<S, R>,
    canon_enode_ids: ENodeIds<S, R>,
    scope: S,
}

impl<S, R> EGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + Timestamp,
    R: Semigroup,
{
    pub fn new(scope: &mut S, summary: <S::Timestamp as Timestamp>::Summary) -> Self
    where
        R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
    {
        let eclass_mergers_feedback = SemigroupVariable::new(scope, summary.clone());
        let enodes_feedback = SemigroupVariable::new(scope, summary);

        let (enode_eclass_lookup, eclass_enode_lookup, canon_enodes, canon_enode_ids) =
            union(scope, &enodes_feedback, &eclass_mergers_feedback);

        Self {
            enodes: vec![],
            eclass_mergers: vec![],
            enode_eclass_lookup,
            eclass_enode_lookup,
            eclass_mergers_feedback,
            enodes_feedback,
            canon_enodes,
            canon_enode_ids,
            scope: scope.clone(),
        }
    }

    pub fn add_enodes(&mut self, enodes: Collection<S, (ENodeId, ENode), R>) -> &mut Self {
        self.enodes.push(enodes);
        self
    }

    pub fn add_rewrite<F>(&mut self, rewrite: F) -> &mut Self
    where
        F: Rewrite<S, R>,
    {
        self.eclass_mergers.push(rewrite.render(
            &mut self.scope,
            &self.enodes_feedback,
            &self.enode_eclass_lookup,
            &self.eclass_enode_lookup,
        ));
        self
    }

    pub fn scope(&self) -> S {
        self.scope.clone()
    }

    pub fn probe_with(&self, probe: &mut Handle<S::Timestamp>) {
        self.enodes.iter().for_each(|enodes| {
            enodes.probe_with(probe);
        });
        self.eclass_mergers.iter().for_each(|merger| {
            merger.probe_with(probe);
        });
    }

    // pub fn enter<'a, T>(&self, scope: &mut Child<'a, S, T>) -> EGraph<Child<'a, S, T>, R>
    // where
    //     T: Refines<S::Timestamp> + Lattice,
    // {
    //     EGraph {
    //         enodes: self.enodes.enter(scope),
    //         eclass_nodes: self.eclass_nodes.enter(scope),
    //         eclass_mergers: self.eclass_mergers.enter(scope),
    //         eclass_canon_lookup: self.eclass_canon_lookup,
    //         canon_enode_ids: self.canon_enode_ids.enter(scope),
    //     }
    // }

    pub fn debug(&self) {
        self.enodes.iter().for_each(|enodes| {
            enodes.debug();
        });
        self.eclass_mergers.iter().for_each(|merger| {
            merger.debug();
        });
        self.enode_eclass_lookup
            .as_collection(|&src, &dest| (src, dest))
            .debug();
        self.eclass_mergers_feedback.debug();
        self.enodes_feedback.debug();
    }

    fn feedback(self) -> (ENodeCollection<S, R>, Collection<S, (ENodeId, EClassId), R>)
    where
        R: ExchangeData,
    {
        let mut scope = self.scope();

        self.enodes_feedback
            .set(&concatenate(&mut scope, self.enodes.into_iter()));
        self.eclass_mergers_feedback
            .set(&concatenate(&mut scope, self.eclass_mergers.into_iter()))
            .debug();

        let canon_enodes = self
            .canon_enodes
            .as_collection(|&enode_id, enode| (enode_id, enode.clone()));

        let eclass_lookup = self
            .enode_eclass_lookup
            .as_collection(|&src, &dest| (src, dest));

        (canon_enodes, eclass_lookup)
    }
}

type UnionReturn<S, R> = (
    ENodeEClassLookup<S, R>,
    EClassENodeLookup<S, R>,
    ENodeLookup<S, R>,
    ENodeIds<S, R>,
);

fn union<S, R>(
    scope: &mut S,
    enodes: &ENodeCollection<S, R>,
    raw_eclass_mergers: &EClassMerger<S, R>,
) -> UnionReturn<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    let (enode_eclass_lookup, eclass_enode_lookup, canon_enodes, canon_enode_ids) = scope
        .iterative::<Time, _, _>(|scope| {
            let enodes = enodes.enter(scope);
            let eclass_mergers = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

            let union_find = derive_canonical_eclass_ids(
                &eclass_mergers
                    .concat(&raw_eclass_mergers.enter(scope))
                    // This distinct could be unnecessary, but it's here to make sure that the
                    // multiplicities from the variable don't overflow within the `propagate_core()`
                    // call inside of canon id derivation
                    .distinct_core(),
                &enodes,
            );

            let eclass_union_find = union_find
                .map(|(enode, eclass)| (enode.as_eclass(), eclass))
                .arrange_by_key();

            let (add_lhs, add_rhs) = enodes.filter_split(|(enode_id, enode)| {
                if let Some(add) = enode.as_add() {
                    (Some((add.lhs(), enode_id)), Some((add.rhs(), enode_id)))
                } else {
                    (None, None)
                }
            });
            let canon_add_lhs = add_lhs
                .join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                    iter::once((parent_enode, eclass))
                });
            let canon_add_rhs = add_rhs
                .join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                    iter::once((parent_enode, eclass))
                });

            let (sub_lhs, sub_rhs) = enodes.filter_split(|(enode_id, enode)| {
                if let Some(sub) = enode.as_sub() {
                    (Some((sub.lhs(), enode_id)), Some((sub.rhs(), enode_id)))
                } else {
                    (None, None)
                }
            });
            let canon_sub_lhs = sub_lhs
                .join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                    iter::once((parent_enode, eclass))
                });
            let canon_sub_rhs = sub_rhs
                .join_core(&eclass_union_find, |_, &parent_enode, &eclass| {
                    iter::once((parent_enode, eclass))
                });

            let canon_enodes = canon_add_lhs
                .join_map(&canon_add_rhs, |&enode, &lhs, &rhs| {
                    (ENode::Add(Add::new(lhs, rhs)), enode)
                })
                .concat(
                    &canon_sub_lhs.join_map(&canon_sub_rhs, |&enode, &lhs, &rhs| {
                        (ENode::Sub(Sub::new(lhs, rhs)), enode)
                    }),
                )
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

            let canon_enodes = canon_enodes.reduce(|_enode, enode_ids, canon_enode_ids| {
                canon_enode_ids.push((*enode_ids[0].0, R::from(1)));
            });

            let canon_enode_ids = canon_enodes
                .map(|(_enode, canonical_enode_id)| canonical_enode_id)
                .leave()
                .arrange_by_self();

            let canon_enode_edges =
                canon_edges.map(|(_enode, (src, dest))| (src.as_eclass(), dest.as_eclass()));
            let canon_eclass_lookup = canon_enode_edges.concat(&canon_enode_edges.reverse());
            eclass_mergers.set(&canon_eclass_lookup);

            let union_find = union_find.leave();

            (
                union_find.arrange_by_key(),
                union_find.reverse().arrange_by_key(),
                canon_enodes.reverse().leave().arrange_by_key(),
                canon_enode_ids,
            )
        });

    (
        enode_eclass_lookup,
        eclass_enode_lookup,
        canon_enodes,
        canon_enode_ids,
    )
}

fn derive_canonical_eclass_ids<S, R>(
    eclass_parents: &EClassMerger<S, R>,
    enodes: &ENodeCollection<S, R>,
) -> Collection<S, (ENodeId, EClassId), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    let canonicalized_edges = eclass_parents.flat_map(|(src, dest)| {
        vec![
            (src.as_enode(), dest.as_enode()),
            (dest.as_enode(), src.as_enode()),
        ]
    });

    let implicit_eclass_assignment = enodes
        .map(|(enode, _)| (enode, enode.as_eclass()))
        .distinct_core();

    propagate::propagate_at(
        &canonicalized_edges,
        &implicit_eclass_assignment,
        |&eclass| eclass.0,
    )
}

pub trait Rewrite<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
{
    fn render(
        self,
        scope: &mut S,
        enodes: &ENodeCollection<S, R>,
        eclass_lookup: &ENodeEClassLookup<S, R>,
        eclass_lookup_reverse: &EClassENodeLookup<S, R>,
    ) -> EClassMerger<S, R>;
}

impl<S, R, F> Rewrite<S, R> for F
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup,
    F: FnOnce(
        &mut S,
        &ENodeCollection<S, R>,
        &ENodeEClassLookup<S, R>,
        &EClassENodeLookup<S, R>,
    ) -> EClassMerger<S, R>,
{
    fn render(
        self,
        scope: &mut S,
        enodes: &ENodeCollection<S, R>,
        eclass_lookup: &ENodeEClassLookup<S, R>,
        eclass_lookup_reverse: &EClassENodeLookup<S, R>,
    ) -> EClassMerger<S, R> {
        (self)(scope, enodes, eclass_lookup, eclass_lookup_reverse)
    }
}

/// `(add ?x (sub ?y ?x)) => ?y`
pub struct RedundantAddSubChain;

impl<S, R> Rewrite<S, R> for RedundantAddSubChain
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R>,
{
    fn render(
        self,
        scope: &mut S,
        enodes: &ENodeCollection<S, R>,
        eclass_lookup: &ENodeEClassLookup<S, R>,
        _eclass_lookup_reverse: &EClassENodeLookup<S, R>,
    ) -> EClassMerger<S, R> {
        // Canonicalizing EClassMergerRaw is optional and
        // can only be done done as an "as-of" delta-join.
        // The following is the core Horn-clause that has to be implemented
        // as a proper "forced-monotonic" delta-join. All delta streams have
        // retractions filtered out, but all permanent arrangements respect
        // retractions (only EClassLookup will experience retractions here).
        //
        // EClassMergerRaw(a_raw, f_raw) :-
        // 1   ENode(a_raw, Add(.lhs=b_raw, .rhs=c_raw)), /* [a (add b c)] */
        // 2   EClassLookup(b_raw, b),
        // 3   EClassLookup(c_raw, c),
        // 4   ENode(d_raw, Sub(.lhs=f_raw, .rhs=e_raw)), /* [d (sub f e)] */
        // 5   EClassLookup(d_raw, c), /* [c d] */
        // 6   EClassLookup(e_raw, b). /* [b e] */
        let add_nodes =
            enodes.filter_map(|(enode_id, enode)| enode.as_add().map(|add| (enode_id, add)));

        let add_nodes_by_lhs = add_nodes
            .map(|(enode_id, add)| (add.lhs(), (enode_id, add)))
            .arrange_by_key();
        let add_nodes_by_rhs = add_nodes
            .map(|(enode_id, add)| (add.rhs(), (enode_id, add)))
            .arrange_by_key();

        let sub_nodes = enodes
            .filter_map(|(enode_id, enode)| enode.as_sub().map(|sub| (enode_id.as_eclass(), sub)));

        let sub_nodes_by_id = sub_nodes.arrange_by_key();
        let sub_nodes_by_rhs = sub_nodes
            .map(|(enode_id, sub)| (sub.rhs(), (enode_id, sub)))
            .arrange_by_key();

        let (eclass_lookup_by_raw, eclass_lookup_by_canon, eclass_lookup_raw_canon_by_self) = {
            let eclass_raw_canon = eclass_lookup
                .as_collection(|&raw_enode, &canon_eclass| (raw_enode.as_eclass(), canon_eclass));

            (
                eclass_raw_canon.arrange_by_key(),
                eclass_raw_canon.reverse().arrange_by_key(),
                eclass_raw_canon.arrange_by_self(),
            )
        };

        scope.scoped("RedundantAddSubChain DeltaQuery", |delta| {
            let d_add_nodes_by_lhs = add_nodes_by_lhs
                .as_collection(|&lhs, add| (lhs, add.clone()))
                .filter_diff(|diff| diff >= &R::zero())
                .differentiate(delta);

            let d_eclass_lookup_by_raw = eclass_lookup_by_raw
                .as_collection(|&enode, &eclass| (enode, eclass))
                .filter_diff(|diff| diff >= &R::zero())
                .differentiate(delta)
                .arrange_by_key();

            let d_sub_nodes_by_id = sub_nodes_by_id
                .as_collection(|&enode, sub| (enode, sub.clone()))
                .filter_diff(|diff| diff >= &R::zero())
                .differentiate(delta)
                .arrange_by_key();

            let add_nodes_by_lhs_alt = add_nodes_by_lhs.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let add_nodes_by_rhs_alt = add_nodes_by_rhs.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_by_raw_neu = eclass_lookup_by_raw.enter_at(
                delta,
                |_, _, time| AltNeu::neu(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_by_raw_alt = eclass_lookup_by_raw.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_by_canon_neu = eclass_lookup_by_canon.enter_at(
                delta,
                |_, _, time| AltNeu::neu(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_by_canon_alt = eclass_lookup_by_canon.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let sub_nodes_by_id_neu = sub_nodes_by_id.enter_at(
                delta,
                |_, _, time| AltNeu::neu(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let sub_nodes_by_id_alt = sub_nodes_by_id.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_raw_canon_by_self_neu = eclass_lookup_raw_canon_by_self.enter_at(
                delta,
                |_, _, time| AltNeu::neu(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let eclass_lookup_raw_canon_by_self_alt = eclass_lookup_raw_canon_by_self.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let sub_nodes_by_rhs_alt = sub_nodes_by_rhs.enter_at(
                delta,
                |_, _, time| AltNeu::alt(time.clone()),
                |_time| Timestamp::minimum(),
            );

            let changes_1 = d_add_nodes_by_lhs // 1
                .join_core(
                    &eclass_lookup_by_raw_neu,
                    |_add_lhs_raw, &(add_enode_raw, ref add), &add_lhs_eclass| {
                        iter::once((add.rhs(), (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 2
                .join_core(
                    &eclass_lookup_by_raw_neu,
                    |_raw_add_rhs, &(add_enode_raw, add_lhs_eclass), &add_rhs_eclass| {
                        iter::once((add_rhs_eclass, (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 3
                .join_core(
                    &eclass_lookup_by_canon_neu,
                    |_add_rhs_eclass, &(add_enode_raw, add_lhs_eclass), &sub_enode_raw| {
                        iter::once((sub_enode_raw, (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 5
                .join_core(
                    &sub_nodes_by_id_neu,
                    |_sub_enode_raw, &(add_enode_raw, add_lhs_eclass), sub| {
                        let sub_rhs_raw = sub.rhs();
                        let sub_lhs_raw = sub.lhs();
                        iter::once(((sub_rhs_raw, add_lhs_eclass), (add_enode_raw, sub_lhs_raw)))
                    },
                ) // 4
                .join_core(
                    &eclass_lookup_raw_canon_by_self_neu,
                    |(_sub_rhs_raw, _add_lhs_eclass), &(add_enode_raw, sub_lhs_raw), _| {
                        iter::once((add_enode_raw.as_eclass(), sub_lhs_raw))
                    },
                ); // 6

            let changes_2 = d_eclass_lookup_by_raw // 2
                .join_core(
                    &add_nodes_by_lhs_alt,
                    |_add_lhs_raw, &add_lhs_eclass, &(add_enode_raw, ref add)| {
                        iter::once((add.rhs(), (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 1
                .join_core(
                    &eclass_lookup_by_raw_neu,
                    |_raw_add_rhs, &(add_enode_raw, add_lhs_eclass), &add_rhs_eclass| {
                        iter::once((add_rhs_eclass, (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 3
                .join_core(
                    &eclass_lookup_by_canon_neu,
                    |_add_rhs_eclass, &(add_enode_raw, add_lhs_eclass), &sub_enode_raw| {
                        iter::once((sub_enode_raw, (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 5
                .join_core(
                    &sub_nodes_by_id_neu,
                    |_sub_enode_raw, &(add_enode_raw, add_lhs_eclass), sub| {
                        let sub_rhs_raw = sub.rhs();
                        let sub_lhs_raw = sub.lhs();
                        iter::once(((sub_rhs_raw, add_lhs_eclass), (add_enode_raw, sub_lhs_raw)))
                    },
                ) // 4
                .join_core(
                    &eclass_lookup_raw_canon_by_self_neu,
                    |(_sub_rhs_raw, _add_lhs_eclass), &(add_enode_raw, sub_lhs_raw), _| {
                        iter::once((add_enode_raw.as_eclass(), sub_lhs_raw))
                    },
                ); // 6

            let changes_3 = d_eclass_lookup_by_raw
                .join_core(
                    &add_nodes_by_rhs_alt,
                    |_add_rhs_raw, &add_rhs_eclass, &(add_enode_raw, ref add)| {
                        iter::once((add.lhs(), (add_rhs_eclass, add_enode_raw)))
                    },
                )
                .join_core(
                    &eclass_lookup_by_raw_alt,
                    |_add_lhs_raw, &(add_rhs_eclass, add_enode_raw), &add_lhs_eclass| {
                        iter::once((add_rhs_eclass, (add_enode_raw, add_lhs_eclass)))
                    },
                )
                .join_core(
                    &eclass_lookup_by_canon_neu,
                    |_add_rhs_eclass, &(add_enode_raw, add_lhs_eclass), &sub_enode_raw| {
                        iter::once((sub_enode_raw, (add_enode_raw, add_lhs_eclass)))
                    },
                ) // 5
                .join_core(
                    &sub_nodes_by_id_neu,
                    |_sub_enode_raw, &(add_enode_raw, add_lhs_eclass), sub| {
                        let sub_rhs_raw = sub.rhs();
                        let sub_lhs_raw = sub.lhs();
                        iter::once(((sub_rhs_raw, add_lhs_eclass), (add_enode_raw, sub_lhs_raw)))
                    },
                ) // 4
                .join_core(
                    &eclass_lookup_raw_canon_by_self_neu,
                    |(_sub_rhs_raw, _add_lhs_eclass), &(add_enode_raw, sub_lhs_raw), _| {
                        iter::once((add_enode_raw.as_eclass(), sub_lhs_raw))
                    },
                ); // 6

            // 456312
            let changes_4 = d_sub_nodes_by_id
                .join_core(
                    &eclass_lookup_by_raw_alt,
                    |_sub_enode_raw, sub, &sub_eclass| {
                        iter::once((sub.rhs(), (sub_eclass, sub.lhs())))
                    },
                )
                .join_core(
                    &eclass_lookup_by_raw_alt,
                    |_sub_rhs_enode_raw, &(sub_eclass, sub_lhs_raw), &sub_rhs_eclass| {
                        iter::once((sub_eclass, (sub_lhs_raw, sub_rhs_eclass)))
                    },
                )
                .join_core(
                    &eclass_lookup_by_canon_alt,
                    |_sub_eclass, &(sub_lhs_raw, sub_rhs_eclass), &add_rhs_raw| {
                        iter::once((add_rhs_raw, (sub_lhs_raw, sub_rhs_eclass)))
                    },
                )
                .join_core(
                    &add_nodes_by_rhs_alt,
                    |_add_rhs_raw, &(sub_lhs_raw, sub_rhs_eclass), &(add_enode_raw, ref add)| {
                        iter::once(((add.lhs(), sub_rhs_eclass), (sub_lhs_raw, add_enode_raw)))
                    },
                )
                .join_core(
                    &eclass_lookup_raw_canon_by_self_alt,
                    |(_add_lhs_raw, _add_lhs_eclass), &(sub_lhs_raw, add_enode_raw), &()| {
                        iter::once((add_enode_raw.as_eclass(), sub_lhs_raw))
                    },
                );

            // 546312
            let changes_5 = d_eclass_lookup_by_raw
                .join_core(&sub_nodes_by_id_alt, |_sub_enode_raw, &sub_eclass, sub| {
                    iter::once((sub.rhs(), (sub_eclass, sub.lhs())))
                })
                .join_core(
                    &eclass_lookup_by_raw_neu,
                    |_sub_rhs_raw, &(sub_lhs_raw, sub_rhs_eclass), &sub_eclass| {
                        iter::once((sub_rhs_eclass, (sub_lhs_raw, sub_eclass)))
                    },
                )
                .join_core(
                    &eclass_lookup_by_canon_alt,
                    |_sub_rhs_eclass, &(sub_lhs_raw, sub_eclass), &add_lhs_raw| {
                        iter::once((add_lhs_raw, (sub_lhs_raw, sub_eclass)))
                    },
                )
                .join_core(
                    &add_nodes_by_lhs_alt,
                    |_add_lhs_raw, &(sub_lhs_raw, sub_eclass), &(add_enode, ref add)| {
                        iter::once(((add.rhs(), sub_eclass), (sub_lhs_raw, add_enode)))
                    },
                )
                .join_core(
                    &eclass_lookup_raw_canon_by_self_alt,
                    |(_add_rhs_raw, _add_rhs_eclass), &(sub_lhs_raw, add_enode), &()| {
                        iter::once((add_enode.as_eclass(), sub_lhs_raw))
                    },
                );

            // 645312
            let changes_6 = d_eclass_lookup_by_raw
                .join_core(
                    &sub_nodes_by_rhs_alt,
                    |_sub_rhs_raw, &sub_rhs_eclass, &(sub_enode, ref sub)| {
                        iter::once((sub_enode, (sub_rhs_eclass, sub.lhs())))
                    },
                )
                .join_core(
                    &eclass_lookup_by_raw_alt,
                    |_sub_enode, &(sub_rhs_eclass, sub_lhs), &sub_eclass| {
                        iter::once((sub_eclass, (sub_rhs_eclass, sub_lhs)))
                    },
                )
                .join_core(
                    &eclass_lookup_by_canon_alt,
                    |_sub_eclass, &(sub_rhs_eclass, sub_lhs), &add_rhs_raw| {
                        iter::once(((add_rhs_raw, sub_rhs_eclass), sub_lhs))
                    },
                )
                .join_core(
                    &eclass_lookup_raw_canon_by_self_alt,
                    |&(add_rhs_raw, _sub_rhs_eclass), &sub_lhs, &()| {
                        iter::once((add_rhs_raw, sub_lhs))
                    },
                )
                .join_core(
                    &add_nodes_by_rhs_alt,
                    |_add_rhs, &sub_lhs, &(add_enode, ref _add)| {
                        iter::once((add_enode.as_eclass(), sub_lhs))
                    },
                );

            concatenate(
                delta,
                vec![
                    changes_1, changes_2, changes_3, changes_4, changes_5, changes_6,
                ],
            )
            .integrate()
        })
    }
}

// RedundantSub(b, c) :-
//     Add(add_a, b, add_d),
//     CanonEClass(add_a, canon_a),
//     CanonEClass(add_d, canon_d),
//     Sub(sub_d, sub_a, c),
//     CanonEClass(sub_d, canon_d),
//     CanonEClass(sub_a, canon_a).

#[cfg(test)]
mod tests {
    use crate::{
        dataflow::{
            operators::{FilterMap, InspectExt, Keys, Reverse},
            Diff,
        },
        equisat::{Add, EClassId, EGraph, ENode, ENodeId, RedundantAddSubChain, Sub},
    };
    use differential_dataflow::{
        input::Input,
        operators::{
            arrange::ArrangeByKey, iterate::Variable, Consolidate, Join, JoinCore, Reduce,
            Threshold,
        },
    };
    use std::iter;
    use timely::{
        dataflow::{operators::probe::Handle, Scope},
        order::Product,
        progress::Timestamp,
    };

    #[test]
    fn union_find() {
        timely::execute_directly(|worker| {
            let mut probe = Handle::new();

            let mut enodes = worker.dataflow::<usize, _, _>(|scope| {
                let (enode_input, enodes) = scope.new_collection();

                let (nodes, edges) = scope.iterative::<usize, _, _>(|scope| {
                    let enodes = enodes.enter(scope);

                    let mut graph =
                        EGraph::<_, Diff>::new(scope, Product::new(Timestamp::minimum(), 1));
                    graph
                        .add_enodes(enodes.clone())
                        .add_rewrite(RedundantAddSubChain);

                    let (nodes_col, edges_col) = graph.feedback();

                    let edges = edges_col.map(|(node, class)| (node, class.as_enode()));
                    let initial_costs = nodes_col.map(|(node_id, node)| {
                        let cost = if node.is_constant() {
                            Some(0)
                        } else if node.is_param() {
                            Some(1)
                        } else {
                            None
                        };

                        (node_id, cost)
                    });

                    let edges_reverse = edges.reverse().arrange_by_key();
                    let (nodes, edges) = (
                        enodes
                            .antijoin(&nodes_col.keys())
                            .concat(&nodes_col)
                            .arrange_by_key(),
                        edges.arrange_by_key(),
                    );

                    let best = scope
                        .iterative::<usize, _, _>(|scope| {
                            let node_costs = Variable::new_from(
                                initial_costs.enter(scope),
                                Product::new(Timestamp::minimum(), 1),
                            );

                            let (nodes, edges, edges_reverse) = (
                                nodes.enter(scope),
                                edges.enter(scope),
                                edges_reverse.enter(scope),
                            );

                            let inputs = nodes
                                .antijoin(&node_costs.keys().debug())
                                .debug()
                                .join_core(&edges, |&node_id, node, &src_id| {
                                    iter::once((src_id, (node_id, node.clone())))
                                })
                                .debug()
                                .join_map(&node_costs, |&src_id, &(node_id, ref node), &cost| {
                                    ((node_id, node.clone()), (src_id, cost))
                                })
                                .debug()
                                .arrange_by_key();

                            let evaluated = inputs
                                .reduce(|&(_node_id, ref node), inputs, output| {
                                    if inputs.iter().all(|((_src_id, cost), _diff)| cost.is_some())
                                    {
                                        let costs = inputs
                                            .iter()
                                            .map(|&(&(src_id, cost), _)| (src_id, cost.unwrap()));

                                        match node {
                                            ENode::Add(add) => {
                                                let (_, lhs) = costs
                                                    .clone()
                                                    .filter(|&(src_id, _)| {
                                                        src_id == add.lhs().as_enode()
                                                    })
                                                    .min_by_key(|&(_, cost)| cost)
                                                    .unwrap();
                                                let (_, rhs) = costs
                                                    .filter(|&(src_id, _)| {
                                                        src_id == add.rhs().as_enode()
                                                    })
                                                    .min_by_key(|&(_, cost)| cost)
                                                    .unwrap();

                                                output.push((Some(1 + lhs + rhs), 1));
                                            }

                                            ENode::Sub(sub) => {
                                                let (_, lhs) = costs
                                                    .clone()
                                                    .filter(|&(src_id, _)| {
                                                        src_id == sub.lhs().as_enode()
                                                    })
                                                    .min_by_key(|&(_, cost)| cost)
                                                    .unwrap();
                                                let (_, rhs) = costs
                                                    .filter(|&(src_id, _)| {
                                                        src_id == sub.rhs().as_enode()
                                                    })
                                                    .min_by_key(|&(_, cost)| cost)
                                                    .unwrap();

                                                output.push((Some(1 + lhs + rhs), 1));
                                            }

                                            ENode::Constant => output.push((Some(0), 1)),
                                            ENode::Param => output.push((Some(1), 1)),
                                        }
                                    }
                                })
                                .debug()
                                .map(|((node_id, _), cost)| (node_id, cost))
                                .debug();

                            let costs = node_costs
                                .antijoin(&evaluated.keys())
                                .debug()
                                .concat(&evaluated)
                                .distinct()
                                .debug();

                            node_costs
                                .set(&costs)
                                .debug()
                                .filter_map(|(node_id, cost)| cost.map(|cost| (node_id, cost)))
                                .leave()
                        })
                        .debug();

                    (nodes_col.leave(), edges_col.leave())
                });

                nodes
                    .consolidate()
                    .inspect(|x| println!("Node: {:?}", x))
                    .probe_with(&mut probe);

                edges
                    .consolidate()
                    .inspect(|x| println!("Edge: {:?}", x))
                    .probe_with(&mut probe);

                enode_input
            });

            enodes.insert((
                ENodeId::new(0),
                ENode::Add(Add::new(EClassId::new(2), EClassId::new(1))),
            ));
            enodes.insert((
                ENodeId::new(1),
                ENode::Sub(Sub::new(EClassId::new(3), EClassId::new(2))),
            ));
            enodes.insert((ENodeId::new(2), ENode::Constant));
            enodes.insert((ENodeId::new(3), ENode::Constant));

            enodes.advance_to(1);

            enodes.insert((
                ENodeId::new(6),
                ENode::Sub(Sub::new(EClassId::new(4), EClassId::new(2))),
            ));
            enodes.insert((ENodeId::new(4), ENode::Constant));

            enodes.advance_to(3);

            enodes.insert((
                ENodeId::new(7),
                ENode::Add(Add::new(EClassId::new(8), EClassId::new(1))),
            ));
            enodes.insert((ENodeId::new(8), ENode::Constant));

            enodes.advance_to(4);

            enodes.insert((
                ENodeId::new(10),
                ENode::Add(Add::new(EClassId::new(2), EClassId::new(11))),
            ));
            enodes.insert((ENodeId::new(11), ENode::Constant));

            enodes.advance_to(5);

            enodes.flush();

            worker.step_while(|| probe.less_than(enodes.time()));
        });
    }
}
