use crate::vsdg::node::Node;
use differential_dataflow::{difference::Semigroup, Collection};
use timely::dataflow::Scope;

#[derive(Debug, PartialEq)]
struct EClassId;

struct ENode;

struct EClass;

struct EGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    classes: Collection<S, (EClassId, EClass), R>,
    worklist: Collection<S, EClassId, R>,
}

impl<S, R> EGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn add(&mut self, enode: ENode) -> EClassId {
        // Canonicalize the enode

        // If the canonicalized enode already exists, return it

        // Otherwise create a new eclass
        // Add the canonicalized enode to it
        // Make the new enode & eclass a parent of all of the enode's children
        // Add the enode and its eclass to the hashcons
        // Return the new eclass's id

        todo!()
    }

    pub fn merge(&mut self, left: EClassId, right: EClassId) -> EClassId {
        // If find(left) == find(right) return find(left)

        // Otherwise union the left and right classes into a new eclass
        // Add the new eclass's id to the worklist
        // Return the new eclass's id

        todo!()
    }

    pub fn canonicalize(&mut self, enode: ENode) -> ENode {
        // find(child) on every child of the enode
        // Create a new enode with the newly found children
        // Return the new enode

        todo!()
    }

    pub fn find(&self, eclass: EClassId) -> EClassId {
        // Find the root eclass id by using a union find's find() function
        // Return the root eclass id

        todo!()
    }

    pub fn rebuild(&mut self) {
        // Until the worklist is empty (iterative scope)
        //     Canonicalize & deduplicate all eclasses within the worklist
        //     Repair each (canonical & deduplicated) eclass, adding any
        //     eclasses which now need rebuilding to the worklist

        todo!()
    }

    pub fn repair(&mut self, eclass: EClassId) {
        // For each parent enode/eclass of the current eclass
        //     Remove them from the hashcons
        //     Canonicalize the parent enode
        //     Set the hashcon of the parent enode to the find of the parent eclass

        // For each parent enode/eclass of the current eclass
        //     Canonicalize the parent enode
        //     If the canonicalized parent enode is already a parent of the current eclass
        //         Merge the current parent and the canonicalized parent
        //         Add (merged_parent_enode, find(eclass)) to the current enode's parents

        todo!()
    }
}
