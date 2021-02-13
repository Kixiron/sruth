use crate::{
    dataflow::{Diff, InputManager, Time},
    repr::{basic_block::BasicBlockMeta, function::FunctionMeta, Instruction},
};
use abomonation_derive::Abomonation;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct EffectEdge {
    pub from: InstId,
    pub to: InstId,
}

pub struct FunctionBuilder {
    meta: FunctionMeta,
    blocks: Rc<RefCell<Vec<BasicBlockMeta>>>,
    instructions: Rc<RefCell<Vec<Instruction>>>,
    effect_edges: Rc<RefCell<Vec<EffectEdge>>>,
}

impl FunctionBuilder {
    pub fn append_basic_block(&mut self) -> BasicBlockBuilder {
        BasicBlockBuilder {
            blocks: self.blocks.clone(),
            instructions: self.instructions.clone(),
            effect_edges: self.effect_edges.clone(),
        }
    }
}

pub struct BasicBlockBuilder {
    blocks: Rc<RefCell<Vec<BasicBlockMeta>>>,
    instructions: Rc<RefCell<Vec<Instruction>>>,
    effect_edges: Rc<RefCell<Vec<EffectEdge>>>,
}

#[derive(Debug, Default)]
pub struct DataCache {
    func_id_counter: AtomicU64,
    block_id_counter: AtomicU64,
    inst_id_counter: AtomicU64,
}

impl DataCache {
    pub fn new() -> Self {
        Self {
            func_id_counter: AtomicU64::new(0),
            block_id_counter: AtomicU64::new(0),
            inst_id_counter: AtomicU64::new(0),
        }
    }
}
