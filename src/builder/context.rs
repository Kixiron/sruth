use crate::{
    builder::Builder,
    repr::{BasicBlockId, FuncId, InstId, VarId},
};
use lasso::ThreadedRodeo;
use std::{
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug)]
pub struct Context {
    pub(super) interner: ThreadedRodeo,
    func_counter: AtomicU64,
    block_counter: AtomicU64,
    inst_counter: AtomicU64,
    var_counter: AtomicU64,
}

// Public API
impl Context {
    pub fn new() -> Self {
        Self {
            interner: ThreadedRodeo::new(),
            func_counter: AtomicU64::new(0),
            block_counter: AtomicU64::new(0),
            inst_counter: AtomicU64::new(0),
            var_counter: AtomicU64::new(0),
        }
    }

    pub fn builder(self: &Arc<Self>) -> Builder {
        Builder::new(self.clone())
    }

    pub fn interner(&self) -> &ThreadedRodeo {
        &self.interner
    }
}

// Private API
impl Context {
    crate fn function_id(&self) -> FuncId {
        FuncId::new(fetch_id(&self.func_counter))
    }

    crate fn block_id(&self) -> BasicBlockId {
        BasicBlockId::new(fetch_id(&self.block_counter))
    }

    crate fn inst_id(&self) -> InstId {
        InstId::new(fetch_id(&self.inst_counter))
    }

    crate fn var_id(&self) -> VarId {
        VarId::new(fetch_id(&self.var_counter))
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn fetch_id(counter: &AtomicU64) -> NonZeroU64 {
    let int = counter.fetch_add(1, Ordering::Relaxed) + 1;

    if cfg!(debug_assertions) {
        if int == u64::max_value() {
            panic!("created the maximum number of ids (how did you even manage that?)");
        }

        NonZeroU64::new(int).expect("created an invalid id")
    } else {
        unsafe { NonZeroU64::new_unchecked(int) }
    }
}
