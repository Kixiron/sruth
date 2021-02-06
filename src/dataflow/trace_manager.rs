use std::any::{Any, TypeId};

use differential_dataflow::trace::TraceReader;
use fxhash::FxHashMap;
use lasso::Spur;
use timely::progress::frontier::AntichainRef;

pub struct TraceManager<T> {
    traces: FxHashMap<Spur, Box<dyn ManagedTrace<T>>>,
}

impl<T> TraceManager<T> {
    pub fn new() -> Self {
        Self {
            traces: FxHashMap::default(),
        }
    }

    pub fn insert_trace<Trace>(
        &mut self,
        key: Spur,
        trace: Trace,
    ) -> Option<Box<dyn ManagedTrace<T>>>
    where
        Trace: ManagedTrace<T> + 'static,
    {
        self.traces.insert(key, Box::new(trace))
    }

    pub fn remove_trace(&mut self, key: Spur) -> Option<Box<dyn ManagedTrace<T>>> {
        self.traces.remove(&key)
    }

    pub fn get_trace<Trace>(&self, key: Spur) -> Option<Trace>
    where
        T: 'static,
        Trace: ManagedTrace<T> + Any + Clone,
    {
        self.traces
            .get(&key)
            .and_then(|trace| {
                let trace: &dyn ManagedTrace<T> = &**trace;
                if trace.inner_type_id() == TypeId::of::<Trace>() {
                    Some(unsafe { &*(trace as *const dyn ManagedTrace<T> as *const Trace) })
                } else {
                    None
                }
            })
            .cloned()
    }

    pub fn advance_by(&mut self, frontier: AntichainRef<'_, T>) {
        for trace in self.traces.values_mut() {
            trace.advance_by(frontier);
        }
    }

    pub fn distinguish_since(&mut self, frontier: AntichainRef<'_, T>) {
        for trace in self.traces.values_mut() {
            trace.distinguish_since(frontier);
        }
    }
}

impl<T> Default for TraceManager<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ManagedTrace<T>: Any {
    fn advance_by(&mut self, frontier: AntichainRef<'_, T>);

    fn distinguish_since(&mut self, frontier: AntichainRef<'_, T>);

    fn inner_type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }
}

impl<Trace, T> ManagedTrace<T> for Trace
where
    Trace: TraceReader<Time = T> + Any + 'static,
{
    #[inline]
    fn advance_by(&mut self, frontier: AntichainRef<'_, T>) {
        TraceReader::advance_by(self, frontier)
    }

    #[inline]
    fn distinguish_since(&mut self, frontier: AntichainRef<'_, T>) {
        TraceReader::distinguish_since(self, frontier)
    }
}
