use crate::vsdg::node::{FuncId, Function, Node, NodeId};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    input::{Input, InputSession},
    lattice::Lattice,
    operators::{iterate::Variable, Consolidate},
    Collection, ExchangeData,
};
use timely::{
    dataflow::{operators::probe::Handle, scopes::Child, Scope},
    progress::{timestamp::Refines, Timestamp},
};

pub type Edge = (NodeId, NodeId);

#[derive(Clone)]
pub struct ProgramGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    pub value_edges: Collection<S, Edge, R>,
    pub effect_edges: Collection<S, Edge, R>,
    pub control_edges: Collection<S, Edge, R>,
    pub nodes: Collection<S, (NodeId, Node), R>,
    pub function_nodes: Collection<S, (NodeId, FuncId), R>,
    pub functions: Collection<S, (FuncId, Function), R>,
}

impl<S, R> ProgramGraph<S, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn new(scope: &mut S) -> (Self, ProgramInputs<S::Timestamp, R>)
    where
        S: Input,
    {
        let (value_edge_input, value_edges) = scope.new_collection();
        let (effect_edge_input, effect_edges) = scope.new_collection();
        let (control_edge_input, control_edges) = scope.new_collection();
        let (node_input, nodes) = scope.new_collection();
        let (function_nodes_input, function_nodes) = scope.new_collection();
        let (function_input, functions) = scope.new_collection();

        let graph = Self {
            value_edges,
            effect_edges,
            control_edges,
            nodes,
            function_nodes,
            functions,
        };

        let inputs = ProgramInputs {
            value_edges: value_edge_input,
            effect_edges: effect_edge_input,
            control_edges: control_edge_input,
            nodes: node_input,
            function_nodes: function_nodes_input,
            functions: function_input,
        };

        (graph, inputs)
    }

    pub fn scope(&self) -> S {
        self.value_edges.scope()
    }

    pub fn enter<'a, T>(&self, scope: &Child<'a, S, T>) -> ProgramGraph<Child<'a, S, T>, R>
    where
        T: Timestamp + Refines<S::Timestamp>,
    {
        ProgramGraph {
            value_edges: self.value_edges.enter(scope),
            effect_edges: self.effect_edges.enter(scope),
            control_edges: self.control_edges.enter(scope),
            nodes: self.nodes.enter(scope),
            function_nodes: self.function_nodes.enter(scope),
            functions: self.functions.enter(scope),
        }
    }

    pub fn enter_region<'a>(
        &self,
        scope: &Child<'a, S, S::Timestamp>,
    ) -> ProgramGraph<Child<'a, S, S::Timestamp>, R> {
        ProgramGraph {
            value_edges: self.value_edges.enter_region(scope),
            effect_edges: self.effect_edges.enter_region(scope),
            control_edges: self.control_edges.enter_region(scope),
            nodes: self.nodes.enter_region(scope),
            function_nodes: self.function_nodes.enter_region(scope),
            functions: self.functions.enter_region(scope),
        }
    }

    pub fn consolidate(&self) -> Self
    where
        S::Timestamp: Lattice,
        R: ExchangeData,
    {
        Self {
            value_edges: self.value_edges.consolidate(),
            effect_edges: self.effect_edges.consolidate(),
            control_edges: self.control_edges.consolidate(),
            nodes: self.nodes.consolidate(),
            function_nodes: self.function_nodes.consolidate(),
            functions: self.functions.consolidate(),
        }
    }

    pub fn probe(&self) -> Handle<S::Timestamp> {
        let mut handle = Handle::new();
        self.probe_with(&mut handle);

        handle
    }

    pub fn probe_with(&self, handle: &mut Handle<S::Timestamp>) -> Self {
        Self {
            value_edges: self.value_edges.probe_with(handle),
            effect_edges: self.effect_edges.probe_with(handle),
            control_edges: self.control_edges.probe_with(handle),
            nodes: self.nodes.probe_with(handle),
            function_nodes: self.function_nodes.probe_with(handle),
            functions: self.functions.probe_with(handle),
        }
    }

    pub fn all_edges(&self) -> Collection<S, Edge, R> {
        self.value_edges
            .concatenate(vec![self.effect_edges.clone(), self.control_edges.clone()])
    }
}

impl<'a, S, T, R> ProgramGraph<Child<'a, S, T>, R>
where
    S: Scope,
    R: Semigroup,
    T: Timestamp + Refines<S::Timestamp>,
{
    pub fn leave(&self) -> ProgramGraph<S, R> {
        ProgramGraph {
            value_edges: self.value_edges.leave(),
            effect_edges: self.effect_edges.leave(),
            control_edges: self.control_edges.leave(),
            nodes: self.nodes.leave(),
            function_nodes: self.function_nodes.leave(),
            functions: self.functions.leave(),
        }
    }
}

impl<'a, S, R> ProgramGraph<Child<'a, S, S::Timestamp>, R>
where
    S: Scope,
    R: Semigroup,
{
    pub fn leave_region(&self) -> ProgramGraph<S, R> {
        ProgramGraph {
            value_edges: self.value_edges.leave_region(),
            effect_edges: self.effect_edges.leave_region(),
            control_edges: self.control_edges.leave_region(),
            nodes: self.nodes.leave_region(),
            function_nodes: self.function_nodes.leave_region(),
            functions: self.functions.leave_region(),
        }
    }
}

impl<S, R> From<&ProgramVariable<S, R>> for ProgramGraph<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian,
{
    fn from(program: &ProgramVariable<S, R>) -> Self {
        Self {
            value_edges: program.value_edges.clone(),
            effect_edges: program.effect_edges.clone(),
            control_edges: program.control_edges.clone(),
            nodes: program.nodes.clone(),
            function_nodes: program.function_nodes.clone(),
            functions: program.functions.clone(),
        }
    }
}

pub struct ProgramVariable<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian,
{
    pub value_edges: Variable<S, Edge, R>,
    pub effect_edges: Variable<S, Edge, R>,
    pub control_edges: Variable<S, Edge, R>,
    pub nodes: Variable<S, (NodeId, Node), R>,
    pub function_nodes: Variable<S, (NodeId, FuncId), R>,
    pub functions: Variable<S, (FuncId, Function), R>,
}

impl<S, R> ProgramVariable<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Abelian,
{
    pub fn new(graph: ProgramGraph<S, R>, summary: <S::Timestamp as Timestamp>::Summary) -> Self {
        Self {
            value_edges: Variable::new_from(graph.value_edges, summary.clone()),
            effect_edges: Variable::new_from(graph.effect_edges, summary.clone()),
            control_edges: Variable::new_from(graph.control_edges, summary.clone()),
            nodes: Variable::new_from(graph.nodes, summary.clone()),
            function_nodes: Variable::new_from(graph.function_nodes, summary.clone()),
            functions: Variable::new_from(graph.functions, summary),
        }
    }

    pub fn set(self, result: &ProgramGraph<S, R>) -> ProgramGraph<S, R> {
        ProgramGraph {
            value_edges: self.value_edges.set(&result.value_edges),
            control_edges: self.control_edges.set(&result.control_edges),
            effect_edges: self.effect_edges.set(&result.effect_edges),
            nodes: self.nodes.set(&result.nodes),
            function_nodes: self.function_nodes.set(&result.function_nodes),
            functions: self.functions.set(&result.functions),
        }
    }

    pub fn set_concat(self, result: &ProgramGraph<S, R>) -> ProgramGraph<S, R> {
        ProgramGraph {
            value_edges: self.value_edges.set_concat(&result.value_edges),
            control_edges: self.control_edges.set_concat(&result.control_edges),
            effect_edges: self.effect_edges.set_concat(&result.effect_edges),
            nodes: self.nodes.set_concat(&result.nodes),
            function_nodes: self.function_nodes.set_concat(&result.function_nodes),
            functions: self.functions.set_concat(&result.functions),
        }
    }

    pub fn program(&self) -> ProgramGraph<S, R> {
        ProgramGraph {
            value_edges: self.value_edges.clone(),
            control_edges: self.control_edges.clone(),
            effect_edges: self.effect_edges.clone(),
            nodes: self.nodes.clone(),
            function_nodes: self.function_nodes.clone(),
            functions: self.functions.clone(),
        }
    }
}

pub struct ProgramInputs<T, R>
where
    T: Timestamp,
    R: Semigroup,
{
    pub value_edges: InputSession<T, Edge, R>,
    pub effect_edges: InputSession<T, Edge, R>,
    pub control_edges: InputSession<T, Edge, R>,
    pub nodes: InputSession<T, (NodeId, Node), R>,
    pub function_nodes: InputSession<T, (NodeId, FuncId), R>,
    pub functions: InputSession<T, (FuncId, Function), R>,
}

impl<T, R> ProgramInputs<T, R>
where
    T: Timestamp,
    R: Semigroup,
{
    pub fn advance_to(&mut self, time: T)
    where
        T: Clone,
    {
        tracing::info!("advancing program inputs to timestamp {:?}", time);
        self.ensure_consistent_timestamps();

        self.value_edges.advance_to(time.clone());
        self.effect_edges.advance_to(time.clone());
        self.control_edges.advance_to(time.clone());
        self.nodes.advance_to(time.clone());
        self.function_nodes.advance_to(time.clone());
        self.functions.advance_to(time);
    }

    pub fn flush(&mut self)
    where
        T: Clone,
    {
        tracing::info!("flushing program inputs");
        self.ensure_consistent_timestamps();

        self.value_edges.flush();
        self.effect_edges.flush();
        self.control_edges.flush();
        self.nodes.flush();
        self.function_nodes.flush();
        self.functions.flush();
    }

    pub fn time(&self) -> &T {
        self.ensure_consistent_timestamps();
        self.value_edges.time()
    }

    /// Asserts (in debug mode only) that the timestamps of all contained collections
    /// are the same in order to ensure consistency.
    fn ensure_consistent_timestamps(&self) {
        debug_assert_eq!(self.value_edges.time(), self.effect_edges.time());
        debug_assert_eq!(self.effect_edges.time(), self.control_edges.time());
        debug_assert_eq!(self.control_edges.time(), self.nodes.time());
        debug_assert_eq!(self.nodes.time(), self.function_nodes.time());
        debug_assert_eq!(self.function_nodes.time(), self.functions.time());
    }
}
