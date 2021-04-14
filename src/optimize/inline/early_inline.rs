use crate::{dataflow::Program, optimize::inline::InlineHeuristics, repr::FuncId};
use differential_dataflow::{
    difference::{Abelian, Multiply, Semigroup},
    lattice::Lattice,
    Collection, ExchangeData,
};
use timely::dataflow::Scope;

pub fn early_inline<S, R>(
    program: &Program<S, R>,
    heuristics: &Collection<S, (FuncId, InlineHeuristics), R>,
) -> Program<S, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    R: Semigroup + Abelian + ExchangeData + Multiply<Output = R>,
{
    let trivially_inlinable = heuristics
        .filter(|(_, heuristics)| heuristics.trivially_inlinable())
        .inspect(|((func, heuristics), _, _)| {
            tracing::trace!(
                "decided to inline trivial function {:?} based on {:?}",
                func,
                heuristics,
            );
        });

    program.inline_functions(&trivially_inlinable.map(|(func, _)| func))
}
