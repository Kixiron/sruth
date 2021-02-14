mod constant_folding;
mod inline;
mod peephole;

pub use constant_folding::constant_folding;
pub use inline::harvest_heuristics;
pub use peephole::peephole;
