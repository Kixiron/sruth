mod constant_folding;
mod dead_code;
mod peephole;

pub use constant_folding::constant_folding;
pub use dead_code::dead_code;
pub use peephole::peephole;
