pub mod basic_block;
pub mod constant;
pub mod function;
pub mod instruction;
pub mod terminator;
pub mod types;
pub mod utils;
pub mod value;

pub use basic_block::{BasicBlock, BasicBlockId};
pub use constant::Constant;
pub use function::{FuncId, Function};
pub use instruction::{InstId, Instruction, VarId};
pub use terminator::Terminator;
pub use types::Type;
pub use utils::{Cast, Ident, InstructionExt, RawCast};
pub use value::{Value, ValueKind};
