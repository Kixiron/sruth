mod input_manager;
mod program;
mod trace_manager;
mod translate;

pub mod operators;

pub use input_manager::InputManager;
pub use program::{Program, ProgramVariable};
pub use trace_manager::TraceManager;
pub use translate::translate;

pub type Diff = isize;

pub type Time = usize;
