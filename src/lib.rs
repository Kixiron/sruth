#![feature(
    crate_visibility_modifier,
    generic_associated_types,
    iter_order_by,
    step_trait
)]
#![allow(incomplete_features)]

pub mod builder;
pub mod dataflow;
pub mod optimize;
pub mod repr;
mod tests;
pub mod verify;
pub mod vsdg;
pub mod wasm;
