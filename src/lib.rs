#![feature(
    step_trait,
    iter_order_by,
    option_expect_none,
    generic_associated_types,
    crate_visibility_modifier
)]
#![allow(incomplete_features)]

pub mod builder;
pub mod dataflow;
mod equisat;
pub mod optimize;
pub mod repr;
mod tests;
pub mod verify;
pub mod vsdg;
pub mod wasm;
