#![doc = include_str!("../../README.md")]
//! Crate-specific API surface for `geolite-sqlite`.

mod ffi;
mod sqlite_compat;
pub use ffi::register_functions;
