#![doc = include_str!("../../README.md")]
//! Crate-specific API surface for `geolite-core`.

pub mod error;
pub mod ewkb;
pub mod functions;

pub use error::{GeoLiteError, Result};
