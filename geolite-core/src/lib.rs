//! `geolite-core` — Pure-Rust PostGIS-compatible spatial geometry library.
//!
//! All functions in this crate are pure Rust with zero C dependencies.
//! SQLite wiring lives in `geolite-sqlite`; Diesel types live in `geolite-diesel`.

pub mod error;
pub mod ewkb;
pub mod functions;

pub use error::{GeoLiteError, Result};
