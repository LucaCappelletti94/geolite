//! `geolite-diesel` — Diesel ORM types for geolite spatial functions.
//!
//! Provides [`Geometry`] and [`Geography`] SQL types that map to `Binary`
//! (BLOB) columns in SQLite and `bytea` in PostgreSQL.  Geometry values are
//! stored as EWKB blobs and can be read/written as `Vec<u8>` (raw bytes) or
//! directly as [`geo::Geometry<f64>`].
//!
//! # Backends
//!
//! Enable the appropriate Cargo feature for your backend:
//!
//! ```toml
//! # SQLite (note: conflicts with rusqlite ≥ 0.38 in the same binary)
//! geolite-diesel = { version = "0.1", features = ["sqlite"] }
//!
//! # PostgreSQL (future)
//! geolite-diesel = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! # Quick start
//!
//! ```rust,ignore
//! use diesel::prelude::*;
//! use geolite_diesel::prelude::*;
//!
//! diesel::table! {
//!     features (id) {
//!         id   -> Integer,
//!         name -> Text,
//!         geom -> geolite_diesel::Geometry,
//!     }
//! }
//!
//! // Method-style
//! let nearby: Vec<(i32, String)> = features::table
//!     .filter(features::geom.nullable().st_dwithin(st_point(13.4050, 52.5200), 1000.0))
//!     .select((features::id, features::name))
//!     .load(&mut conn)?;
//!
//! // Function-style (still works)
//! let nearby: Vec<(i32, String)> = features::table
//!     .filter(st_dwithin(features::geom, st_point(13.4050, 52.5200), 1000.0))
//!     .select((features::id, features::name))
//!     .load(&mut conn)?;
//! ```

pub mod expression_methods;
pub mod functions;
pub mod prelude;
pub mod types;

pub use expression_methods::GeometryExpressionMethods;
pub use types::{Geography, Geometry};
