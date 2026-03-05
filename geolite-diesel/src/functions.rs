//! Diesel SQL function definitions for spatial operations.
//!
//! Most declarations in this module are generated from the canonical function
//! catalog via `cargo run -p xtask -- gen-function-surfaces`.

use crate::types::Geometry;
use diesel::sql_types::{Binary, Double, Integer, Nullable, Text};

include!("generated/functions.rs");
