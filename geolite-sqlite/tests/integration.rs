#![cfg(not(target_arch = "wasm32"))]
//! Integration tests for geolite-sqlite.

use libsqlite3_sys::*;
use std::ffi::{CStr, CString};

include!("test_db_macro.rs");
define_test_db!(TestDb);
type ActiveTestDb = TestDb;

include!("support/shared_cases.rs");
define_shared_cases!(test);
