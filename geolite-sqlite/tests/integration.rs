#![cfg(not(target_arch = "wasm32"))]
//! Integration tests for geolite-sqlite.

use libsqlite3_sys::*;
use std::ffi::{CStr, CString};
use std::path::PathBuf;
use std::process::Command;

include!("test_db_macro.rs");
define_test_db!(TestDb);
type ActiveTestDb = TestDb;

include!("support/shared_cases.rs");
define_shared_cases!(test);

#[test]
fn exports_default_sqlite_entrypoint_symbol() {
    let dylib: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("debug")
        .join("libgeolite_sqlite.so");
    assert!(
        dylib.exists(),
        "expected {} to exist before symbol inspection",
        dylib.display()
    );

    let output = Command::new("nm")
        .arg("-D")
        .arg(&dylib)
        .output()
        .expect("`nm -D` must be available to inspect exported symbols");
    assert!(
        output.status.success(),
        "nm failed with status {:?}",
        output.status.code()
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("sqlite3_geolite_init"),
        "missing sqlite3_geolite_init export:\n{stdout}"
    );
    assert!(
        stdout.contains("sqlite3_geolitesqlite_init"),
        "missing sqlite3_geolitesqlite_init export:\n{stdout}"
    );
}
