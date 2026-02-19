#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
//! Native SQLite integration tests for geolite-diesel.
//!
//! Uses `sqlite3_auto_extension` to register geolite functions on every
//! `SqliteConnection::establish()` call, then exercises spatial functions
//! through the Diesel query builder against a real SQLite database.

use std::sync::Once;

use diesel::prelude::*;
use diesel::sql_query;

// ── Auto-extension registration ──────────────────────────────────────────────

static INIT: Once = Once::new();

/// Entry point called by SQLite for each new connection.
unsafe extern "C" fn geolite_init(
    db: *mut libsqlite3_sys::sqlite3,
    _pz_err_msg: *mut *mut std::ffi::c_char,
    _p_api: *const libsqlite3_sys::sqlite3_api_routines,
) -> std::ffi::c_int {
    geolite_sqlite::register_functions(db)
}

fn conn() -> SqliteConnection {
    INIT.call_once(|| unsafe {
        libsqlite3_sys::sqlite3_auto_extension(Some(geolite_init));
    });
    SqliteConnection::establish(":memory:").unwrap()
}

// ── Shared test definitions ──────────────────────────────────────────────────

include!("diesel_test_helpers.rs");
define_diesel_sqlite_tests!(test);

// ── Native-only: spatial index performance ───────────────────────────────────

#[test]
fn spatial_index_performance() {
    let mut c = conn();

    // Create table with 10K points on a 100x100 grid
    sql_query(
        "CREATE TABLE perf_grid (
            id   INTEGER PRIMARY KEY,
            geom BLOB
        )",
    )
    .execute(&mut c)
    .unwrap();

    for i in 0..100 {
        for j in 0..100 {
            let id = i * 100 + j;
            sql_query(format!(
                "INSERT INTO perf_grid (id, geom) VALUES ({id}, ST_Point({i}, {j}))"
            ))
            .execute(&mut c)
            .unwrap();
        }
    }

    // Non-indexed query: scan all rows
    let non_indexed_sql = "SELECT COUNT(*) AS val FROM perf_grid
        WHERE ST_Intersects(geom, ST_MakeEnvelope(20, 20, 30, 30)) = 1";

    // Measure non-indexed
    let start = std::time::Instant::now();
    for _ in 0..10 {
        let _: I32Result = sql_query(non_indexed_sql).get_result(&mut c).unwrap();
    }
    let non_indexed_time = start.elapsed();

    // Create spatial index
    sql_query("SELECT CreateSpatialIndex('perf_grid', 'geom')")
        .execute(&mut c)
        .unwrap();

    // Indexed query: R-tree join
    let indexed_sql = "SELECT COUNT(*) AS val FROM perf_grid g
        JOIN perf_grid_geom_rtree r ON g.rowid = r.id
        WHERE r.xmin >= 20 AND r.xmax <= 30 AND r.ymin >= 20 AND r.ymax <= 30
        AND ST_Intersects(g.geom, ST_MakeEnvelope(20, 20, 30, 30)) = 1";

    // Measure indexed
    let start = std::time::Instant::now();
    for _ in 0..10 {
        let _: I32Result = sql_query(indexed_sql).get_result(&mut c).unwrap();
    }
    let indexed_time = start.elapsed();

    // Verify correctness: both should return 11*11 = 121 points (20..=30 inclusive)
    let non_indexed_count: I32Result = sql_query(non_indexed_sql).get_result(&mut c).unwrap();
    let indexed_count: I32Result = sql_query(indexed_sql).get_result(&mut c).unwrap();
    assert_eq!(non_indexed_count.val, indexed_count.val);
    assert_eq!(non_indexed_count.val, Some(121));

    // Assert indexed is at least 2x faster
    assert!(
        indexed_time < non_indexed_time / 2,
        "Expected indexed ({indexed_time:?}) to be at least 2x faster than non-indexed ({non_indexed_time:?})"
    );
}
