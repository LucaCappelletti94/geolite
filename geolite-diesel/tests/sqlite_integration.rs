#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
//! Native SQLite integration tests for geolite-diesel.
//!
//! Uses `sqlite3_auto_extension` to register geolite functions on every
//! `SqliteConnection::establish()` call, then exercises spatial functions
//! through the Diesel query builder against a real SQLite database.

use std::sync::Once;

use diesel::prelude::*;
use diesel::sql_query;

mod predicate_bool_helpers;

#[derive(QueryableByName, Debug)]
struct PlanRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    detail: String,
}

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

#[test]
fn shared_predicates_and_relate_bool_semantics() {
    let mut c = conn();
    predicate_bool_helpers::assert_predicates_and_relate_bool_semantics_sqlite(&mut c);
}

// ── Native-only: deterministic spatial index behavior ───────────────────────

#[test]
fn spatial_index_narrows_candidates_deterministically() {
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

    // Baseline table cardinality (all rows)
    let full_scan_count: I32Result = sql_query("SELECT COUNT(*) AS val FROM perf_grid")
        .get_result(&mut c)
        .unwrap();
    assert_eq!(full_scan_count.val, Some(10_000));

    // Create spatial index
    sql_query("SELECT CreateSpatialIndex('perf_grid', 'geom')")
        .execute(&mut c)
        .unwrap();

    // Indexed query: R-tree join
    let indexed_sql = "SELECT COUNT(*) AS val FROM perf_grid g
        JOIN perf_grid_geom_rtree r ON g.rowid = r.id
         WHERE r.xmin >= 20 AND r.xmax <= 30 AND r.ymin >= 20 AND r.ymax <= 30
        AND ST_Intersects(g.geom, ST_MakeEnvelope(20, 20, 30, 30)) = 1";

    // Deterministic coarse candidate count from the R-tree join only.
    let candidate_sql = "SELECT COUNT(*) AS val FROM perf_grid g
        JOIN perf_grid_geom_rtree r ON g.rowid = r.id
        WHERE r.xmin >= 20 AND r.xmax <= 30 AND r.ymin >= 20 AND r.ymax <= 30";
    let candidate_count: I32Result = sql_query(candidate_sql).get_result(&mut c).unwrap();
    assert_eq!(candidate_count.val, Some(121));
    assert!(
        candidate_count
            .val
            .expect("candidate count should not be NULL")
            < full_scan_count
                .val
                .expect("full scan count should not be NULL")
    );

    // Verify correctness: both should return 11*11 = 121 points (20..=30 inclusive).
    let non_indexed_count: I32Result = sql_query(non_indexed_sql).get_result(&mut c).unwrap();
    let indexed_count: I32Result = sql_query(indexed_sql).get_result(&mut c).unwrap();
    assert_eq!(non_indexed_count.val, indexed_count.val);
    assert_eq!(non_indexed_count.val, Some(121));

    // Query-plan sanity checks (deterministic structure, not wall-clock timing).
    let non_indexed_plan: Vec<PlanRow> = sql_query(format!("EXPLAIN QUERY PLAN {non_indexed_sql}"))
        .load(&mut c)
        .unwrap();
    assert!(
        non_indexed_plan
            .iter()
            .any(|row| row.detail.contains("SCAN perf_grid")),
        "expected a table scan in non-indexed plan, got: {non_indexed_plan:?}"
    );

    let indexed_plan: Vec<PlanRow> = sql_query(format!("EXPLAIN QUERY PLAN {indexed_sql}"))
        .load(&mut c)
        .unwrap();
    assert!(
        indexed_plan.iter().any(|row| {
            row.detail.contains("perf_grid_geom_rtree")
                || row.detail.contains("VIRTUAL TABLE INDEX")
                || row.detail.contains("USING INDEX")
        }),
        "expected indexed plan to reference the R-tree/index path, got: {indexed_plan:?}"
    );
}
