//! Head-to-head benchmark: sqlitegis vs SpatiaLite on identical workloads.
//!
//! sqlitegis is registered in-process on a `libsqlite3-sys` connection via
//! `register_functions(db)`. SpatiaLite is loaded as an external loadable
//! extension via `sqlite3_load_extension('mod_spatialite')`. Both sides
//! use the same underlying libsqlite3 (the one `libsqlite3-sys`'s bundled
//! C amalgamation produces), so the comparison isolates predicate-callback
//! cost from SQLite engine differences.
//!
//! Run with:
//!
//! ```sh
//! cargo bench --features bench-spatialite spatialite_vs_sqlitegis
//! ```
//!
//! Requires `libsqlite3-mod-spatialite` to be installed system-wide so the
//! SQLite loader can find `mod_spatialite`.

#![cfg(all(feature = "bench-spatialite", not(target_arch = "wasm32")))]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use libsqlite3_sys::*;
use std::ffi::{CStr, CString};
use std::hint::black_box;
use std::ptr;

const SPATIALITE_LIB: &str = "mod_spatialite";

// Dataset size. Large enough for the unindexed scan to be measurable,
// small enough that the bench finishes in well under a minute.
const N_POINTS: usize = 50_000;

// PRNG seed so every bench iteration sees the same dataset.
const RNG_SEED: u64 = 0xC0FFEE;

unsafe fn open_sqlitegis_db() -> *mut sqlite3 {
    unsafe {
        let mut db = ptr::null_mut();
        let memdb = CString::new(":memory:").unwrap();
        assert_eq!(
            sqlite3_open(memdb.as_ptr(), &mut db),
            SQLITE_OK,
            "sqlite3_open failed"
        );
        let rc = sqlitegis::sqlite::register_functions(db);
        assert_eq!(rc, SQLITE_OK, "register_functions failed (rc={rc})");
        seed(db, Kind::Sqlitegis);
        db
    }
}

unsafe fn open_spatialite_db() -> *mut sqlite3 {
    unsafe {
        let mut db = ptr::null_mut();
        let memdb = CString::new(":memory:").unwrap();
        assert_eq!(
            sqlite3_open(memdb.as_ptr(), &mut db),
            SQLITE_OK,
            "sqlite3_open failed"
        );
        assert_eq!(
            sqlite3_enable_load_extension(db, 1),
            SQLITE_OK,
            "enable_load_extension failed"
        );
        let cpath = CString::new(SPATIALITE_LIB).unwrap();
        let mut err: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = sqlite3_load_extension(db, cpath.as_ptr(), ptr::null(), &mut err);
        if rc != SQLITE_OK {
            let msg = if err.is_null() {
                "(no message)".to_string()
            } else {
                CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            panic!("load_extension({SPATIALITE_LIB}) failed: rc={rc} err={msg}");
        }
        exec(db, "SELECT InitSpatialMetaData(1)");
        seed(db, Kind::Spatialite);
        db
    }
}

unsafe fn exec(db: *mut sqlite3, sql: &str) {
    unsafe {
        let csql = CString::new(sql).unwrap();
        let mut err: *mut std::os::raw::c_char = ptr::null_mut();
        let rc = sqlite3_exec(db, csql.as_ptr(), None, ptr::null_mut(), &mut err);
        if rc != SQLITE_OK {
            let msg = if err.is_null() {
                "(no message)".to_string()
            } else {
                CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            sqlite3_free(err.cast());
            panic!("exec failed (rc={rc}): {sql}: {msg}");
        }
        if !err.is_null() {
            sqlite3_free(err.cast());
        }
    }
}

unsafe fn query_count(db: *mut sqlite3, sql: &str) -> i64 {
    unsafe {
        let csql = CString::new(sql).unwrap();
        let mut stmt = ptr::null_mut();
        let rc = sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, ptr::null_mut());
        if rc != SQLITE_OK {
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_string_lossy();
            panic!("prepare failed (rc={rc}): {sql}: {msg}");
        }
        let step_rc = sqlite3_step(stmt);
        assert_eq!(step_rc, SQLITE_ROW, "expected a row from: {sql}");
        let v = sqlite3_column_int64(stmt, 0);
        sqlite3_finalize(stmt);
        v
    }
}

#[derive(Copy, Clone)]
enum Kind {
    Sqlitegis,
    Spatialite,
}

/// Seed N random WGS84 points into a `places(id, geom)` table. SpatiaLite
/// needs its geometry column declared via `AddGeometryColumn`; sqlitegis
/// is fine with a plain BLOB column.
unsafe fn seed(db: *mut sqlite3, kind: Kind) {
    unsafe {
        match kind {
            Kind::Sqlitegis => {
                exec(
                    db,
                    "CREATE TABLE places (id INTEGER PRIMARY KEY, geom BLOB)",
                );
            }
            Kind::Spatialite => {
                exec(db, "CREATE TABLE places (id INTEGER PRIMARY KEY)");
                exec(
                    db,
                    "SELECT AddGeometryColumn('places', 'geom', 4326, 'POINT', 'XY')",
                );
            }
        }
        let mut state: u64 = RNG_SEED;
        exec(db, "BEGIN");
        let insert_sql =
            CString::new("INSERT INTO places(geom) VALUES (ST_GeomFromText(?, 4326))").unwrap();
        let mut stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(db, insert_sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
            SQLITE_OK,
        );
        for _ in 0..N_POINTS {
            let (x, y) = next_xy(&mut state);
            let wkt = format!("POINT({x} {y})");
            let cwkt = CString::new(wkt).unwrap();
            sqlite3_bind_text(stmt, 1, cwkt.as_ptr(), -1, SQLITE_TRANSIENT());
            let step = sqlite3_step(stmt);
            assert_eq!(step, SQLITE_DONE, "INSERT step rc {step}");
            sqlite3_reset(stmt);
        }
        sqlite3_finalize(stmt);
        exec(db, "COMMIT");
    }
}

/// Tiny LCG (Numerical Recipes constants) for deterministic point coords.
/// Plenty good for bench seeding.
fn next_xy(state: &mut u64) -> (f64, f64) {
    let x = {
        *state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        ((*state >> 33) as f64) / (u32::MAX as f64) * 360.0 - 180.0
    };
    let y = {
        *state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        ((*state >> 33) as f64) / (u32::MAX as f64) * 180.0 - 90.0
    };
    (x, y)
}

// ---- benches ----

fn bench_bulk_intersects_unindexed(c: &mut Criterion) {
    let db_g = unsafe { open_sqlitegis_db() };
    let db_s = unsafe { open_spatialite_db() };
    let window = "POLYGON((10 20, 11 20, 11 21, 10 21, 10 20))";
    let sql = format!(
        "SELECT COUNT(*) FROM places WHERE ST_Intersects(geom, ST_GeomFromText('{window}', 4326))"
    );

    let mut group = c.benchmark_group("Unindexed ST_Intersects bulk");
    group.throughput(Throughput::Elements(N_POINTS as u64));
    group.bench_function(BenchmarkId::new("sqlitegis", N_POINTS), |b| {
        b.iter(|| unsafe { black_box(query_count(db_g, &sql)) })
    });
    group.bench_function(BenchmarkId::new("spatialite", N_POINTS), |b| {
        b.iter(|| unsafe { black_box(query_count(db_s, &sql)) })
    });
    group.finish();

    unsafe {
        sqlite3_close(db_g);
        sqlite3_close(db_s);
    }
}

criterion_group!(benches, bench_bulk_intersects_unindexed);
criterion_main!(benches);
