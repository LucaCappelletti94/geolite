#![cfg(target_arch = "wasm32")]
//! Headless WASM integration tests for geolite-sqlite.
//!
//! Mirrors the native integration tests but uses `sqlite-wasm-rs` directly
//! and runs inside a headless browser via `wasm-bindgen-test`.

use sqlite_wasm_rs::*;
use std::ffi::{CStr, CString};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

// ── Test DB helper ───────────────────────────────────────────────────────────

struct WasmTestDb(*mut sqlite3);

impl WasmTestDb {
    fn open() -> Self {
        let mut db = std::ptr::null_mut();
        let path = CString::new(":memory:").unwrap();
        unsafe {
            assert_eq!(SQLITE_OK, sqlite3_open(path.as_ptr(), &mut db));
            assert_eq!(SQLITE_OK, geolite_sqlite::register_functions(db));
        }
        WasmTestDb(db)
    }

    unsafe fn query_row<T, F: Fn(*mut sqlite3_stmt) -> T>(&self, sql: &str, extract: F) -> T {
        let sql_c = CString::new(sql).unwrap();
        let mut stmt = std::ptr::null_mut();
        let rc = sqlite3_prepare_v2(self.0, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
        assert_eq!(SQLITE_OK, rc, "prepare failed for: {sql}");
        let step = sqlite3_step(stmt);
        assert_eq!(SQLITE_ROW, step, "step failed for: {sql}");
        let val = extract(stmt);
        sqlite3_finalize(stmt);
        val
    }

    fn query_text(&self, sql: &str) -> String {
        unsafe {
            self.query_row(sql, |stmt| {
                let ptr = sqlite3_column_text(stmt, 0);
                CStr::from_ptr(ptr as _).to_string_lossy().into_owned()
            })
        }
    }

    fn query_f64(&self, sql: &str) -> f64 {
        unsafe { self.query_row(sql, |stmt| sqlite3_column_double(stmt, 0)) }
    }

    fn query_i64(&self, sql: &str) -> i64 {
        unsafe { self.query_row(sql, |stmt| sqlite3_column_int64(stmt, 0)) }
    }

    fn query_is_null(&self, sql: &str) -> bool {
        unsafe { self.query_row(sql, |stmt| sqlite3_column_type(stmt, 0) == SQLITE_NULL) }
    }

    fn exec(&self, sql: &str) {
        let sql_c = CString::new(sql).unwrap();
        unsafe {
            let rc = sqlite3_exec(
                self.0,
                sql_c.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(SQLITE_OK, rc, "exec failed for: {sql}");
        }
    }

    fn query_all_i64(&self, sql: &str) -> Vec<i64> {
        let sql_c = CString::new(sql).unwrap();
        unsafe {
            let mut stmt = std::ptr::null_mut();
            let rc =
                sqlite3_prepare_v2(self.0, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            assert_eq!(SQLITE_OK, rc, "prepare failed for: {sql}");
            let mut vals = Vec::new();
            while sqlite3_step(stmt) == SQLITE_ROW {
                vals.push(sqlite3_column_int64(stmt, 0));
            }
            sqlite3_finalize(stmt);
            vals
        }
    }

    fn try_query_i64(&self, sql: &str) -> Result<i64, String> {
        let sql_c = CString::new(sql).unwrap();
        unsafe {
            let mut stmt = std::ptr::null_mut();
            let rc =
                sqlite3_prepare_v2(self.0, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
            if rc != SQLITE_OK {
                let err = sqlite3_errmsg(self.0);
                return Err(CStr::from_ptr(err).to_string_lossy().into_owned());
            }
            let step = sqlite3_step(stmt);
            if step != SQLITE_ROW {
                sqlite3_finalize(stmt);
                let err = sqlite3_errmsg(self.0);
                return Err(CStr::from_ptr(err).to_string_lossy().into_owned());
            }
            let val = sqlite3_column_int64(stmt, 0);
            sqlite3_finalize(stmt);
            Ok(val)
        }
    }
}

impl Drop for WasmTestDb {
    fn drop(&mut self) {
        unsafe {
            sqlite3_close(self.0);
        }
    }
}

// ── I/O round-trips ──────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn wkt_round_trip() {
    let db = WasmTestDb::open();
    let wkt = db.query_text("SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!(wkt.contains("POLYGON"), "got: {wkt}");
}

#[wasm_bindgen_test]
fn geojson_round_trip() {
    let db = WasmTestDb::open();
    let json = db.query_text(
        r#"SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1.0,2.0]}'))"#,
    );
    assert!(json.contains("Point"), "got: {json}");
}

#[wasm_bindgen_test]
fn wkb_round_trip() {
    let db = WasmTestDb::open();
    let wkt = db
        .query_text("SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(3 4)'))))");
    assert!(wkt.contains("POINT"), "got: {wkt}");
}

#[wasm_bindgen_test]
fn ewkb_round_trip() {
    let db = WasmTestDb::open();
    let wkt = db
        .query_text("SELECT ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromText('POINT(1 2)'))))");
    assert!(wkt.contains("POINT"), "got: {wkt}");
}

#[wasm_bindgen_test]
fn ewkt_round_trip() {
    let db = WasmTestDb::open();
    let ewkt = db.query_text("SELECT ST_AsEWKT(ST_GeomFromText('POINT(1 2)', 4326))");
    assert!(ewkt.starts_with("SRID=4326;"), "got: {ewkt}");
}

#[wasm_bindgen_test]
fn geomfromwkb_with_srid() {
    let db = WasmTestDb::open();
    let srid = db.query_i64(
        "SELECT ST_SRID(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(0 0)')), 4326))",
    );
    assert_eq!(srid, 4326);
}

#[wasm_bindgen_test]
fn geomfromgeojson_default_srid() {
    let db = WasmTestDb::open();
    let srid = db
        .query_i64(r#"SELECT ST_SRID(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2]}'))"#);
    assert_eq!(srid, 4326);
}

// ── Constructors ─────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_make_envelope() {
    let db = WasmTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_MakeEnvelope(0, 0, 2, 3))");
    assert!((area - 6.0).abs() < 1e-10, "area = {area}");
}

#[wasm_bindgen_test]
fn st_tile_envelope_zoom0() {
    let db = WasmTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_TileEnvelope(0, 0, 0))");
    assert!(area > 1e15, "area = {area}");
}

#[wasm_bindgen_test]
fn st_point_with_srid() {
    let db = WasmTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_Point(1, 2, 4326))");
    assert_eq!(srid, 4326);
}

#[wasm_bindgen_test]
fn st_make_line() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumPoints(ST_MakeLine(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_make_polygon() {
    let db = WasmTestDb::open();
    let t = db.query_text(
        "SELECT ST_GeometryType(ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 1,0 0)')))",
    );
    assert_eq!(t, "ST_Polygon");
}

#[wasm_bindgen_test]
fn st_make_envelope_with_srid() {
    let db = WasmTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_MakeEnvelope(0,0,1,1,4326))");
    assert_eq!(srid, 4326);
}

#[wasm_bindgen_test]
fn st_collect() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Collect(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

// ── Accessors ────────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_srid_default() {
    let db = WasmTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(srid, 0);
}

#[wasm_bindgen_test]
fn st_srid_set() {
    let db = WasmTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)', 4326))");
    assert_eq!(srid, 4326);
}

#[wasm_bindgen_test]
fn st_set_srid() {
    let db = WasmTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_SetSRID(ST_GeomFromText('POINT(0 0)'), 4326))");
    assert_eq!(srid, 4326);
}

#[wasm_bindgen_test]
fn st_geometry_type() {
    let db = WasmTestDb::open();
    let t = db.query_text("SELECT ST_GeometryType(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(t, "ST_Point");
}

#[wasm_bindgen_test]
fn st_x_y() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_Point(3.0, 4.0))");
    let y = db.query_f64("SELECT ST_Y(ST_Point(3.0, 4.0))");
    assert!((x - 3.0).abs() < 1e-10, "x = {x}");
    assert!((y - 4.0).abs() < 1e-10, "y = {y}");
}

#[wasm_bindgen_test]
fn st_is_empty() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(e, 0);
}

#[wasm_bindgen_test]
fn st_ndims() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NDims(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_coord_dim() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_CoordDim(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_zmflag() {
    let db = WasmTestDb::open();
    let z = db.query_i64("SELECT ST_Zmflag(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(z, 0);
}

#[wasm_bindgen_test]
fn st_mem_size() {
    let db = WasmTestDb::open();
    let s = db.query_i64("SELECT ST_MemSize(ST_GeomFromText('POINT(1 2)'))");
    assert!(s > 0, "mem_size = {s}");
}

#[wasm_bindgen_test]
fn st_num_points() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'))");
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn st_npoints() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NPoints(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(n, 5);
}

#[wasm_bindgen_test]
fn st_num_geometries() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Collect(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_num_interior_rings() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumInteriorRings(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 1);
}

#[wasm_bindgen_test]
fn st_num_rings() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumRings(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(n, 1);
}

#[wasm_bindgen_test]
fn st_point_n() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_PointN(ST_GeomFromText('LINESTRING(10 20,30 40)'), 2))");
    assert!((x - 30.0).abs() < 1e-10, "x = {x}");
}

#[wasm_bindgen_test]
fn st_start_point() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_StartPoint(ST_GeomFromText('LINESTRING(10 20,30 40)')))");
    assert!((x - 10.0).abs() < 1e-10, "x = {x}");
}

#[wasm_bindgen_test]
fn st_end_point() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_EndPoint(ST_GeomFromText('LINESTRING(10 20,30 40)')))");
    assert!((x - 30.0).abs() < 1e-10, "x = {x}");
}

#[wasm_bindgen_test]
fn st_exterior_ring() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_ExteriorRing(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')))",
    );
    assert_eq!(n, 5);
}

#[wasm_bindgen_test]
fn st_interior_ring_n() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_InteriorRingN(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'), 1))",
    );
    assert_eq!(n, 5);
}

#[wasm_bindgen_test]
fn st_geometry_n() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_GeometryN(ST_Collect(ST_Point(5,6), ST_Point(7,8)), 1))");
    assert!((x - 5.0).abs() < 1e-10, "x = {x}");
}

#[wasm_bindgen_test]
fn st_dimension() {
    let db = WasmTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(d, 2);
}

#[wasm_bindgen_test]
fn st_envelope() {
    let db = WasmTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,2 3)')))");
    assert!((area - 6.0).abs() < 1e-10, "area = {area}");
}

#[wasm_bindgen_test]
fn st_is_valid() {
    let db = WasmTestDb::open();
    let v = db.query_i64("SELECT ST_IsValid(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_is_valid_reason() {
    let db = WasmTestDb::open();
    let r =
        db.query_text("SELECT ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(r, "Valid Geometry");
}

// ── Measurement ──────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_area_unit_square() {
    let db = WasmTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((area - 1.0).abs() < 1e-10, "area = {area}");
}

#[wasm_bindgen_test]
fn st_distance_3_4_5() {
    let db = WasmTestDb::open();
    let d = db.query_f64("SELECT ST_Distance(ST_Point(0,0), ST_Point(3,4))");
    assert!((d - 5.0).abs() < 1e-10, "distance = {d}");
}

#[wasm_bindgen_test]
fn st_centroid_square() {
    let db = WasmTestDb::open();
    let cx =
        db.query_f64("SELECT ST_X(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')))");
    let cy =
        db.query_f64("SELECT ST_Y(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')))");
    assert!((cx - 1.0).abs() < 1e-10, "cx = {cx}");
    assert!((cy - 1.0).abs() < 1e-10, "cy = {cy}");
}

#[wasm_bindgen_test]
fn st_bbox() {
    let db = WasmTestDb::open();
    let xmin = db.query_f64("SELECT ST_XMin(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let xmax = db.query_f64("SELECT ST_XMax(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let ymin = db.query_f64("SELECT ST_YMin(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let ymax = db.query_f64("SELECT ST_YMax(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    assert!((xmin - 1.0).abs() < 1e-10);
    assert!((xmax - 3.0).abs() < 1e-10);
    assert!((ymin - 2.0).abs() < 1e-10);
    assert!((ymax - 4.0).abs() < 1e-10);
}

#[wasm_bindgen_test]
fn st_length() {
    let db = WasmTestDb::open();
    let l = db.query_f64("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0,3 4)'))");
    assert!((l - 5.0).abs() < 1e-10, "length = {l}");
}

#[wasm_bindgen_test]
fn st_perimeter() {
    let db = WasmTestDb::open();
    let p = db.query_f64("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((p - 4.0).abs() < 1e-10, "perimeter = {p}");
}

#[wasm_bindgen_test]
fn st_point_on_surface() {
    let db = WasmTestDb::open();
    let c = db.query_i64(
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_PointOnSurface(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')))",
    );
    assert_eq!(c, 1);
}

#[wasm_bindgen_test]
fn st_hausdorff_distance() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_HausdorffDistance(ST_GeomFromText('LINESTRING(0 0,1 0)'), ST_GeomFromText('LINESTRING(0 1,1 1)'))",
    );
    assert!((d - 1.0).abs() < 1e-10, "hausdorff = {d}");
}

#[wasm_bindgen_test]
fn st_distance_sphere() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_DistanceSphere(ST_Point(-0.1278, 51.5074), ST_Point(2.3522, 48.8566))",
    );
    assert!(d > 300_000.0 && d < 400_000.0, "distance_sphere = {d}");
}

#[wasm_bindgen_test]
fn st_distance_spheroid() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_DistanceSpheroid(ST_Point(-0.1278, 51.5074), ST_Point(2.3522, 48.8566))",
    );
    assert!(d > 300_000.0 && d < 400_000.0, "distance_spheroid = {d}");
}

#[wasm_bindgen_test]
fn st_length_sphere() {
    let db = WasmTestDb::open();
    let l = db.query_f64(
        "SELECT ST_LengthSphere(ST_GeomFromText('LINESTRING(-0.1278 51.5074, 2.3522 48.8566)'))",
    );
    assert!(l > 300_000.0, "length_sphere = {l}");
}

#[wasm_bindgen_test]
fn st_azimuth() {
    let db = WasmTestDb::open();
    let a = db.query_f64("SELECT ST_Azimuth(ST_Point(0,0), ST_Point(0,1))");
    assert!(a.abs() < 1e-6, "azimuth = {a}");
}

#[wasm_bindgen_test]
fn st_project() {
    let db = WasmTestDb::open();
    let y = db.query_f64("SELECT ST_Y(ST_Project(ST_Point(0,0), 111000.0, 0.0))");
    assert!((y - 1.0).abs() < 0.1, "y = {y}");
}

#[wasm_bindgen_test]
fn st_closest_point() {
    let db = WasmTestDb::open();
    let y = db.query_f64(
        "SELECT ST_Y(ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_Point(5,5)))",
    );
    assert!(y.abs() < 1e-10, "y = {y}");
}

// ── Predicates ───────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_intersects() {
    let db = WasmTestDb::open();
    let yes = db.query_i64(
        "SELECT ST_Intersects(
            ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'),
            ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))')
         )",
    );
    assert_eq!(yes, 1);

    let no = db.query_i64(
        "SELECT ST_Intersects(
            ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),
            ST_GeomFromText('POLYGON((2 2,3 2,3 3,2 3,2 2))')
         )",
    );
    assert_eq!(no, 0);
}

#[wasm_bindgen_test]
fn st_contains() {
    let db = WasmTestDb::open();
    let yes = db.query_i64(
        "SELECT ST_Contains(
            ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'),
            ST_Point(2,2)
         )",
    );
    assert_eq!(yes, 1);
}

#[wasm_bindgen_test]
fn st_dwithin() {
    let db = WasmTestDb::open();
    let yes = db.query_i64("SELECT ST_DWithin(ST_Point(0,0), ST_Point(3,4), 5.0)");
    assert_eq!(yes, 1);
    let no = db.query_i64("SELECT ST_DWithin(ST_Point(0,0), ST_Point(3,4), 4.9)");
    assert_eq!(no, 0);
}

#[wasm_bindgen_test]
fn st_within() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Within(ST_Point(2,2), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_disjoint() {
    let db = WasmTestDb::open();
    let v = db.query_i64("SELECT ST_Disjoint(ST_Point(0,0), ST_Point(10,10))");
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_covers() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_Point(2,2))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_covered_by() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_CoveredBy(ST_Point(2,2), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_equals() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('LINESTRING(1 1,0 0)'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_touches() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((1 0,2 0,2 1,1 1,1 0))'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_crosses() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(-1 0.5,2 0.5)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_overlaps() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Overlaps(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'), ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))'))",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_relate() {
    let db = WasmTestDb::open();
    let r = db.query_text("SELECT ST_Relate(ST_Point(0,0), ST_Point(0,0))");
    assert_eq!(r, "0FFFFFFF2");
}

#[wasm_bindgen_test]
fn st_relate_pattern() {
    let db = WasmTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Relate(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_Point(2,2), 'T*****FF*')",
    );
    assert_eq!(v, 1);
}

#[wasm_bindgen_test]
fn st_relate_match() {
    let db = WasmTestDb::open();
    let v = db.query_i64("SELECT ST_RelateMatch('0FFFFFFF2', '0FFF*FFF2')");
    assert_eq!(v, 1);
}

// ── Alias function tests ─────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_make_point_alias() {
    let db = WasmTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_MakePoint(7, 8))");
    assert!((x - 7.0).abs() < 1e-10, "x = {x}");
}

#[wasm_bindgen_test]
fn geometry_type_alias() {
    let db = WasmTestDb::open();
    let t = db.query_text("SELECT GeometryType(ST_GeomFromText('LINESTRING(0 0,1 1)'))");
    assert_eq!(t, "ST_LineString");
}

#[wasm_bindgen_test]
fn st_num_interior_ring_alias() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumInteriorRing(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 1);
}

#[wasm_bindgen_test]
fn st_length2d_alias() {
    let db = WasmTestDb::open();
    let l = db.query_f64("SELECT ST_Length2D(ST_GeomFromText('LINESTRING(0 0,3 4)'))");
    assert!((l - 5.0).abs() < 1e-10, "length2d = {l}");
}

#[wasm_bindgen_test]
fn st_perimeter2d_alias() {
    let db = WasmTestDb::open();
    let p =
        db.query_f64("SELECT ST_Perimeter2D(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((p - 4.0).abs() < 1e-10, "perimeter2d = {p}");
}

// ── NULL input handling tests ────────────────────────────────────────────────

#[wasm_bindgen_test]
fn null_input_st_astext() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_AsText(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_geomfromtext() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromText(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_area() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Area(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_distance() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Distance(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Distance(ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_intersects() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Intersects(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Intersects(ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_srid() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_SRID(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_x() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_X(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_geometrytype() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeometryType(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_isempty() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_IsEmpty(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_centroid() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Centroid(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_geomfromgeojson() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromGeoJSON(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_relate() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Relate(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_relate_pattern() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Relate(NULL, ST_Point(0,0), 'T*****FF*')"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), NULL, 'T*****FF*')"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_relatematch() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_RelateMatch(NULL, '0FFF*FFF2')"));
    assert!(db.query_is_null("SELECT ST_RelateMatch('0FFFFFFF2', NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_closestpoint() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_ClosestPoint(NULL, ST_Point(0,0))"));
    assert!(
        db.query_is_null("SELECT ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), NULL)")
    );
}

#[wasm_bindgen_test]
fn null_input_st_makeline() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_MakeLine(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_MakeLine(ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_collect() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Collect(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Collect(ST_Point(0,0), NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_setsrid() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_SetSRID(NULL, 4326)"));
}

#[wasm_bindgen_test]
fn null_input_st_pointn() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_PointN(NULL, 1)"));
}

#[wasm_bindgen_test]
fn null_input_st_geometryn() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeometryN(NULL, 1)"));
}

#[wasm_bindgen_test]
fn null_input_st_interiorringn() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_InteriorRingN(NULL, 1)"));
}

#[wasm_bindgen_test]
fn null_input_st_project() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_Project(NULL, 100.0, 0.0)"));
}

#[wasm_bindgen_test]
fn null_input_st_dwithin() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_DWithin(NULL, ST_Point(0,0), 5.0)"));
    assert!(db.query_is_null("SELECT ST_DWithin(ST_Point(0,0), NULL, 5.0)"));
}

#[wasm_bindgen_test]
fn null_input_st_geomfromwkb() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromWKB(NULL)"));
}

#[wasm_bindgen_test]
fn null_input_st_geomfromewkb() {
    let db = WasmTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromEWKB(NULL)"));
}

// ── Multi-geometry tests ─────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_npoints_multipoint() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NPoints(ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'))");
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn st_npoints_multilinestring() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 4))'))",
    );
    assert_eq!(n, 5);
}

#[wasm_bindgen_test]
fn st_npoints_multipolygon() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'))",
    );
    assert_eq!(n, 10);
}

#[wasm_bindgen_test]
fn st_npoints_geometrycollection() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))'))",
    );
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn st_num_geometries_multipoint() {
    let db = WasmTestDb::open();
    let n =
        db.query_i64("SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'))");
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn st_num_geometries_multilinestring() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumGeometries(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))",
    );
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_num_geometries_multipolygon() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'))",
    );
    assert_eq!(n, 2);
}

#[wasm_bindgen_test]
fn st_num_geometries_single_point() {
    let db = WasmTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Point(1, 2))");
    assert_eq!(n, 1);
}

#[wasm_bindgen_test]
fn st_geometry_n_multilinestring() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_GeometryN(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 4))'), 2))",
    );
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn st_geometry_n_multipolygon() {
    let db = WasmTestDb::open();
    let t = db.query_text(
        "SELECT ST_GeometryType(ST_GeometryN(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'), 1))",
    );
    assert_eq!(t, "ST_Polygon");
}

#[wasm_bindgen_test]
fn st_is_empty_linestring() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_is_empty_polygon() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('POLYGON EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_is_empty_multipoint() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTIPOINT EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_is_empty_multilinestring() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTILINESTRING EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_is_empty_multipolygon() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTIPOLYGON EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_is_empty_geometrycollection() {
    let db = WasmTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))");
    assert_eq!(e, 1);
}

#[wasm_bindgen_test]
fn st_perimeter_multipolygon() {
    let db = WasmTestDb::open();
    let p = db.query_f64(
        "SELECT ST_Perimeter(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,4 2,4 4,2 4,2 2)))'))",
    );
    assert!((p - 12.0).abs() < 1e-10, "perimeter = {p}");
}

// ── Mixed-type distance tests ────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_distance_point_to_linestring() {
    let db = WasmTestDb::open();
    let d =
        db.query_f64("SELECT ST_Distance(ST_Point(0,5), ST_GeomFromText('LINESTRING(0 0,10 0)'))");
    assert!((d - 5.0).abs() < 1e-10, "distance = {d}");
}

#[wasm_bindgen_test]
fn st_distance_point_to_polygon() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_Point(0,5), ST_GeomFromText('POLYGON((1 0,3 0,3 2,1 2,1 0))'))",
    );
    assert!(d > 0.0, "distance = {d}");
}

#[wasm_bindgen_test]
fn st_distance_linestring_to_linestring() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('LINESTRING(0 3,10 3)'))",
    );
    assert!((d - 3.0).abs() < 1e-10, "distance = {d}");
}

#[wasm_bindgen_test]
fn st_distance_linestring_to_polygon() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('LINESTRING(0 5,10 5)'), ST_GeomFromText('POLYGON((0 0,10 0,10 2,0 2,0 0))'))",
    );
    assert!((d - 3.0).abs() < 1e-10, "distance = {d}");
}

#[wasm_bindgen_test]
fn st_distance_polygon_to_polygon() {
    let db = WasmTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((3 0,4 0,4 1,3 1,3 0))'))",
    );
    assert!((d - 2.0).abs() < 1e-10, "distance = {d}");
}

// ── Validity edge cases ──────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_is_valid_invalid_polygon() {
    let db = WasmTestDb::open();
    let v = db.query_i64("SELECT ST_IsValid(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))'))");
    assert_eq!(v, 0);
}

#[wasm_bindgen_test]
fn st_is_valid_reason_invalid_polygon() {
    let db = WasmTestDb::open();
    let r =
        db.query_text("SELECT ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))'))");
    assert_ne!(r, "Valid Geometry", "got: {r}");
}

// ── MultiLineString spherical length ─────────────────────────────────────────

#[wasm_bindgen_test]
fn st_length_sphere_multilinestring() {
    let db = WasmTestDb::open();
    let l = db.query_f64(
        "SELECT ST_LengthSphere(ST_GeomFromText('MULTILINESTRING((-0.1278 51.5074, 2.3522 48.8566),(2.3522 48.8566, 13.4050 52.5200))'))",
    );
    assert!(l > 600_000.0, "length_sphere = {l}");
}

// ── MultiLineString planar length ────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_length_multilinestring() {
    let db = WasmTestDb::open();
    let l =
        db.query_f64("SELECT ST_Length(ST_GeomFromText('MULTILINESTRING((0 0,3 4),(10 0,10 5))'))");
    assert!((l - 10.0).abs() < 1e-10, "length = {l}");
}

// ── Dimension for various types ──────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_dimension_point() {
    let db = WasmTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_Point(0, 0))");
    assert_eq!(d, 0);
}

#[wasm_bindgen_test]
fn st_dimension_linestring() {
    let db = WasmTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0,1 1)'))");
    assert_eq!(d, 1);
}

#[wasm_bindgen_test]
fn st_dimension_multipoint() {
    let db = WasmTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTIPOINT((0 0),(1 1))'))");
    assert_eq!(d, 0);
}

#[wasm_bindgen_test]
fn st_dimension_multilinestring() {
    let db = WasmTestDb::open();
    let d = db
        .query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))");
    assert_eq!(d, 1);
}

#[wasm_bindgen_test]
fn st_dimension_multipolygon() {
    let db = WasmTestDb::open();
    let d = db
        .query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))'))");
    assert_eq!(d, 2);
}

// ── Centroid of a LineString ─────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_centroid_linestring() {
    let db = WasmTestDb::open();
    let cx = db.query_f64("SELECT ST_X(ST_Centroid(ST_GeomFromText('LINESTRING(0 0,10 0)')))");
    assert!((cx - 5.0).abs() < 1e-10, "cx = {cx}");
}

// ── Num rings with holes ─────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn st_num_rings_with_hole() {
    let db = WasmTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumRings(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 2);
}

// ── Spatial Index tests ──────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn spatial_index_create_query_drop() {
    let db = WasmTestDb::open();
    db.exec("CREATE TABLE places (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec(
        "INSERT INTO places (geom) VALUES (ST_GeomFromText('POINT(1 2)')),\
         (ST_GeomFromText('POINT(3 4)')),\
         (ST_GeomFromText('POINT(5 6)'))",
    );

    let rc = db.query_i64("SELECT CreateSpatialIndex('places', 'geom')");
    assert_eq!(rc, 1);

    let count = db.query_i64("SELECT COUNT(*) FROM places_geom_rtree");
    assert_eq!(count, 3);

    let hits = db.query_all_i64(
        "SELECT id FROM places_geom_rtree WHERE xmin >= 2 AND xmax <= 6 AND ymin >= 3 AND ymax <= 7",
    );
    assert_eq!(hits.len(), 2);

    let rc = db.query_i64("SELECT DropSpatialIndex('places', 'geom')");
    assert_eq!(rc, 1);

    let count = db.query_i64("SELECT COUNT(*) FROM sqlite_master WHERE name = 'places_geom_rtree'");
    assert_eq!(count, 0);
}

#[wasm_bindgen_test]
fn spatial_index_rtree_plus_exact_predicate() {
    let db = WasmTestDb::open();
    db.exec("CREATE TABLE polys (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec(
        "INSERT INTO polys (geom) VALUES \
         (ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')),\
         (ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))')),\
         (ST_GeomFromText('POLYGON((10 10,11 10,11 11,10 11,10 10))'))",
    );
    db.exec("SELECT CreateSpatialIndex('polys', 'geom')");

    let hits = db.query_all_i64(
        "SELECT p.id FROM polys p \
         JOIN polys_geom_rtree r ON p.rowid = r.id \
         WHERE r.xmax >= 0.5 AND r.xmin <= 2.5 AND r.ymax >= 0.5 AND r.ymin <= 2.5 \
         AND ST_Intersects(p.geom, ST_MakeEnvelope(0.5, 0.5, 2.5, 2.5))",
    );
    assert_eq!(hits.len(), 2);
}

#[wasm_bindgen_test]
fn spatial_index_trigger_sync() {
    let db = WasmTestDb::open();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec("SELECT CreateSpatialIndex('t', 'geom')");

    db.exec("INSERT INTO t (geom) VALUES (ST_Point(1, 2))");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 1);

    db.exec("INSERT INTO t (geom) VALUES (NULL)");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 1);

    db.exec("UPDATE t SET geom = ST_Point(10, 20) WHERE id = 1");
    let xmin = db.query_f64("SELECT xmin FROM t_geom_rtree WHERE id = 1");
    assert!((xmin - 10.0).abs() < 1e-10, "xmin = {xmin}");

    db.exec("UPDATE t SET geom = NULL WHERE id = 1");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree WHERE id = 1");
    assert_eq!(count, 0);

    db.exec("UPDATE t SET geom = ST_Point(7, 8) WHERE id = 2");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree WHERE id = 2");
    assert_eq!(count, 1);

    db.exec("DELETE FROM t WHERE id = 2");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 0);
}

#[wasm_bindgen_test]
fn spatial_index_narrows_candidates() {
    let db = WasmTestDb::open();
    db.exec("CREATE TABLE grid (id INTEGER PRIMARY KEY, geom BLOB)");

    for x in 0..10 {
        for y in 0..10 {
            db.exec(&format!(
                "INSERT INTO grid (geom) VALUES (ST_Point({x}, {y}))"
            ));
        }
    }
    db.exec("SELECT CreateSpatialIndex('grid', 'geom')");

    let full_scan = db.query_i64("SELECT COUNT(*) FROM grid");
    assert_eq!(full_scan, 100);

    let rtree_hits = db.query_all_i64(
        "SELECT g.id FROM grid g \
         JOIN grid_geom_rtree r ON g.rowid = r.id \
         WHERE r.xmin >= 1.5 AND r.xmax <= 3.5 AND r.ymin >= 1.5 AND r.ymax <= 3.5",
    );
    assert_eq!(rtree_hits.len(), 4);
}

#[wasm_bindgen_test]
fn spatial_index_rejects_invalid_names() {
    let db = WasmTestDb::open();

    let res = db.try_query_i64("SELECT CreateSpatialIndex('places; DROP TABLE x', 'geom')");
    assert!(res.is_err(), "should reject: {res:?}");

    let res = db.try_query_i64("SELECT CreateSpatialIndex('', 'geom')");
    assert!(res.is_err(), "should reject empty: {res:?}");

    let res = db.try_query_i64("SELECT CreateSpatialIndex('my table', 'geom')");
    assert!(res.is_err(), "should reject spaces: {res:?}");

    let res = db.try_query_i64("SELECT DropSpatialIndex('ok', 'col name')");
    assert!(res.is_err(), "should reject spaces in col: {res:?}");
}

#[wasm_bindgen_test]
fn spatial_index_drop_idempotent() {
    let db = WasmTestDb::open();
    db.exec("CREATE TABLE pts (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec("SELECT CreateSpatialIndex('pts', 'geom')");

    let rc = db.query_i64("SELECT DropSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);

    let rc = db.query_i64("SELECT DropSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);
}
