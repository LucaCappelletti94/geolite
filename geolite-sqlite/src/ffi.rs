//! SQLite extension registration via raw FFI.
//!
//! Registers all geolite functions on a raw `*mut sqlite3` handle.
//! On native targets also exports the `sqlite3_geolite_init` C entry point
//! so SQLite can load this library as a loadable extension.

use crate::sqlite_compat::sqlite_transient;
use crate::sqlite_compat::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_int;

use geolite_core::functions::accessors::*;
use geolite_core::functions::constructors::*;
use geolite_core::functions::io::*;
use geolite_core::functions::measurement::*;
use geolite_core::functions::predicates::*;

// ── Constants ────────────────────────────────────────────────────────────────

const DET: c_int = SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS;

/// `SQLITE_DIRECTONLY` (0x80000) prevents use from triggers/views.
/// Not yet exported by all `libsqlite3-sys` versions, so we define it here.
const SQLITE_DIRECTONLY_FLAG: c_int = 0x0008_0000;
const DIRECT: c_int = SQLITE_UTF8 | SQLITE_DIRECTONLY_FLAG;

// ── Argument-extraction helpers ──────────────────────────────────────────────

unsafe fn get_blob<'a>(argv: *mut *mut sqlite3_value, i: usize) -> Option<&'a [u8]> {
    let v = *argv.add(i);
    if sqlite3_value_type(v) == SQLITE_NULL {
        return None;
    }
    let ptr = sqlite3_value_blob(v) as *const u8;
    let len = sqlite3_value_bytes(v) as usize;
    if ptr.is_null() || len == 0 {
        return None;
    }
    Some(std::slice::from_raw_parts(ptr, len))
}

unsafe fn get_text<'a>(argv: *mut *mut sqlite3_value, i: usize) -> Option<&'a str> {
    let v = *argv.add(i);
    if sqlite3_value_type(v) == SQLITE_NULL {
        return None;
    }
    let ptr = sqlite3_value_text(v);
    let len = sqlite3_value_bytes(v) as usize;
    if ptr.is_null() {
        return None;
    }
    std::str::from_utf8(std::slice::from_raw_parts(ptr as _, len)).ok()
}

unsafe fn get_f64(argv: *mut *mut sqlite3_value, i: usize) -> f64 {
    sqlite3_value_double(*argv.add(i))
}

unsafe fn get_i32(argv: *mut *mut sqlite3_value, i: usize) -> i32 {
    sqlite3_value_int(*argv.add(i))
}

// ── Result-setting helpers ───────────────────────────────────────────────────

unsafe fn set_blob(ctx: *mut sqlite3_context, data: &[u8]) {
    sqlite3_result_blob(
        ctx,
        data.as_ptr() as _,
        data.len() as c_int,
        sqlite_transient(),
    );
}

unsafe fn set_text(ctx: *mut sqlite3_context, s: &str) {
    sqlite3_result_text(ctx, s.as_ptr() as _, s.len() as c_int, sqlite_transient());
}

unsafe fn set_f64(ctx: *mut sqlite3_context, v: f64) {
    sqlite3_result_double(ctx, v);
}
unsafe fn set_i64(ctx: *mut sqlite3_context, v: i64) {
    sqlite3_result_int64(ctx, v);
}
unsafe fn set_i32(ctx: *mut sqlite3_context, v: i32) {
    sqlite3_result_int(ctx, v);
}
unsafe fn set_null(ctx: *mut sqlite3_context) {
    sqlite3_result_null(ctx);
}

unsafe fn set_error(ctx: *mut sqlite3_context, msg: &str) {
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("geolite error").unwrap());
    sqlite3_result_error(ctx, c.as_ptr(), -1);
}

// ── Convenience setter wrappers ──────────────────────────────────────────────

unsafe fn set_bool(ctx: *mut sqlite3_context, v: bool) {
    set_i32(ctx, v as i32);
}
unsafe fn set_blob_owned(ctx: *mut sqlite3_context, v: Vec<u8>) {
    set_blob(ctx, &v);
}
unsafe fn set_text_owned(ctx: *mut sqlite3_context, v: impl AsRef<str>) {
    set_text(ctx, v.as_ref());
}

// ── Callback macros ──────────────────────────────────────────────────────────
//
// Each macro generates an `unsafe extern "C" fn` with the standard SQLite
// scalar-function signature. NULL blob/text inputs produce NULL output
// (PostGIS-compatible). Errors produce sqlite3_result_error.

/// 1 blob → Result<T>, with a custom setter expression.
macro_rules! xfunc_blob {
    ($name:ident, $label:expr, $func:expr, $set:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(b) = get_blob(argv, 0) else {
                set_null(ctx);
                return;
            };
            match $func(b) {
                Ok(v) => $set(ctx, v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
    };
}

/// 2 blobs → Result<T>, with a custom setter expression.
macro_rules! xfunc_blob2 {
    ($name:ident, $label:expr, $func:expr, $set:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(a) = get_blob(argv, 0) else {
                set_null(ctx);
                return;
            };
            let Some(b) = get_blob(argv, 1) else {
                set_null(ctx);
                return;
            };
            match $func(a, b) {
                Ok(v) => $set(ctx, v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
    };
}

// ── I/O callbacks ────────────────────────────────────────────────────────────

/// text + optional SRID → blob
macro_rules! xfunc_text_optsrid_blob {
    ($name1:ident, $name2:ident, $label:expr, $func:expr) => {
        unsafe extern "C" fn $name1(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(t) = get_text(argv, 0) else {
                set_null(ctx);
                return;
            };
            match $func(t, None) {
                Ok(v) => set_blob(ctx, &v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
        unsafe extern "C" fn $name2(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(t) = get_text(argv, 0) else {
                set_null(ctx);
                return;
            };
            let srid = get_i32(argv, 1);
            match $func(t, Some(srid)) {
                Ok(v) => set_blob(ctx, &v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
    };
}

/// blob + optional SRID → blob
macro_rules! xfunc_blob_optsrid_blob {
    ($name1:ident, $name2:ident, $label:expr, $func:expr) => {
        unsafe extern "C" fn $name1(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(b) = get_blob(argv, 0) else {
                set_null(ctx);
                return;
            };
            match $func(b, None) {
                Ok(v) => set_blob(ctx, &v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
        unsafe extern "C" fn $name2(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            let Some(b) = get_blob(argv, 0) else {
                set_null(ctx);
                return;
            };
            let srid = get_i32(argv, 1);
            match $func(b, Some(srid)) {
                Ok(v) => set_blob(ctx, &v),
                Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
            }
        }
    };
}

xfunc_text_optsrid_blob!(
    st_geomfromtext_1_xfunc,
    st_geomfromtext_2_xfunc,
    "ST_GeomFromText",
    geom_from_text
);
xfunc_blob_optsrid_blob!(
    st_geomfromwkb_1_xfunc,
    st_geomfromwkb_2_xfunc,
    "ST_GeomFromWKB",
    geom_from_wkb
);
xfunc_blob!(
    st_geomfromewkb_xfunc,
    "ST_GeomFromEWKB",
    geom_from_ewkb,
    set_blob_owned
);

unsafe extern "C" fn st_geomfromgeojson_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(json) = get_text(argv, 0) else {
        set_null(ctx);
        return;
    };
    match geom_from_geojson(json, None) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_GeomFromGeoJSON: {e}")),
    }
}

xfunc_blob!(st_astext_xfunc, "ST_AsText", as_text, set_text_owned);
xfunc_blob!(st_asewkt_xfunc, "ST_AsEWKT", as_ewkt, set_text_owned);
xfunc_blob!(st_asbinary_xfunc, "ST_AsBinary", as_binary, set_blob_owned);
xfunc_blob!(st_asewkb_xfunc, "ST_AsEWKB", as_ewkb, set_blob_owned);
xfunc_blob!(
    st_asgeojson_xfunc,
    "ST_AsGeoJSON",
    as_geojson,
    set_text_owned
);

// ── Constructor callbacks ────────────────────────────────────────────────────

unsafe extern "C" fn st_point_2_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if sqlite3_value_type(*argv.add(0)) == SQLITE_NULL
        || sqlite3_value_type(*argv.add(1)) == SQLITE_NULL
    {
        set_null(ctx);
        return;
    }
    let x = get_f64(argv, 0);
    let y = get_f64(argv, 1);
    match st_point(x, y, None) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_Point: {e}")),
    }
}

unsafe extern "C" fn st_point_3_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    if sqlite3_value_type(*argv.add(0)) == SQLITE_NULL
        || sqlite3_value_type(*argv.add(1)) == SQLITE_NULL
        || sqlite3_value_type(*argv.add(2)) == SQLITE_NULL
    {
        set_null(ctx);
        return;
    }
    let x = get_f64(argv, 0);
    let y = get_f64(argv, 1);
    let srid = get_i32(argv, 2);
    match st_point(x, y, Some(srid)) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_Point: {e}")),
    }
}

xfunc_blob2!(
    st_makeline_xfunc,
    "ST_MakeLine",
    st_make_line,
    set_blob_owned
);
xfunc_blob!(
    st_makepolygon_xfunc,
    "ST_MakePolygon",
    st_make_polygon,
    set_blob_owned
);

unsafe extern "C" fn st_makeenvelope_4_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let xmin = get_f64(argv, 0);
    let ymin = get_f64(argv, 1);
    let xmax = get_f64(argv, 2);
    let ymax = get_f64(argv, 3);
    match st_make_envelope(xmin, ymin, xmax, ymax, None) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_MakeEnvelope: {e}")),
    }
}

unsafe extern "C" fn st_makeenvelope_5_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let xmin = get_f64(argv, 0);
    let ymin = get_f64(argv, 1);
    let xmax = get_f64(argv, 2);
    let ymax = get_f64(argv, 3);
    let srid = get_i32(argv, 4);
    match st_make_envelope(xmin, ymin, xmax, ymax, Some(srid)) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_MakeEnvelope: {e}")),
    }
}

xfunc_blob2!(st_collect_xfunc, "ST_Collect", st_collect, set_blob_owned);

unsafe extern "C" fn st_tileenvelope_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let zoom = get_i32(argv, 0) as u32;
    let tile_x = get_i32(argv, 1) as u32;
    let tile_y = get_i32(argv, 2) as u32;
    match st_tile_envelope(zoom, tile_x, tile_y) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_TileEnvelope: {e}")),
    }
}

// ── Accessor callbacks ───────────────────────────────────────────────────────

xfunc_blob!(st_srid_xfunc, "ST_SRID", st_srid, set_i32);

unsafe extern "C" fn st_setsrid_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(b) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let srid = get_i32(argv, 1);
    match st_set_srid(b, srid) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_SetSRID: {e}")),
    }
}

xfunc_blob!(
    st_geometrytype_xfunc,
    "ST_GeometryType",
    st_geometry_type,
    set_text_owned
);
xfunc_blob!(st_ndims_xfunc, "ST_NDims", st_ndims, set_i32);
xfunc_blob!(st_coorddim_xfunc, "ST_CoordDim", st_coord_dim, set_i32);
xfunc_blob!(st_zmflag_xfunc, "ST_Zmflag", st_zmflag, set_i32);
xfunc_blob!(st_isempty_xfunc, "ST_IsEmpty", st_is_empty, set_bool);
xfunc_blob!(st_memsize_xfunc, "ST_MemSize", st_mem_size, set_i64);
xfunc_blob!(st_x_xfunc, "ST_X", st_x, set_f64);
xfunc_blob!(st_y_xfunc, "ST_Y", st_y, set_f64);
xfunc_blob!(st_numpoints_xfunc, "ST_NumPoints", st_num_points, set_i32);
xfunc_blob!(st_npoints_xfunc, "ST_NPoints", st_npoints, set_i32);
xfunc_blob!(
    st_numgeometries_xfunc,
    "ST_NumGeometries",
    st_num_geometries,
    set_i32
);
xfunc_blob!(
    st_numinteriorrings_xfunc,
    "ST_NumInteriorRings",
    st_num_interior_rings,
    set_i32
);
xfunc_blob!(st_numrings_xfunc, "ST_NumRings", st_num_rings, set_i32);

unsafe extern "C" fn st_pointn_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(b) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let n = get_i32(argv, 1);
    match st_point_n(b, n, None) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_PointN: {e}")),
    }
}

xfunc_blob!(
    st_startpoint_xfunc,
    "ST_StartPoint",
    st_start_point,
    set_blob_owned
);
xfunc_blob!(
    st_endpoint_xfunc,
    "ST_EndPoint",
    st_end_point,
    set_blob_owned
);
xfunc_blob!(
    st_exteriorring_xfunc,
    "ST_ExteriorRing",
    st_exterior_ring,
    set_blob_owned
);

unsafe extern "C" fn st_interiorringn_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(b) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let n = get_i32(argv, 1);
    match st_interior_ring_n(b, n) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_InteriorRingN: {e}")),
    }
}

unsafe extern "C" fn st_geometryn_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(b) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let n = get_i32(argv, 1);
    match st_geometry_n(b, n) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_GeometryN: {e}")),
    }
}

xfunc_blob!(st_dimension_xfunc, "ST_Dimension", st_dimension, set_i32);
xfunc_blob!(
    st_envelope_xfunc,
    "ST_Envelope",
    st_envelope,
    set_blob_owned
);
xfunc_blob!(st_isvalid_xfunc, "ST_IsValid", st_is_valid, set_bool);
xfunc_blob!(
    st_isvalidreason_xfunc,
    "ST_IsValidReason",
    st_is_valid_reason,
    set_text_owned
);

// ── Measurement callbacks ────────────────────────────────────────────────────

xfunc_blob!(st_area_xfunc, "ST_Area", st_area, set_f64);
xfunc_blob!(st_length_xfunc, "ST_Length", st_length, set_f64);
xfunc_blob!(st_perimeter_xfunc, "ST_Perimeter", st_perimeter, set_f64);
xfunc_blob2!(st_distance_xfunc, "ST_Distance", st_distance, set_f64);
xfunc_blob!(
    st_centroid_xfunc,
    "ST_Centroid",
    st_centroid,
    set_blob_owned
);
xfunc_blob!(
    st_pointonsurface_xfunc,
    "ST_PointOnSurface",
    st_point_on_surface,
    set_blob_owned
);
xfunc_blob2!(
    st_hausdorffdistance_xfunc,
    "ST_HausdorffDistance",
    st_hausdorff_distance,
    set_f64
);
xfunc_blob!(st_xmin_xfunc, "ST_XMin", st_xmin, set_f64);
xfunc_blob!(st_xmax_xfunc, "ST_XMax", st_xmax, set_f64);
xfunc_blob!(st_ymin_xfunc, "ST_YMin", st_ymin, set_f64);
xfunc_blob!(st_ymax_xfunc, "ST_YMax", st_ymax, set_f64);
xfunc_blob2!(
    st_distancesphere_xfunc,
    "ST_DistanceSphere",
    st_distance_sphere,
    set_f64
);
xfunc_blob2!(
    st_distancespheroid_xfunc,
    "ST_DistanceSpheroid",
    st_distance_spheroid,
    set_f64
);
xfunc_blob!(
    st_lengthsphere_xfunc,
    "ST_LengthSphere",
    st_length_sphere,
    set_f64
);
xfunc_blob2!(st_azimuth_xfunc, "ST_Azimuth", st_azimuth, set_f64);

unsafe extern "C" fn st_project_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(origin) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let distance = get_f64(argv, 1);
    let azimuth = get_f64(argv, 2);
    match st_project(origin, distance, azimuth) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_Project: {e}")),
    }
}

xfunc_blob2!(
    st_closestpoint_xfunc,
    "ST_ClosestPoint",
    st_closest_point,
    set_blob_owned
);

// ── Predicate callbacks ──────────────────────────────────────────────────────

xfunc_blob2!(
    st_intersects_xfunc,
    "ST_Intersects",
    st_intersects,
    set_bool
);
xfunc_blob2!(st_contains_xfunc, "ST_Contains", st_contains, set_bool);
xfunc_blob2!(st_within_xfunc, "ST_Within", st_within, set_bool);
xfunc_blob2!(st_disjoint_xfunc, "ST_Disjoint", st_disjoint, set_bool);

unsafe extern "C" fn st_dwithin_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(a) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let Some(b) = get_blob(argv, 1) else {
        set_null(ctx);
        return;
    };
    let d = get_f64(argv, 2);
    match st_dwithin(a, b, d) {
        Ok(v) => set_i32(ctx, v as i32),
        Err(e) => set_error(ctx, &format!("ST_DWithin: {e}")),
    }
}

xfunc_blob2!(st_covers_xfunc, "ST_Covers", st_covers, set_bool);
xfunc_blob2!(st_coveredby_xfunc, "ST_CoveredBy", st_covered_by, set_bool);
xfunc_blob2!(st_equals_xfunc, "ST_Equals", st_equals, set_bool);
xfunc_blob2!(st_touches_xfunc, "ST_Touches", st_touches, set_bool);
xfunc_blob2!(st_crosses_xfunc, "ST_Crosses", st_crosses, set_bool);
xfunc_blob2!(st_overlaps_xfunc, "ST_Overlaps", st_overlaps, set_bool);

xfunc_blob2!(st_relate_2_xfunc, "ST_Relate", st_relate, set_text_owned);

unsafe extern "C" fn st_relate_3_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(a) = get_blob(argv, 0) else {
        set_null(ctx);
        return;
    };
    let Some(b) = get_blob(argv, 1) else {
        set_null(ctx);
        return;
    };
    let Some(pattern) = get_text(argv, 2) else {
        set_null(ctx);
        return;
    };
    match st_relate_match_geoms(a, b, pattern) {
        Ok(v) => set_i32(ctx, v as i32),
        Err(e) => set_error(ctx, &format!("ST_Relate: {e}")),
    }
}

unsafe extern "C" fn st_relatematch_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some(matrix) = get_text(argv, 0) else {
        set_null(ctx);
        return;
    };
    let Some(pattern) = get_text(argv, 1) else {
        set_null(ctx);
        return;
    };
    set_i32(ctx, st_relate_match(matrix, pattern) as i32);
}

// ── Spatial index helpers ─────────────────────────────────────────────────────

fn validate_identifier(s: &str) -> Option<&str> {
    if s.is_empty() {
        return None;
    }
    if s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Some(s)
    } else {
        None
    }
}

/// Run SQL via `sqlite3_exec`, returning `SQLITE_OK` on success.
/// On failure, sets `sqlite3_result_error` on `ctx` with the error message
/// from SQLite and frees it via `sqlite3_free`.
unsafe fn exec_sql(db: *mut sqlite3, ctx: *mut sqlite3_context, sql: &str) -> c_int {
    let c_sql = CString::new(sql).unwrap();
    let mut err_msg: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = sqlite3_exec(db, c_sql.as_ptr(), None, std::ptr::null_mut(), &mut err_msg);
    if rc != SQLITE_OK {
        if !err_msg.is_null() {
            let msg = CStr::from_ptr(err_msg).to_string_lossy();
            set_error(ctx, &msg);
            sqlite3_free(err_msg as _);
        } else {
            set_error(ctx, "exec_sql failed");
        }
    }
    rc
}

// ── Spatial index callbacks ──────────────────────────────────────────────────

/// Extract and validate `(table, column)` identifiers from the first two args.
/// On failure, sets an error on `ctx` and returns `None`.
unsafe fn get_table_column<'a>(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    label: &str,
) -> Option<(&'a str, &'a str)> {
    let Some(table) = get_text(argv, 0) else {
        set_error(ctx, &format!("{label}: table name must not be NULL"));
        return None;
    };
    let Some(column) = get_text(argv, 1) else {
        set_error(ctx, &format!("{label}: column name must not be NULL"));
        return None;
    };
    let Some(table) = validate_identifier(table) else {
        set_error(
            ctx,
            &format!("{label}: invalid table name (only [a-zA-Z0-9_] allowed)"),
        );
        return None;
    };
    let Some(column) = validate_identifier(column) else {
        set_error(
            ctx,
            &format!("{label}: invalid column name (only [a-zA-Z0-9_] allowed)"),
        );
        return None;
    };
    Some((table, column))
}

unsafe extern "C" fn create_spatial_index_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some((table, column)) = get_table_column(ctx, argv, "CreateSpatialIndex") else {
        return;
    };

    let db = sqlite3_context_db_handle(ctx);
    let rtree = format!("{table}_{column}_rtree");

    // 1. Create R-tree virtual table
    let sql = format!("CREATE VIRTUAL TABLE [{rtree}] USING rtree(id, xmin, xmax, ymin, ymax)");
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        return;
    }

    // 2. Populate from existing data
    let sql = format!(
        "INSERT INTO [{rtree}] \
         SELECT rowid, ST_XMin([{column}]), ST_XMax([{column}]), \
         ST_YMin([{column}]), ST_YMax([{column}]) \
         FROM [{table}] WHERE [{column}] IS NOT NULL"
    );
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        // Cleanup: drop the R-tree we just created
        let _ = exec_sql(db, ctx, &format!("DROP TABLE IF EXISTS [{rtree}]"));
        return;
    }

    // 3. AFTER INSERT trigger
    let trigger_insert = format!("{table}_{column}_insert");
    let sql = format!(
        "CREATE TRIGGER [{trigger_insert}] AFTER INSERT ON [{table}] \
         WHEN NEW.[{column}] IS NOT NULL \
         BEGIN \
           INSERT INTO [{rtree}] VALUES ( \
             NEW.rowid, \
             ST_XMin(NEW.[{column}]), ST_XMax(NEW.[{column}]), \
             ST_YMin(NEW.[{column}]), ST_YMax(NEW.[{column}]) \
           ); \
         END"
    );
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        let _ = exec_sql(db, ctx, &format!("DROP TABLE IF EXISTS [{rtree}]"));
        return;
    }

    // 4. AFTER UPDATE trigger
    let trigger_update = format!("{table}_{column}_update");
    let sql = format!(
        "CREATE TRIGGER [{trigger_update}] AFTER UPDATE OF [{column}] ON [{table}] \
         BEGIN \
           DELETE FROM [{rtree}] WHERE id = OLD.rowid; \
           INSERT INTO [{rtree}] \
             SELECT NEW.rowid, \
               ST_XMin(NEW.[{column}]), ST_XMax(NEW.[{column}]), \
               ST_YMin(NEW.[{column}]), ST_YMax(NEW.[{column}]) \
             WHERE NEW.[{column}] IS NOT NULL; \
         END"
    );
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        let _ = exec_sql(
            db,
            ctx,
            &format!("DROP TRIGGER IF EXISTS [{trigger_insert}]"),
        );
        let _ = exec_sql(db, ctx, &format!("DROP TABLE IF EXISTS [{rtree}]"));
        return;
    }

    // 5. AFTER DELETE trigger
    let trigger_delete = format!("{table}_{column}_delete");
    let sql = format!(
        "CREATE TRIGGER [{trigger_delete}] AFTER DELETE ON [{table}] \
         BEGIN \
           DELETE FROM [{rtree}] WHERE id = OLD.rowid; \
         END"
    );
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        let _ = exec_sql(
            db,
            ctx,
            &format!("DROP TRIGGER IF EXISTS [{trigger_update}]"),
        );
        let _ = exec_sql(
            db,
            ctx,
            &format!("DROP TRIGGER IF EXISTS [{trigger_insert}]"),
        );
        let _ = exec_sql(db, ctx, &format!("DROP TABLE IF EXISTS [{rtree}]"));
        return;
    }

    set_i32(ctx, 1);
}

unsafe extern "C" fn drop_spatial_index_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    let Some((table, column)) = get_table_column(ctx, argv, "DropSpatialIndex") else {
        return;
    };

    let db = sqlite3_context_db_handle(ctx);

    // Drop triggers first, then the R-tree table
    let prefix = format!("{table}_{column}");
    for suffix in &["_insert", "_update", "_delete"] {
        let sql = format!("DROP TRIGGER IF EXISTS [{prefix}{suffix}]");
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            return;
        }
    }
    let sql = format!("DROP TABLE IF EXISTS [{prefix}_rtree]");
    if exec_sql(db, ctx, &sql) != SQLITE_OK {
        return;
    }

    set_i32(ctx, 1);
}

// ── Registration ─────────────────────────────────────────────────────────────

unsafe fn reg(
    db: *mut sqlite3,
    name: &str,
    n_arg: c_int,
    xfunc: unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value),
) -> c_int {
    let c_name = CString::new(name).unwrap();
    sqlite3_create_function_v2(
        db,
        c_name.as_ptr(),
        n_arg,
        DET,
        std::ptr::null_mut(),
        Some(xfunc),
        None,
        None,
        None,
    )
}

unsafe fn reg_direct(
    db: *mut sqlite3,
    name: &str,
    n_arg: c_int,
    xfunc: unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value),
) -> c_int {
    let c_name = CString::new(name).unwrap();
    sqlite3_create_function_v2(
        db,
        c_name.as_ptr(),
        n_arg,
        DIRECT,
        std::ptr::null_mut(),
        Some(xfunc),
        None,
        None,
        None,
    )
}

/// Register all geolite spatial functions into an open SQLite database.
///
/// Returns `SQLITE_OK` (0) on success, or the first error code on failure.
///
/// # Safety
/// `db` must be a valid, open SQLite database handle for the lifetime of the call.
pub unsafe fn register_functions(db: *mut sqlite3) -> c_int {
    macro_rules! r {
        ($name:expr, $n:expr, $f:expr) => {
            let rc = reg(db, $name, $n, $f);
            if rc != SQLITE_OK {
                return rc;
            }
        };
    }

    macro_rules! rd {
        ($name:expr, $n:expr, $f:expr) => {
            let rc = reg_direct(db, $name, $n, $f);
            if rc != SQLITE_OK {
                return rc;
            }
        };
    }

    // ── I/O ──────────────────────────────────────────────────────────────
    r!("ST_GeomFromText", 1, st_geomfromtext_1_xfunc);
    r!("ST_GeomFromText", 2, st_geomfromtext_2_xfunc);
    r!("ST_GeomFromWKB", 1, st_geomfromwkb_1_xfunc);
    r!("ST_GeomFromWKB", 2, st_geomfromwkb_2_xfunc);
    r!("ST_GeomFromEWKB", 1, st_geomfromewkb_xfunc);
    r!("ST_GeomFromGeoJSON", 1, st_geomfromgeojson_xfunc);
    r!("ST_AsText", 1, st_astext_xfunc);
    r!("ST_AsEWKT", 1, st_asewkt_xfunc);
    r!("ST_AsBinary", 1, st_asbinary_xfunc);
    r!("ST_AsEWKB", 1, st_asewkb_xfunc);
    r!("ST_AsGeoJSON", 1, st_asgeojson_xfunc);

    // ── Constructors ─────────────────────────────────────────────────────
    r!("ST_Point", 2, st_point_2_xfunc);
    r!("ST_Point", 3, st_point_3_xfunc);
    r!("ST_MakePoint", 2, st_point_2_xfunc);
    r!("ST_MakeLine", 2, st_makeline_xfunc);
    r!("ST_MakePolygon", 1, st_makepolygon_xfunc);
    r!("ST_MakeEnvelope", 4, st_makeenvelope_4_xfunc);
    r!("ST_MakeEnvelope", 5, st_makeenvelope_5_xfunc);
    r!("ST_Collect", 2, st_collect_xfunc);
    r!("ST_TileEnvelope", 3, st_tileenvelope_xfunc);

    // ── Accessors ────────────────────────────────────────────────────────
    r!("ST_SRID", 1, st_srid_xfunc);
    r!("ST_SetSRID", 2, st_setsrid_xfunc);
    r!("ST_GeometryType", 1, st_geometrytype_xfunc);
    r!("GeometryType", 1, st_geometrytype_xfunc);
    r!("ST_NDims", 1, st_ndims_xfunc);
    r!("ST_CoordDim", 1, st_coorddim_xfunc);
    r!("ST_Zmflag", 1, st_zmflag_xfunc);
    r!("ST_IsEmpty", 1, st_isempty_xfunc);
    r!("ST_MemSize", 1, st_memsize_xfunc);
    r!("ST_X", 1, st_x_xfunc);
    r!("ST_Y", 1, st_y_xfunc);
    r!("ST_NumPoints", 1, st_numpoints_xfunc);
    r!("ST_NPoints", 1, st_npoints_xfunc);
    r!("ST_NumGeometries", 1, st_numgeometries_xfunc);
    r!("ST_NumInteriorRings", 1, st_numinteriorrings_xfunc);
    r!("ST_NumInteriorRing", 1, st_numinteriorrings_xfunc);
    r!("ST_NumRings", 1, st_numrings_xfunc);
    r!("ST_PointN", 2, st_pointn_xfunc);
    r!("ST_StartPoint", 1, st_startpoint_xfunc);
    r!("ST_EndPoint", 1, st_endpoint_xfunc);
    r!("ST_ExteriorRing", 1, st_exteriorring_xfunc);
    r!("ST_InteriorRingN", 2, st_interiorringn_xfunc);
    r!("ST_GeometryN", 2, st_geometryn_xfunc);
    r!("ST_Dimension", 1, st_dimension_xfunc);
    r!("ST_Envelope", 1, st_envelope_xfunc);
    r!("ST_IsValid", 1, st_isvalid_xfunc);
    r!("ST_IsValidReason", 1, st_isvalidreason_xfunc);

    // ── Measurement ──────────────────────────────────────────────────────
    r!("ST_Area", 1, st_area_xfunc);
    r!("ST_Length", 1, st_length_xfunc);
    r!("ST_Length2D", 1, st_length_xfunc);
    r!("ST_Perimeter", 1, st_perimeter_xfunc);
    r!("ST_Perimeter2D", 1, st_perimeter_xfunc);
    r!("ST_Distance", 2, st_distance_xfunc);
    r!("ST_Centroid", 1, st_centroid_xfunc);
    r!("ST_PointOnSurface", 1, st_pointonsurface_xfunc);
    r!("ST_HausdorffDistance", 2, st_hausdorffdistance_xfunc);
    r!("ST_XMin", 1, st_xmin_xfunc);
    r!("ST_XMax", 1, st_xmax_xfunc);
    r!("ST_YMin", 1, st_ymin_xfunc);
    r!("ST_YMax", 1, st_ymax_xfunc);
    r!("ST_DistanceSphere", 2, st_distancesphere_xfunc);
    r!("ST_DistanceSpheroid", 2, st_distancespheroid_xfunc);
    r!("ST_LengthSphere", 1, st_lengthsphere_xfunc);
    r!("ST_Azimuth", 2, st_azimuth_xfunc);
    r!("ST_Project", 3, st_project_xfunc);
    r!("ST_ClosestPoint", 2, st_closestpoint_xfunc);

    // ── Predicates ───────────────────────────────────────────────────────
    r!("ST_Intersects", 2, st_intersects_xfunc);
    r!("ST_Contains", 2, st_contains_xfunc);
    r!("ST_Within", 2, st_within_xfunc);
    r!("ST_Disjoint", 2, st_disjoint_xfunc);
    r!("ST_DWithin", 3, st_dwithin_xfunc);
    r!("ST_Covers", 2, st_covers_xfunc);
    r!("ST_CoveredBy", 2, st_coveredby_xfunc);
    r!("ST_Equals", 2, st_equals_xfunc);
    r!("ST_Touches", 2, st_touches_xfunc);
    r!("ST_Crosses", 2, st_crosses_xfunc);
    r!("ST_Overlaps", 2, st_overlaps_xfunc);
    r!("ST_Relate", 2, st_relate_2_xfunc);
    r!("ST_Relate", 3, st_relate_3_xfunc);
    r!("ST_RelateMatch", 2, st_relatematch_xfunc);

    // ── Spatial Index ─────────────────────────────────────────────────────
    rd!("CreateSpatialIndex", 2, create_spatial_index_xfunc);
    rd!("DropSpatialIndex", 2, drop_spatial_index_xfunc);

    SQLITE_OK
}

// ── C entry point for loadable extension (native only) ───────────────────────

/// `sqlite3_geolite_init` is the entry point called by SQLite when loading
/// this library as a loadable extension (`.load_extension('geolite')`).
#[cfg(not(target_arch = "wasm32"))]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_geolite_init(
    db: *mut sqlite3,
    _pz_err_msg: *mut *mut std::ffi::c_char,
    _p_api: *mut sqlite3_api_routines,
) -> c_int {
    register_functions(db)
}
