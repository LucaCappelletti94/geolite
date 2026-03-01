//! SQLite extension registration via raw FFI.
//!
//! Registers all geolite functions on a raw `*mut sqlite3` handle.
//! On native targets also exports the `sqlite3_geolite_init` C entry point
//! so SQLite can load this library as a loadable extension.

use crate::sqlite_compat::sqlite_transient;
use crate::sqlite_compat::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_int;

use geolite_core::function_catalog::{
    SQLITE_DETERMINISTIC_FUNCTIONS, SQLITE_DIRECT_ONLY_FUNCTIONS,
};
use geolite_core::functions::accessors::*;
use geolite_core::functions::constructors::*;
use geolite_core::functions::io::*;
use geolite_core::functions::measurement::*;
use geolite_core::functions::operations::*;
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
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    Some(std::slice::from_raw_parts(ptr, len))
}

enum SqlTextArg<'a> {
    Null,
    Value(&'a str),
    InvalidUtf8,
}

unsafe fn get_text<'a>(argv: *mut *mut sqlite3_value, i: usize) -> SqlTextArg<'a> {
    let v = *argv.add(i);
    if sqlite3_value_type(v) == SQLITE_NULL {
        return SqlTextArg::Null;
    }
    let ptr = sqlite3_value_text(v);
    let len = sqlite3_value_bytes(v) as usize;
    if ptr.is_null() {
        return SqlTextArg::InvalidUtf8;
    }
    match std::str::from_utf8(std::slice::from_raw_parts(ptr as _, len)) {
        Ok(s) => SqlTextArg::Value(s),
        Err(_) => SqlTextArg::InvalidUtf8,
    }
}

enum SqlArg<T> {
    Null,
    Value(T),
    InvalidType,
}

unsafe fn get_f64_arg(argv: *mut *mut sqlite3_value, i: usize) -> SqlArg<f64> {
    let v = *argv.add(i);
    match sqlite3_value_type(v) {
        SQLITE_NULL => SqlArg::Null,
        SQLITE_INTEGER | SQLITE_FLOAT => SqlArg::Value(sqlite3_value_double(v)),
        _ => SqlArg::InvalidType,
    }
}

unsafe fn get_i32_arg(argv: *mut *mut sqlite3_value, i: usize) -> SqlArg<i32> {
    let v = *argv.add(i);
    match sqlite3_value_type(v) {
        SQLITE_NULL => SqlArg::Null,
        SQLITE_INTEGER => SqlArg::Value(sqlite3_value_int(v)),
        _ => SqlArg::InvalidType,
    }
}

// ── Result-setting helpers ───────────────────────────────────────────────────

fn checked_c_int_len(len: usize) -> Option<c_int> {
    c_int::try_from(len).ok()
}

const ERROR_MSG_TOO_LARGE: &str = "internal error: error message too large";
const PANIC_IN_CALLBACK_MSG: &str = "panic in SQLite callback";

unsafe fn set_blob(ctx: *mut sqlite3_context, data: &[u8]) {
    let Some(len) = checked_c_int_len(data.len()) else {
        set_error(ctx, "internal error: BLOB result too large");
        return;
    };
    sqlite3_result_blob(ctx, data.as_ptr().cast(), len, sqlite_transient());
}

unsafe fn set_text(ctx: *mut sqlite3_context, s: &str) {
    let Some(len) = checked_c_int_len(s.len()) else {
        set_error(ctx, "internal error: text result too large");
        return;
    };
    sqlite3_result_text(ctx, s.as_ptr().cast(), len, sqlite_transient());
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
    if let Some(len) = checked_c_int_len(msg.len()) {
        sqlite3_result_error(ctx, msg.as_ptr().cast(), len);
        return;
    }

    let len = c_int::try_from(ERROR_MSG_TOO_LARGE.len())
        .expect("fallback error length must fit in c_int");
    sqlite3_result_error(ctx, ERROR_MSG_TOO_LARGE.as_ptr().cast(), len);
}

unsafe fn xfunc_guard<F>(ctx: *mut sqlite3_context, label: &str, f: F)
where
    F: FnOnce(),
{
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    if result.is_err() {
        set_error(ctx, &format!("{label}: {PANIC_IN_CALLBACK_MSG}"));
    }
}

unsafe fn require_f64_arg(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    i: usize,
    fn_name: &str,
    arg_name: &str,
) -> Option<f64> {
    match get_f64_arg(argv, i) {
        SqlArg::Value(v) => Some(v),
        SqlArg::Null => {
            set_null(ctx);
            None
        }
        SqlArg::InvalidType => {
            set_error(ctx, &format!("{fn_name}: {arg_name} must be numeric"));
            None
        }
    }
}

unsafe fn require_i32_arg(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    i: usize,
    fn_name: &str,
    arg_name: &str,
) -> Option<i32> {
    match get_i32_arg(argv, i) {
        SqlArg::Value(v) => Some(v),
        SqlArg::Null => {
            set_null(ctx);
            None
        }
        SqlArg::InvalidType => {
            set_error(ctx, &format!("{fn_name}: {arg_name} must be integer"));
            None
        }
    }
}

unsafe fn require_text_arg<'a>(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    i: usize,
    fn_name: &str,
    arg_name: &str,
) -> Option<&'a str> {
    match get_text(argv, i) {
        SqlTextArg::Value(v) => Some(v),
        SqlTextArg::Null => {
            set_null(ctx);
            None
        }
        SqlTextArg::InvalidUtf8 => {
            set_error(
                ctx,
                &format!("{fn_name}: {arg_name} must be valid UTF-8 text"),
            );
            None
        }
    }
}

unsafe fn any_arg_is_null(argv: *mut *mut sqlite3_value, arg_count: usize) -> bool {
    for i in 0..arg_count {
        if sqlite3_value_type(*argv.add(i)) == SQLITE_NULL {
            return true;
        }
    }
    false
}

unsafe fn optional_srid_arg(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    with_srid: bool,
    index: usize,
    fn_name: &str,
) -> Option<Option<i32>> {
    if with_srid {
        let srid = require_i32_arg(ctx, argv, index, fn_name, "srid")?;
        Some(Some(srid))
    } else {
        Some(None)
    }
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
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                match $func(b) {
                    Ok(v) => $set(ctx, v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
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
            xfunc_guard(ctx, $label, || {
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
            });
        }
    };
}

/// 1 blob → Result<Option<f64>>, where `None` maps to SQL NULL.
macro_rules! xfunc_blob_opt_f64 {
    ($name:ident, $label:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(blob) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                match $func(blob) {
                    Ok(Some(v)) => set_f64(ctx, v),
                    Ok(None) => set_null(ctx),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// blob + integer arg → Result<Vec<u8>>.
macro_rules! xfunc_blob_i32_blob {
    ($name:ident, $label:expr, $arg_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(n) = require_i32_arg(ctx, argv, 1, $label, $arg_name) else {
                    return;
                };
                match ($func)(b, n) {
                    Ok(v) => set_blob(ctx, &v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// blob + numeric arg → Result<Vec<u8>>.
macro_rules! xfunc_blob_f64_blob {
    ($name:ident, $label:expr, $arg_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(v) = require_f64_arg(ctx, argv, 1, $label, $arg_name) else {
                    return;
                };
                match ($func)(b, v) {
                    Ok(out) => set_blob(ctx, &out),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// blob + numeric arg + numeric arg → Result<Vec<u8>>.
macro_rules! xfunc_blob_f64_f64_blob {
    ($name:ident, $label:expr, $arg1_name:expr, $arg2_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(v1) = require_f64_arg(ctx, argv, 1, $label, $arg1_name) else {
                    return;
                };
                let Some(v2) = require_f64_arg(ctx, argv, 2, $label, $arg2_name) else {
                    return;
                };
                match ($func)(b, v1, v2) {
                    Ok(out) => set_blob(ctx, &out),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// 2 blobs + numeric arg → Result<bool>.
macro_rules! xfunc_blob2_f64_bool {
    ($name:ident, $label:expr, $arg_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(a) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(b) = get_blob(argv, 1) else {
                    set_null(ctx);
                    return;
                };
                let Some(v) = require_f64_arg(ctx, argv, 2, $label, $arg_name) else {
                    return;
                };
                match ($func)(a, b, v) {
                    Ok(out) => set_bool(ctx, out),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// 2 blobs + text arg → Result<bool>.
macro_rules! xfunc_blob2_text_bool {
    ($name:ident, $label:expr, $arg_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(a) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(b) = get_blob(argv, 1) else {
                    set_null(ctx);
                    return;
                };
                let Some(v) = require_text_arg(ctx, argv, 2, $label, $arg_name) else {
                    return;
                };
                match ($func)(a, b, v) {
                    Ok(out) => set_bool(ctx, out),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
    };
}

/// 2 text args → Result<bool>.
macro_rules! xfunc_text2_bool {
    ($name:ident, $label:expr, $arg1_name:expr, $arg2_name:expr, $func:expr) => {
        unsafe extern "C" fn $name(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(a) = require_text_arg(ctx, argv, 0, $label, $arg1_name) else {
                    return;
                };
                let Some(b) = require_text_arg(ctx, argv, 1, $label, $arg2_name) else {
                    return;
                };
                match ($func)(a, b) {
                    Ok(out) => set_bool(ctx, out),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
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
            xfunc_guard(ctx, $label, || {
                let Some(t) = require_text_arg(ctx, argv, 0, $label, "wkt") else {
                    return;
                };
                match $func(t, None) {
                    Ok(v) => set_blob(ctx, &v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
        unsafe extern "C" fn $name2(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(t) = require_text_arg(ctx, argv, 0, $label, "wkt") else {
                    return;
                };
                let Some(srid) = require_i32_arg(ctx, argv, 1, $label, "srid") else {
                    return;
                };
                match $func(t, Some(srid)) {
                    Ok(v) => set_blob(ctx, &v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
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
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                match $func(b, None) {
                    Ok(v) => set_blob(ctx, &v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
        }
        unsafe extern "C" fn $name2(
            ctx: *mut sqlite3_context,
            _n: c_int,
            argv: *mut *mut sqlite3_value,
        ) {
            xfunc_guard(ctx, $label, || {
                let Some(b) = get_blob(argv, 0) else {
                    set_null(ctx);
                    return;
                };
                let Some(srid) = require_i32_arg(ctx, argv, 1, $label, "srid") else {
                    return;
                };
                match $func(b, Some(srid)) {
                    Ok(v) => set_blob(ctx, &v),
                    Err(e) => set_error(ctx, &format!(concat!($label, ": {}"), e)),
                }
            });
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
    xfunc_guard(ctx, "ST_GeomFromGeoJSON", || {
        let Some(json) = require_text_arg(ctx, argv, 0, "ST_GeomFromGeoJSON", "json") else {
            return;
        };
        match geom_from_geojson(json, None) {
            Ok(v) => set_blob(ctx, &v),
            Err(e) => set_error(ctx, &format!("ST_GeomFromGeoJSON: {e}")),
        }
    });
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

unsafe fn st_point_impl(ctx: *mut sqlite3_context, argv: *mut *mut sqlite3_value, with_srid: bool) {
    let arg_count = if with_srid { 3 } else { 2 };
    if any_arg_is_null(argv, arg_count) {
        set_null(ctx);
        return;
    }

    let Some(x) = require_f64_arg(ctx, argv, 0, "ST_Point", "x") else {
        return;
    };
    let Some(y) = require_f64_arg(ctx, argv, 1, "ST_Point", "y") else {
        return;
    };
    let Some(srid) = optional_srid_arg(ctx, argv, with_srid, 2, "ST_Point") else {
        return;
    };

    match st_point(x, y, srid) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_Point: {e}")),
    }
}

unsafe extern "C" fn st_point_2_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_Point", || {
        st_point_impl(ctx, argv, false);
    });
}

unsafe extern "C" fn st_point_3_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_Point", || {
        st_point_impl(ctx, argv, true);
    });
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

unsafe fn st_makeenvelope_impl(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    with_srid: bool,
) {
    let arg_count = if with_srid { 5 } else { 4 };
    if any_arg_is_null(argv, arg_count) {
        set_null(ctx);
        return;
    }

    let Some(xmin) = require_f64_arg(ctx, argv, 0, "ST_MakeEnvelope", "xmin") else {
        return;
    };
    let Some(ymin) = require_f64_arg(ctx, argv, 1, "ST_MakeEnvelope", "ymin") else {
        return;
    };
    let Some(xmax) = require_f64_arg(ctx, argv, 2, "ST_MakeEnvelope", "xmax") else {
        return;
    };
    let Some(ymax) = require_f64_arg(ctx, argv, 3, "ST_MakeEnvelope", "ymax") else {
        return;
    };
    let Some(srid) = optional_srid_arg(ctx, argv, with_srid, 4, "ST_MakeEnvelope") else {
        return;
    };

    match st_make_envelope(xmin, ymin, xmax, ymax, srid) {
        Ok(v) => set_blob(ctx, &v),
        Err(e) => set_error(ctx, &format!("ST_MakeEnvelope: {e}")),
    }
}

unsafe extern "C" fn st_makeenvelope_4_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_MakeEnvelope", || {
        st_makeenvelope_impl(ctx, argv, false);
    });
}

unsafe extern "C" fn st_makeenvelope_5_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_MakeEnvelope", || {
        st_makeenvelope_impl(ctx, argv, true);
    });
}

xfunc_blob2!(st_collect_xfunc, "ST_Collect", st_collect, set_blob_owned);

unsafe extern "C" fn st_tileenvelope_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_TileEnvelope", || {
        let Some(zoom_i32) = require_i32_arg(ctx, argv, 0, "ST_TileEnvelope", "zoom") else {
            return;
        };
        let Some(tile_x_i32) = require_i32_arg(ctx, argv, 1, "ST_TileEnvelope", "tile x") else {
            return;
        };
        let Some(tile_y_i32) = require_i32_arg(ctx, argv, 2, "ST_TileEnvelope", "tile y") else {
            return;
        };

        if zoom_i32 < 0 {
            set_error(ctx, "ST_TileEnvelope: zoom must be non-negative");
            return;
        }
        if tile_x_i32 < 0 {
            set_error(ctx, "ST_TileEnvelope: tile x must be non-negative");
            return;
        }
        if tile_y_i32 < 0 {
            set_error(ctx, "ST_TileEnvelope: tile y must be non-negative");
            return;
        }

        let zoom = zoom_i32 as u32;
        let tile_x = tile_x_i32 as u32;
        let tile_y = tile_y_i32 as u32;
        match st_tile_envelope(zoom, tile_x, tile_y) {
            Ok(v) => set_blob(ctx, &v),
            Err(e) => set_error(ctx, &format!("ST_TileEnvelope: {e}")),
        }
    });
}

// ── Accessor callbacks ───────────────────────────────────────────────────────

xfunc_blob!(st_srid_xfunc, "ST_SRID", st_srid, set_i32);

unsafe extern "C" fn st_setsrid_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "ST_SetSRID", || {
        let Some(b) = get_blob(argv, 0) else {
            set_null(ctx);
            return;
        };
        let Some(srid) = require_i32_arg(ctx, argv, 1, "ST_SetSRID", "srid") else {
            return;
        };
        match st_set_srid(b, srid) {
            Ok(v) => set_blob(ctx, &v),
            Err(e) => set_error(ctx, &format!("ST_SetSRID: {e}")),
        }
    });
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
xfunc_blob_opt_f64!(st_x_xfunc, "ST_X", st_x);
xfunc_blob_opt_f64!(st_y_xfunc, "ST_Y", st_y);

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
xfunc_blob_i32_blob!(st_pointn_xfunc, "ST_PointN", "n", |b, n| st_point_n(
    b, n, None
));

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
xfunc_blob_i32_blob!(
    st_interiorringn_xfunc,
    "ST_InteriorRingN",
    "n",
    st_interior_ring_n
);
xfunc_blob_i32_blob!(st_geometryn_xfunc, "ST_GeometryN", "n", st_geometry_n);

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

xfunc_blob_f64_f64_blob!(
    st_project_xfunc,
    "ST_Project",
    "distance",
    "azimuth",
    st_project
);

xfunc_blob2!(
    st_closestpoint_xfunc,
    "ST_ClosestPoint",
    st_closest_point,
    set_blob_owned
);

// ── Operation callbacks ──────────────────────────────────────────────────────

xfunc_blob2!(st_union_xfunc, "ST_Union", st_union, set_blob_owned);
xfunc_blob2!(
    st_intersection_xfunc,
    "ST_Intersection",
    st_intersection,
    set_blob_owned
);
xfunc_blob2!(
    st_difference_xfunc,
    "ST_Difference",
    st_difference,
    set_blob_owned
);
xfunc_blob2!(
    st_symdifference_xfunc,
    "ST_SymDifference",
    st_sym_difference,
    set_blob_owned
);

xfunc_blob_f64_blob!(st_buffer_xfunc, "ST_Buffer", "distance", st_buffer);

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

xfunc_blob2_f64_bool!(st_dwithin_xfunc, "ST_DWithin", "distance", st_dwithin);

xfunc_blob2!(st_covers_xfunc, "ST_Covers", st_covers, set_bool);
xfunc_blob2!(st_coveredby_xfunc, "ST_CoveredBy", st_covered_by, set_bool);
xfunc_blob2!(st_equals_xfunc, "ST_Equals", st_equals, set_bool);
xfunc_blob2!(st_touches_xfunc, "ST_Touches", st_touches, set_bool);
xfunc_blob2!(st_crosses_xfunc, "ST_Crosses", st_crosses, set_bool);
xfunc_blob2!(st_overlaps_xfunc, "ST_Overlaps", st_overlaps, set_bool);

xfunc_blob2!(st_relate_2_xfunc, "ST_Relate", st_relate, set_text_owned);

xfunc_blob2_text_bool!(
    st_relate_3_xfunc,
    "ST_Relate",
    "pattern",
    st_relate_match_geoms
);
xfunc_text2_bool!(
    st_relatematch_xfunc,
    "ST_RelateMatch",
    "matrix",
    "pattern",
    st_relate_match
);

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

fn sql_to_cstring(sql: &str) -> std::result::Result<CString, std::ffi::NulError> {
    CString::new(sql)
}

unsafe fn exec_sql_inner(db: *mut sqlite3, sql: &str, ctx: Option<*mut sqlite3_context>) -> c_int {
    let c_sql = match sql_to_cstring(sql) {
        Ok(v) => v,
        Err(_) => {
            if let Some(ctx) = ctx {
                set_error(ctx, "internal error: generated SQL contains NUL byte");
            }
            return SQLITE_ERROR;
        }
    };

    let mut err_msg: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = sqlite3_exec(db, c_sql.as_ptr(), None, std::ptr::null_mut(), &mut err_msg);

    if rc != SQLITE_OK {
        if let Some(ctx) = ctx {
            if err_msg.is_null() {
                set_error(ctx, "exec_sql failed");
            } else {
                let msg = CStr::from_ptr(err_msg).to_string_lossy();
                set_error(ctx, &msg);
            }
        }
    }

    if !err_msg.is_null() {
        sqlite3_free(err_msg.cast());
    }
    rc
}

/// Run SQL via `sqlite3_exec`, returning `SQLITE_OK` on success.
/// On failure, sets `sqlite3_result_error` on `ctx` with the error message
/// from SQLite and frees it via `sqlite3_free`.
unsafe fn exec_sql(db: *mut sqlite3, ctx: *mut sqlite3_context, sql: &str) -> c_int {
    exec_sql_inner(db, sql, Some(ctx))
}

/// Run SQL via `sqlite3_exec` but never touch sqlite3_result_error.
/// Used for best-effort rollback paths where the original error should win.
unsafe fn exec_sql_silent(db: *mut sqlite3, sql: &str) -> c_int {
    exec_sql_inner(db, sql, None)
}

unsafe fn rollback_savepoint(db: *mut sqlite3, ctx: *mut sqlite3_context, savepoint: &str) {
    let _ = ctx;
    let _ = exec_sql_silent(db, &format!("ROLLBACK TO {savepoint}"));
    let _ = exec_sql_silent(db, &format!("RELEASE {savepoint}"));
}

// ── Spatial index callbacks ──────────────────────────────────────────────────

/// Extract and validate `(table, column)` identifiers from the first two args.
/// On failure, sets an error on `ctx` and returns `None`.
unsafe fn get_table_column<'a>(
    ctx: *mut sqlite3_context,
    argv: *mut *mut sqlite3_value,
    label: &str,
) -> Option<(&'a str, &'a str)> {
    let table = match get_text(argv, 0) {
        SqlTextArg::Value(v) => v,
        SqlTextArg::Null => {
            set_error(ctx, &format!("{label}: table name must not be NULL"));
            return None;
        }
        SqlTextArg::InvalidUtf8 => {
            set_error(
                ctx,
                &format!("{label}: table name must be valid UTF-8 text"),
            );
            return None;
        }
    };
    let column = match get_text(argv, 1) {
        SqlTextArg::Value(v) => v,
        SqlTextArg::Null => {
            set_error(ctx, &format!("{label}: column name must not be NULL"));
            return None;
        }
        SqlTextArg::InvalidUtf8 => {
            set_error(
                ctx,
                &format!("{label}: column name must be valid UTF-8 text"),
            );
            return None;
        }
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
    xfunc_guard(ctx, "CreateSpatialIndex", || {
        let Some((table, column)) = get_table_column(ctx, argv, "CreateSpatialIndex") else {
            return;
        };

        let db = sqlite3_context_db_handle(ctx);
        let rtree = format!("{table}_{column}_rtree");
        let savepoint = "geolite_create_spatial_index";

        if exec_sql(db, ctx, &format!("SAVEPOINT {savepoint}")) != SQLITE_OK {
            return;
        }

        // 1. Create the R-tree virtual table if missing.
        let sql = format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS [{rtree}] USING rtree(id, xmin, xmax, ymin, ymax)"
        );
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        // 2. Rebuild index contents from the base table (idempotent on repeated calls).
        let sql = format!("DELETE FROM [{rtree}]");
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        let sql = format!(
            "INSERT INTO [{rtree}] \
             SELECT rowid, ST_XMin([{column}]), ST_XMax([{column}]), \
             ST_YMin([{column}]), ST_YMax([{column}]) \
             FROM [{table}] WHERE [{column}] IS NOT NULL AND ST_IsEmpty([{column}]) = 0"
        );
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        // 3. AFTER INSERT trigger
        let trigger_insert = format!("{table}_{column}_insert");
        let sql = format!(
            "CREATE TRIGGER IF NOT EXISTS [{trigger_insert}] AFTER INSERT ON [{table}] \
             WHEN NEW.[{column}] IS NOT NULL AND ST_IsEmpty(NEW.[{column}]) = 0 \
             BEGIN \
               INSERT INTO [{rtree}] VALUES ( \
                 NEW.rowid, \
                 ST_XMin(NEW.[{column}]), ST_XMax(NEW.[{column}]), \
                 ST_YMin(NEW.[{column}]), ST_YMax(NEW.[{column}]) \
               ); \
             END"
        );
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        // 4. AFTER UPDATE trigger
        let trigger_update = format!("{table}_{column}_update");
        let sql = format!(
            "CREATE TRIGGER IF NOT EXISTS [{trigger_update}] AFTER UPDATE OF [{column}] ON [{table}] \
             BEGIN \
               DELETE FROM [{rtree}] WHERE id = OLD.rowid; \
               INSERT INTO [{rtree}] \
                 SELECT NEW.rowid, \
                   ST_XMin(NEW.[{column}]), ST_XMax(NEW.[{column}]), \
                   ST_YMin(NEW.[{column}]), ST_YMax(NEW.[{column}]) \
                 WHERE NEW.[{column}] IS NOT NULL AND ST_IsEmpty(NEW.[{column}]) = 0; \
             END"
        );
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        // 5. AFTER DELETE trigger
        let trigger_delete = format!("{table}_{column}_delete");
        let sql = format!(
            "CREATE TRIGGER IF NOT EXISTS [{trigger_delete}] AFTER DELETE ON [{table}] \
             BEGIN \
               DELETE FROM [{rtree}] WHERE id = OLD.rowid; \
             END"
        );
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        if exec_sql(db, ctx, &format!("RELEASE {savepoint}")) != SQLITE_OK {
            return;
        }

        set_i32(ctx, 1);
    });
}

unsafe extern "C" fn drop_spatial_index_xfunc(
    ctx: *mut sqlite3_context,
    _n: c_int,
    argv: *mut *mut sqlite3_value,
) {
    xfunc_guard(ctx, "DropSpatialIndex", || {
        let Some((table, column)) = get_table_column(ctx, argv, "DropSpatialIndex") else {
            return;
        };

        let db = sqlite3_context_db_handle(ctx);
        let savepoint = "geolite_drop_spatial_index";

        if exec_sql(db, ctx, &format!("SAVEPOINT {savepoint}")) != SQLITE_OK {
            return;
        }

        // Drop triggers first, then the R-tree table
        let prefix = format!("{table}_{column}");
        for suffix in &["_insert", "_update", "_delete"] {
            let sql = format!("DROP TRIGGER IF EXISTS [{prefix}{suffix}]");
            if exec_sql(db, ctx, &sql) != SQLITE_OK {
                rollback_savepoint(db, ctx, savepoint);
                return;
            }
        }
        let sql = format!("DROP TABLE IF EXISTS [{prefix}_rtree]");
        if exec_sql(db, ctx, &sql) != SQLITE_OK {
            rollback_savepoint(db, ctx, savepoint);
            return;
        }

        if exec_sql(db, ctx, &format!("RELEASE {savepoint}")) != SQLITE_OK {
            return;
        }

        set_i32(ctx, 1);
    });
}

// ── Registration ─────────────────────────────────────────────────────────────

unsafe fn reg(
    db: *mut sqlite3,
    name: &str,
    n_arg: c_int,
    flags: c_int,
    xfunc: unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value),
) -> c_int {
    let c_name = match CString::new(name) {
        Ok(v) => v,
        Err(_) => return SQLITE_ERROR,
    };
    sqlite3_create_function_v2(
        db,
        c_name.as_ptr(),
        n_arg,
        flags,
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
    type XFunc = unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value);

    let deterministic_callbacks: &[(&str, c_int, XFunc)] = &[
        // I/O
        ("ST_GeomFromText", 1, st_geomfromtext_1_xfunc),
        ("ST_GeomFromText", 2, st_geomfromtext_2_xfunc),
        ("ST_GeomFromWKB", 1, st_geomfromwkb_1_xfunc),
        ("ST_GeomFromWKB", 2, st_geomfromwkb_2_xfunc),
        ("ST_GeomFromEWKB", 1, st_geomfromewkb_xfunc),
        ("ST_GeomFromGeoJSON", 1, st_geomfromgeojson_xfunc),
        ("ST_AsText", 1, st_astext_xfunc),
        ("ST_AsEWKT", 1, st_asewkt_xfunc),
        ("ST_AsBinary", 1, st_asbinary_xfunc),
        ("ST_AsEWKB", 1, st_asewkb_xfunc),
        ("ST_AsGeoJSON", 1, st_asgeojson_xfunc),
        // Constructors
        ("ST_Point", 2, st_point_2_xfunc),
        ("ST_Point", 3, st_point_3_xfunc),
        ("ST_MakePoint", 2, st_point_2_xfunc),
        ("ST_MakeLine", 2, st_makeline_xfunc),
        ("ST_MakePolygon", 1, st_makepolygon_xfunc),
        ("ST_MakeEnvelope", 4, st_makeenvelope_4_xfunc),
        ("ST_MakeEnvelope", 5, st_makeenvelope_5_xfunc),
        ("ST_Collect", 2, st_collect_xfunc),
        ("ST_TileEnvelope", 3, st_tileenvelope_xfunc),
        // Accessors
        ("ST_SRID", 1, st_srid_xfunc),
        ("ST_SetSRID", 2, st_setsrid_xfunc),
        ("ST_GeometryType", 1, st_geometrytype_xfunc),
        ("GeometryType", 1, st_geometrytype_xfunc),
        ("ST_NDims", 1, st_ndims_xfunc),
        ("ST_CoordDim", 1, st_coorddim_xfunc),
        ("ST_Zmflag", 1, st_zmflag_xfunc),
        ("ST_IsEmpty", 1, st_isempty_xfunc),
        ("ST_MemSize", 1, st_memsize_xfunc),
        ("ST_X", 1, st_x_xfunc),
        ("ST_Y", 1, st_y_xfunc),
        ("ST_NumPoints", 1, st_numpoints_xfunc),
        ("ST_NPoints", 1, st_npoints_xfunc),
        ("ST_NumGeometries", 1, st_numgeometries_xfunc),
        ("ST_NumInteriorRings", 1, st_numinteriorrings_xfunc),
        ("ST_NumInteriorRing", 1, st_numinteriorrings_xfunc),
        ("ST_NumRings", 1, st_numrings_xfunc),
        ("ST_PointN", 2, st_pointn_xfunc),
        ("ST_StartPoint", 1, st_startpoint_xfunc),
        ("ST_EndPoint", 1, st_endpoint_xfunc),
        ("ST_ExteriorRing", 1, st_exteriorring_xfunc),
        ("ST_InteriorRingN", 2, st_interiorringn_xfunc),
        ("ST_GeometryN", 2, st_geometryn_xfunc),
        ("ST_Dimension", 1, st_dimension_xfunc),
        ("ST_Envelope", 1, st_envelope_xfunc),
        ("ST_IsValid", 1, st_isvalid_xfunc),
        ("ST_IsValidReason", 1, st_isvalidreason_xfunc),
        // Measurement
        ("ST_Area", 1, st_area_xfunc),
        ("ST_Length", 1, st_length_xfunc),
        ("ST_Length2D", 1, st_length_xfunc),
        ("ST_Perimeter", 1, st_perimeter_xfunc),
        ("ST_Perimeter2D", 1, st_perimeter_xfunc),
        ("ST_Distance", 2, st_distance_xfunc),
        ("ST_Centroid", 1, st_centroid_xfunc),
        ("ST_PointOnSurface", 1, st_pointonsurface_xfunc),
        ("ST_HausdorffDistance", 2, st_hausdorffdistance_xfunc),
        ("ST_XMin", 1, st_xmin_xfunc),
        ("ST_XMax", 1, st_xmax_xfunc),
        ("ST_YMin", 1, st_ymin_xfunc),
        ("ST_YMax", 1, st_ymax_xfunc),
        ("ST_DistanceSphere", 2, st_distancesphere_xfunc),
        ("ST_DistanceSpheroid", 2, st_distancespheroid_xfunc),
        ("ST_LengthSphere", 1, st_lengthsphere_xfunc),
        ("ST_Azimuth", 2, st_azimuth_xfunc),
        ("ST_Project", 3, st_project_xfunc),
        ("ST_ClosestPoint", 2, st_closestpoint_xfunc),
        // Operations
        ("ST_Union", 2, st_union_xfunc),
        ("ST_Intersection", 2, st_intersection_xfunc),
        ("ST_Difference", 2, st_difference_xfunc),
        ("ST_SymDifference", 2, st_symdifference_xfunc),
        ("ST_Buffer", 2, st_buffer_xfunc),
        // Predicates
        ("ST_Intersects", 2, st_intersects_xfunc),
        ("ST_Contains", 2, st_contains_xfunc),
        ("ST_Within", 2, st_within_xfunc),
        ("ST_Disjoint", 2, st_disjoint_xfunc),
        ("ST_DWithin", 3, st_dwithin_xfunc),
        ("ST_Covers", 2, st_covers_xfunc),
        ("ST_CoveredBy", 2, st_coveredby_xfunc),
        ("ST_Equals", 2, st_equals_xfunc),
        ("ST_Touches", 2, st_touches_xfunc),
        ("ST_Crosses", 2, st_crosses_xfunc),
        ("ST_Overlaps", 2, st_overlaps_xfunc),
        ("ST_Relate", 2, st_relate_2_xfunc),
        ("ST_Relate", 3, st_relate_3_xfunc),
        ("ST_RelateMatch", 2, st_relatematch_xfunc),
    ];

    if deterministic_callbacks.len() != SQLITE_DETERMINISTIC_FUNCTIONS.len() {
        return SQLITE_ERROR;
    }

    for spec in SQLITE_DETERMINISTIC_FUNCTIONS {
        let Some((_, _, xfunc)) = deterministic_callbacks
            .iter()
            .find(|(name, n_arg, _)| *name == spec.name && *n_arg == spec.n_arg as c_int)
        else {
            return SQLITE_ERROR;
        };

        let rc = reg(db, spec.name, spec.n_arg as c_int, DET, *xfunc);
        if rc != SQLITE_OK {
            return rc;
        }
    }

    let direct_only_callbacks: &[(&str, c_int, XFunc)] = &[
        ("CreateSpatialIndex", 2, create_spatial_index_xfunc),
        ("DropSpatialIndex", 2, drop_spatial_index_xfunc),
    ];

    if direct_only_callbacks.len() != SQLITE_DIRECT_ONLY_FUNCTIONS.len() {
        return SQLITE_ERROR;
    }

    for spec in SQLITE_DIRECT_ONLY_FUNCTIONS {
        let Some((_, _, xfunc)) = direct_only_callbacks
            .iter()
            .find(|(name, n_arg, _)| *name == spec.name && *n_arg == spec.n_arg as c_int)
        else {
            return SQLITE_ERROR;
        };

        let rc = reg(db, spec.name, spec.n_arg as c_int, DIRECT, *xfunc);
        if rc != SQLITE_OK {
            return rc;
        }
    }

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
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| register_functions(db))) {
        Ok(rc) => rc,
        Err(_) => SQLITE_ERROR,
    }
}

/// Compatibility entry point name expected by SQLite's default loader rules for
/// libraries named like `libgeolite_sqlite`.
#[cfg(not(target_arch = "wasm32"))]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_geolitesqlite_init(
    db: *mut sqlite3,
    pz_err_msg: *mut *mut std::ffi::c_char,
    p_api: *mut sqlite3_api_routines,
) -> c_int {
    sqlite3_geolite_init(db, pz_err_msg, p_api)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::ptr;

    unsafe extern "C" fn guarded_constant_xfunc(
        ctx: *mut sqlite3_context,
        _n: c_int,
        _argv: *mut *mut sqlite3_value,
    ) {
        xfunc_guard(ctx, "GuardedConstant", || {
            set_i32(ctx, 7);
        });
    }

    unsafe extern "C" fn guarded_panic_xfunc(
        ctx: *mut sqlite3_context,
        _n: c_int,
        _argv: *mut *mut sqlite3_value,
    ) {
        xfunc_guard(ctx, "GuardedPanic", || {
            panic!("boom");
        });
    }

    unsafe fn open_db() -> *mut sqlite3 {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("valid sqlite path");
        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), SQLITE_OK);
        db
    }

    unsafe fn close_db(db: *mut sqlite3) {
        assert_eq!(sqlite3_close(db), SQLITE_OK);
    }

    unsafe fn query_i64(db: *mut sqlite3, sql: &str) -> Result<i64, String> {
        let sql_c = CString::new(sql).expect("valid SQL");
        let mut stmt = ptr::null_mut();
        let rc = sqlite3_prepare_v2(db, sql_c.as_ptr(), -1, &mut stmt, ptr::null_mut());
        if rc != SQLITE_OK {
            let err = CStr::from_ptr(sqlite3_errmsg(db))
                .to_string_lossy()
                .into_owned();
            return Err(err);
        }

        let step = sqlite3_step(stmt);
        if step != SQLITE_ROW {
            sqlite3_finalize(stmt);
            let err = CStr::from_ptr(sqlite3_errmsg(db))
                .to_string_lossy()
                .into_owned();
            return Err(err);
        }

        let value = sqlite3_column_int64(stmt, 0);
        sqlite3_finalize(stmt);
        Ok(value)
    }

    #[test]
    fn checked_c_int_len_accepts_small_and_boundary_values() {
        assert_eq!(checked_c_int_len(0), Some(0));
        assert_eq!(checked_c_int_len(1), Some(1));
        assert_eq!(checked_c_int_len(c_int::MAX as usize), Some(c_int::MAX));
    }

    #[test]
    fn checked_c_int_len_rejects_values_larger_than_c_int() {
        assert_eq!(checked_c_int_len((c_int::MAX as usize) + 1), None);
        assert_eq!(checked_c_int_len(usize::MAX), None);
    }

    #[test]
    fn sql_to_cstring_accepts_sql_without_nul() {
        let c_sql = sql_to_cstring("SELECT 1").expect("valid SQL should convert to CString");
        assert_eq!(c_sql.as_c_str().to_bytes(), b"SELECT 1");
    }

    #[test]
    fn sql_to_cstring_rejects_sql_with_nul() {
        assert!(sql_to_cstring("SELECT\0 1").is_err());
    }

    #[test]
    fn xfunc_guard_allows_normal_execution() {
        unsafe {
            let db = open_db();

            let func_name = CString::new("GuardedConstant").expect("valid function name");
            let rc = sqlite3_create_function_v2(
                db,
                func_name.as_ptr(),
                0,
                SQLITE_UTF8,
                ptr::null_mut(),
                Some(guarded_constant_xfunc),
                None,
                None,
                None,
            );
            assert_eq!(rc, SQLITE_OK, "function registration should succeed");

            let value = query_i64(db, "SELECT GuardedConstant()").expect("query should succeed");
            assert_eq!(value, 7);

            close_db(db);
        }
    }

    #[test]
    fn xfunc_guard_converts_panic_into_sqlite_error() {
        unsafe {
            let db = open_db();

            let func_name = CString::new("GuardedPanic").expect("valid function name");
            let rc = sqlite3_create_function_v2(
                db,
                func_name.as_ptr(),
                0,
                SQLITE_UTF8,
                ptr::null_mut(),
                Some(guarded_panic_xfunc),
                None,
                None,
                None,
            );
            assert_eq!(rc, SQLITE_OK, "function registration should succeed");

            let err = query_i64(db, "SELECT GuardedPanic()")
                .expect_err("panic should be surfaced as SQL error");
            assert!(
                err.contains("panic in SQLite callback"),
                "unexpected error message: {err}"
            );

            close_db(db);
        }
    }
}
