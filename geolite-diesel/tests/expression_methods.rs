#![cfg(feature = "sqlite")]

//! Verify that every `GeometryExpressionMethods` method produces identical SQL
//! to the corresponding free function in `geolite_diesel::functions`.

use diesel::dsl::select;
use diesel::sql_types::{Integer, Nullable};
use geolite_core::function_catalog::SQLITE_DETERMINISTIC_FUNCTIONS;
use geolite_diesel::prelude::*;
use std::collections::BTreeSet;

/// Geometry literal helper (not Clone, so create fresh each time via macro).
macro_rules! g {
    () => {
        diesel::dsl::sql::<Nullable<Geometry>>("x")
    };
}

macro_rules! d {
    () => {
        diesel::dsl::sql::<diesel::sql_types::Double>("1.0")
    };
}

macro_rules! i {
    () => {
        diesel::dsl::sql::<Integer>("1")
    };
}

macro_rules! t {
    () => {
        diesel::dsl::sql::<diesel::sql_types::Text>("'T*****FF*'")
    };
}

/// Assert method-style and function-style produce identical SQL.
macro_rules! assert_method_eq_func {
    ($method_expr:expr, $func_expr:expr) => {{
        let method_sql =
            diesel::debug_query::<diesel::sqlite::Sqlite, _>(&select($method_expr)).to_string();
        let func_sql =
            diesel::debug_query::<diesel::sqlite::Sqlite, _>(&select($func_expr)).to_string();
        assert_eq!(method_sql, func_sql);
    }};
}

fn parse_name_and_args_after_fn(src: &str, fn_start: usize) -> Option<(String, String)> {
    let rest = &src[fn_start..];
    let open_paren = rest.find('(')?;
    let name = rest[..open_paren].trim().to_string();

    let mut depth = 1usize;
    let mut idx = open_paren + 1;
    let bytes = rest.as_bytes();
    while idx < rest.len() {
        match bytes[idx] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let args = rest[open_paren + 1..idx].trim().to_string();
                    return Some((name, args));
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

fn normalize_fn_name(name: &str) -> String {
    name.split('<').next().unwrap_or(name).trim().to_string()
}

fn geometry_first_sql_functions(src: &str) -> BTreeSet<String> {
    src.split("diesel::define_sql_function! {")
        .skip(1)
        .filter_map(|block| {
            let fn_idx = block.find("fn st_")?;
            let fn_start = fn_idx + "fn ".len();
            let (name, args) = parse_name_and_args_after_fn(block, fn_start)?;
            let first_arg = args.split(',').next()?.trim();
            if first_arg.contains("Nullable<Geometry>") {
                Some(normalize_fn_name(&name))
            } else {
                None
            }
        })
        .collect()
}

fn geometry_expression_methods(src: &str) -> BTreeSet<String> {
    let trait_start = src
        .find("pub trait GeometryExpressionMethods")
        .expect("GeometryExpressionMethods trait must exist");
    let impl_start = src
        .find("impl<E> GeometryExpressionMethods for E")
        .unwrap_or(src.len());
    let trait_body = &src[trait_start..impl_start];

    trait_body
        .match_indices("fn st_")
        .filter_map(|(idx, _)| {
            let fn_start = idx + "fn ".len();
            parse_name_and_args_after_fn(trait_body, fn_start)
                .map(|(name, _)| normalize_fn_name(&name))
        })
        .collect()
}

fn sql_name_override(block: &str) -> Option<String> {
    for line in block.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("#[sql_name") {
            continue;
        }
        let first_quote = trimmed.find('"')?;
        let rest = &trimmed[first_quote + 1..];
        let second_quote = rest.find('"')?;
        return Some(rest[..second_quote].to_string());
    }
    None
}

fn diesel_sql_signatures(src: &str) -> BTreeSet<(String, usize)> {
    src.split("diesel::define_sql_function! {")
        .skip(1)
        .filter_map(|block| {
            let fn_idx = block.find("fn ")?;
            let fn_start = fn_idx + "fn ".len();
            let (fn_name, args) = parse_name_and_args_after_fn(block, fn_start)?;
            let sql_name = sql_name_override(block).unwrap_or(fn_name);
            let arg_count = if args.trim().is_empty() {
                0
            } else {
                args.split(',').filter(|arg| !arg.trim().is_empty()).count()
            };
            Some((sql_name.to_ascii_uppercase(), arg_count))
        })
        .collect()
}

#[test]
fn diesel_functions_and_methods_surface_parity() {
    let sql_surface = geometry_first_sql_functions(include_str!("../src/functions.rs"));
    let method_surface = geometry_expression_methods(include_str!("../src/expression_methods.rs"));

    let missing_methods: Vec<_> = sql_surface.difference(&method_surface).cloned().collect();
    let extra_methods: Vec<_> = method_surface.difference(&sql_surface).cloned().collect();

    assert!(
        missing_methods.is_empty(),
        "missing method wrappers for SQL functions: {missing_methods:?}"
    );
    assert!(
        extra_methods.is_empty(),
        "method wrappers without matching SQL function declarations: {extra_methods:?}"
    );
}

#[test]
fn diesel_sql_functions_are_backed_by_sqlite_catalog() {
    let diesel_signatures = diesel_sql_signatures(include_str!("../src/functions.rs"));
    let catalog_signatures: BTreeSet<(String, usize)> = SQLITE_DETERMINISTIC_FUNCTIONS
        .iter()
        .map(|spec| (spec.name.to_ascii_uppercase(), spec.n_arg as usize))
        .collect();

    let missing_catalog_entries: Vec<_> = diesel_signatures
        .difference(&catalog_signatures)
        .cloned()
        .collect();

    assert!(
        missing_catalog_entries.is_empty(),
        "diesel SQL functions missing from canonical SQLite catalog: {missing_catalog_entries:?}"
    );
}

#[test]
fn catalog_functions_are_covered_by_diesel_declarations() {
    let catalog_signatures: BTreeSet<(String, usize)> = SQLITE_DETERMINISTIC_FUNCTIONS
        .iter()
        .map(|spec| (spec.name.to_ascii_uppercase(), spec.n_arg as usize))
        .collect();

    let diesel_signatures = diesel_sql_signatures(include_str!("../src/functions.rs"));

    let missing_diesel: Vec<_> = catalog_signatures
        .difference(&diesel_signatures)
        .cloned()
        .collect();

    assert!(
        missing_diesel.is_empty(),
        "catalog functions not covered by Diesel declarations: {missing_diesel:?}"
    );
}

// ── I/O ─────────────────────────────────────────────────────────────────────

#[test]
fn method_st_astext() {
    assert_method_eq_func!(g!().st_astext(), st_astext(g!()));
}

#[test]
fn method_st_asewkt() {
    assert_method_eq_func!(g!().st_asewkt(), st_asewkt(g!()));
}

#[test]
fn method_st_asbinary() {
    assert_method_eq_func!(g!().st_asbinary(), st_asbinary(g!()));
}

#[test]
fn method_st_asewkb() {
    assert_method_eq_func!(g!().st_asewkb(), st_asewkb(g!()));
}

#[test]
fn method_st_asgeojson() {
    assert_method_eq_func!(g!().st_asgeojson(), st_asgeojson(g!()));
}

// ── Constructors / transforms ─────────────────────────────────────────────

#[test]
fn method_st_makeline() {
    assert_method_eq_func!(g!().st_makeline(g!()), st_makeline(g!(), g!()));
}

#[test]
fn method_st_makepolygon() {
    assert_method_eq_func!(g!().st_makepolygon(), st_makepolygon(g!()));
}

#[test]
fn method_st_collect() {
    assert_method_eq_func!(g!().st_collect(g!()), st_collect(g!(), g!()));
}

// ── Accessors ───────────────────────────────────────────────────────────────

#[test]
fn method_st_srid() {
    assert_method_eq_func!(g!().st_srid(), st_srid(g!()));
}

#[test]
fn method_st_setsrid() {
    assert_method_eq_func!(g!().st_setsrid(i!()), st_setsrid(g!(), i!()));
}

#[test]
fn method_st_geometrytype() {
    assert_method_eq_func!(g!().st_geometrytype(), st_geometrytype(g!()));
}

#[test]
fn method_st_x() {
    assert_method_eq_func!(g!().st_x(), st_x(g!()));
}

#[test]
fn method_st_y() {
    assert_method_eq_func!(g!().st_y(), st_y(g!()));
}

#[test]
fn method_st_isempty() {
    assert_method_eq_func!(g!().st_isempty(), st_isempty(g!()));
}

#[test]
fn method_st_ndims() {
    assert_method_eq_func!(g!().st_ndims(), st_ndims(g!()));
}

#[test]
fn method_st_coorddim() {
    assert_method_eq_func!(g!().st_coorddim(), st_coorddim(g!()));
}

#[test]
fn method_st_zmflag() {
    assert_method_eq_func!(g!().st_zmflag(), st_zmflag(g!()));
}

#[test]
fn method_st_memsize() {
    assert_method_eq_func!(g!().st_memsize(), st_memsize(g!()));
}

#[test]
fn method_st_isvalid() {
    assert_method_eq_func!(g!().st_isvalid(), st_isvalid(g!()));
}

#[test]
fn method_st_isvalidreason() {
    assert_method_eq_func!(g!().st_isvalidreason(), st_isvalidreason(g!()));
}

#[test]
fn method_st_numpoints() {
    assert_method_eq_func!(g!().st_numpoints(), st_numpoints(g!()));
}

#[test]
fn method_st_npoints() {
    assert_method_eq_func!(g!().st_npoints(), st_npoints(g!()));
}

#[test]
fn method_st_numgeometries() {
    assert_method_eq_func!(g!().st_numgeometries(), st_numgeometries(g!()));
}

#[test]
fn method_st_numinteriorrings() {
    assert_method_eq_func!(g!().st_numinteriorrings(), st_numinteriorrings(g!()));
}

#[test]
fn method_st_numrings() {
    assert_method_eq_func!(g!().st_numrings(), st_numrings(g!()));
}

#[test]
fn method_st_dimension() {
    assert_method_eq_func!(g!().st_dimension(), st_dimension(g!()));
}

#[test]
fn method_st_envelope() {
    assert_method_eq_func!(g!().st_envelope(), st_envelope(g!()));
}

#[test]
fn method_st_pointn() {
    assert_method_eq_func!(g!().st_pointn(i!()), st_pointn(g!(), i!()));
}

#[test]
fn method_st_startpoint() {
    assert_method_eq_func!(g!().st_startpoint(), st_startpoint(g!()));
}

#[test]
fn method_st_endpoint() {
    assert_method_eq_func!(g!().st_endpoint(), st_endpoint(g!()));
}

#[test]
fn method_st_exteriorring() {
    assert_method_eq_func!(g!().st_exteriorring(), st_exteriorring(g!()));
}

#[test]
fn method_st_interiorringn() {
    assert_method_eq_func!(g!().st_interiorringn(i!()), st_interiorringn(g!(), i!()));
}

#[test]
fn method_st_geometryn() {
    assert_method_eq_func!(g!().st_geometryn(i!()), st_geometryn(g!(), i!()));
}

#[test]
fn method_st_xmin() {
    assert_method_eq_func!(g!().st_xmin(), st_xmin(g!()));
}

#[test]
fn method_st_xmax() {
    assert_method_eq_func!(g!().st_xmax(), st_xmax(g!()));
}

#[test]
fn method_st_ymin() {
    assert_method_eq_func!(g!().st_ymin(), st_ymin(g!()));
}

#[test]
fn method_st_ymax() {
    assert_method_eq_func!(g!().st_ymax(), st_ymax(g!()));
}

// ── Measurement ─────────────────────────────────────────────────────────────

#[test]
fn method_st_area() {
    assert_method_eq_func!(g!().st_area(), st_area(g!()));
}

#[test]
fn method_st_length() {
    assert_method_eq_func!(g!().st_length(), st_length(g!()));
}

#[test]
fn method_st_perimeter() {
    assert_method_eq_func!(g!().st_perimeter(), st_perimeter(g!()));
}

#[test]
fn method_st_distance() {
    assert_method_eq_func!(g!().st_distance(g!()), st_distance(g!(), g!()));
}

#[test]
fn method_st_distancesphere() {
    assert_method_eq_func!(g!().st_distancesphere(g!()), st_distancesphere(g!(), g!()));
}

#[test]
fn method_st_distancespheroid() {
    assert_method_eq_func!(
        g!().st_distancespheroid(g!()),
        st_distancespheroid(g!(), g!())
    );
}

#[test]
fn method_st_hausdorffdistance() {
    assert_method_eq_func!(
        g!().st_hausdorffdistance(g!()),
        st_hausdorffdistance(g!(), g!())
    );
}

#[test]
fn method_st_centroid() {
    assert_method_eq_func!(g!().st_centroid(), st_centroid(g!()));
}

#[test]
fn method_st_pointonsurface() {
    assert_method_eq_func!(g!().st_pointonsurface(), st_pointonsurface(g!()));
}

// ── Predicates ──────────────────────────────────────────────────────────────

#[test]
fn method_st_intersects() {
    assert_method_eq_func!(g!().st_intersects(g!()), st_intersects(g!(), g!()));
}

#[test]
fn method_st_contains() {
    assert_method_eq_func!(g!().st_contains(g!()), st_contains(g!(), g!()));
}

#[test]
fn method_st_within() {
    assert_method_eq_func!(g!().st_within(g!()), st_within(g!(), g!()));
}

#[test]
fn method_st_covers() {
    assert_method_eq_func!(g!().st_covers(g!()), st_covers(g!(), g!()));
}

#[test]
fn method_st_coveredby() {
    assert_method_eq_func!(g!().st_coveredby(g!()), st_coveredby(g!(), g!()));
}

#[test]
fn method_st_disjoint() {
    assert_method_eq_func!(g!().st_disjoint(g!()), st_disjoint(g!(), g!()));
}

#[test]
fn method_st_equals() {
    assert_method_eq_func!(g!().st_equals(g!()), st_equals(g!(), g!()));
}

#[test]
fn method_st_dwithin() {
    assert_method_eq_func!(g!().st_dwithin(g!(), d!()), st_dwithin(g!(), g!(), d!()));
}

#[test]
fn method_st_relate() {
    assert_method_eq_func!(g!().st_relate(g!()), st_relate(g!(), g!()));
}

#[test]
fn method_st_relate_match_geoms() {
    assert_method_eq_func!(
        g!().st_relate_match_geoms(g!(), t!()),
        st_relate_match_geoms(g!(), g!(), t!())
    );
}

// ── Geography variants ──────────────────────────────────────────────────────

#[test]
fn method_st_lengthsphere() {
    assert_method_eq_func!(g!().st_lengthsphere(), st_lengthsphere(g!()));
}

#[test]
fn method_st_azimuth() {
    assert_method_eq_func!(g!().st_azimuth(g!()), st_azimuth(g!(), g!()));
}

#[test]
fn method_st_project() {
    assert_method_eq_func!(g!().st_project(d!(), d!()), st_project(g!(), d!(), d!()));
}

#[test]
fn method_st_closestpoint() {
    assert_method_eq_func!(g!().st_closestpoint(g!()), st_closestpoint(g!(), g!()));
}
