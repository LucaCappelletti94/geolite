#![cfg(feature = "sqlite")]

//! Verify that every `GeometryExpressionMethods` method produces identical SQL
//! to the corresponding free function in `geolite_diesel::functions`.

use diesel::dsl::select;
use diesel::sql_types::{Integer, Nullable};
use geolite_diesel::prelude::*;

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
fn method_st_asgeojson() {
    assert_method_eq_func!(g!().st_asgeojson(), st_asgeojson(g!()));
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
    assert_method_eq_func!(
        g!().st_dwithin(g!(), d!()),
        st_dwithin(g!(), g!(), d!())
    );
}

#[test]
fn method_st_relate() {
    assert_method_eq_func!(g!().st_relate(g!()), st_relate(g!(), g!()));
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
    assert_method_eq_func!(
        g!().st_project(d!(), d!()),
        st_project(g!(), d!(), d!())
    );
}

#[test]
fn method_st_closestpoint() {
    assert_method_eq_func!(g!().st_closestpoint(g!()), st_closestpoint(g!(), g!()));
}
