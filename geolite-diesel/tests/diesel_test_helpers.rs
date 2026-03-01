// Shared test logic for Diesel + SQLite integration tests (native & WASM).
//
// Included via `include!()` by both `sqlite_integration.rs` and
// `wasm_integration.rs`, following the same pattern as
// `geolite-sqlite/tests/test_db_macro.rs`.

diesel::table! {
    features (id) {
        id   -> Integer,
        name -> Text,
        geom -> Nullable<geolite_diesel::Geometry>,
    }
}

#[derive(Queryable, Debug)]
#[diesel(table_name = features)]
struct Feature {
    #[allow(dead_code)]
    id: i32,
    name: String,
    geom: Option<geo::Geometry<f64>>,
}

#[derive(QueryableByName, Debug)]
struct TextResult {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    val: Option<String>,
}

#[derive(QueryableByName, Debug)]
struct F64Result {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    val: Option<f64>,
}

#[derive(QueryableByName, Debug)]
struct I32Result {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    val: Option<i32>,
}

fn setup_features_table(c: &mut SqliteConnection) {
    diesel::sql_query(
        "CREATE TABLE features (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            geom BLOB
        )",
    )
    .execute(c)
    .unwrap();
}

// ── Spatial function execution via Diesel ────────────────────────────────────

macro_rules! define_diesel_sqlite_tests {
    ($test_attr:meta) => {

// ── ST_Point + ST_AsText ─────────────────────────────────────────────────────

#[$test_attr]
fn st_point_and_astext() {
    let mut c = conn();
    let result: TextResult = diesel::sql_query(
        "SELECT ST_AsText(ST_Point(1.5, 2.5)) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = result.val.expect("should not be NULL");
    assert!(wkt.contains("POINT"), "got: {wkt}");
    assert!(wkt.contains("1.5"), "got: {wkt}");
    assert!(wkt.contains("2.5"), "got: {wkt}");
}

#[$test_attr]
fn st_point_srid_execution() {
    let mut c = conn();
    let result: I32Result = diesel::sql_query("SELECT ST_SRID(ST_Point(1.5, 2.5, 4326)) AS val")
        .get_result(&mut c)
        .unwrap();
    assert_eq!(result.val, Some(4326));
}

#[$test_attr]
fn st_point_rejects_non_finite_coordinates_execution() {
    let mut c = conn();
    let err = diesel::sql_query("SELECT ST_IsValid(ST_Point(1e309, 0)) AS val")
        .get_result::<I32Result>(&mut c)
        .expect_err("non-finite ST_Point coordinates must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("coordinates must be finite"), "got: {msg}");
}

#[$test_attr]
fn st_makeenvelope_srid_execution() {
    let mut c = conn();
    let result: I32Result =
        diesel::sql_query("SELECT ST_SRID(ST_MakeEnvelope(0, 0, 1, 1, 3857)) AS val")
            .get_result(&mut c)
            .unwrap();
    assert_eq!(result.val, Some(3857));
}

#[$test_attr]
fn st_geomfromwkb_srid_execution() {
    let mut c = conn();
    let result: I32Result = diesel::sql_query(
        "SELECT ST_SRID(ST_GeomFromWKB(ST_AsBinary(ST_Point(1.5, 2.5)), 4326)) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result.val, Some(4326));
}

// ── ST_Distance ──────────────────────────────────────────────────────────────

#[$test_attr]
fn st_distance_execution() {
    let mut c = conn();
    let result: F64Result = diesel::sql_query(
        "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4)) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let dist = result.val.expect("should not be NULL");
    assert!((dist - 5.0).abs() < 1e-10, "expected 5.0, got {dist}");
}

#[$test_attr]
fn st_makeline_rejects_empty_points_execution() {
    let mut c = conn();
    let err = diesel::sql_query(
        "SELECT ST_NumPoints(ST_MakeLine(ST_GeomFromText('POINT EMPTY'), ST_Point(1,1))) AS val",
    )
    .get_result::<I32Result>(&mut c)
    .expect_err("empty point input must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("point must not be empty"), "got: {msg}");
}

// ── ST_GeomFromText ──────────────────────────────────────────────────────────

#[$test_attr]
fn st_geomfromtext_execution() {
    let mut c = conn();
    let result: TextResult = diesel::sql_query(
        "SELECT ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 0)')) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = result.val.expect("should not be NULL");
    assert!(wkt.contains("LINESTRING"), "got: {wkt}");
}

// ── ST_Area ──────────────────────────────────────────────────────────────────

#[$test_attr]
fn st_area_execution() {
    let mut c = conn();
    let result: F64Result = diesel::sql_query(
        "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))')) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let area = result.val.expect("should not be NULL");
    assert!((area - 100.0).abs() < 1e-10, "expected 100.0, got {area}");
}

// ── ST_Centroid ──────────────────────────────────────────────────────────────

#[$test_attr]
fn st_centroid_execution() {
    let mut c = conn();
    let result: TextResult = diesel::sql_query(
        "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = result.val.expect("should not be NULL");
    assert!(wkt.contains("POINT"), "centroid should be a point, got: {wkt}");
    assert!(wkt.contains("5"), "centroid should contain 5, got: {wkt}");
}

// ── Diesel query builder: function-style ─────────────────────────────────────

#[$test_attr]
fn diesel_select_st_point() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let wkt: Option<String> = diesel::dsl::select(st_astext(st_point(13.4, 52.5).nullable()))
        .get_result(&mut c)
        .unwrap();
    let wkt = wkt.expect("should not be NULL");
    assert!(wkt.contains("POINT"), "got: {wkt}");
    assert!(wkt.contains("13.4"), "got: {wkt}");
}

#[$test_attr]
fn diesel_select_st_distance() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let dist: Option<f64> = diesel::dsl::select(st_distance(
        st_point(0.0, 0.0).nullable(),
        st_point(3.0, 4.0).nullable(),
    ))
    .get_result(&mut c)
    .unwrap();
    let dist = dist.expect("should not be NULL");
    assert!((dist - 5.0).abs() < 1e-10, "expected 5.0, got {dist}");
}

#[$test_attr]
fn diesel_select_st_area() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let area: Option<f64> = diesel::dsl::select(st_area(
        st_geomfromtext("POLYGON((0 0,10 0,10 10,0 10,0 0))"),
    ))
    .get_result(&mut c)
    .unwrap();
    let area = area.expect("should not be NULL");
    assert!((area - 100.0).abs() < 1e-10, "expected 100.0, got {area}");
}

#[$test_attr]
fn diesel_select_st_intersects() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    // Two overlapping squares
    let result: Option<bool> = diesel::dsl::select(st_intersects(
        st_geomfromtext("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        st_geomfromtext("POLYGON((1 1,3 1,3 3,1 3,1 1))"),
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result, Some(true));

    // Two non-overlapping squares
    let result: Option<bool> = diesel::dsl::select(st_intersects(
        st_geomfromtext("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
        st_geomfromtext("POLYGON((5 5,6 5,6 6,5 6,5 5))"),
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result, Some(false));
}

#[$test_attr]
fn diesel_select_st_dwithin() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    // Points within distance
    let result: Option<bool> = diesel::dsl::select(st_dwithin(
        st_point(0.0, 0.0).nullable(),
        st_point(1.0, 0.0).nullable(),
        2.0,
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result, Some(true));

    // Points not within distance
    let result: Option<bool> = diesel::dsl::select(st_dwithin(
        st_point(0.0, 0.0).nullable(),
        st_point(10.0, 0.0).nullable(),
        2.0,
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result, Some(false));
}

#[$test_attr]
fn diesel_select_st_makeenvelope() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let wkt: Option<String> = diesel::dsl::select(
        st_astext(st_makeenvelope(0.0, 0.0, 10.0, 10.0).nullable()),
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = wkt.expect("should not be NULL");
    assert!(wkt.contains("POLYGON"), "got: {wkt}");
}

#[$test_attr]
fn diesel_select_st_geomfromtext() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let wkt: Option<String> = diesel::dsl::select(
        st_astext(st_geomfromtext("MULTIPOINT((0 0),(1 1),(2 2))")),
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = wkt.expect("should not be NULL");
    assert!(wkt.contains("MULTIPOINT"), "got: {wkt}");
}

#[$test_attr]
fn diesel_select_st_geomfromewkb_rejects_little_endian_zm_payload() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let ewkb_expr = diesel::dsl::sql::<diesel::sql_types::Nullable<diesel::sql_types::Binary>>(
        "X'01010000C0000000000000F03F000000000000004000000000000008400000000000001040'",
    );

    let err = diesel::dsl::select(st_asewkb(st_geomfromewkb(ewkb_expr)))
        .get_result::<Option<Vec<u8>>>(&mut c)
        .expect_err("ZM payload must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("unsupported coordinate dimensions"), "got: {msg}");
}

#[$test_attr]
fn diesel_select_st_geomfromewkb_rejects_big_endian_zm_payload() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let ewkb_expr = diesel::dsl::sql::<diesel::sql_types::Nullable<diesel::sql_types::Binary>>(
        "X'00C00000013FF0000000000000400000000000000040080000000000004010000000000000'",
    );

    let err = diesel::dsl::select(st_asewkb(st_geomfromewkb(ewkb_expr)))
        .get_result::<Option<Vec<u8>>>(&mut c)
        .expect_err("big-endian ZM payload must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("unsupported coordinate dimensions"), "got: {msg}");
}

#[$test_attr]
fn diesel_select_st_centroid() {
    use geolite_diesel::functions::*;

    let mut c = conn();
    let wkt: Option<String> = diesel::dsl::select(
        st_astext(st_centroid(st_geomfromtext("POLYGON((0 0,4 0,4 4,0 4,0 0))"))),
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = wkt.expect("should not be NULL");
    assert!(wkt.contains("POINT"), "got: {wkt}");
    assert!(wkt.contains("2"), "centroid should be at (2,2), got: {wkt}");
}

// ── Method-style ORM queries ─────────────────────────────────────────────────

#[$test_attr]
fn method_st_astext_in_select() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    diesel::sql_query(
        "INSERT INTO features (id, name, geom) VALUES (1, 'Berlin', ST_Point(13.4050, 52.5200))",
    )
    .execute(&mut c)
    .unwrap();

    let wkt: Option<String> = features::table
        .select(features::geom.st_astext())
        .first(&mut c)
        .unwrap();
    let wkt = wkt.expect("should not be NULL");
    assert!(wkt.contains("POINT"), "got: {wkt}");
    assert!(wkt.contains("13.405"), "got: {wkt}");
}

#[$test_attr]
fn method_st_distance_in_select() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    diesel::sql_query(
        "INSERT INTO features (id, name, geom) VALUES (1, 'origin', ST_Point(0, 0))",
    )
    .execute(&mut c)
    .unwrap();

    let dist: Option<f64> = features::table
        .select(features::geom.st_distance(st_point(3.0, 4.0).nullable()))
        .first(&mut c)
        .unwrap();
    let dist = dist.expect("should not be NULL");
    assert!((dist - 5.0).abs() < 1e-10, "expected 5.0, got {dist}");
}

#[$test_attr]
fn method_st_dwithin_in_filter() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    diesel::sql_query(
        "INSERT INTO features (id, name, geom) VALUES
            (1, 'near', ST_Point(0.5, 0.5)),
            (2, 'far',  ST_Point(100, 100))",
    )
    .execute(&mut c)
    .unwrap();

    let names: Vec<String> = features::table
        .filter(features::geom.st_dwithin(st_point(0.0, 0.0).nullable(), 2.0).eq(true))
        .select(features::name)
        .load(&mut c)
        .unwrap();
    assert_eq!(names, vec!["near"]);
}

#[$test_attr]
fn method_st_intersects_in_filter() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    diesel::sql_query(
        "INSERT INTO features (id, name, geom) VALUES
            (1, 'inside',  ST_Point(5, 5)),
            (2, 'outside', ST_Point(50, 50))",
    )
    .execute(&mut c)
    .unwrap();

    let names: Vec<String> = features::table
        .filter(features::geom.st_intersects(
            st_geomfromtext("POLYGON((0 0,10 0,10 10,0 10,0 0))"),
        ).eq(true))
        .select(features::name)
        .load(&mut c)
        .unwrap();
    assert_eq!(names, vec!["inside"]);
}

#[$test_attr]
fn method_st_contains_in_filter() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    // Insert a polygon and check which points it contains
    diesel::sql_query(
        "INSERT INTO features (id, name, geom) VALUES
            (1, 'box', ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))",
    )
    .execute(&mut c)
    .unwrap();

    let result: Option<i32> = features::table
        .filter(features::geom.st_contains(st_point(5.0, 5.0).nullable()).eq(true))
        .select(features::id)
        .first(&mut c)
        .optional()
        .unwrap();
    assert_eq!(result, Some(1));

    let result: Option<i32> = features::table
        .filter(features::geom.st_contains(st_point(50.0, 50.0).nullable()).eq(true))
        .select(features::id)
        .first(&mut c)
        .optional()
        .unwrap();
    assert_eq!(result, None);
}

#[$test_attr]
fn method_st_relate_match_geoms_in_select() {
    use geolite_diesel::prelude::*;

    let mut c = conn();
    let polygon = "POLYGON((0 0,0 3,3 3,3 0,0 0))";

    let matrix: Option<String> = diesel::dsl::select(st_relate(
        st_point(1.0, 1.0).nullable(),
        st_geomfromtext(polygon),
    ))
    .get_result(&mut c)
    .unwrap();
    let matrix = matrix.expect("st_relate should return a DE-9IM matrix");

    let inside: Option<bool> = diesel::dsl::select(st_point(1.0, 1.0).nullable().st_relate_match_geoms(
        st_geomfromtext(polygon),
        matrix.as_str(),
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(inside, Some(true));

    let impossible_pattern: Option<bool> = diesel::dsl::select(
        st_point(1.0, 1.0)
            .nullable()
            .st_relate_match_geoms(st_geomfromtext(polygon), "FFFFFFFFF"),
    )
    .get_result(&mut c)
    .unwrap();
    assert_eq!(impossible_pattern, Some(false));
}

// ── Full ORM roundtrip ──────────────────────────────────────────────────────

#[$test_attr]
fn orm_geometry_roundtrip() {
    use geolite_diesel::types::Geometry as GeomType;

    let mut c = conn();
    setup_features_table(&mut c);

    let point = geo::Geometry::Point(geo::Point::new(13.4050, 52.5200));
    diesel::sql_query("INSERT INTO features (id, name, geom) VALUES (1, 'Berlin', ?)")
        .bind::<GeomType, _>(&point)
        .execute(&mut c)
        .unwrap();

    let row: Feature = features::table.find(1).first(&mut c).unwrap();
    assert_eq!(row.name, "Berlin");
    let geom = row.geom.expect("geom should not be NULL");
    match geom {
        geo::Geometry::Point(p) => {
            assert!((p.x() - 13.4050).abs() < 1e-10);
            assert!((p.y() - 52.5200).abs() < 1e-10);
        }
        other => panic!("expected Point, got {other:?}"),
    }
}

#[$test_attr]
fn orm_polygon_roundtrip() {
    use geolite_diesel::types::Geometry as GeomType;
    use geolite_diesel::prelude::*;

    let mut c = conn();
    setup_features_table(&mut c);

    let polygon = geo::Geometry::Polygon(geo::Polygon::new(
        geo::LineString::from(vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]),
        vec![],
    ));
    diesel::sql_query("INSERT INTO features (id, name, geom) VALUES (1, 'square', ?)")
        .bind::<GeomType, _>(&polygon)
        .execute(&mut c)
        .unwrap();

    // Verify area via query builder
    let area: Option<f64> = features::table
        .select(features::geom.st_area())
        .first(&mut c)
        .unwrap();
    let area = area.expect("should not be NULL");
    assert!((area - 100.0).abs() < 1e-10, "expected 100.0, got {area}");
}

#[$test_attr]
fn orm_null_geometry() {
    let mut c = conn();
    setup_features_table(&mut c);

    diesel::sql_query("INSERT INTO features (id, name, geom) VALUES (1, 'empty', NULL)")
        .execute(&mut c)
        .unwrap();

    let row: Feature = features::table.find(1).first(&mut c).unwrap();
    assert_eq!(row.name, "empty");
    assert!(row.geom.is_none());
}

// ── Spatial index correctness ────────────────────────────────────────────────

#[$test_attr]
fn spatial_index_correctness() {
    let mut c = conn();

    diesel::sql_query(
        "CREATE TABLE grid (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            geom BLOB
        )",
    )
    .execute(&mut c)
    .unwrap();

    // Insert a grid of points
    for i in 0..10 {
        for j in 0..10 {
            let id = i * 10 + j;
            diesel::sql_query(format!(
                "INSERT INTO grid (id, name, geom) VALUES ({id}, 'p{id}', ST_Point({i}, {j}))"
            ))
            .execute(&mut c)
            .unwrap();
        }
    }

    // Create spatial index
    diesel::sql_query("SELECT CreateSpatialIndex('grid', 'geom')")
        .execute(&mut c)
        .unwrap();

    // Query using R-tree index join: find points inside envelope (2,2)-(5,5)
    let results: Vec<I32Result> = diesel::sql_query(
        "SELECT g.id AS val FROM grid g
         JOIN grid_geom_rtree r ON g.rowid = r.id
         WHERE r.xmin >= 2 AND r.xmax <= 5 AND r.ymin >= 2 AND r.ymax <= 5
         AND ST_Intersects(g.geom, ST_MakeEnvelope(2, 2, 5, 5)) = 1
         ORDER BY g.id",
    )
    .load(&mut c)
    .unwrap();

    let ids: Vec<i32> = results.iter().filter_map(|r| r.val).collect();

    // Points at (2..=5, 2..=5) should be found
    let mut expected = Vec::new();
    for i in 2..=5 {
        for j in 2..=5 {
            expected.push(i * 10 + j);
        }
    }
    expected.sort();
    assert_eq!(ids, expected, "spatial index query returned wrong points");
}

// ── Alias SQL functions ───────────────────────────────────────────────────────
// Verify that every name alias registered in the SQLite extension is also
// reachable through a Diesel connection (raw SQL path).

#[$test_attr]
fn st_makepoint_alias_works_via_sql() {
    let mut c = conn();
    let result: TextResult =
        diesel::sql_query("SELECT ST_AsText(ST_MakePoint(3.0, 4.0)) AS val")
            .get_result(&mut c)
            .unwrap();
    let wkt = result.val.unwrap();
    assert!(wkt.contains("POINT"), "ST_MakePoint WKT = {wkt}");
}

#[$test_attr]
fn geometry_type_alias_works_via_sql() {
    let mut c = conn();
    let result: TextResult =
        diesel::sql_query("SELECT GeometryType(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS val")
            .get_result(&mut c)
            .unwrap();
    assert_eq!(result.val.unwrap(), "ST_Polygon");
}

#[$test_attr]
fn st_numinteriorring_alias_works_via_sql() {
    let mut c = conn();
    let result: I32Result = diesel::sql_query(
        "SELECT ST_NumInteriorRing(\
            ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))')\
         ) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    assert_eq!(result.val, Some(1));
}

#[$test_attr]
fn st_length2d_alias_works_via_sql() {
    let mut c = conn();
    let result: F64Result =
        diesel::sql_query("SELECT ST_Length2D(ST_GeomFromText('LINESTRING(0 0,3 4)')) AS val")
            .get_result(&mut c)
            .unwrap();
    let len = result.val.unwrap();
    assert!((len - 5.0).abs() < 1e-10, "ST_Length2D = {len}");
}

#[$test_attr]
fn st_perimeter2d_alias_works_via_sql() {
    let mut c = conn();
    let result: F64Result = diesel::sql_query(
        "SELECT ST_Perimeter2D(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS val",
    )
    .get_result(&mut c)
    .unwrap();
    let perim = result.val.unwrap();
    assert!((perim - 4.0).abs() < 1e-10, "ST_Perimeter2D = {perim}");
}

// ── Typed Diesel alias functions ──────────────────────────────────────────────
// These use the declared Diesel SQL functions (not raw SQL), verifying that
// the type-system wrappers are wired to the correct SQL names.

#[$test_attr]
fn st_makepoint_typed_diesel_function() {
    use geolite_diesel::functions::st_makepoint;
    let mut c = conn();
    let wkt: Option<String> = diesel::dsl::select(
        geolite_diesel::functions::st_astext(st_makepoint(1.5_f64, 2.5_f64).nullable()),
    )
    .get_result(&mut c)
    .unwrap();
    let wkt = wkt.unwrap();
    assert!(wkt.contains("POINT") && wkt.contains("1.5"), "got: {wkt}");
}

#[$test_attr]
fn geometry_type_typed_diesel_function() {
    use geolite_diesel::functions::{geometry_type, st_geomfromtext};
    let mut c = conn();
    let val: Option<String> = diesel::dsl::select(geometry_type(
        st_geomfromtext("POLYGON((0 0,1 0,1 1,0 1,0 0))"),
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(val.unwrap(), "ST_Polygon");
}

#[$test_attr]
fn st_numinteriorring_typed_diesel_function() {
    use geolite_diesel::functions::{st_geomfromtext, st_numinteriorring};
    let mut c = conn();
    let val: Option<i32> = diesel::dsl::select(st_numinteriorring(
        st_geomfromtext("POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))"),
    ))
    .get_result(&mut c)
    .unwrap();
    assert_eq!(val, Some(1));
}

#[$test_attr]
fn st_length2d_typed_diesel_function() {
    use geolite_diesel::functions::{st_geomfromtext, st_length2d};
    let mut c = conn();
    let len: Option<f64> =
        diesel::dsl::select(st_length2d(st_geomfromtext("LINESTRING(0 0,3 4)")))
            .get_result(&mut c)
            .unwrap();
    let len = len.unwrap();
    assert!((len - 5.0).abs() < 1e-10, "ST_Length2D = {len}");
}

#[$test_attr]
fn st_perimeter2d_typed_diesel_function() {
    use geolite_diesel::functions::{st_geomfromtext, st_perimeter2d};
    let mut c = conn();
    let perim: Option<f64> = diesel::dsl::select(st_perimeter2d(st_geomfromtext(
        "POLYGON((0 0,1 0,1 1,0 1,0 0))",
    )))
    .get_result(&mut c)
    .unwrap();
    let perim = perim.unwrap();
    assert!((perim - 4.0).abs() < 1e-10, "ST_Perimeter2D = {perim}");
}

    }; // end macro
}
