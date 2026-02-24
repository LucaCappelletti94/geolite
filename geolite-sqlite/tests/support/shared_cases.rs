macro_rules! define_shared_cases {
    ($test_attr:meta) => {
// ── I/O round-trips ───────────────────────────────────────────────────────────

#[$test_attr]
fn wkt_round_trip() {
    let db = ActiveTestDb::open();
    let wkt = db.query_text("SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!(wkt.contains("POLYGON"), "got: {wkt}");
}

#[$test_attr]
fn point_empty_round_trip() {
    let db = ActiveTestDb::open();
    let wkt = db.query_text("SELECT ST_AsText(ST_GeomFromText('POINT EMPTY'))");
    assert_eq!(wkt, "POINT EMPTY");

    let is_empty = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('POINT EMPTY'))");
    assert_eq!(is_empty, 1);
}

#[$test_attr]
fn geometrycollection_with_empty_point_round_trip() {
    let db = ActiveTestDb::open();
    let npoints = db.query_i64("SELECT ST_NPoints(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)'))");
    assert_eq!(npoints, 0);

    let is_empty = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION(POINT EMPTY)'))");
    assert_eq!(is_empty, 1);
}

#[$test_attr]
fn geojson_round_trip() {
    let db = ActiveTestDb::open();
    let json = db.query_text(
        "SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}'))",
    );
    assert!(json.contains("Point"), "got: {json}");
}

#[$test_attr]
fn point_empty_geojson_is_postgis_compatible() {
    let db = ActiveTestDb::open();
    let json = db.query_text("SELECT ST_AsGeoJSON(ST_GeomFromText('POINT EMPTY'))");
    assert_eq!(json, r#"{"type":"Point","coordinates":[]}"#);
}

#[$test_attr]
fn point_empty_geojson_round_trip() {
    let db = ActiveTestDb::open();
    let wkt = db.query_text(
        "SELECT ST_AsText(ST_GeomFromGeoJSON(ST_AsGeoJSON(ST_GeomFromText('POINT EMPTY'))))",
    );
    assert_eq!(wkt, "POINT EMPTY");
}

#[$test_attr]
fn wkb_round_trip() {
    let db = ActiveTestDb::open();
    let wkt = db
        .query_text("SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(3 4)'))))");
    assert!(wkt.contains("POINT"), "got: {wkt}");
}

#[$test_attr]
fn ewkb_round_trip() {
    let db = ActiveTestDb::open();
    let wkt = db
        .query_text("SELECT ST_AsText(ST_GeomFromEWKB(ST_AsEWKB(ST_GeomFromText('POINT(1 2)'))))");
    assert!(wkt.contains("POINT"), "got: {wkt}");
}

#[$test_attr]
fn ewkb_round_trip_preserves_zm_payload() {
    let db = ActiveTestDb::open();
    let hex = db.query_text(
        "SELECT hex(ST_AsEWKB(ST_GeomFromEWKB(X'01010000C0000000000000F03F000000000000004000000000000008400000000000001040')))",
    );
    assert_eq!(
        hex,
        "01010000C0000000000000F03F000000000000004000000000000008400000000000001040"
    );
}

#[$test_attr]
fn ewkb_round_trip_preserves_big_endian_payload() {
    let db = ActiveTestDb::open();
    let hex = db.query_text(
        "SELECT hex(ST_AsEWKB(ST_GeomFromEWKB(X'00C00000013FF0000000000000400000000000000040080000000000004010000000000000')))",
    );
    assert_eq!(
        hex,
        "00C00000013FF0000000000000400000000000000040080000000000004010000000000000"
    );
}

#[$test_attr]
fn ewkt_round_trip() {
    let db = ActiveTestDb::open();
    let ewkt = db.query_text("SELECT ST_AsEWKT(ST_GeomFromText('POINT(1 2)', 4326))");
    assert!(ewkt.starts_with("SRID=4326;"), "got: {ewkt}");
}

#[$test_attr]
fn geomfromwkb_with_srid() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64(
        "SELECT ST_SRID(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(0 0)')), 4326))",
    );
    assert_eq!(srid, 4326);
}

#[$test_attr]
fn geomfromgeojson_default_srid() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64(
        "SELECT ST_SRID(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1,2]}'))",
    );
    assert_eq!(srid, 4326);
}

// ── Constructors ──────────────────────────────────────────────────────────────

#[$test_attr]
fn st_make_envelope() {
    let db = ActiveTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_MakeEnvelope(0, 0, 2, 3))");
    assert!((area - 6.0).abs() < 1e-10, "area = {area}");
}

#[$test_attr]
fn st_tile_envelope_zoom0() {
    let db = ActiveTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_TileEnvelope(0, 0, 0))");
    // Full web-mercator extent squared: (2 * 20037508.34)^2 ≈ 1.607e15
    assert!(area > 1e15, "area = {area}");
}

#[$test_attr]
fn st_tile_envelope_negative_args_rejected_with_clear_error() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Area(ST_TileEnvelope(-1, 0, 0))")
        .expect_err("negative zoom should return an error");
    assert!(
        err.contains("must be non-negative"),
        "unexpected error message: {err}"
    );
}

#[$test_attr]
fn st_point_with_srid() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_Point(1, 2, 4326))");
    assert_eq!(srid, 4326);
}

#[$test_attr]
fn null_numeric_arg_st_point_returns_null() {
    let db = ActiveTestDb::open();
    let is_null = db.query_i64("SELECT ST_Point(1,2,NULL) IS NULL");
    assert_eq!(is_null, 1);
}

#[$test_attr]
fn st_point_rejects_non_numeric_args() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Point('abc', 2) IS NULL")
        .expect_err("non-numeric ST_Point argument should be a hard error");
    assert!(
        err.contains("must be numeric"),
        "unexpected error message: {err}"
    );
}

#[$test_attr]
fn st_make_envelope_null_short_circuits_invalid_numeric_args() {
    let db = ActiveTestDb::open();
    let result = db.try_query_i64("SELECT ST_MakeEnvelope('abc', 0, 1, 1, NULL) IS NULL");
    assert_eq!(
        result,
        Ok(1),
        "NULL argument should short-circuit before numeric type errors: {result:?}"
    );
}

#[$test_attr]
fn st_make_line() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumPoints(ST_MakeLine(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_make_polygon() {
    let db = ActiveTestDb::open();
    let t = db.query_text(
        "SELECT ST_GeometryType(ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 1,0 0)')))",
    );
    assert_eq!(t, "ST_Polygon");
}

#[$test_attr]
fn st_make_envelope_with_srid() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_MakeEnvelope(0,0,1,1,4326))");
    assert_eq!(srid, 4326);
}

#[$test_attr]
fn st_collect() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Collect(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

// ── Accessors ─────────────────────────────────────────────────────────────────

#[$test_attr]
fn st_srid_default() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(srid, 0);
}

#[$test_attr]
fn st_srid_rejects_malformed_ewkb() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_SRID(X'01') IS NULL")
        .expect_err("malformed EWKB must be a hard error");
    assert!(err.contains("invalid EWKB"), "unexpected error message: {err}");
}

#[$test_attr]
fn st_srid_set() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_GeomFromText('POINT(0 0)', 4326))");
    assert_eq!(srid, 4326);
}

#[$test_attr]
fn st_set_srid() {
    let db = ActiveTestDb::open();
    let srid = db.query_i64("SELECT ST_SRID(ST_SetSRID(ST_GeomFromText('POINT(0 0)'), 4326))");
    assert_eq!(srid, 4326);
}

#[$test_attr]
fn st_geometry_type() {
    let db = ActiveTestDb::open();
    let t = db.query_text("SELECT ST_GeometryType(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(t, "ST_Point");
}

#[$test_attr]
fn st_geometry_type_rejects_truncated_ewkb() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_GeometryType(X'0101000000') IS NULL")
        .expect_err("truncated EWKB must be a hard error");
    assert!(
        err.contains("truncated"),
        "unexpected error message for truncated EWKB: {err}"
    );
}

#[$test_attr]
fn st_x_y() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_Point(3.0, 4.0))");
    let y = db.query_f64("SELECT ST_Y(ST_Point(3.0, 4.0))");
    assert!((x - 3.0).abs() < 1e-10, "x = {x}");
    assert!((y - 4.0).abs() < 1e-10, "y = {y}");
}

#[$test_attr]
fn st_x_y_point_empty_returns_null() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_X(ST_GeomFromText('POINT EMPTY'))"));
    assert!(db.query_is_null("SELECT ST_Y(ST_GeomFromText('POINT EMPTY'))"));
}

#[$test_attr]
fn st_is_empty() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('POINT(0 0)'))");
    assert_eq!(e, 0);
}

#[$test_attr]
fn st_ndims() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NDims(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_coord_dim() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_CoordDim(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_zmflag() {
    let db = ActiveTestDb::open();
    let z = db.query_i64("SELECT ST_Zmflag(ST_GeomFromText('POINT(1 2)'))");
    assert_eq!(z, 0);
}

#[$test_attr]
fn st_mem_size() {
    let db = ActiveTestDb::open();
    let s = db.query_i64("SELECT ST_MemSize(ST_GeomFromText('POINT(1 2)'))");
    assert!(s > 0, "mem_size = {s}");
}

#[$test_attr]
fn st_num_points() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'))");
    assert_eq!(n, 3);
}

#[$test_attr]
fn st_npoints() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NPoints(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(n, 5);
}

#[$test_attr]
fn st_num_geometries() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Collect(ST_Point(0,0), ST_Point(1,1)))");
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_num_interior_rings() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumInteriorRings(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 1);
}

#[$test_attr]
fn st_num_rings() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumRings(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(n, 1);
}

#[$test_attr]
fn st_point_n() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_PointN(ST_GeomFromText('LINESTRING(10 20,30 40)'), 2))");
    assert!((x - 30.0).abs() < 1e-10, "x = {x}");
}

#[$test_attr]
fn st_start_point() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_StartPoint(ST_GeomFromText('LINESTRING(10 20,30 40)')))");
    assert!((x - 10.0).abs() < 1e-10, "x = {x}");
}

#[$test_attr]
fn st_end_point() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_EndPoint(ST_GeomFromText('LINESTRING(10 20,30 40)')))");
    assert!((x - 30.0).abs() < 1e-10, "x = {x}");
}

#[$test_attr]
fn st_exterior_ring() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_ExteriorRing(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')))",
    );
    assert_eq!(n, 5);
}

#[$test_attr]
fn st_interior_ring_n() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_InteriorRingN(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'), 1))",
    );
    assert_eq!(n, 5);
}

#[$test_attr]
fn st_geometry_n() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_GeometryN(ST_Collect(ST_Point(5,6), ST_Point(7,8)), 1))");
    assert!((x - 5.0).abs() < 1e-10, "x = {x}");
}

#[$test_attr]
fn st_dimension() {
    let db = ActiveTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(d, 2);
}

#[$test_attr]
fn st_envelope() {
    let db = ActiveTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,2 3)')))");
    assert!((area - 6.0).abs() < 1e-10, "area = {area}");
}

#[$test_attr]
fn st_is_valid() {
    let db = ActiveTestDb::open();
    let v = db.query_i64("SELECT ST_IsValid(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_is_valid_reason() {
    let db = ActiveTestDb::open();
    let r =
        db.query_text("SELECT ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert_eq!(r, "Valid Geometry");
}

// ── Measurement ───────────────────────────────────────────────────────────────

#[$test_attr]
fn st_area_unit_square() {
    let db = ActiveTestDb::open();
    let area = db.query_f64("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((area - 1.0).abs() < 1e-10, "area = {area}");
}

#[$test_attr]
fn st_distance_3_4_5() {
    let db = ActiveTestDb::open();
    let d = db.query_f64("SELECT ST_Distance(ST_Point(0,0), ST_Point(3,4))");
    assert!((d - 5.0).abs() < 1e-10, "distance = {d}");
}

#[$test_attr]
fn st_centroid_square() {
    let db = ActiveTestDb::open();
    let cx =
        db.query_f64("SELECT ST_X(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')))");
    let cy =
        db.query_f64("SELECT ST_Y(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')))");
    assert!((cx - 1.0).abs() < 1e-10, "cx = {cx}");
    assert!((cy - 1.0).abs() < 1e-10, "cy = {cy}");
}

#[$test_attr]
fn st_bbox() {
    let db = ActiveTestDb::open();
    let xmin = db.query_f64("SELECT ST_XMin(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let xmax = db.query_f64("SELECT ST_XMax(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let ymin = db.query_f64("SELECT ST_YMin(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    let ymax = db.query_f64("SELECT ST_YMax(ST_GeomFromText('POLYGON((1 2,3 2,3 4,1 4,1 2))'))");
    assert!((xmin - 1.0).abs() < 1e-10);
    assert!((xmax - 3.0).abs() < 1e-10);
    assert!((ymin - 2.0).abs() < 1e-10);
    assert!((ymax - 4.0).abs() < 1e-10);
}

#[$test_attr]
fn st_length() {
    let db = ActiveTestDb::open();
    let l = db.query_f64("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0,3 4)'))");
    assert!((l - 5.0).abs() < 1e-10, "length = {l}");
}

#[$test_attr]
fn st_perimeter() {
    let db = ActiveTestDb::open();
    let p = db.query_f64("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((p - 4.0).abs() < 1e-10, "perimeter = {p}");
}

#[$test_attr]
fn st_point_on_surface() {
    let db = ActiveTestDb::open();
    let c = db.query_i64(
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_PointOnSurface(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')))",
    );
    assert_eq!(c, 1);
}

#[$test_attr]
fn st_hausdorff_distance() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_HausdorffDistance(ST_GeomFromText('LINESTRING(0 0,1 0)'), ST_GeomFromText('LINESTRING(0 1,1 1)'))",
    );
    assert!((d - 1.0).abs() < 1e-10, "hausdorff = {d}");
}

#[$test_attr]
fn st_distance_sphere() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_DistanceSphere(ST_Point(-0.1278, 51.5074, 4326), ST_Point(2.3522, 48.8566, 4326))",
    );
    assert!(d > 300_000.0 && d < 400_000.0, "distance_sphere = {d}");
}

#[$test_attr]
fn st_distance_spheroid() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_DistanceSpheroid(ST_Point(-0.1278, 51.5074, 4326), ST_Point(2.3522, 48.8566, 4326))",
    );
    assert!(d > 300_000.0 && d < 400_000.0, "distance_spheroid = {d}");
}

#[$test_attr]
fn st_length_sphere() {
    let db = ActiveTestDb::open();
    let l = db.query_f64(
        "SELECT ST_LengthSphere(ST_GeomFromText('LINESTRING(-0.1278 51.5074, 2.3522 48.8566)', 4326))",
    );
    assert!(l > 300_000.0, "length_sphere = {l}");
}

#[$test_attr]
fn st_azimuth() {
    let db = ActiveTestDb::open();
    let a = db.query_f64("SELECT ST_Azimuth(ST_Point(0,0,4326), ST_Point(0,1,4326))");
    assert!(a.abs() < 1e-6, "azimuth = {a}");
}

#[$test_attr]
fn st_project() {
    let db = ActiveTestDb::open();
    let y = db.query_f64("SELECT ST_Y(ST_Project(ST_Point(0,0,4326), 111000.0, 0.0))");
    assert!((y - 1.0).abs() < 0.1, "y = {y}");
}

#[$test_attr]
fn st_distance_sphere_requires_explicit_4326_srid() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_DistanceSphere(ST_Point(0,0), ST_Point(1,1))")
        .expect_err("SRID-less geodesic distance should error");
    assert!(err.contains("requires SRID 4326"), "unexpected error: {err}");
}

#[$test_attr]
fn st_distance_spheroid_requires_explicit_4326_srid() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_DistanceSpheroid(ST_Point(0,0), ST_Point(1,1))")
        .expect_err("SRID-less geodesic distance should error");
    assert!(err.contains("requires SRID 4326"), "unexpected error: {err}");
}

#[$test_attr]
fn st_length_sphere_requires_explicit_4326_srid() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_LengthSphere(ST_GeomFromText('LINESTRING(0 0,1 1)'))")
        .expect_err("SRID-less geodesic length should error");
    assert!(err.contains("requires SRID 4326"), "unexpected error: {err}");
}

#[$test_attr]
fn st_azimuth_requires_explicit_4326_srid() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Azimuth(ST_Point(0,0), ST_Point(0,1))")
        .expect_err("SRID-less geodesic azimuth should error");
    assert!(err.contains("requires SRID 4326"), "unexpected error: {err}");
}

#[$test_attr]
fn st_project_requires_explicit_4326_srid() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Project(ST_Point(0,0), 111000.0, 0.0)")
        .expect_err("SRID-less geodesic projection should error");
    assert!(err.contains("requires SRID 4326"), "unexpected error: {err}");
}

#[$test_attr]
fn st_distance_empty_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Distance(ST_GeomFromText('POINT EMPTY'), ST_Point(0,0))")
        .expect_err("empty point should be rejected");
    assert!(
        err.contains("does not accept empty geometries"),
        "unexpected error: {err}"
    );
}

#[$test_attr]
fn st_distance_sphere_empty_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64(
            "SELECT ST_DistanceSphere(ST_GeomFromText('POINT EMPTY', 4326), ST_Point(0,0,4326))",
        )
        .expect_err("empty point should be rejected");
    assert!(err.contains("does not accept empty points"), "unexpected error: {err}");
}

#[$test_attr]
fn st_distance_spheroid_empty_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64(
            "SELECT ST_DistanceSpheroid(ST_GeomFromText('POINT EMPTY', 4326), ST_Point(0,0,4326))",
        )
        .expect_err("empty point should be rejected");
    assert!(err.contains("does not accept empty points"), "unexpected error: {err}");
}

#[$test_attr]
fn st_azimuth_empty_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Azimuth(ST_GeomFromText('POINT EMPTY', 4326), ST_Point(0,1,4326))")
        .expect_err("empty point should be rejected");
    assert!(err.contains("does not accept empty points"), "unexpected error: {err}");
}

#[$test_attr]
fn st_project_empty_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Project(ST_GeomFromText('POINT EMPTY', 4326), 1000.0, 0.0)")
        .expect_err("empty point should be rejected");
    assert!(err.contains("does not accept empty points"), "unexpected error: {err}");
}

#[$test_attr]
fn st_closest_point() {
    let db = ActiveTestDb::open();
    let y = db.query_f64(
        "SELECT ST_Y(ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_Point(5,5)))",
    );
    assert!(y.abs() < 1e-10, "y = {y}");
}

#[$test_attr]
fn st_closest_point_empty_target_point_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64(
            "SELECT ST_X(ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('POINT EMPTY')))",
        )
        .expect_err("empty target point should be rejected");
    assert!(err.contains("does not accept empty points"), "unexpected error: {err}");
}

// ── Predicates ────────────────────────────────────────────────────────────────

#[$test_attr]
fn st_intersects() {
    let db = ActiveTestDb::open();
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

#[$test_attr]
fn st_contains() {
    let db = ActiveTestDb::open();
    let yes = db.query_i64(
        "SELECT ST_Contains(
            ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'),
            ST_Point(2,2)
         )",
    );
    assert_eq!(yes, 1);
}

#[$test_attr]
fn st_dwithin() {
    let db = ActiveTestDb::open();
    let yes = db.query_i64("SELECT ST_DWithin(ST_Point(0,0), ST_Point(3,4), 5.0)");
    assert_eq!(yes, 1);
    let no = db.query_i64("SELECT ST_DWithin(ST_Point(0,0), ST_Point(3,4), 4.9)");
    assert_eq!(no, 0);
}

#[$test_attr]
fn st_within() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Within(ST_Point(2,2), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_disjoint() {
    let db = ActiveTestDb::open();
    let v = db.query_i64("SELECT ST_Disjoint(ST_Point(0,0), ST_Point(10,10))");
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_covers() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Covers(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_Point(2,2))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_covered_by() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_CoveredBy(ST_Point(2,2), ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_equals() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('LINESTRING(1 1,0 0)'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_touches() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((1 0,2 0,2 1,1 1,1 0))'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_crosses() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Crosses(ST_GeomFromText('LINESTRING(-1 0.5,2 0.5)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_overlaps() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Overlaps(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'), ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))'))",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_relate() {
    let db = ActiveTestDb::open();
    let r = db.query_text("SELECT ST_Relate(ST_Point(0,0), ST_Point(0,0))");
    assert_eq!(r, "0FFFFFFF2");
}

#[$test_attr]
fn st_relate_pattern() {
    let db = ActiveTestDb::open();
    let v = db.query_i64(
        "SELECT ST_Relate(ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'), ST_Point(2,2), 'T*****FF*')",
    );
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_relate_match() {
    let db = ActiveTestDb::open();
    let v = db.query_i64("SELECT ST_RelateMatch('0FFFFFFF2', '0FFF*FFF2')");
    assert_eq!(v, 1);
}

#[$test_attr]
fn st_relate_pattern_invalid_pattern_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_Relate(ST_Point(0,0), ST_Point(0,0), 'INVALID')")
        .expect_err("invalid DE-9IM pattern should return an error");
    assert!(
        err.contains("invalid DE-9IM pattern"),
        "unexpected error message: {err}"
    );
}

#[$test_attr]
fn st_relate_match_invalid_pattern_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_RelateMatch('0FFFFFFF2', 'INVALID')")
        .expect_err("invalid DE-9IM pattern should return an error");
    assert!(
        err.contains("invalid DE-9IM pattern"),
        "unexpected error message: {err}"
    );
}

#[$test_attr]
fn st_relate_match_invalid_matrix_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_RelateMatch('INVALID', 'T*****FF*')")
        .expect_err("invalid DE-9IM matrix should return an error");
    assert!(
        err.contains("invalid DE-9IM matrix"),
        "unexpected error message: {err}"
    );
}

// ── Alias function tests ─────────────────────────────────────────────────────

#[$test_attr]
fn st_make_point_alias() {
    let db = ActiveTestDb::open();
    let x = db.query_f64("SELECT ST_X(ST_MakePoint(7, 8))");
    assert!((x - 7.0).abs() < 1e-10, "x = {x}");
}

#[$test_attr]
fn geometry_type_alias() {
    let db = ActiveTestDb::open();
    let t = db.query_text("SELECT GeometryType(ST_GeomFromText('LINESTRING(0 0,1 1)'))");
    assert_eq!(t, "ST_LineString");
}

#[$test_attr]
fn st_num_interior_ring_alias() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumInteriorRing(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 1);
}

#[$test_attr]
fn st_length2d_alias() {
    let db = ActiveTestDb::open();
    let l = db.query_f64("SELECT ST_Length2D(ST_GeomFromText('LINESTRING(0 0,3 4)'))");
    assert!((l - 5.0).abs() < 1e-10, "length2d = {l}");
}

#[$test_attr]
fn st_perimeter2d_alias() {
    let db = ActiveTestDb::open();
    let p =
        db.query_f64("SELECT ST_Perimeter2D(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'))");
    assert!((p - 4.0).abs() < 1e-10, "perimeter2d = {p}");
}

// ── NULL input handling tests ────────────────────────────────────────────────

#[$test_attr]
fn null_input_st_astext() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_AsText(NULL)"));
}

#[$test_attr]
fn null_input_st_geomfromtext() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromText(NULL)"));
}

#[$test_attr]
fn st_geomfromtext_invalid_utf8_errors() {
    let db = ActiveTestDb::open();
    let err = db
        .try_query_i64("SELECT ST_GeomFromText(CAST(X'80' AS TEXT)) IS NULL")
        .expect_err("invalid UTF-8 ST_GeomFromText input should be a hard error");
    assert!(err.contains("UTF-8"), "unexpected error message: {err}");
}

#[$test_attr]
fn null_input_st_area() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Area(NULL)"));
}

#[$test_attr]
fn null_input_st_distance() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Distance(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Distance(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_intersects() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Intersects(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Intersects(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_srid() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_SRID(NULL)"));
}

#[$test_attr]
fn null_input_st_x() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_X(NULL)"));
}

#[$test_attr]
fn null_input_st_geometrytype() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeometryType(NULL)"));
}

#[$test_attr]
fn null_input_st_isempty() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_IsEmpty(NULL)"));
}

#[$test_attr]
fn null_input_st_centroid() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Centroid(NULL)"));
}

#[$test_attr]
fn null_input_st_geomfromgeojson() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromGeoJSON(NULL)"));
}

#[$test_attr]
fn null_input_st_relate() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Relate(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_relate_pattern() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Relate(NULL, ST_Point(0,0), 'T*****FF*')"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), NULL, 'T*****FF*')"));
    assert!(db.query_is_null("SELECT ST_Relate(ST_Point(0,0), ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_relatematch() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_RelateMatch(NULL, '0FFF*FFF2')"));
    assert!(db.query_is_null("SELECT ST_RelateMatch('0FFFFFFF2', NULL)"));
}

#[$test_attr]
fn null_input_st_closestpoint() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_ClosestPoint(NULL, ST_Point(0,0))"));
    assert!(
        db.query_is_null("SELECT ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0,1 1)'), NULL)")
    );
}

#[$test_attr]
fn null_input_st_makeline() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_MakeLine(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_MakeLine(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_collect() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Collect(NULL, ST_Point(0,0))"));
    assert!(db.query_is_null("SELECT ST_Collect(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_setsrid() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_SetSRID(NULL, 4326)"));
}

#[$test_attr]
fn null_numeric_arg_st_setsrid_returns_null() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_SetSRID(ST_Point(0,0), NULL)"));
}

#[$test_attr]
fn null_input_st_pointn() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_PointN(NULL, 1)"));
}

#[$test_attr]
fn null_input_st_geometryn() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeometryN(NULL, 1)"));
}

#[$test_attr]
fn null_input_st_interiorringn() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_InteriorRingN(NULL, 1)"));
}

#[$test_attr]
fn null_input_st_project() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_Project(NULL, 100.0, 0.0)"));
}

#[$test_attr]
fn null_input_st_dwithin() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_DWithin(NULL, ST_Point(0,0), 5.0)"));
    assert!(db.query_is_null("SELECT ST_DWithin(ST_Point(0,0), NULL, 5.0)"));
}

#[$test_attr]
fn null_input_st_geomfromwkb() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromWKB(NULL)"));
}

#[$test_attr]
fn null_input_st_geomfromewkb() {
    let db = ActiveTestDb::open();
    assert!(db.query_is_null("SELECT ST_GeomFromEWKB(NULL)"));
}

#[$test_attr]
fn empty_blob_input_reports_error_not_null() {
    let db = ActiveTestDb::open();
    let res = db.try_query_i64("SELECT ST_IsEmpty(X'')");
    assert!(res.is_err(), "empty blob should be rejected, got: {res:?}");
}

// ── Multi-geometry tests ─────────────────────────────────────────────────────

#[$test_attr]
fn st_npoints_multipoint() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NPoints(ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'))");
    assert_eq!(n, 3);
}

#[$test_attr]
fn st_npoints_multilinestring() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 4))'))",
    );
    assert_eq!(n, 5); // 2 + 3
}

#[$test_attr]
fn st_npoints_multipolygon() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'))",
    );
    assert_eq!(n, 10); // 5 + 5
}

#[$test_attr]
fn st_npoints_geometrycollection() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NPoints(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))'))",
    );
    assert_eq!(n, 3); // 1 + 2
}

#[$test_attr]
fn st_num_geometries_multipoint() {
    let db = ActiveTestDb::open();
    let n =
        db.query_i64("SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'))");
    assert_eq!(n, 3);
}

#[$test_attr]
fn st_num_geometries_multilinestring() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumGeometries(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))",
    );
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_num_geometries_multipolygon() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'))",
    );
    assert_eq!(n, 2);
}

#[$test_attr]
fn st_num_geometries_single_point() {
    let db = ActiveTestDb::open();
    let n = db.query_i64("SELECT ST_NumGeometries(ST_Point(1, 2))");
    assert_eq!(n, 1);
}

#[$test_attr]
fn st_geometry_n_multilinestring() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumPoints(ST_GeometryN(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 4))'), 2))",
    );
    assert_eq!(n, 3);
}

#[$test_attr]
fn st_geometry_n_multipolygon() {
    let db = ActiveTestDb::open();
    let t = db.query_text(
        "SELECT ST_GeometryType(ST_GeometryN(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'), 1))",
    );
    assert_eq!(t, "ST_Polygon");
}

#[$test_attr]
fn st_is_empty_linestring() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_polygon() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('POLYGON EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_multipoint() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTIPOINT EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_multilinestring() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTILINESTRING EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_multipolygon() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTIPOLYGON EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_geometrycollection() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_is_empty_collections_with_only_empty_members() {
    let db = ActiveTestDb::open();
    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTILINESTRING(EMPTY,EMPTY)'))");
    assert_eq!(e, 1);

    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('MULTIPOLYGON(EMPTY)'))");
    assert_eq!(e, 1);

    let e = db.query_i64("SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION(LINESTRING EMPTY)'))");
    assert_eq!(e, 1);
}

#[$test_attr]
fn st_perimeter_multipolygon() {
    let db = ActiveTestDb::open();
    let p = db.query_f64(
        "SELECT ST_Perimeter(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,4 2,4 4,2 4,2 2)))'))",
    );
    // First polygon perimeter = 4.0, second = 8.0, total = 12.0
    assert!((p - 12.0).abs() < 1e-10, "perimeter = {p}");
}

// ── Mixed-type distance tests ────────────────────────────────────────────────

#[$test_attr]
fn st_distance_point_to_linestring() {
    let db = ActiveTestDb::open();
    let d =
        db.query_f64("SELECT ST_Distance(ST_Point(0,5), ST_GeomFromText('LINESTRING(0 0,10 0)'))");
    assert!((d - 5.0).abs() < 1e-10, "distance = {d}");
}

#[$test_attr]
fn st_distance_point_to_polygon() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_Point(0,5), ST_GeomFromText('POLYGON((1 0,3 0,3 2,1 2,1 0))'))",
    );
    // Point (0,5) to nearest point on polygon border
    assert!(d > 0.0, "distance = {d}");
}

#[$test_attr]
fn st_distance_linestring_to_linestring() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('LINESTRING(0 0,10 0)'), ST_GeomFromText('LINESTRING(0 3,10 3)'))",
    );
    assert!((d - 3.0).abs() < 1e-10, "distance = {d}");
}

#[$test_attr]
fn st_distance_linestring_to_polygon() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('LINESTRING(0 5,10 5)'), ST_GeomFromText('POLYGON((0 0,10 0,10 2,0 2,0 0))'))",
    );
    assert!((d - 3.0).abs() < 1e-10, "distance = {d}");
}

#[$test_attr]
fn st_distance_polygon_to_polygon() {
    let db = ActiveTestDb::open();
    let d = db.query_f64(
        "SELECT ST_Distance(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'), ST_GeomFromText('POLYGON((3 0,4 0,4 1,3 1,3 0))'))",
    );
    assert!((d - 2.0).abs() < 1e-10, "distance = {d}");
}

// ── Validity edge cases ──────────────────────────────────────────────────────

#[$test_attr]
fn st_is_valid_invalid_polygon() {
    let db = ActiveTestDb::open();
    // Bowtie / self-intersecting polygon
    let v = db.query_i64("SELECT ST_IsValid(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))'))");
    assert_eq!(v, 0);
}

#[$test_attr]
fn st_is_valid_reason_invalid_polygon() {
    let db = ActiveTestDb::open();
    let r =
        db.query_text("SELECT ST_IsValidReason(ST_GeomFromText('POLYGON((0 0,2 2,2 0,0 2,0 0))'))");
    // Should return something other than "Valid Geometry"
    assert_ne!(r, "Valid Geometry", "got: {r}");
}

// ── MultiLineString spherical length ─────────────────────────────────────────

#[$test_attr]
fn st_length_sphere_multilinestring() {
    let db = ActiveTestDb::open();
    let l = db.query_f64(
        "SELECT ST_LengthSphere(ST_GeomFromText('MULTILINESTRING((-0.1278 51.5074, 2.3522 48.8566),(2.3522 48.8566, 13.4050 52.5200))', 4326))",
    );
    // London→Paris + Paris→Berlin, should be > 600km
    assert!(l > 600_000.0, "length_sphere = {l}");
}

// ── MultiLineString planar length ────────────────────────────────────────────

#[$test_attr]
fn st_length_multilinestring() {
    let db = ActiveTestDb::open();
    let l =
        db.query_f64("SELECT ST_Length(ST_GeomFromText('MULTILINESTRING((0 0,3 4),(10 0,10 5))'))");
    // sqrt(9+16)=5 + 5=10, total=10
    assert!((l - 10.0).abs() < 1e-10, "length = {l}");
}

// ── Dimension for various types ──────────────────────────────────────────────

#[$test_attr]
fn st_dimension_point() {
    let db = ActiveTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_Point(0, 0))");
    assert_eq!(d, 0);
}

#[$test_attr]
fn st_dimension_linestring() {
    let db = ActiveTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0,1 1)'))");
    assert_eq!(d, 1);
}

#[$test_attr]
fn st_dimension_multipoint() {
    let db = ActiveTestDb::open();
    let d = db.query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTIPOINT((0 0),(1 1))'))");
    assert_eq!(d, 0);
}

#[$test_attr]
fn st_dimension_multilinestring() {
    let db = ActiveTestDb::open();
    let d = db
        .query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))");
    assert_eq!(d, 1);
}

#[$test_attr]
fn st_dimension_multipolygon() {
    let db = ActiveTestDb::open();
    let d = db
        .query_i64("SELECT ST_Dimension(ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))'))");
    assert_eq!(d, 2);
}

// ── Centroid of a LineString ─────────────────────────────────────────────────

#[$test_attr]
fn st_centroid_linestring() {
    let db = ActiveTestDb::open();
    let cx = db.query_f64("SELECT ST_X(ST_Centroid(ST_GeomFromText('LINESTRING(0 0,10 0)')))");
    assert!((cx - 5.0).abs() < 1e-10, "cx = {cx}");
}

// ── Num rings with holes ─────────────────────────────────────────────────────

#[$test_attr]
fn st_num_rings_with_hole() {
    let db = ActiveTestDb::open();
    let n = db.query_i64(
        "SELECT ST_NumRings(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1))'))",
    );
    assert_eq!(n, 2); // exterior + 1 interior
}

// ── Spatial Index tests ──────────────────────────────────────────────────────

#[$test_attr]
fn spatial_index_create_query_drop() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE places (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec(
        "INSERT INTO places (geom) VALUES (ST_GeomFromText('POINT(1 2)')),\
         (ST_GeomFromText('POINT(3 4)')),\
         (ST_GeomFromText('POINT(5 6)'))",
    );

    // Create the spatial index
    let rc = db.query_i64("SELECT CreateSpatialIndex('places', 'geom')");
    assert_eq!(rc, 1);

    // R-tree should have 3 entries
    let count = db.query_i64("SELECT COUNT(*) FROM places_geom_rtree");
    assert_eq!(count, 3);

    // Query the R-tree directly
    let hits = db.query_all_i64(
        "SELECT id FROM places_geom_rtree WHERE xmin >= 2 AND xmax <= 6 AND ymin >= 3 AND ymax <= 7",
    );
    assert_eq!(hits.len(), 2); // POINT(3,4) and POINT(5,6)

    // Drop the spatial index
    let rc = db.query_i64("SELECT DropSpatialIndex('places', 'geom')");
    assert_eq!(rc, 1);

    // R-tree table should be gone
    let count = db.query_i64("SELECT COUNT(*) FROM sqlite_master WHERE name = 'places_geom_rtree'");
    assert_eq!(count, 0);
}

#[$test_attr]
fn spatial_index_create_idempotent() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE pts (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec(
        "INSERT INTO pts (geom) VALUES (ST_Point(1, 2)), (ST_Point(3, 4)), (ST_Point(5, 6))",
    );

    let rc = db.query_i64("SELECT CreateSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);
    let rc = db.query_i64("SELECT CreateSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);

    // No duplicate rows after repeated create.
    let count = db.query_i64("SELECT COUNT(*) FROM pts_geom_rtree");
    assert_eq!(count, 3);
}

#[$test_attr]
fn spatial_index_create_rolls_back_when_population_fails() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE broken (id INTEGER PRIMARY KEY, geom INTEGER)");
    db.exec("INSERT INTO broken (geom) VALUES (42)");

    let err = db
        .try_query_i64("SELECT CreateSpatialIndex('broken', 'geom')")
        .expect_err("index creation should fail for invalid geometry payloads");
    assert!(
        err.contains("invalid EWKB"),
        "unexpected error message: {err}"
    );
    assert!(
        !err.to_ascii_uppercase().contains("ROLLBACK"),
        "original populate error should not be overwritten by rollback errors: {err}"
    );

    let rtree_exists = db.query_i64(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'broken_geom_rtree'",
    );
    assert_eq!(rtree_exists, 0, "failed create should not leave rtree table");

    let trigger_count = db.query_i64(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'broken_geom_%'",
    );
    assert_eq!(trigger_count, 0, "failed create should not leave triggers");
}

#[$test_attr]
fn spatial_index_drop_rolls_back_when_drop_table_fails() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE broken (id INTEGER PRIMARY KEY, geom BLOB)");

    // Simulate an unexpected schema shape: object exists with the expected rtree name,
    // but it is a VIEW, so DROP TABLE will fail after trigger drops.
    db.exec("CREATE VIEW broken_geom_rtree AS SELECT 1 AS id, 0.0 AS xmin, 0.0 AS xmax, 0.0 AS ymin, 0.0 AS ymax");
    db.exec("CREATE TRIGGER broken_geom_insert AFTER INSERT ON broken BEGIN SELECT 1; END");
    db.exec("CREATE TRIGGER broken_geom_update AFTER UPDATE OF geom ON broken BEGIN SELECT 1; END");
    db.exec("CREATE TRIGGER broken_geom_delete AFTER DELETE ON broken BEGIN SELECT 1; END");

    let err = db
        .try_query_i64("SELECT DropSpatialIndex('broken', 'geom')")
        .expect_err("dropping a view with DROP TABLE should fail");
    assert!(err.contains("DROP VIEW"), "unexpected error message: {err}");

    // Rollback should preserve pre-existing objects on failure.
    let trigger_count = db.query_i64(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'broken_geom_%'",
    );
    assert_eq!(trigger_count, 3, "all triggers should remain after rollback");

    let view_exists = db.query_i64(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'view' AND name = 'broken_geom_rtree'",
    );
    assert_eq!(view_exists, 1, "view should remain after rollback");
}

#[$test_attr]
fn spatial_index_rtree_plus_exact_predicate() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE polys (id INTEGER PRIMARY KEY, geom BLOB)");
    // Two overlapping squares and one far away
    db.exec(
        "INSERT INTO polys (geom) VALUES \
         (ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')),\
         (ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))')),\
         (ST_GeomFromText('POLYGON((10 10,11 10,11 11,10 11,10 10))'))",
    );
    db.exec("SELECT CreateSpatialIndex('polys', 'geom')");

    // Two-stage query: coarse R-tree filter + exact ST_Intersects refinement
    let hits = db.query_all_i64(
        "SELECT p.id FROM polys p \
         JOIN polys_geom_rtree r ON p.rowid = r.id \
         WHERE r.xmax >= 0.5 AND r.xmin <= 2.5 AND r.ymax >= 0.5 AND r.ymin <= 2.5 \
         AND ST_Intersects(p.geom, ST_MakeEnvelope(0.5, 0.5, 2.5, 2.5))",
    );
    assert_eq!(hits.len(), 2); // polys 1 and 2
}

#[$test_attr]
fn spatial_index_trigger_sync() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec("SELECT CreateSpatialIndex('t', 'geom')");

    // INSERT with non-NULL geom → appears in R-tree
    db.exec("INSERT INTO t (geom) VALUES (ST_Point(1, 2))");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 1);

    // INSERT with NULL geom → not in R-tree
    db.exec("INSERT INTO t (geom) VALUES (NULL)");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 1); // still 1

    // UPDATE geom → R-tree updated
    db.exec("UPDATE t SET geom = ST_Point(10, 20) WHERE id = 1");
    let xmin = db.query_f64("SELECT xmin FROM t_geom_rtree WHERE id = 1");
    assert!((xmin - 10.0).abs() < 1e-10, "xmin = {xmin}");

    // UPDATE geom to NULL → removed from R-tree
    db.exec("UPDATE t SET geom = NULL WHERE id = 1");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree WHERE id = 1");
    assert_eq!(count, 0);

    // UPDATE NULL → non-NULL → added to R-tree
    db.exec("UPDATE t SET geom = ST_Point(7, 8) WHERE id = 2");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree WHERE id = 2");
    assert_eq!(count, 1);

    // DELETE → removed from R-tree
    db.exec("DELETE FROM t WHERE id = 2");
    let count = db.query_i64("SELECT COUNT(*) FROM t_geom_rtree");
    assert_eq!(count, 0);
}

#[$test_attr]
fn spatial_index_narrows_candidates() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE grid (id INTEGER PRIMARY KEY, geom BLOB)");

    // Insert 100 points in a 10×10 grid: (0,0) through (9,9)
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

    // R-tree query for bbox [1.5,1.5 → 3.5,3.5] should return only 4 points: (2,2),(2,3),(3,2),(3,3)
    let rtree_hits = db.query_all_i64(
        "SELECT g.id FROM grid g \
         JOIN grid_geom_rtree r ON g.rowid = r.id \
         WHERE r.xmin >= 1.5 AND r.xmax <= 3.5 AND r.ymin >= 1.5 AND r.ymax <= 3.5",
    );
    assert_eq!(rtree_hits.len(), 4);
}

#[$test_attr]
fn spatial_index_ignores_empty_geometries() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE empties (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec(
        "INSERT INTO empties (id, geom) VALUES \
         (1, ST_Point(1, 2)), \
         (2, ST_GeomFromText('POLYGON EMPTY')), \
         (3, ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))",
    );

    let rc = db.query_i64("SELECT CreateSpatialIndex('empties', 'geom')");
    assert_eq!(rc, 1);

    let count = db.query_i64("SELECT COUNT(*) FROM empties_geom_rtree");
    assert_eq!(count, 1, "only non-empty geometries should be indexed");

    db.exec("UPDATE empties SET geom = ST_GeomFromText('POINT EMPTY') WHERE id = 1");
    let count = db.query_i64("SELECT COUNT(*) FROM empties_geom_rtree");
    assert_eq!(count, 0, "row should be removed when geometry becomes empty");

    db.exec("UPDATE empties SET geom = ST_Point(5, 6) WHERE id = 2");
    let count = db.query_i64("SELECT COUNT(*) FROM empties_geom_rtree");
    assert_eq!(count, 1, "row should be indexed when geometry becomes non-empty");
}

#[$test_attr]
fn spatial_index_rejects_invalid_names() {
    let db = ActiveTestDb::open();

    // SQL injection attempt
    let res = db.try_query_i64("SELECT CreateSpatialIndex('places; DROP TABLE x', 'geom')");
    assert!(res.is_err(), "should reject: {res:?}");

    // Empty name
    let res = db.try_query_i64("SELECT CreateSpatialIndex('', 'geom')");
    assert!(res.is_err(), "should reject empty: {res:?}");

    // Name with spaces
    let res = db.try_query_i64("SELECT CreateSpatialIndex('my table', 'geom')");
    assert!(res.is_err(), "should reject spaces: {res:?}");

    // DropSpatialIndex also validates
    let res = db.try_query_i64("SELECT DropSpatialIndex('ok', 'col name')");
    assert!(res.is_err(), "should reject spaces in col: {res:?}");
}

#[$test_attr]
fn spatial_index_drop_idempotent() {
    let db = ActiveTestDb::open();
    db.exec("CREATE TABLE pts (id INTEGER PRIMARY KEY, geom BLOB)");
    db.exec("SELECT CreateSpatialIndex('pts', 'geom')");

    let rc = db.query_i64("SELECT DropSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);

    // Second drop should also succeed (IF EXISTS)
    let rc = db.query_i64("SELECT DropSpatialIndex('pts', 'geom')");
    assert_eq!(rc, 1);
}
    };
}
