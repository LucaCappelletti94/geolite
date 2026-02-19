#![cfg(feature = "postgres")]
#![allow(dead_code)]

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Double, Integer, Nullable, Text};
use geolite_diesel::types::{Geography, Geometry};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;

// ── QueryableByName row types ────────────────────────────────────────────────

#[derive(QueryableByName, Debug)]
struct GeomRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Geometry>)]
    geom: Option<Vec<u8>>,
}

#[derive(QueryableByName, Debug)]
struct GeogRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Geography>)]
    geom: Option<Vec<u8>>,
}

#[derive(QueryableByName, Debug)]
struct GeoGeomRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Geometry>)]
    geom: Option<geo::Geometry<f64>>,
}

#[derive(QueryableByName, Debug)]
struct TextRow {
    #[diesel(sql_type = Nullable<Text>)]
    val: Option<String>,
}

#[derive(QueryableByName, Debug)]
struct IntRow {
    #[diesel(sql_type = Nullable<Integer>)]
    val: Option<i32>,
}

#[derive(QueryableByName, Debug)]
struct DoubleRow {
    #[diesel(sql_type = Nullable<Double>)]
    val: Option<f64>,
}

// ── Helper: start a PostGIS container and return (container, connection) ──────

async fn pg_conn(
    tag: &str,
) -> (
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    PgConnection,
) {
    let container = Postgres::default()
        .with_name("postgis/postgis")
        .with_tag(tag)
        .start()
        .await
        .expect("failed to start PostGIS container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    // PostGIS needs a moment; retry connection a few times.
    let mut conn = None;
    for _ in 0..30 {
        match PgConnection::establish(&url) {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    let mut conn = conn.expect("could not connect to PostGIS container");

    // Ensure PostGIS extension is loaded and create test table.
    sql_query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(&mut conn)
        .unwrap();
    sql_query(
        "CREATE TABLE t (
            id   SERIAL PRIMARY KEY,
            geom geometry,
            geog geography
        )",
    )
    .execute(&mut conn)
    .unwrap();

    (container, conn)
}

// ── Test macro: generate a module per PG version ─────────────────────────────

macro_rules! postgis_tests {
    ($mod_name:ident, $tag:expr) => {
        mod $mod_name {
            use super::*;

            // ── 1. Type roundtrips ───────────────────────────────────────

            #[tokio::test]
            async fn type_roundtrips() {
                let (_container, mut c) = pg_conn($tag).await;

                // Vec<u8> EWKB roundtrip — Geometry
                let ewkb = geolite_core::ewkb::write_ewkb(
                    &geo::Geometry::Point(geo::Point::new(1.0, 2.0)),
                    None,
                )
                .unwrap();

                sql_query("INSERT INTO t (id, geom) VALUES (1, $1)")
                    .bind::<Geometry, _>(&ewkb)
                    .execute(&mut c)
                    .unwrap();

                let row: GeomRow = sql_query("SELECT id, geom FROM t WHERE id = 1")
                    .get_result(&mut c)
                    .unwrap();
                assert_eq!(row.geom.unwrap(), ewkb);

                // Vec<u8> EWKB roundtrip — Geography
                let ewkb_geog = geolite_core::ewkb::write_ewkb(
                    &geo::Geometry::Point(geo::Point::new(13.4, 52.5)),
                    Some(4326),
                )
                .unwrap();

                sql_query("INSERT INTO t (id, geog) VALUES (2, $1)")
                    .bind::<Geography, _>(&ewkb_geog)
                    .execute(&mut c)
                    .unwrap();

                let row: GeogRow =
                    sql_query("SELECT id, geog AS geom FROM t WHERE id = 2")
                        .get_result(&mut c)
                        .unwrap();
                assert_eq!(row.geom.unwrap(), ewkb_geog);

                // geo::Geometry<f64> roundtrip — Geometry
                let point = geo::Geometry::Point(geo::Point::new(3.5, 7.25));

                sql_query("INSERT INTO t (id, geom) VALUES (3, $1)")
                    .bind::<Geometry, _>(&point)
                    .execute(&mut c)
                    .unwrap();

                let row: GeoGeomRow = sql_query("SELECT id, geom FROM t WHERE id = 3")
                    .get_result(&mut c)
                    .unwrap();
                match row.geom.unwrap() {
                    geo::Geometry::Point(p) => {
                        assert!((p.x() - 3.5).abs() < 1e-10);
                        assert!((p.y() - 7.25).abs() < 1e-10);
                    }
                    other => panic!("expected Point, got {other:?}"),
                }

                // geo::Geometry<f64> roundtrip — Geography (verifies SRID=4326)
                let geo_point = geo::Geometry::Point(geo::Point::new(13.4, 52.5));

                sql_query("INSERT INTO t (id, geog) VALUES (4, $1)")
                    .bind::<Geography, _>(&geo_point)
                    .execute(&mut c)
                    .unwrap();

                // Read back as raw bytes and verify SRID
                let row: GeogRow =
                    sql_query("SELECT id, geog AS geom FROM t WHERE id = 4")
                        .get_result(&mut c)
                        .unwrap();
                let blob = row.geom.unwrap();
                let (_geom, srid) = geolite_core::ewkb::parse_ewkb(&blob).unwrap();
                assert_eq!(srid, Some(4326));

                // Geometry ToSql writes no SRID
                let point_no_srid = geo::Geometry::Point(geo::Point::new(1.0, 2.0));
                sql_query("INSERT INTO t (id, geom) VALUES (5, $1)")
                    .bind::<Geometry, _>(&point_no_srid)
                    .execute(&mut c)
                    .unwrap();

                let row: GeomRow = sql_query("SELECT id, geom FROM t WHERE id = 5")
                    .get_result(&mut c)
                    .unwrap();
                let blob = row.geom.unwrap();
                let (_geom, srid) = geolite_core::ewkb::parse_ewkb(&blob).unwrap();
                assert_eq!(srid, None);

                // [u8] slice ToSql
                let ewkb_slice = geolite_core::ewkb::write_ewkb(
                    &geo::Geometry::Point(geo::Point::new(9.0, 10.0)),
                    None,
                )
                .unwrap();

                sql_query("INSERT INTO t (id, geom) VALUES (6, $1)")
                    .bind::<Geometry, _>(&ewkb_slice[..])
                    .execute(&mut c)
                    .unwrap();

                let row: GeomRow = sql_query("SELECT id, geom FROM t WHERE id = 6")
                    .get_result(&mut c)
                    .unwrap();
                assert_eq!(row.geom.unwrap(), ewkb_slice);

                // NULL handling
                sql_query("INSERT INTO t (id, geom) VALUES (7, NULL)")
                    .execute(&mut c)
                    .unwrap();

                let row: GeomRow = sql_query("SELECT id, geom FROM t WHERE id = 7")
                    .get_result(&mut c)
                    .unwrap();
                assert!(row.geom.is_none());
            }

            // ── 2. PostGIS I/O functions ─────────────────────────────────

            #[tokio::test]
            async fn postgis_io_functions() {
                let (_container, mut c) = pg_conn($tag).await;

                // ST_GeomFromText / ST_AsText roundtrip
                let row: TextRow = sql_query(
                    "SELECT ST_AsText(ST_GeomFromText('POINT(1 2)')) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "POINT(1 2)");

                // ST_GeomFromText with SRID
                let row: IntRow = sql_query(
                    "SELECT ST_SRID(ST_GeomFromText('POINT(1 2)', 4326)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), 4326);

                // ST_AsEWKT (verify SRID in output)
                let row: TextRow = sql_query(
                    "SELECT ST_AsEWKT(ST_GeomFromText('POINT(1 2)', 4326)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "SRID=4326;POINT(1 2)");

                // ST_AsGeoJSON / ST_GeomFromGeoJSON roundtrip
                let row: TextRow = sql_query(
                    "SELECT ST_AsText(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[1,2]}')) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "POINT(1 2)");

                // ST_Point constructor
                let row: TextRow =
                    sql_query("SELECT ST_AsText(ST_Point(3.0, 4.0)) AS val")
                        .get_result(&mut c)
                        .unwrap();
                assert_eq!(row.val.unwrap(), "POINT(3 4)");

                // ST_MakeEnvelope constructor
                let row: TextRow = sql_query(
                    "SELECT ST_AsText(ST_MakeEnvelope(0, 0, 1, 1)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(
                    row.val.unwrap(),
                    "POLYGON((0 0,0 1,1 1,1 0,0 0))"
                );
            }

            // ── 3. PostGIS accessor functions ────────────────────────────

            #[tokio::test]
            async fn postgis_accessor_functions() {
                let (_container, mut c) = pg_conn($tag).await;

                // ST_SRID / ST_SetSRID
                let row: IntRow = sql_query(
                    "SELECT ST_SRID(ST_SetSRID(ST_Point(0, 0), 4326)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), 4326);

                // ST_GeometryType
                let row: TextRow = sql_query(
                    "SELECT ST_GeometryType(ST_Point(0, 0)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "ST_Point");

                // ST_X / ST_Y
                let row: DoubleRow =
                    sql_query("SELECT ST_X(ST_Point(3.5, 7.25)) AS val")
                        .get_result(&mut c)
                        .unwrap();
                assert!((row.val.unwrap() - 3.5).abs() < 1e-10);

                let row: DoubleRow =
                    sql_query("SELECT ST_Y(ST_Point(3.5, 7.25)) AS val")
                        .get_result(&mut c)
                        .unwrap();
                assert!((row.val.unwrap() - 7.25).abs() < 1e-10);

                // ST_Area (polygon)
                let row: DoubleRow = sql_query(
                    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 1.0).abs() < 1e-10);

                // ST_Distance (two points)
                let row: DoubleRow = sql_query(
                    "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 5.0).abs() < 1e-10);

                // ST_Length (linestring)
                let row: DoubleRow = sql_query(
                    "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)')) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 5.0).abs() < 1e-10);

                // ST_Centroid
                let row: TextRow = sql_query(
                    "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'))) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "POINT(1 1)");

                // ST_Buffer (basic check: result is non-null and a polygon)
                let row: TextRow = sql_query(
                    "SELECT ST_GeometryType(ST_Buffer(ST_Point(0, 0), 1.0)) AS val",
                )
                .get_result(&mut c)
                .unwrap();
                assert_eq!(row.val.unwrap(), "ST_Polygon");
            }

            // ── 4. PostGIS spatial operations ────────────────────────────

            #[tokio::test]
            async fn postgis_spatial_operations() {
                let (_container, mut c) = pg_conn($tag).await;

                let poly_a = "ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')";
                let poly_b = "ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))')";

                // ST_Union — area should be 7.0 (4 + 4 - 1 overlap)
                let row: DoubleRow = sql_query(&format!(
                    "SELECT ST_Area(ST_Union({poly_a}, {poly_b})) AS val"
                ))
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 7.0).abs() < 1e-10);

                // ST_Intersection — area should be 1.0
                let row: DoubleRow = sql_query(&format!(
                    "SELECT ST_Area(ST_Intersection({poly_a}, {poly_b})) AS val"
                ))
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 1.0).abs() < 1e-10);

                // ST_Difference (A - B) — area should be 3.0
                let row: DoubleRow = sql_query(&format!(
                    "SELECT ST_Area(ST_Difference({poly_a}, {poly_b})) AS val"
                ))
                .get_result(&mut c)
                .unwrap();
                assert!((row.val.unwrap() - 3.0).abs() < 1e-10);
            }
        }
    };
}

postgis_tests!(pg15, "15-3.5");
postgis_tests!(pg16, "16-3.5");
postgis_tests!(pg17, "17-3.5");
