//! Diesel SQL function definitions for spatial operations.
//!
//! Import the functions you need and use them directly in Diesel query builder
//! expressions.
//!
//! # Example
//!
//! ```rust,ignore
//! use geolite_diesel::functions::*;
//! use diesel::prelude::*;
//!
//! let nearby: Vec<Feature> = features::table
//!     .filter(st_dwithin(features::geom, st_point(lon, lat), 1000.0))
//!     .load(&mut conn)?;
//! ```

use crate::types::Geometry;
use diesel::sql_types::{Double, Integer, Nullable, Text};

// ── I/O ───────────────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Parse WKT text into a geometry BLOB.
    fn st_geomfromtext(wkt: Text) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Parse WKT text with explicit SRID into a geometry BLOB.
    #[sql_name = "ST_GeomFromText"]
    fn st_geomfromtext_srid(wkt: Text, srid: Integer) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Serialize a geometry BLOB to WKT text.
    fn st_astext(geom: Nullable<Geometry>) -> Nullable<Text>;
}

diesel::define_sql_function! {
    /// Serialize a geometry BLOB to EWKT text (`SRID=n;WKT`).
    fn st_asewkt(geom: Nullable<Geometry>) -> Nullable<Text>;
}

diesel::define_sql_function! {
    /// Serialize a geometry BLOB to ISO WKB bytes (strips SRID).
    fn st_asbinary(geom: Nullable<Geometry>) -> Nullable<Binary>;
}

diesel::define_sql_function! {
    /// Parse ISO WKB bytes into a geometry BLOB.
    fn st_geomfromwkb(wkb: Geometry) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Serialize a geometry BLOB to GeoJSON text.
    fn st_asgeojson(geom: Nullable<Geometry>) -> Nullable<Text>;
}

diesel::define_sql_function! {
    /// Parse a GeoJSON string into a geometry BLOB.
    fn st_geomfromgeojson(json: Text) -> Nullable<Geometry>;
}

// ── Constructors ──────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Construct a Point geometry from X and Y coordinates.
    fn st_point(x: Double, y: Double) -> Geometry;
}

diesel::define_sql_function! {
    /// Construct a rectangular envelope polygon from corner coordinates.
    fn st_makeenvelope(xmin: Double, ymin: Double, xmax: Double, ymax: Double) -> Geometry;
}

diesel::define_sql_function! {
    /// Construct a web-mercator tile envelope for the given zoom/x/y.
    fn st_tileenvelope(zoom: Integer, x: Integer, y: Integer) -> Geometry;
}

// ── Accessors ─────────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Return the SRID embedded in the geometry EWKB header.
    fn st_srid(geom: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Set (replace) the SRID in the geometry EWKB header.
    fn st_setsrid(geom: Nullable<Geometry>, srid: Integer) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Return the OGC geometry type name (e.g. `ST_Point`, `ST_Polygon`).
    fn st_geometrytype(geom: Nullable<Geometry>) -> Nullable<Text>;
}

diesel::define_sql_function! {
    /// Return the X coordinate of a Point geometry.
    fn st_x(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the Y coordinate of a Point geometry.
    fn st_y(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return 1 if the geometry is empty, 0 otherwise.
    fn st_isempty(geom: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return the X coordinate of the bounding-box minimum corner.
    fn st_xmin(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the X coordinate of the bounding-box maximum corner.
    fn st_xmax(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the Y coordinate of the bounding-box minimum corner.
    fn st_ymin(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the Y coordinate of the bounding-box maximum corner.
    fn st_ymax(geom: Nullable<Geometry>) -> Nullable<Double>;
}

// ── Measurement ───────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Return the planar area of a polygon geometry.
    fn st_area(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the planar length of a linestring geometry.
    fn st_length(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the planar perimeter of a polygon geometry.
    fn st_perimeter(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the minimum Euclidean distance between two geometries.
    fn st_distance(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the Haversine (spherical) distance in metres between two points.
    fn st_distancesphere(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the geodesic distance in metres between two points (Karney algorithm).
    fn st_distancespheroid(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Return the centroid of a geometry.
    fn st_centroid(geom: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Return a point guaranteed to lie on or inside the geometry.
    fn st_pointonsurface(geom: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Return the Hausdorff distance between two geometries.
    fn st_hausdorffdistance(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Double>;
}

// ── Operations ────────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Compute the geometric union of two polygon geometries.
    fn st_union(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Compute the geometric intersection of two polygon geometries.
    fn st_intersection(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Compute the geometric difference (A minus B) of two polygon geometries.
    fn st_difference(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Compute the symmetric difference (XOR) of two polygon geometries.
    fn st_symdifference(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Expand or shrink a geometry by a given distance.
    fn st_buffer(geom: Nullable<Geometry>, distance: Double) -> Nullable<Geometry>;
}

// ── Predicates ────────────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Return 1 if geometries share any interior or boundary points.
    fn st_intersects(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometry A fully contains geometry B.
    fn st_contains(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometry A is fully contained within geometry B.
    fn st_within(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if A covers B (every point of B lies within A).
    fn st_covers(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if A is covered by B.
    fn st_coveredby(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometries share no points.
    fn st_disjoint(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometries are spatially equal.
    fn st_equals(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if A and B are within the given Euclidean distance.
    fn st_dwithin(a: Nullable<Geometry>, b: Nullable<Geometry>, distance: Double) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometries share boundary points but no interior points.
    fn st_touches(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometries cross each other (intersect with a lower-dimensional result).
    fn st_crosses(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return 1 if geometries overlap (same dimension, intersect but neither contains the other).
    fn st_overlaps(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Integer>;
}

diesel::define_sql_function! {
    /// Return the DE-9IM relationship matrix string between two geometries.
    fn st_relate(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Text>;
}

// ── Geography variants ────────────────────────────────────────────────────────

diesel::define_sql_function! {
    /// Haversine arc length of a linestring in metres.
    fn st_lengthsphere(geom: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Geodesic bearing from origin to target in radians (0 = north, clockwise).
    fn st_azimuth(origin: Nullable<Geometry>, target: Nullable<Geometry>) -> Nullable<Double>;
}

diesel::define_sql_function! {
    /// Destination point from origin given distance (metres) and azimuth (radians).
    fn st_project(origin: Nullable<Geometry>, distance: Double, azimuth: Double) -> Nullable<Geometry>;
}

diesel::define_sql_function! {
    /// Closest point on geometry A to point B.
    fn st_closestpoint(a: Nullable<Geometry>, b: Nullable<Geometry>) -> Nullable<Geometry>;
}
