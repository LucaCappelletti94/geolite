//! Extension trait for method-style spatial operations on geometry expressions.
//!
//! Import [`GeometryExpressionMethods`] (or `use geolite_diesel::prelude::*`)
//! to call spatial functions as methods on any `Nullable<Geometry>` expression:
//!
//! ```rust,ignore
//! use geolite_diesel::prelude::*;
//!
//! features::table
//!     .filter(features::geom.st_dwithin(st_point(13.4050, 52.5200), 1000.0))
//!     .select((features::id, features::geom.st_astext()))
//!     .load(&mut conn)?;
//! ```

use diesel::expression::{AsExpression, Expression};
use diesel::sql_types::{Double, Integer, Nullable};

use crate::functions;
use crate::types::Geometry;

/// Method-style access to spatial SQL functions for `Nullable<Geometry>` expressions.
///
/// This trait is automatically implemented for any Diesel expression with
/// `SqlType = Nullable<Geometry>`. Each method delegates to the corresponding
/// free function in [`crate::functions`].
///
/// For non-nullable `Geometry` columns, call `.nullable()` first — this is
/// the standard Diesel pattern.
pub trait GeometryExpressionMethods: Expression<SqlType = Nullable<Geometry>> + Sized {
    // ── I/O ─────────────────────────────────────────────────────────────

    /// Serialize this geometry to WKT text.
    fn st_astext(self) -> functions::st_astext<Self> {
        functions::st_astext(self)
    }

    /// Serialize this geometry to EWKT text (`SRID=n;WKT`).
    fn st_asewkt(self) -> functions::st_asewkt<Self> {
        functions::st_asewkt(self)
    }

    /// Serialize this geometry to ISO WKB bytes.
    fn st_asbinary(self) -> functions::st_asbinary<Self> {
        functions::st_asbinary(self)
    }

    /// Serialize this geometry to GeoJSON text.
    fn st_asgeojson(self) -> functions::st_asgeojson<Self> {
        functions::st_asgeojson(self)
    }

    // ── Accessors ───────────────────────────────────────────────────────

    /// Return the SRID embedded in the geometry EWKB header.
    fn st_srid(self) -> functions::st_srid<Self> {
        functions::st_srid(self)
    }

    /// Set (replace) the SRID in the geometry EWKB header.
    fn st_setsrid<S>(self, srid: S) -> functions::st_setsrid<Self, S>
    where
        S: AsExpression<Integer>,
    {
        functions::st_setsrid(self, srid)
    }

    /// Return the OGC geometry type name (e.g. `ST_Point`, `ST_Polygon`).
    fn st_geometrytype(self) -> functions::st_geometrytype<Self> {
        functions::st_geometrytype(self)
    }

    /// Return the X coordinate of a Point geometry.
    fn st_x(self) -> functions::st_x<Self> {
        functions::st_x(self)
    }

    /// Return the Y coordinate of a Point geometry.
    fn st_y(self) -> functions::st_y<Self> {
        functions::st_y(self)
    }

    /// Return 1 if the geometry is empty, 0 otherwise.
    fn st_isempty(self) -> functions::st_isempty<Self> {
        functions::st_isempty(self)
    }

    /// Return the X coordinate of the bounding-box minimum corner.
    fn st_xmin(self) -> functions::st_xmin<Self> {
        functions::st_xmin(self)
    }

    /// Return the X coordinate of the bounding-box maximum corner.
    fn st_xmax(self) -> functions::st_xmax<Self> {
        functions::st_xmax(self)
    }

    /// Return the Y coordinate of the bounding-box minimum corner.
    fn st_ymin(self) -> functions::st_ymin<Self> {
        functions::st_ymin(self)
    }

    /// Return the Y coordinate of the bounding-box maximum corner.
    fn st_ymax(self) -> functions::st_ymax<Self> {
        functions::st_ymax(self)
    }

    // ── Measurement ─────────────────────────────────────────────────────

    /// Return the planar area of a polygon geometry.
    fn st_area(self) -> functions::st_area<Self> {
        functions::st_area(self)
    }

    /// Return the planar length of a linestring geometry.
    fn st_length(self) -> functions::st_length<Self> {
        functions::st_length(self)
    }

    /// Return the planar perimeter of a polygon geometry.
    fn st_perimeter(self) -> functions::st_perimeter<Self> {
        functions::st_perimeter(self)
    }

    /// Return the minimum Euclidean distance to another geometry.
    fn st_distance<T>(self, other: T) -> functions::st_distance<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_distance(self, other)
    }

    /// Return the Haversine (spherical) distance in metres to another geometry.
    fn st_distancesphere<T>(self, other: T) -> functions::st_distancesphere<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_distancesphere(self, other)
    }

    /// Return the geodesic distance in metres to another geometry (Karney algorithm).
    fn st_distancespheroid<T>(self, other: T) -> functions::st_distancespheroid<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_distancespheroid(self, other)
    }

    /// Return the Hausdorff distance to another geometry.
    fn st_hausdorffdistance<T>(self, other: T) -> functions::st_hausdorffdistance<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_hausdorffdistance(self, other)
    }

    /// Return the centroid of this geometry.
    fn st_centroid(self) -> functions::st_centroid<Self> {
        functions::st_centroid(self)
    }

    /// Return a point guaranteed to lie on or inside this geometry.
    fn st_pointonsurface(self) -> functions::st_pointonsurface<Self> {
        functions::st_pointonsurface(self)
    }

    // ── Predicates ──────────────────────────────────────────────────────

    /// Return 1 if this geometry shares any interior or boundary points with another.
    fn st_intersects<T>(self, other: T) -> functions::st_intersects<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_intersects(self, other)
    }

    /// Return 1 if this geometry fully contains another.
    fn st_contains<T>(self, other: T) -> functions::st_contains<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_contains(self, other)
    }

    /// Return 1 if this geometry is fully contained within another.
    fn st_within<T>(self, other: T) -> functions::st_within<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_within(self, other)
    }

    /// Return 1 if this geometry covers another.
    fn st_covers<T>(self, other: T) -> functions::st_covers<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_covers(self, other)
    }

    /// Return 1 if this geometry is covered by another.
    fn st_coveredby<T>(self, other: T) -> functions::st_coveredby<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_coveredby(self, other)
    }

    /// Return 1 if this geometry shares no points with another.
    fn st_disjoint<T>(self, other: T) -> functions::st_disjoint<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_disjoint(self, other)
    }

    /// Return 1 if this geometry is spatially equal to another.
    fn st_equals<T>(self, other: T) -> functions::st_equals<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_equals(self, other)
    }

    /// Return 1 if this geometry and another are within the given Euclidean distance.
    fn st_dwithin<T, D>(self, other: T, distance: D) -> functions::st_dwithin<Self, T, D>
    where
        T: AsExpression<Nullable<Geometry>>,
        D: AsExpression<Double>,
    {
        functions::st_dwithin(self, other, distance)
    }

    /// Return the DE-9IM relationship matrix string between this and another geometry.
    fn st_relate<T>(self, other: T) -> functions::st_relate<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_relate(self, other)
    }

    // ── Geography variants ──────────────────────────────────────────────

    /// Haversine arc length of a linestring in metres.
    fn st_lengthsphere(self) -> functions::st_lengthsphere<Self> {
        functions::st_lengthsphere(self)
    }

    /// Geodesic bearing from this geometry to target in radians (0 = north, clockwise).
    fn st_azimuth<T>(self, target: T) -> functions::st_azimuth<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_azimuth(self, target)
    }

    /// Destination point from this geometry given distance (metres) and azimuth (radians).
    fn st_project<D, A>(self, distance: D, azimuth: A) -> functions::st_project<Self, D, A>
    where
        D: AsExpression<Double>,
        A: AsExpression<Double>,
    {
        functions::st_project(self, distance, azimuth)
    }

    /// Closest point on this geometry to another.
    fn st_closestpoint<T>(self, other: T) -> functions::st_closestpoint<Self, T>
    where
        T: AsExpression<Nullable<Geometry>>,
    {
        functions::st_closestpoint(self, other)
    }
}

impl<E> GeometryExpressionMethods for E where E: Expression<SqlType = Nullable<Geometry>> + Sized {}
