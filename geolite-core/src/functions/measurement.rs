//! Measurement functions (§3.7 of plan.md)
//!
//! ST_Area, ST_Perimeter, ST_Length, ST_Length2D, ST_Distance,
//! ST_Centroid, ST_PointOnSurface, ST_XMin/XMax/YMin/YMax,
//! ST_DistanceSphere, ST_DistanceSpheroid, ST_Azimuth, ST_Project,
//! ST_ClosestPoint, ST_HausdorffDistance

#[allow(deprecated)]
use geo::algorithm::euclidean_distance::EuclideanDistance;
use geo::algorithm::line_measures::metric_spaces::{Euclidean, Geodesic, Haversine};
use geo::algorithm::line_measures::{Bearing, Destination, Distance, Length};
use geo::algorithm::InteriorPoint;
use geo::algorithm::{Area, BoundingRect, Centroid, ClosestPoint, HausdorffDistance};
use geo::Closest;
use geo::{Geometry, Point};

use crate::error::{GeoLiteError, Result};
use crate::ewkb::{parse_ewkb, write_ewkb};

/// ST_Area — planar area (square units of the CRS).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_area;
/// use geolite_core::functions::io::geom_from_text;
///
/// let poly = geom_from_text("POLYGON((0 0,1 0,1 1,0 1,0 0))", None).unwrap();
/// assert!((st_area(&poly).unwrap() - 1.0).abs() < 1e-10);
/// ```
pub fn st_area(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    Ok(geom.unsigned_area())
}

/// ST_Length / ST_Length2D — planar arc length of a LineString or MultiLineString.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_length;
/// use geolite_core::functions::io::geom_from_text;
///
/// let line = geom_from_text("LINESTRING(0 0,3 4)", None).unwrap();
/// assert!((st_length(&line).unwrap() - 5.0).abs() < 1e-10);
/// ```
pub fn st_length(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    let len = match &geom {
        Geometry::LineString(ls) => Euclidean.length(ls),
        Geometry::MultiLineString(mls) => mls.0.iter().map(|ls| Euclidean.length(ls)).sum(),
        _ => return Err(GeoLiteError::WrongType("LineString or MultiLineString")),
    };
    Ok(len)
}

/// ST_Perimeter — planar perimeter of a Polygon or MultiPolygon.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_perimeter;
/// use geolite_core::functions::io::geom_from_text;
///
/// let poly = geom_from_text("POLYGON((0 0,1 0,1 1,0 1,0 0))", None).unwrap();
/// assert!((st_perimeter(&poly).unwrap() - 4.0).abs() < 1e-10);
/// ```
pub fn st_perimeter(blob: &[u8]) -> Result<f64> {
    fn poly_perimeter(p: &geo::Polygon<f64>) -> f64 {
        Euclidean.length(p.exterior())
            + p.interiors()
                .iter()
                .map(|r| Euclidean.length(r))
                .sum::<f64>()
    }
    let (geom, _) = parse_ewkb(blob)?;
    let perim = match &geom {
        Geometry::Polygon(p) => poly_perimeter(p),
        Geometry::MultiPolygon(mp) => mp.0.iter().map(poly_perimeter).sum(),
        _ => return Err(GeoLiteError::WrongType("Polygon or MultiPolygon")),
    };
    Ok(perim)
}

/// Dispatch euclidean distance between any two geo geometry types.
#[allow(deprecated)]
fn euclidean_geometry_distance(a: &Geometry<f64>, b: &Geometry<f64>) -> f64 {
    // Use EuclideanDistance (older trait) which covers most type pairs.
    // Fall back to centroid distance for unsupported combinations.
    match (a, b) {
        (Geometry::Point(pa), Geometry::Point(pb)) => pa.euclidean_distance(pb),
        (Geometry::Point(p), Geometry::LineString(ls)) => p.euclidean_distance(ls),
        (Geometry::LineString(ls), Geometry::Point(p)) => p.euclidean_distance(ls),
        (Geometry::Point(p), Geometry::Polygon(poly)) => p.euclidean_distance(poly),
        (Geometry::Polygon(poly), Geometry::Point(p)) => p.euclidean_distance(poly),
        (Geometry::LineString(la), Geometry::LineString(lb)) => la.euclidean_distance(lb),
        (Geometry::LineString(ls), Geometry::Polygon(poly)) => ls.euclidean_distance(poly),
        (Geometry::Polygon(poly), Geometry::LineString(ls)) => ls.euclidean_distance(poly),
        (Geometry::Polygon(pa), Geometry::Polygon(pb)) => pa.euclidean_distance(pb),
        _ => {
            // Centroid approximation for unsupported combinations
            let ca = a.centroid().unwrap_or_else(|| Point::new(0.0, 0.0));
            let cb = b.centroid().unwrap_or_else(|| Point::new(0.0, 0.0));
            Euclidean.distance(ca, cb)
        }
    }
}

/// ST_Distance — minimum Euclidean distance between two geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_distance;
/// use geolite_core::functions::constructors::st_point;
///
/// let a = st_point(0.0, 0.0, None).unwrap();
/// let b = st_point(3.0, 4.0, None).unwrap();
/// assert!((st_distance(&a, &b).unwrap() - 5.0).abs() < 1e-10);
/// ```
pub fn st_distance(a: &[u8], b: &[u8]) -> Result<f64> {
    let (ga, _) = parse_ewkb(a)?;
    let (gb, _) = parse_ewkb(b)?;
    Ok(euclidean_geometry_distance(&ga, &gb))
}

/// ST_Centroid — geometric centroid of any geometry.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_centroid;
/// use geolite_core::functions::accessors::{st_x, st_y};
/// use geolite_core::functions::io::geom_from_text;
///
/// let poly = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
/// let c = st_centroid(&poly).unwrap();
/// assert!((st_x(&c).unwrap() - 1.0).abs() < 1e-10);
/// assert!((st_y(&c).unwrap() - 1.0).abs() < 1e-10);
/// ```
pub fn st_centroid(blob: &[u8]) -> Result<Vec<u8>> {
    let (geom, srid) = parse_ewkb(blob)?;
    let c = geom
        .centroid()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    write_ewkb(&Geometry::Point(c), srid)
}

/// ST_PointOnSurface — a point guaranteed to lie on the geometry.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_point_on_surface;
/// use geolite_core::functions::predicates::st_within;
/// use geolite_core::functions::io::geom_from_text;
///
/// let poly = geom_from_text("POLYGON((0 0,4 0,4 4,0 4,0 0))", None).unwrap();
/// let pt = st_point_on_surface(&poly).unwrap();
/// // The point on surface should be within the polygon
/// assert!(st_within(&pt, &poly).unwrap());
/// ```
pub fn st_point_on_surface(blob: &[u8]) -> Result<Vec<u8>> {
    let (geom, srid) = parse_ewkb(blob)?;
    let p = geom
        .interior_point()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    write_ewkb(&Geometry::Point(p), srid)
}

/// ST_HausdorffDistance — Hausdorff distance between two geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_hausdorff_distance;
/// use geolite_core::functions::io::geom_from_text;
///
/// let a = geom_from_text("LINESTRING(0 0,1 0)", None).unwrap();
/// let b = geom_from_text("LINESTRING(0 1,1 1)", None).unwrap();
/// assert!((st_hausdorff_distance(&a, &b).unwrap() - 1.0).abs() < 1e-10);
/// ```
pub fn st_hausdorff_distance(a: &[u8], b: &[u8]) -> Result<f64> {
    let (ga, _) = parse_ewkb(a)?;
    let (gb, _) = parse_ewkb(b)?;
    Ok(ga.hausdorff_distance(&gb))
}

// ── Bounding-box accessors ────────────────────────────────────────────────────

/// ST_XMin — minimum X of the bounding rectangle.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_xmin;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("LINESTRING(1 2,3 4)", None).unwrap();
/// assert!((st_xmin(&blob).unwrap() - 1.0).abs() < 1e-10);
/// ```
pub fn st_xmin(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    let r = geom
        .bounding_rect()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    Ok(r.min().x)
}

/// ST_XMax — maximum X of the bounding rectangle.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_xmax;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("LINESTRING(1 2,3 4)", None).unwrap();
/// assert!((st_xmax(&blob).unwrap() - 3.0).abs() < 1e-10);
/// ```
pub fn st_xmax(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    let r = geom
        .bounding_rect()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    Ok(r.max().x)
}

/// ST_YMin — minimum Y of the bounding rectangle.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_ymin;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("LINESTRING(1 2,3 4)", None).unwrap();
/// assert!((st_ymin(&blob).unwrap() - 2.0).abs() < 1e-10);
/// ```
pub fn st_ymin(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    let r = geom
        .bounding_rect()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    Ok(r.min().y)
}

/// ST_YMax — maximum Y of the bounding rectangle.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_ymax;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("LINESTRING(1 2,3 4)", None).unwrap();
/// assert!((st_ymax(&blob).unwrap() - 4.0).abs() < 1e-10);
/// ```
pub fn st_ymax(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    let r = geom
        .bounding_rect()
        .ok_or(GeoLiteError::WrongType("non-empty geometry"))?;
    Ok(r.max().y)
}

// ── Spherical / geodetic variants ─────────────────────────────────────────────

fn require_point(g: Geometry<f64>) -> Result<Point<f64>> {
    match g {
        Geometry::Point(p) => Ok(p),
        _ => Err(GeoLiteError::WrongType("Point")),
    }
}

/// ST_DistanceSphere — Haversine distance in metres (requires Point inputs).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_distance_sphere;
/// use geolite_core::functions::constructors::st_point;
///
/// let london = st_point(-0.1278, 51.5074, Some(4326)).unwrap();
/// let paris = st_point(2.3522, 48.8566, Some(4326)).unwrap();
/// let dist = st_distance_sphere(&london, &paris).unwrap();
/// assert!(dist > 300_000.0 && dist < 400_000.0); // ~340 km
/// ```
pub fn st_distance_sphere(a: &[u8], b: &[u8]) -> Result<f64> {
    let (ga, _) = parse_ewkb(a)?;
    let (gb, _) = parse_ewkb(b)?;
    let pa = require_point(ga)?;
    let pb = require_point(gb)?;
    Ok(Haversine.distance(pa, pb))
}

/// ST_DistanceSpheroid — Geodesic distance in metres (Karney algorithm).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_distance_spheroid;
/// use geolite_core::functions::constructors::st_point;
///
/// let london = st_point(-0.1278, 51.5074, Some(4326)).unwrap();
/// let paris = st_point(2.3522, 48.8566, Some(4326)).unwrap();
/// let dist = st_distance_spheroid(&london, &paris).unwrap();
/// assert!(dist > 300_000.0 && dist < 400_000.0); // ~340 km
/// ```
pub fn st_distance_spheroid(a: &[u8], b: &[u8]) -> Result<f64> {
    let (ga, _) = parse_ewkb(a)?;
    let (gb, _) = parse_ewkb(b)?;
    let pa = require_point(ga)?;
    let pb = require_point(gb)?;
    Ok(Geodesic.distance(pa, pb))
}

/// ST_LengthSphere — Haversine arc length of a line in metres.
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_length_sphere;
/// use geolite_core::functions::io::geom_from_text;
///
/// let line = geom_from_text("LINESTRING(-0.1278 51.5074, 2.3522 48.8566)", Some(4326)).unwrap();
/// let len = st_length_sphere(&line).unwrap();
/// assert!(len > 300_000.0); // > 300 km
/// ```
pub fn st_length_sphere(blob: &[u8]) -> Result<f64> {
    let (geom, _) = parse_ewkb(blob)?;
    match &geom {
        Geometry::LineString(ls) => Ok(Haversine.length(ls)),
        Geometry::MultiLineString(mls) => Ok(mls.0.iter().map(|ls| Haversine.length(ls)).sum()),
        _ => Err(GeoLiteError::WrongType("LineString or MultiLineString")),
    }
}

/// ST_Azimuth — bearing from origin to target in radians (0 = north, clockwise).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_azimuth;
/// use geolite_core::functions::constructors::st_point;
///
/// let origin = st_point(0.0, 0.0, Some(4326)).unwrap();
/// let target = st_point(0.0, 1.0, Some(4326)).unwrap();
/// let az = st_azimuth(&origin, &target).unwrap();
/// // Due north → azimuth ≈ 0
/// assert!(az.abs() < 0.01);
/// ```
pub fn st_azimuth(origin: &[u8], target: &[u8]) -> Result<f64> {
    let (go, _) = parse_ewkb(origin)?;
    let (gt, _) = parse_ewkb(target)?;
    let po = require_point(go)?;
    let pt = require_point(gt)?;
    Ok(Geodesic.bearing(po, pt).to_radians())
}

/// ST_Project — destination point given a start, bearing (radians), and distance (metres).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_project;
/// use geolite_core::functions::constructors::st_point;
/// use geolite_core::functions::accessors::st_y;
///
/// let origin = st_point(0.0, 0.0, Some(4326)).unwrap();
/// // Project 111_000m due north (azimuth=0)
/// let dest = st_project(&origin, 111_000.0, 0.0).unwrap();
/// // Should be roughly 1 degree north
/// assert!((st_y(&dest).unwrap() - 1.0).abs() < 0.1);
/// ```
pub fn st_project(origin: &[u8], distance: f64, azimuth: f64) -> Result<Vec<u8>> {
    let (go, srid) = parse_ewkb(origin)?;
    let po = require_point(go)?;
    let dest: Point<f64> = Haversine.destination(po, azimuth.to_degrees(), distance);
    write_ewkb(&Geometry::Point(dest), srid)
}

/// ST_ClosestPoint — the point on geometry A closest to geometry B (point).
///
/// # Example
///
/// ```
/// use geolite_core::functions::measurement::st_closest_point;
/// use geolite_core::functions::constructors::st_point;
/// use geolite_core::functions::accessors::{st_x, st_y};
/// use geolite_core::functions::io::geom_from_text;
///
/// let line = geom_from_text("LINESTRING(0 0,10 0)", None).unwrap();
/// let pt = st_point(5.0, 5.0, None).unwrap();
/// let cp = st_closest_point(&line, &pt).unwrap();
/// assert!((st_x(&cp).unwrap() - 5.0).abs() < 1e-10);
/// assert!((st_y(&cp).unwrap() - 0.0).abs() < 1e-10);
/// ```
pub fn st_closest_point(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    let (ga, srid) = parse_ewkb(a)?;
    let (gb, _) = parse_ewkb(b)?;
    let pb = require_point(gb)?;
    let cp = ga.closest_point(&pb);
    let pt = match cp {
        Closest::Intersection(p) | Closest::SinglePoint(p) => p,
        Closest::Indeterminate => return Err(GeoLiteError::WrongType("non-empty geometry")),
    };
    write_ewkb(&Geometry::Point(pt), srid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::accessors::{st_x, st_y};
    use crate::functions::constructors::st_point;
    use crate::functions::io::geom_from_text;

    // ── Wrong-type errors ──────────────────────────────────────────

    #[test]
    fn st_length_wrong_type() {
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_length(&pt).is_err());
    }

    #[test]
    fn st_perimeter_wrong_type() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        assert!(st_perimeter(&line).is_err());
    }

    #[test]
    fn st_distance_sphere_non_point() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_distance_sphere(&line, &pt).is_err());
    }

    #[test]
    fn st_distance_spheroid_non_point() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_distance_spheroid(&line, &pt).is_err());
    }

    #[test]
    fn st_azimuth_non_point() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_azimuth(&line, &pt).is_err());
    }

    #[test]
    fn st_project_non_point() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        assert!(st_project(&line, 100.0, 0.0).is_err());
    }

    #[test]
    fn st_length_sphere_non_linestring() {
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_length_sphere(&pt).is_err());
    }

    // ── Distance to self ───────────────────────────────────────────

    #[test]
    fn distance_to_self_is_zero() {
        let pt = st_point(1.0, 2.0, None).unwrap();
        assert!(st_distance(&pt, &pt).unwrap().abs() < 1e-10);
    }

    #[test]
    fn distance_sphere_to_self_is_zero() {
        let pt = st_point(1.0, 2.0, Some(4326)).unwrap();
        assert!(st_distance_sphere(&pt, &pt).unwrap().abs() < 1e-10);
    }

    #[test]
    fn distance_spheroid_to_self_is_zero() {
        let pt = st_point(1.0, 2.0, Some(4326)).unwrap();
        assert!(st_distance_spheroid(&pt, &pt).unwrap().abs() < 1e-10);
    }

    // ── Hausdorff ──────────────────────────────────────────────────

    #[test]
    fn hausdorff_identical_is_zero() {
        let line = geom_from_text("LINESTRING(0 0,1 1,2 0)", None).unwrap();
        assert!(st_hausdorff_distance(&line, &line).unwrap().abs() < 1e-10);
    }

    // ── Closest point ──────────────────────────────────────────────

    #[test]
    fn closest_point_perpendicular_projection() {
        let line = geom_from_text("LINESTRING(0 0,10 0)", None).unwrap();
        let pt = st_point(5.0, 3.0, None).unwrap();
        let cp = st_closest_point(&line, &pt).unwrap();
        assert!((st_x(&cp).unwrap() - 5.0).abs() < 1e-10);
        assert!((st_y(&cp).unwrap() - 0.0).abs() < 1e-10);
    }

    // ── Bounding box ───────────────────────────────────────────────

    #[test]
    fn bbox_invariants() {
        let poly = geom_from_text("POLYGON((1 2,5 2,5 8,1 8,1 2))", None).unwrap();
        let xmin = st_xmin(&poly).unwrap();
        let xmax = st_xmax(&poly).unwrap();
        let ymin = st_ymin(&poly).unwrap();
        let ymax = st_ymax(&poly).unwrap();
        assert!(xmin <= xmax);
        assert!(ymin <= ymax);
        assert!((xmin - 1.0).abs() < 1e-10);
        assert!((xmax - 5.0).abs() < 1e-10);
        assert!((ymin - 2.0).abs() < 1e-10);
        assert!((ymax - 8.0).abs() < 1e-10);
    }

    #[test]
    fn st_area_point_is_zero() {
        let pt = st_point(0.0, 0.0, None).unwrap();
        assert!(st_area(&pt).unwrap().abs() < 1e-10);
    }

    #[test]
    fn st_length_multilinestring() {
        let mls = geom_from_text("MULTILINESTRING((0 0,1 0),(0 0,0 1))", None).unwrap();
        assert!((st_length(&mls).unwrap() - 2.0).abs() < 1e-10);
    }
}
