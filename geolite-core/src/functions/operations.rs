//! Spatial operations
//!
//! ST_Union, ST_Intersection, ST_Difference, ST_SymDifference, ST_Buffer

use geo::algorithm::bool_ops::BooleanOps;
use geo::algorithm::Buffer;
use geo::{Geometry, MultiPolygon};

use crate::error::{GeoLiteError, Result};
use crate::ewkb::{ensure_matching_srid, parse_ewkb, write_ewkb};

/// Extract a Polygon or MultiPolygon from a geometry, converting single
/// Polygons into MultiPolygon for uniform BooleanOps handling.
fn require_multi_polygon(geom: Geometry<f64>) -> Result<MultiPolygon<f64>> {
    match geom {
        Geometry::Polygon(p) => Ok(MultiPolygon::new(vec![p])),
        Geometry::MultiPolygon(mp) => Ok(mp),
        _ => Err(GeoLiteError::WrongType("Polygon or MultiPolygon")),
    }
}

/// ST_Union — compute the geometric union of two polygon geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::operations::st_union;
/// use geolite_core::functions::io::geom_from_text;
/// use geolite_core::functions::measurement::st_area;
///
/// let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
/// let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
/// let u = st_union(&a, &b).unwrap();
/// assert!((st_area(&u).unwrap() - 6.0).abs() < 1e-10);
/// ```
pub fn st_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    let (ga, srid_a) = parse_ewkb(a)?;
    let (gb, srid_b) = parse_ewkb(b)?;
    let srid = ensure_matching_srid(srid_a, srid_b)?;
    let ma = require_multi_polygon(ga)?;
    let mb = require_multi_polygon(gb)?;
    let result = ma.union(&mb);
    write_ewkb(&Geometry::MultiPolygon(result), srid)
}

/// ST_Intersection — compute the geometric intersection of two polygon geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::operations::st_intersection;
/// use geolite_core::functions::io::geom_from_text;
/// use geolite_core::functions::measurement::st_area;
///
/// let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
/// let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
/// let i = st_intersection(&a, &b).unwrap();
/// assert!((st_area(&i).unwrap() - 2.0).abs() < 1e-10);
/// ```
pub fn st_intersection(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    let (ga, srid_a) = parse_ewkb(a)?;
    let (gb, srid_b) = parse_ewkb(b)?;
    let srid = ensure_matching_srid(srid_a, srid_b)?;
    let ma = require_multi_polygon(ga)?;
    let mb = require_multi_polygon(gb)?;
    let result = ma.intersection(&mb);
    write_ewkb(&Geometry::MultiPolygon(result), srid)
}

/// ST_Difference — compute the geometric difference (A minus B) of two polygon geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::operations::st_difference;
/// use geolite_core::functions::io::geom_from_text;
/// use geolite_core::functions::measurement::st_area;
///
/// let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
/// let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
/// let d = st_difference(&a, &b).unwrap();
/// assert!((st_area(&d).unwrap() - 2.0).abs() < 1e-10);
/// ```
pub fn st_difference(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    let (ga, srid_a) = parse_ewkb(a)?;
    let (gb, srid_b) = parse_ewkb(b)?;
    let srid = ensure_matching_srid(srid_a, srid_b)?;
    let ma = require_multi_polygon(ga)?;
    let mb = require_multi_polygon(gb)?;
    let result = ma.difference(&mb);
    write_ewkb(&Geometry::MultiPolygon(result), srid)
}

/// ST_SymDifference — compute the symmetric difference (XOR) of two polygon geometries.
///
/// # Example
///
/// ```
/// use geolite_core::functions::operations::st_sym_difference;
/// use geolite_core::functions::io::geom_from_text;
/// use geolite_core::functions::measurement::st_area;
///
/// let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
/// let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
/// let sd = st_sym_difference(&a, &b).unwrap();
/// assert!((st_area(&sd).unwrap() - 4.0).abs() < 1e-10);
/// ```
pub fn st_sym_difference(a: &[u8], b: &[u8]) -> Result<Vec<u8>> {
    let (ga, srid_a) = parse_ewkb(a)?;
    let (gb, srid_b) = parse_ewkb(b)?;
    let srid = ensure_matching_srid(srid_a, srid_b)?;
    let ma = require_multi_polygon(ga)?;
    let mb = require_multi_polygon(gb)?;
    let result = ma.xor(&mb);
    write_ewkb(&Geometry::MultiPolygon(result), srid)
}

/// ST_Buffer — expand or shrink a geometry by a given distance.
///
/// # Example
///
/// ```
/// use geolite_core::functions::operations::st_buffer;
/// use geolite_core::functions::constructors::st_point;
/// use geolite_core::functions::measurement::st_area;
///
/// let pt = st_point(0.0, 0.0, None).unwrap();
/// let buffered = st_buffer(&pt, 1.0).unwrap();
/// let area = st_area(&buffered).unwrap();
/// // Area of a circle with radius 1 ≈ π
/// assert!((area - std::f64::consts::PI).abs() < 0.1);
/// ```
pub fn st_buffer(blob: &[u8], distance: f64) -> Result<Vec<u8>> {
    let (geom, srid) = parse_ewkb(blob)?;
    let result = geom.buffer(distance);
    write_ewkb(&Geometry::MultiPolygon(result), srid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::constructors::st_point;
    use crate::functions::io::geom_from_text;
    use crate::functions::measurement::st_area;

    #[test]
    fn union_overlapping() {
        let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
        let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
        let u = st_union(&a, &b).unwrap();
        assert!((st_area(&u).unwrap() - 6.0).abs() < 1e-10);
    }

    #[test]
    fn intersection_overlapping() {
        let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
        let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
        let i = st_intersection(&a, &b).unwrap();
        assert!((st_area(&i).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn difference_overlapping() {
        let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
        let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
        let d = st_difference(&a, &b).unwrap();
        assert!((st_area(&d).unwrap() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn sym_difference_overlapping() {
        let a = geom_from_text("POLYGON((0 0,2 0,2 2,0 2,0 0))", None).unwrap();
        let b = geom_from_text("POLYGON((1 0,3 0,3 2,1 2,1 0))", None).unwrap();
        let sd = st_sym_difference(&a, &b).unwrap();
        assert!((st_area(&sd).unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn buffer_point() {
        let pt = st_point(0.0, 0.0, None).unwrap();
        let buffered = st_buffer(&pt, 1.0).unwrap();
        let area = st_area(&buffered).unwrap();
        assert!((area - std::f64::consts::PI).abs() < 0.1);
    }

    #[test]
    fn union_wrong_type() {
        let line = geom_from_text("LINESTRING(0 0,1 1)", None).unwrap();
        let poly = geom_from_text("POLYGON((0 0,1 0,1 1,0 1,0 0))", None).unwrap();
        assert!(st_union(&line, &poly).is_err());
    }

    #[test]
    fn union_accepts_multipolygon_inputs() {
        let mp = geom_from_text("MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))", None).unwrap();
        let poly = geom_from_text("POLYGON((1 0,2 0,2 1,1 1,1 0))", None).unwrap();
        let u = st_union(&mp, &poly).unwrap();
        assert!(st_area(&u).unwrap() > 1.0);
    }

    #[test]
    fn buffer_negative_shrinks() {
        let poly = geom_from_text("POLYGON((0 0,10 0,10 10,0 10,0 0))", None).unwrap();
        let shrunk = st_buffer(&poly, -1.0).unwrap();
        let area = st_area(&shrunk).unwrap();
        assert!(area < 100.0 && area > 0.0);
    }
}
