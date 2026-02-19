//! I/O and serialization functions.
//!
//! ST_AsText, ST_AsEWKT, ST_AsBinary, ST_AsEWKB, ST_AsGeoJSON,
//! ST_GeomFromText, ST_GeomFromWKB, ST_GeomFromEWKB, ST_GeomFromGeoJSON

use geo::Geometry;
use geozero::wkb::Ewkb;
use geozero::{CoordDimensions, ToGeo, ToJson, ToWkb, ToWkt};

use crate::error::{GeoLiteError, Result};
use crate::ewkb::{extract_srid, parse_ewkb, write_ewkb};

// ── Deserialization helpers ───────────────────────────────────────────────────

/// Parse WKT (optionally with an SRID) into an EWKB blob.
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// assert!(!blob.is_empty());
/// ```
pub fn geom_from_text(wkt: &str, srid: Option<i32>) -> Result<Vec<u8>> {
    let geom: Geometry<f64> = geozero::wkt::Wkt(wkt.as_bytes()).to_geo()?;
    write_ewkb(&geom, srid)
}

/// Parse ISO WKB bytes (optionally override SRID) into an EWKB blob.
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_binary, geom_from_wkb};
/// use geolite_core::ewkb::extract_srid;
///
/// let blob = geom_from_text("POINT(1 2)", None).unwrap();
/// let wkb = as_binary(&blob).unwrap();
/// let restored = geom_from_wkb(&wkb, Some(4326)).unwrap();
/// assert_eq!(extract_srid(&restored), Some(4326));
/// ```
pub fn geom_from_wkb(wkb: &[u8], srid: Option<i32>) -> Result<Vec<u8>> {
    let geom: Geometry<f64> = Ewkb(wkb).to_geo()?;
    write_ewkb(&geom, srid)
}

/// Pass-through / re-normalise an EWKB blob (validates + strips unknown bits).
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, geom_from_ewkb};
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let normalised = geom_from_ewkb(&blob).unwrap();
/// assert!(!normalised.is_empty());
/// ```
pub fn geom_from_ewkb(ewkb: &[u8]) -> Result<Vec<u8>> {
    let (geom, srid) = parse_ewkb(ewkb)?;
    write_ewkb(&geom, srid)
}

/// Parse a GeoJSON string into an EWKB blob (SRID = 4326 by default, per spec).
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::geom_from_geojson;
/// use geolite_core::ewkb::extract_srid;
///
/// let blob = geom_from_geojson(r#"{"type":"Point","coordinates":[1,2]}"#, None).unwrap();
/// assert_eq!(extract_srid(&blob), Some(4326));
/// ```
pub fn geom_from_geojson(json: &str, srid: Option<i32>) -> Result<Vec<u8>> {
    let geom: Geometry<f64> = geozero::geojson::GeoJson(json).to_geo()?;
    let effective_srid = srid.or(Some(4326));
    write_ewkb(&geom, effective_srid)
}

// ── Serialization helpers ─────────────────────────────────────────────────────

/// Convert an EWKB blob to WKT text.
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_text};
///
/// let blob = geom_from_text("POINT(1 2)", None).unwrap();
/// let wkt = as_text(&blob).unwrap();
/// assert!(wkt.contains("POINT"));
/// ```
pub fn as_text(blob: &[u8]) -> Result<String> {
    Ok(Ewkb(blob).to_wkt()?)
}

/// Convert an EWKB blob to EWKT text (`SRID=n;WKT`).
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_ewkt};
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let ewkt = as_ewkt(&blob).unwrap();
/// assert!(ewkt.starts_with("SRID=4326;"));
/// ```
pub fn as_ewkt(blob: &[u8]) -> Result<String> {
    let srid = extract_srid(blob);
    let wkt = Ewkb(blob).to_wkt()?;
    if let Some(s) = srid {
        Ok(format!("SRID={s};{wkt}"))
    } else {
        Ok(wkt)
    }
}

/// Convert an EWKB blob to ISO WKB bytes (strips SRID).
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_binary};
/// use geolite_core::ewkb::extract_srid;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let wkb = as_binary(&blob).unwrap();
/// // ISO WKB has no SRID
/// assert_eq!(extract_srid(&wkb), None);
/// ```
pub fn as_binary(blob: &[u8]) -> Result<Vec<u8>> {
    let (geom, _srid) = parse_ewkb(blob)?;
    geom.to_wkb(CoordDimensions::xy())
        .map_err(GeoLiteError::Geozero)
}

/// Return the EWKB blob as-is (identity for well-formed input).
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_ewkb};
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let copy = as_ewkb(&blob).unwrap();
/// assert_eq!(blob.len(), copy.len());
/// ```
pub fn as_ewkb(blob: &[u8]) -> Result<Vec<u8>> {
    geom_from_ewkb(blob)
}

/// Convert an EWKB blob to GeoJSON text.
///
/// # Example
///
/// ```
/// use geolite_core::functions::io::{geom_from_text, as_geojson};
///
/// let blob = geom_from_text("POINT(1 2)", None).unwrap();
/// let json = as_geojson(&blob).unwrap();
/// assert!(json.contains("Point"));
/// assert!(json.contains("coordinates"));
/// ```
pub fn as_geojson(blob: &[u8]) -> Result<String> {
    Ok(Ewkb(blob).to_json()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_wkt_returns_err() {
        assert!(geom_from_text("NOT_VALID_WKT", None).is_err());
    }

    #[test]
    fn invalid_wkb_returns_err() {
        assert!(geom_from_wkb(&[0xFF, 0x00], None).is_err());
    }

    #[test]
    fn invalid_geojson_returns_err() {
        assert!(geom_from_geojson("{not json}", None).is_err());
    }

    #[test]
    fn geojson_default_srid_4326() {
        let blob = geom_from_geojson(r#"{"type":"Point","coordinates":[1,2]}"#, None).unwrap();
        assert_eq!(extract_srid(&blob), Some(4326));
    }

    #[test]
    fn geojson_custom_srid_overrides() {
        let blob =
            geom_from_geojson(r#"{"type":"Point","coordinates":[1,2]}"#, Some(3857)).unwrap();
        assert_eq!(extract_srid(&blob), Some(3857));
    }

    #[test]
    fn as_ewkt_with_srid() {
        let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
        let ewkt = as_ewkt(&blob).unwrap();
        assert!(ewkt.starts_with("SRID=4326;"));
        assert!(ewkt.contains("POINT"));
    }

    #[test]
    fn as_ewkt_without_srid() {
        let blob = geom_from_text("POINT(1 2)", None).unwrap();
        let ewkt = as_ewkt(&blob).unwrap();
        assert!(!ewkt.contains("SRID="));
    }

    #[test]
    fn as_binary_strips_srid() {
        let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
        let wkb = as_binary(&blob).unwrap();
        assert_eq!(extract_srid(&wkb), None);
    }

    #[test]
    fn roundtrip_wkb() {
        let blob = geom_from_text("POINT(3 4)", Some(4326)).unwrap();
        let wkb = as_binary(&blob).unwrap();
        let restored = geom_from_wkb(&wkb, Some(4326)).unwrap();
        let (g1, _) = parse_ewkb(&blob).unwrap();
        let (g2, _) = parse_ewkb(&restored).unwrap();
        assert_eq!(format!("{g1:?}"), format!("{g2:?}"));
    }

    #[test]
    fn roundtrip_geojson() {
        let blob = geom_from_text("POINT(1 2)", None).unwrap();
        let json = as_geojson(&blob).unwrap();
        let restored = geom_from_geojson(&json, None).unwrap();
        let (g1, _) = parse_ewkb(&blob).unwrap();
        let (g2, _) = parse_ewkb(&restored).unwrap();
        assert_eq!(format!("{g1:?}"), format!("{g2:?}"));
    }

    #[test]
    fn as_text_roundtrip() {
        let blob = geom_from_text("LINESTRING(0 0,1 1,2 2)", None).unwrap();
        let wkt = as_text(&blob).unwrap();
        assert!(wkt.contains("LINESTRING"));
    }
}
