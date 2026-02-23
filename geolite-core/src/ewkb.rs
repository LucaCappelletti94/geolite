//! EWKB (Extended Well-Known Binary) parser and writer.
//!
//! Wire format (little-endian):
//!   [0x01]        — byte order marker
//!   [u32 LE]      — geometry type with flags
//!                   Bit 29 (0x20000000): SRID present
//!                   Bit 31 (0x80000000): Z dimension
//!                   Bit 30 (0x40000000): M dimension
//!                   Bits 0–28: geometry type (1=Point, 2=LineString, …)
//!   [i32 LE]      — SRID (only when SRID flag set)
//!   …             — ISO WKB geometry payload

use geo::Geometry;
use geozero::wkb::Ewkb;
use geozero::{CoordDimensions, ToGeo, ToWkb};

use crate::error::{GeoLiteError, Result};

// ── EWKB flag constants ───────────────────────────────────────────────────────
pub const EWKB_SRID_FLAG: u32 = 0x20000000;
pub const EWKB_Z_FLAG: u32 = 0x80000000;
pub const EWKB_M_FLAG: u32 = 0x40000000;

// ── Geometry type codes (ISO WKB) ─────────────────────────────────────────────
pub const WKB_POINT: u32 = 1;
pub const WKB_LINESTRING: u32 = 2;
pub const WKB_POLYGON: u32 = 3;
pub const WKB_MULTIPOINT: u32 = 4;
pub const WKB_MULTILINESTRING: u32 = 5;
pub const WKB_MULTIPOLYGON: u32 = 6;
pub const WKB_GEOMETRYCOLLECTION: u32 = 7;

/// Parsed EWKB header metadata.
#[derive(Debug, Clone)]
pub struct EwkbHeader {
    /// Base geometry type code (1=Point, 2=LineString, ..., 7=GeometryCollection).
    pub geom_type: u32,
    /// SRID embedded in the EWKB, if the SRID flag is set.
    pub srid: Option<i32>,
    /// Whether the geometry has Z coordinates.
    pub has_z: bool,
    /// Whether the geometry has M coordinates.
    pub has_m: bool,
    /// Byte offset where the geometry payload starts (after header + optional SRID).
    pub data_offset: usize,
}

/// Peek at the EWKB header without fully parsing the geometry.
///
/// # Example
///
/// ```
/// use geolite_core::ewkb::parse_ewkb_header;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let hdr = parse_ewkb_header(&blob).unwrap();
/// assert_eq!(hdr.geom_type, 1); // WKB_POINT
/// assert_eq!(hdr.srid, Some(4326));
/// ```
pub fn parse_ewkb_header(blob: &[u8]) -> Result<EwkbHeader> {
    if blob.len() < 5 {
        return Err(GeoLiteError::InvalidEwkb("blob too short"));
    }
    if blob[0] != 0x01 {
        return Err(GeoLiteError::InvalidEwkb("big-endian EWKB not supported"));
    }

    let raw_type = u32::from_le_bytes([blob[1], blob[2], blob[3], blob[4]]);
    let has_srid = (raw_type & EWKB_SRID_FLAG) != 0;
    let has_z = (raw_type & EWKB_Z_FLAG) != 0;
    let has_m = (raw_type & EWKB_M_FLAG) != 0;
    let geom_type = raw_type & 0x1FFFFFFF;

    let mut offset = 5usize;
    let srid = if has_srid {
        if blob.len() < 9 {
            return Err(GeoLiteError::InvalidEwkb(
                "SRID flag set but blob too short",
            ));
        }
        let s = i32::from_le_bytes([blob[5], blob[6], blob[7], blob[8]]);
        offset += 4;
        Some(s)
    } else {
        None
    };

    Ok(EwkbHeader {
        geom_type,
        srid,
        has_z,
        has_m,
        data_offset: offset,
    })
}

/// Extract only the SRID from an EWKB blob (cheap, no geometry parsing).
///
/// # Example
///
/// ```
/// use geolite_core::ewkb::extract_srid;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// assert_eq!(extract_srid(&blob), Some(4326));
///
/// let no_srid = geom_from_text("POINT(1 2)", None).unwrap();
/// assert_eq!(extract_srid(&no_srid), None);
/// ```
pub fn extract_srid(blob: &[u8]) -> Option<i32> {
    parse_ewkb_header(blob).ok().and_then(|h| h.srid)
}

/// Enforce equal SRIDs for binary geometry operations.
///
/// Returns the shared SRID when both inputs are compatible.
pub fn ensure_matching_srid(left: Option<i32>, right: Option<i32>) -> Result<Option<i32>> {
    if left == right {
        Ok(left)
    } else {
        let l = left.unwrap_or(0);
        let r = right.unwrap_or(0);
        Err(GeoLiteError::InvalidInput(format!(
            "operation on mixed SRID geometries ({l} != {r})"
        )))
    }
}

/// Parse an EWKB blob into a `geo::Geometry<f64>`.
/// Returns `(geometry, srid)`.
///
/// # Example
///
/// ```
/// use geolite_core::ewkb::parse_ewkb;
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let (geom, srid) = parse_ewkb(&blob).unwrap();
/// assert_eq!(srid, Some(4326));
/// ```
pub fn parse_ewkb(blob: &[u8]) -> Result<(Geometry<f64>, Option<i32>)> {
    let header = parse_ewkb_header(blob)?;
    if header.has_z || header.has_m {
        return Err(GeoLiteError::InvalidInput(
            "Z/M geometries are not supported".to_string(),
        ));
    }
    let geom = Ewkb(blob).to_geo()?;
    Ok((geom, header.srid))
}

/// Serialise a `geo::Geometry<f64>` to EWKB with an optional SRID.
///
/// If `srid` is `None`, produces standard ISO WKB (no SRID flag).
///
/// # Example
///
/// ```
/// use geo::{Geometry, Point};
/// use geolite_core::ewkb::{write_ewkb, parse_ewkb};
///
/// let geom = Geometry::Point(Point::new(1.0, 2.0));
/// let blob = write_ewkb(&geom, Some(4326)).unwrap();
/// let (parsed, srid) = parse_ewkb(&blob).unwrap();
/// assert_eq!(srid, Some(4326));
/// ```
pub fn write_ewkb(geom: &Geometry<f64>, srid: Option<i32>) -> Result<Vec<u8>> {
    // Use geozero to produce ISO WKB (XY only for now)
    let iso_wkb = geom
        .to_wkb(CoordDimensions::xy())
        .map_err(GeoLiteError::Geozero)?;

    if let Some(srid_val) = srid {
        // Patch the ISO WKB header to add the SRID flag + SRID value.
        // ISO WKB: [byte_order(1)][type_u32(4)][payload…]
        // EWKB:    [byte_order(1)][type_u32_with_flag(4)][srid_i32(4)][payload…]
        let mut out = Vec::with_capacity(iso_wkb.len() + 4);
        out.push(iso_wkb[0]); // byte order (0x01)

        let raw_type = u32::from_le_bytes([iso_wkb[1], iso_wkb[2], iso_wkb[3], iso_wkb[4]]);
        let ewkb_type = raw_type | EWKB_SRID_FLAG;
        out.extend_from_slice(&ewkb_type.to_le_bytes());
        out.extend_from_slice(&srid_val.to_le_bytes());
        out.extend_from_slice(&iso_wkb[5..]);
        Ok(out)
    } else {
        Ok(iso_wkb)
    }
}

/// Rewrite the SRID in an existing EWKB blob without re-parsing the geometry.
///
/// # Example
///
/// ```
/// use geolite_core::ewkb::{set_srid, extract_srid};
/// use geolite_core::functions::io::geom_from_text;
///
/// let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
/// let updated = set_srid(&blob, 3857).unwrap();
/// assert_eq!(extract_srid(&updated), Some(3857));
/// ```
pub fn set_srid(blob: &[u8], new_srid: i32) -> Result<Vec<u8>> {
    let header = parse_ewkb_header(blob)?;

    let mut out = Vec::with_capacity(blob.len() + 4);
    out.push(0x01); // byte order

    let raw_type = u32::from_le_bytes([blob[1], blob[2], blob[3], blob[4]]);
    let ewkb_type = raw_type | EWKB_SRID_FLAG;
    out.extend_from_slice(&ewkb_type.to_le_bytes());
    out.extend_from_slice(&new_srid.to_le_bytes());

    // Skip old SRID bytes if they were present, copy remaining payload
    out.extend_from_slice(&blob[header.data_offset..]);
    Ok(out)
}

/// Return a human-readable geometry type name (PostGIS convention).
///
/// # Example
///
/// ```
/// use geolite_core::ewkb::{geom_type_name, WKB_POINT, WKB_POLYGON};
///
/// assert_eq!(geom_type_name(WKB_POINT), "ST_Point");
/// assert_eq!(geom_type_name(WKB_POLYGON), "ST_Polygon");
/// assert_eq!(geom_type_name(999), "ST_Unknown");
/// ```
pub fn geom_type_name(raw_type: u32) -> &'static str {
    match raw_type & 0x1FFF_FFFF {
        WKB_POINT => "ST_Point",
        WKB_LINESTRING => "ST_LineString",
        WKB_POLYGON => "ST_Polygon",
        WKB_MULTIPOINT => "ST_MultiPoint",
        WKB_MULTILINESTRING => "ST_MultiLineString",
        WKB_MULTIPOLYGON => "ST_MultiPolygon",
        WKB_GEOMETRYCOLLECTION => "ST_GeometryCollection",
        _ => "ST_Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::io::geom_from_text;

    #[test]
    fn header_blob_too_short() {
        assert!(parse_ewkb_header(&[0x01, 0x02]).is_err());
        assert!(parse_ewkb_header(&[]).is_err());
    }

    #[test]
    fn header_big_endian_rejected() {
        // big-endian byte order marker = 0x00
        assert!(parse_ewkb_header(&[0x00, 0x01, 0x00, 0x00, 0x00]).is_err());
    }

    #[test]
    fn header_srid_flag_but_truncated() {
        // byte order + type word with SRID flag, but no SRID bytes
        let mut blob = vec![0x01];
        let raw_type = WKB_POINT | EWKB_SRID_FLAG;
        blob.extend_from_slice(&raw_type.to_le_bytes());
        assert!(parse_ewkb_header(&blob).is_err());
    }

    #[test]
    fn header_valid_point_with_srid() {
        let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
        let hdr = parse_ewkb_header(&blob).unwrap();
        assert_eq!(hdr.geom_type, WKB_POINT);
        assert_eq!(hdr.srid, Some(4326));
        assert!(!hdr.has_z);
        assert!(!hdr.has_m);
        assert_eq!(hdr.data_offset, 9); // 1 + 4 + 4
    }

    #[test]
    fn header_valid_point_without_srid() {
        let blob = geom_from_text("POINT(1 2)", None).unwrap();
        let hdr = parse_ewkb_header(&blob).unwrap();
        assert_eq!(hdr.geom_type, WKB_POINT);
        assert_eq!(hdr.srid, None);
        assert_eq!(hdr.data_offset, 5); // 1 + 4
    }

    #[test]
    fn extract_srid_empty_blob() {
        assert_eq!(extract_srid(&[]), None);
    }

    #[test]
    fn extract_srid_malformed_blob() {
        assert_eq!(extract_srid(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn write_ewkb_without_srid() {
        let geom = geo::Geometry::Point(geo::Point::new(1.0, 2.0));
        let blob = write_ewkb(&geom, None).unwrap();
        assert_eq!(extract_srid(&blob), None);
        // ISO WKB: byte order(1) + type(4) + x(8) + y(8) = 21 bytes
        assert_eq!(blob.len(), 21);
    }

    #[test]
    fn write_ewkb_with_srid() {
        let geom = geo::Geometry::Point(geo::Point::new(1.0, 2.0));
        let blob = write_ewkb(&geom, Some(4326)).unwrap();
        assert_eq!(extract_srid(&blob), Some(4326));
        // EWKB: byte order(1) + type(4) + srid(4) + x(8) + y(8) = 25 bytes
        assert_eq!(blob.len(), 25);
    }

    #[test]
    fn set_srid_replaces_existing() {
        let blob = geom_from_text("POINT(1 2)", Some(4326)).unwrap();
        let updated = set_srid(&blob, 3857).unwrap();
        assert_eq!(extract_srid(&updated), Some(3857));
        // Geometry should still parse correctly
        let (_, srid) = parse_ewkb(&updated).unwrap();
        assert_eq!(srid, Some(3857));
    }

    #[test]
    fn set_srid_adds_to_blob_without_srid() {
        let blob = geom_from_text("POINT(1 2)", None).unwrap();
        let updated = set_srid(&blob, 4326).unwrap();
        assert_eq!(extract_srid(&updated), Some(4326));
    }

    #[test]
    fn geom_type_name_all_types() {
        assert_eq!(geom_type_name(WKB_POINT), "ST_Point");
        assert_eq!(geom_type_name(WKB_LINESTRING), "ST_LineString");
        assert_eq!(geom_type_name(WKB_POLYGON), "ST_Polygon");
        assert_eq!(geom_type_name(WKB_MULTIPOINT), "ST_MultiPoint");
        assert_eq!(geom_type_name(WKB_MULTILINESTRING), "ST_MultiLineString");
        assert_eq!(geom_type_name(WKB_MULTIPOLYGON), "ST_MultiPolygon");
        assert_eq!(
            geom_type_name(WKB_GEOMETRYCOLLECTION),
            "ST_GeometryCollection"
        );
        assert_eq!(geom_type_name(42), "ST_Unknown");
    }

    #[test]
    fn parse_ewkb_roundtrip() {
        let blob = geom_from_text("LINESTRING(0 0, 1 1, 2 2)", Some(4326)).unwrap();
        let (geom, srid) = parse_ewkb(&blob).unwrap();
        assert_eq!(srid, Some(4326));
        let blob2 = write_ewkb(&geom, srid).unwrap();
        let (geom2, srid2) = parse_ewkb(&blob2).unwrap();
        assert_eq!(srid, srid2);
        assert_eq!(format!("{geom:?}"), format!("{geom2:?}"));
    }

    #[test]
    fn parse_ewkb_invalid_blob() {
        assert!(parse_ewkb(&[0x01, 0x02]).is_err());
    }

    #[test]
    fn ensure_matching_srid_accepts_equal() {
        assert_eq!(ensure_matching_srid(Some(4326), Some(4326)).unwrap(), Some(4326));
        assert_eq!(ensure_matching_srid(None, None).unwrap(), None);
    }

    #[test]
    fn ensure_matching_srid_rejects_mismatch() {
        assert!(ensure_matching_srid(Some(4326), Some(3857)).is_err());
        assert!(ensure_matching_srid(Some(4326), None).is_err());
    }
}
