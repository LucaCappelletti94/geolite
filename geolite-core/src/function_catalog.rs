//! Canonical SQLite function catalog shared across adapters.

/// Canonical SQLite function declaration metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteFunctionSpec {
    pub name: &'static str,
    pub n_arg: i32,
}

pub const SQLITE_DETERMINISTIC_FUNCTIONS: &[SqliteFunctionSpec] = &[
    // I/O
    SqliteFunctionSpec {
        name: "ST_GeomFromText",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_GeomFromText",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_GeomFromWKB",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_GeomFromWKB",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_GeomFromEWKB",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_GeomFromGeoJSON",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_AsText",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_AsEWKT",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_AsBinary",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_AsEWKB",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_AsGeoJSON",
        n_arg: 1,
    },
    // Constructors
    SqliteFunctionSpec {
        name: "ST_Point",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Point",
        n_arg: 3,
    },
    SqliteFunctionSpec {
        name: "ST_MakePoint",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_MakeLine",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_MakePolygon",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_MakeEnvelope",
        n_arg: 4,
    },
    SqliteFunctionSpec {
        name: "ST_MakeEnvelope",
        n_arg: 5,
    },
    SqliteFunctionSpec {
        name: "ST_Collect",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_TileEnvelope",
        n_arg: 3,
    },
    // Accessors
    SqliteFunctionSpec {
        name: "ST_SRID",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_SetSRID",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_GeometryType",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "GeometryType",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NDims",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_CoordDim",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Zmflag",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_IsEmpty",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_MemSize",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_X",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Y",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NumPoints",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NPoints",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NumGeometries",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NumInteriorRings",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NumInteriorRing",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_NumRings",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_PointN",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_StartPoint",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_EndPoint",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_ExteriorRing",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_InteriorRingN",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_GeometryN",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Dimension",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Envelope",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_IsValid",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_IsValidReason",
        n_arg: 1,
    },
    // Measurement
    SqliteFunctionSpec {
        name: "ST_Area",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Length",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Length2D",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Perimeter",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Perimeter2D",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Distance",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Centroid",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_PointOnSurface",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_HausdorffDistance",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_XMin",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_XMax",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_YMin",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_YMax",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_DistanceSphere",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_DistanceSpheroid",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_LengthSphere",
        n_arg: 1,
    },
    SqliteFunctionSpec {
        name: "ST_Azimuth",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Project",
        n_arg: 3,
    },
    SqliteFunctionSpec {
        name: "ST_ClosestPoint",
        n_arg: 2,
    },
    // Operations
    SqliteFunctionSpec {
        name: "ST_Union",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Intersection",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Difference",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_SymDifference",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Buffer",
        n_arg: 2,
    },
    // Predicates
    SqliteFunctionSpec {
        name: "ST_Intersects",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Contains",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Within",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Disjoint",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_DWithin",
        n_arg: 3,
    },
    SqliteFunctionSpec {
        name: "ST_Covers",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_CoveredBy",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Equals",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Touches",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Crosses",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Overlaps",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Relate",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "ST_Relate",
        n_arg: 3,
    },
    SqliteFunctionSpec {
        name: "ST_RelateMatch",
        n_arg: 2,
    },
];

pub const SQLITE_DIRECT_ONLY_FUNCTIONS: &[SqliteFunctionSpec] = &[
    SqliteFunctionSpec {
        name: "CreateSpatialIndex",
        n_arg: 2,
    },
    SqliteFunctionSpec {
        name: "DropSpatialIndex",
        n_arg: 2,
    },
];
