# geolite

PostGIS-compatible spatial functions for SQLite, written in pure Rust.

Load geolite as a SQLite extension to get `ST_*` spatial functions — the same API used by PostGIS — in any SQLite database. Works as a native loadable extension and in WebAssembly browsers.

## Workspace

| Crate | Description |
|---|---|
| `geolite-core` | Pure-Rust spatial function library with zero C dependencies |
| `geolite-sqlite` | SQLite loadable extension (native + WASM) |
| `geolite-diesel` | Diesel ORM types and query builder integration |

## Quick start

### As a SQLite loadable extension

```sql
.load geolite

SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1.0));
SELECT ST_Distance(
  ST_GeomFromText('POINT(0 0)'),
  ST_GeomFromText('POINT(3 4)')
);  -- 5.0
```

### From Rust with Diesel

```rust
use geolite_diesel::prelude::*;
use geolite_diesel::functions::*;

let nearby = features::table
    .filter(features::geom.st_dwithin(st_point(13.4, 52.5), 1000.0))
    .select((features::id, features::geom.st_astext()))
    .load(&mut conn)?;
```

## Supported functions

### I/O

`ST_GeomFromText`, `ST_GeomFromWKB`, `ST_GeomFromEWKB`, `ST_GeomFromGeoJSON`,
`ST_AsText`, `ST_AsEWKT`, `ST_AsBinary`, `ST_AsEWKB`, `ST_AsGeoJSON`

### Constructors

`ST_Point`, `ST_MakePoint`, `ST_MakeLine`, `ST_MakePolygon`,
`ST_MakeEnvelope`, `ST_Collect`, `ST_TileEnvelope`

### Accessors

`ST_SRID`, `ST_SetSRID`, `ST_GeometryType`, `ST_NDims`, `ST_CoordDim`,
`ST_Zmflag`, `ST_IsEmpty`, `ST_MemSize`, `ST_X`, `ST_Y`,
`ST_NumPoints`, `ST_NPoints`, `ST_NumGeometries`,
`ST_NumInteriorRings`, `ST_NumRings`, `ST_PointN`, `ST_StartPoint`,
`ST_EndPoint`, `ST_ExteriorRing`, `ST_InteriorRingN`, `ST_GeometryN`,
`ST_Dimension`, `ST_Envelope`, `ST_IsValid`, `ST_IsValidReason`

### Measurement

`ST_Area`, `ST_Length`, `ST_Perimeter`, `ST_Distance`, `ST_Centroid`,
`ST_PointOnSurface`, `ST_HausdorffDistance`, `ST_XMin`, `ST_XMax`,
`ST_YMin`, `ST_YMax`, `ST_DistanceSphere`, `ST_DistanceSpheroid`,
`ST_LengthSphere`, `ST_Azimuth`, `ST_Project`, `ST_ClosestPoint`

### Operations

`ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_SymDifference`, `ST_Buffer`

### Predicates

`ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Disjoint`, `ST_DWithin`,
`ST_Covers`, `ST_CoveredBy`, `ST_Equals`, `ST_Touches`, `ST_Crosses`,
`ST_Overlaps`, `ST_Relate`, `ST_RelateMatch`

### Spatial Index

`CreateSpatialIndex`, `DropSpatialIndex`

## Building

```sh
# Native loadable extension
cargo build --release -p geolite-sqlite

# WASM
cargo build --release -p geolite-sqlite --target wasm32-unknown-unknown

# Run tests
cargo test --workspace
```

## Contributor workflow

Install the Rust-powered pre-commit hook:

```sh
cargo run -p xtask -- install-hooks
```

Run the same local pre-commit checks manually:

```sh
# Fast default checks
cargo run -p xtask -- precommit

# Full checks (includes PostGIS + wasm tests)
cargo run -p xtask -- precommit --full
```

`--full` requires Docker plus the `wasm32-unknown-unknown` target and `wasm-bindgen-test-runner` installed.

CI executes the same hook checks plus an extended matrix:

- formatting and clippy checks
- workspace-wide clippy (`--workspace` and `--workspace --all-features`)
- native tests
- PostGIS testcontainer integration tests
- wasm tests
- MSRV checks
- cross-platform compile checks (Linux/macOS/Windows)

## Storage format

Geometries are stored as EWKB (Extended Well-Known Binary) BLOBs, the same wire format used by PostGIS. This means existing tools that read EWKB can interoperate with geolite databases directly.

## License

MIT OR Apache-2.0
