# geolite

[![CI](https://github.com/LucaCappelletti94/geolite/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/geolite/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LucaCappelletti94/geolite/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/geolite)
[![MSRV](https://img.shields.io/badge/MSRV-1.86-blue)](https://github.com/LucaCappelletti94/geolite)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](https://github.com/LucaCappelletti94/geolite/blob/main/LICENSE)

PostGIS-style spatial functions for SQLite in pure Rust.

Use `geolite` as:

- a native SQLite loadable extension,
- a WASM-compatible SQLite extension,
- or Diesel ORM integration.

## Workspace crates

| Crate | Purpose |
|---|---|
| `geolite-core` | Core geometry functions and EWKB handling |
| `geolite-sqlite` | SQLite extension exposing `geolite-core` functions |
| `geolite-diesel` | Diesel geometry types and query extensions |

## Quick start

### SQLite extension

```sh
cargo build --release -p geolite-sqlite
```

```sql
SELECT load_extension('./target/release/libgeolite_sqlite');
-- If your SQLite build requires an explicit entrypoint:
-- SELECT load_extension('./target/release/libgeolite_sqlite', 'sqlite3_geolite_init');
SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1.0));
SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'));
```

### Rust API (`geolite-core`)

```rust
use geolite_core::functions::constructors::st_point;
use geolite_core::functions::measurement::st_distance;

let a = st_point(0.0, 0.0, None).unwrap();
let b = st_point(3.0, 4.0, None).unwrap();
assert!((st_distance(&a, &b).unwrap() - 5.0).abs() < 1e-10);
```

### Diesel integration

```toml
[dependencies]
geolite-diesel = { path = "geolite-diesel", features = ["sqlite"] }
```

```rust,no_run
# #[cfg(feature = "sqlite")]
# {
use diesel::debug_query;
use diesel::prelude::*;
use diesel::sqlite::Sqlite;
use geolite_diesel::functions::st_point;
use geolite_diesel::prelude::*;

diesel::table! {
    features (id) {
        id -> Integer,
        geom -> Nullable<geolite_diesel::Geometry>,
    }
}

let query = features::table
    .filter(
        features::geom
            .st_dwithin(st_point(13.4, 52.5).nullable(), 1000.0)
            .eq(true),
    )
    .select(features::geom.st_astext());

let sql = debug_query::<Sqlite, _>(&query).to_string();
assert!(sql.contains("ST_DWithin"));
# }
```

Relate aliases in Diesel:

- `st_relate_match_geoms(a, b, pattern)` maps to `ST_Relate(a, b, pattern)`.
- `st_relate_match(matrix, pattern)` maps to `ST_RelateMatch(matrix, pattern)`.
- Method-style geometry matching is available via `geolite_diesel::prelude::*`
  as `.st_relate_match_geoms(...)`.

Spatial index lifecycle in Diesel (`sqlite` feature):

- `CreateSpatialIndex` / `DropSpatialIndex` are called through raw SQL
  (`diesel::sql_query`) intentionally.
- Typed wrappers are not exposed in `geolite_diesel::functions` for these two
  lifecycle helpers.
- For migration-style setup/teardown, prefer SQL migrations.

Spatial index catalog lifecycle semantics (`sqlite` feature):

- Ownership for managed spatial index objects is tracked in
  `geolite_spatial_index_catalog` using `prefix`, `table_name`, and
  `column_name`.
- `CreateSpatialIndex` and `DropSpatialIndex` both lazily create
  `geolite_spatial_index_catalog` when it is missing.
- `DropSpatialIndex` removes the ownership row for the requested index, but the
  catalog table itself remains present even when it becomes empty.
- Lifecycle helpers fail closed when ownership cannot be proven. If managed
  objects exist without a matching catalog marker, create/drop returns an error
  instead of mutating schema objects.
- Manual catalog/object edits are treated as external drift. Operators should
  clean up or rebuild managed objects/markers before calling lifecycle helpers
  again.

```rust,no_run
# #[cfg(feature = "sqlite")]
# {
use diesel::prelude::*;
use diesel::sql_query;

let mut conn = SqliteConnection::establish(":memory:").unwrap();
sql_query("CREATE TABLE places (id INTEGER PRIMARY KEY, geom BLOB)")
    .execute(&mut conn)
    .unwrap();
sql_query("SELECT CreateSpatialIndex('places', 'geom')")
    .execute(&mut conn)
    .unwrap();
sql_query("SELECT DropSpatialIndex('places', 'geom')")
    .execute(&mut conn)
    .unwrap();
# }
```

## Documentation

- `geolite` docs are the source of truth for API surface.
- Generate local docs:

```sh
cargo doc --workspace --no-deps
```

## Build and test

Requires Rust `1.86+`.

```sh
# Native extension
cargo build --release -p geolite-sqlite

# WASM extension
cargo build --release -p geolite-sqlite --target wasm32-unknown-unknown

# Workspace tests
cargo test --workspace

# Diesel integration (feature-gated)
cargo test -p geolite-diesel --features sqlite
cargo check -p geolite-diesel --features postgres

# Perf assertions (ignored in normal test runs)
cargo test -p geolite-sqlite spatial_index_accelerates_intersects_window -- --ignored --exact --nocapture
cargo test -p geolite-sqlite spatial_index_accelerates_knn -- --ignored --exact --nocapture
cargo test -p geolite-sqlite type_partitioned_vs_mixed_index -- --ignored --exact --nocapture
cargo test -p geolite-diesel --features sqlite indexed_intersects_window_is_faster -- --ignored --exact --nocapture
cargo test -p geolite-diesel --features sqlite indexed_knn_is_faster -- --ignored --exact --nocapture
```

## Development

Install hooks:

```sh
cargo run -p xtask -- install-hooks
```

Run checks locally:

```sh
cargo run -p xtask -- precommit
cargo run -p xtask -- precommit --full
```

`precommit --full` requires Docker plus `wasm32-unknown-unknown` and `wasm-bindgen-test-runner`.

## Storage format

Geometries are stored as EWKB (Extended Well-Known Binary) BLOBs, matching the PostGIS wire format.

## Geographic SRID requirements

Geodesic and spherical functions (`ST_DistanceSphere`, `ST_DistanceSpheroid`,
`ST_LengthSphere`, `ST_Azimuth`, `ST_Project`, `ST_DWithinSphere`,
`ST_DWithinSpheroid`) require geometries with explicit
`SRID=4326`.

Inputs with missing SRID or non-4326 SRID are rejected with an error.

## Geodesic input type support

Current support matrix for geodesic pairwise distance and radius predicates:

| Functions | Supported input pairs | Unsupported input pairs |
|---|---|---|
| `ST_DistanceSphere`, `ST_DistanceSpheroid`, `ST_DWithinSphere`, `ST_DWithinSpheroid` | Point ↔ Point (non-empty) | Point ↔ LineString, Point ↔ Polygon, LineString ↔ LineString, LineString ↔ Polygon, Polygon ↔ Polygon |

Unsupported pairs fail with an explicit `requires Point` error.

## Distance predicate argument validation

`ST_DWithin`, `ST_DWithinSphere`, and `ST_DWithinSpheroid` require a finite,
non-negative distance argument.

## GeoJSON SRID behavior

`ST_GeomFromGeoJSON` follows PostGIS signature parity and is exposed as a
single-argument SQL function. When SRID is not provided in SQL, GeoJSON input
defaults to `SRID=4326`.

To override SRID, wrap with `ST_SetSRID`:

```sql
SELECT ST_SetSRID(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2]}'), 3857);
```

## License

MIT OR Apache-2.0
