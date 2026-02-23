# geolite

[![CI](https://github.com/LucaCappelletti94/geolite/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/geolite/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LucaCappelletti94/geolite/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/geolite)
[![MSRV](https://img.shields.io/badge/MSRV-1.86-blue)](https://github.com/LucaCappelletti94/geolite)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](LICENSE)

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

## License

MIT OR Apache-2.0
