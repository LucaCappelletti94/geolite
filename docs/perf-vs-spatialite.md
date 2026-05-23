# sqlitegis vs SpatiaLite: unindexed `ST_Intersects` bench

`benches/spatialite_vs_sqlitegis.rs` (gated behind the `bench-spatialite` Cargo feature) runs a 50,000-row WGS84-point dataset against a single 1-by-1-degree query window through both libraries. sqlitegis is registered in-process on a `libsqlite3-sys` connection via `register_functions(db)`. SpatiaLite is loaded as an external loadable extension via `sqlite3_load_extension('mod_spatialite')`. Both sides use the same underlying libsqlite3 (the bundled C amalgamation from `libsqlite3-sys`), so the comparison isolates predicate-callback cost from SQLite engine differences.

## Background: the inline-MBR gap

SpatiaLite blobs carry a precomputed minimum bounding rectangle at bytes 6-37 of the blob payload, as 4 little-endian doubles (MinX, MinY, MaxX, MaxY). Reading the MBR is therefore an O(1), 32-byte load with no allocation. For the 99.99%-negative-row distribution of "filter many points against one window", their bbox-vs-bbox reject completes in tens of nanoseconds per row.

EWKB (the wire format sqlitegis uses for PostGIS compatibility) has no such header. Naively, computing a bbox required fully decoding the geometry first via `geozero` plus a heap allocation for the `geo::Geometry` enum, which dominated cost on the negative-row path.

Adding an inline MBR to our blob format would close the gap but break PostGIS compatibility (PostGIS EWKB also has no MBR header). Off the table.

## The fix: MBR-only fastpath in the EWKB parser

`core::ewkb::extract_mbr(blob: &[u8]) -> Result<Option<Rect<f64>>>` walks the EWKB bytes for X/Y coordinates only, computes the running min/max, and returns the MBR without allocating a `Geometry`. For Points it is effectively O(1); for LineStrings/Polygons it is O(n) but allocation-free; for Multi-/Collection types it recurses through nested WKB mini-headers.

`core::functions::predicates::st_intersects` now calls `extract_mbr` on both operands first. If both MBRs exist and do not overlap, it short-circuits to `false` without ever decoding the geometries. The 99.99% negative-row path no longer pays the heap-allocating decode at all.

## Measured impact

Same bench, same machine, before/after the MBR fastpath:

| | Before | After | Change |
|---|---|---|---|
| sqlitegis (50k unindexed `ST_Intersects`) | 57.0 ms | **5.72 ms** | criterion: -89.96%, p < 0.05 |
| SpatiaLite (unchanged) | 8.6 ms | 9.26 ms | within run-to-run noise |
| Ratio | SpatiaLite 6.6x faster | **sqlitegis 1.62x faster** | -- |

The 10x speedup is consistent with the cost analysis: the negative-row path went from "parse two EWKB blobs into `geo::Geometry` + check bbox" (~1.1 us/row) to "walk two EWKB blobs reading only X/Y for bbox" (~0.1 us/row).

SpatiaLite still wins for workloads where the LEFT operand is very large (large polygons / linestrings); their O(1) inline-MBR read beats our O(n) byte walk in that regime. For point-heavy filter workloads, which is the typical "find features in a window" shape, sqlitegis is now ahead.

## What we did not do (and why)

- **Add an MBR header to our blob format.** Would be O(1) like SpatiaLite, but breaks PostGIS wire-compatibility. The README explicitly sells "queries port between SQLite and PostGIS without rewriting" as a goal, so this option was rejected.
- **`sqlite3_set_auxdata` caching of the RHS geometry.** This would amortise the per-row decode of the constant polygon across the 50k rows. With the MBR fastpath already in place, the residual cost is small and the additional FFI plumbing in the callbacks did not feel worth it. Reconsider if a future workload shows otherwise.

## Reproducing

`libsqlite3-mod-spatialite` must be installed system-wide so the SQLite loader can find `mod_spatialite`. Then:

```sh
cargo bench --features "bench-spatialite sqlite bundled-sqlite" --bench spatialite_vs_sqlitegis -- --warm-up-time 2 --measurement-time 6
```

CI does not run this bench. SpatiaLite is not a default CI dep, and the bench is feature-gated off so the rest of the matrix stays unaffected.
