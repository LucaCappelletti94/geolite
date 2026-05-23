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

## Broader workload picture

The bench extends to four more groups so the comparison is not just one shape. Same 50k-WGS84-point dataset; each row reports criterion median time + tight CI on the same Threadripper machine.

| Workload | sqlitegis | SpatiaLite | Ratio |
|---|---|---|---|
| Unindexed `ST_Intersects` bulk (constant window) | 5.77 ms | 9.17 ms | sqlitegis 1.59x |
| Indexed `ST_Intersects` window (R-tree-prefiltered) | 10.63 us | 13.12 us | sqlitegis 1.23x |
| Geodesic distance bulk (sphere/Haversine) | 30.96 ms | 254.20 ms | **sqlitegis 8.2x** |
| `ST_AsText` scalar throughput | 28.20 ms | 49.33 ms | sqlitegis 1.75x |
| `ST_Buffer + ST_Intersection` bulk | 171.21 ms | 28.11 ms | **SpatiaLite 6.1x** |

Two highlights worth understanding.

**Geodesic distance: sqlitegis 8.2x faster.** Surprising at first because GEOS+PROJ should be at least as fast as a Rust Haversine. Investigation pending; the likely cause is that SpatiaLite's 3-arg `ST_Distance(g1, g2, use_ellipsoid)` does full ellipsoid-aware setup regardless of the `use_ellipsoid` flag value, so even the `0` (sphere) branch pays for machinery the Haversine path does not need. Our `ST_DistanceSphere` is a direct Haversine on `f64` lat/lon pairs with no allocation.

**`ST_Buffer + ST_Intersection`: SpatiaLite 6.1x faster.** This is the GEOS-heavy workload where we expected SpatiaLite to win. The dominant cost is `ST_Buffer`, which GEOS implements with decades of optimisation; the `geo` Rust crate's offset-curve algorithm is correct but slower. The per-row `ST_Intersection` (polygon-vs-point) is cheap on both sides. Worth noting: the workload as benched does the buffer once per query (SQLite folds the constant subexpression), so the cost asymmetry shows up amortised across 50k per-row intersection checks. The actual buffer cost gap is bigger than the 6.1x ratio suggests. Originally this bench used `ST_Contains` because sqlitegis's `ST_Intersection` was Polygon-only; it now uses real `ST_Intersection` after the decompose/intersect/pack dispatch landed.

## SpatiaLite naming quirks worth knowing

While porting the bench between libraries, the following function-name differences mattered. None of them are sqlitegis bugs; documented in [`docs/limitations.md`](limitations.md) for completeness.

- `ST_DistanceSphere(g1, g2)` (PostGIS / sqlitegis) is `ST_Distance(g1, g2, 0)` in SpatiaLite 5.1.0.
- `ST_DistanceSpheroid(g1, g2)` is `ST_Distance(g1, g2, 1)` in SpatiaLite 5.1.0.
- `ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid)` is not present in SpatiaLite 5.1.0; bench code constructs the envelope as a `POLYGON` WKT literal instead.
- `GreatCircleDistance` was present in SpatiaLite 4.x but removed in 5.x.

## What we did not do (and why)

- **Add an MBR header to our blob format.** Would be O(1) like SpatiaLite, but breaks PostGIS wire-compatibility. The README explicitly sells "queries port between SQLite and PostGIS without rewriting" as a goal, so this option was rejected.
- **`sqlite3_set_auxdata` caching of the RHS geometry.** This would amortise the per-row decode of the constant polygon across the 50k rows. With the MBR fastpath already in place, the residual cost is small and the additional FFI plumbing in the callbacks did not feel worth it. Reconsider if a future workload shows otherwise.

## Reproducing

`libsqlite3-mod-spatialite` must be installed system-wide so the SQLite loader can find `mod_spatialite`. Then:

```sh
cargo bench --features "bench-spatialite sqlite bundled-sqlite" --bench spatialite_vs_sqlitegis -- --warm-up-time 2 --measurement-time 6
```

CI does not run this bench. SpatiaLite is not a default CI dep, and the bench is feature-gated off so the rest of the matrix stays unaffected.
