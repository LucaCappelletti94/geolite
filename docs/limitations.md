# Known sqlitegis limitations

A running list of API gaps and incompatibilities surfaced during development. Each entry names where it was discovered and what would close it.

## `ST_Intersection` rejects non-Polygon first argument

Surfaced by: `benches/spatialite_vs_sqlitegis.rs` while wiring the GEOS-heavy `ST_Buffer + ST_Intersection` workload. The query

```sql
SELECT COUNT(*) FROM places
WHERE NOT ST_IsEmpty(ST_Intersection(geom, ST_Buffer(?, 0.1)))
```

errors with `ST_Intersection: geometry is not a Polygon or MultiPolygon; got Point` whenever the first argument is anything other than a Polygon or MultiPolygon. PostGIS and SpatiaLite (via GEOS) accept any geometry type for both arguments and return the appropriate intersection shape (Point, LineString, Polygon, GeometryCollection, etc.).

Current implementation in `src/core/functions/operations.rs` short-circuits on type before delegating to the `geo` crate. The `geo` crate's `BooleanOps` trait does support polygon-vs-anything via `BooleanOps::intersection`, but our wrapper accepts only Polygon/MultiPolygon.

The bench worked around this by switching to `ST_Contains(buffered_polygon, point)`, which loses the per-row intersection cost the workload was meant to exercise.

To close: extend `core::functions::operations::st_intersection` to accept any geometry type for both arguments. Likely requires dispatching by geometry-type pair to the right `geo` algorithm (Point-Point identity, Point-Polygon containment, Line-Polygon clip, Polygon-Polygon BooleanOp). Add doctests covering each shape combination.

## Naming differences vs SpatiaLite (not sqlitegis bugs, but worth noting)

These are SpatiaLite's own naming quirks vs the PostGIS surface sqlitegis follows. They affect bench portability and any user porting queries between the two libraries.

| Operation | PostGIS / sqlitegis | SpatiaLite 5.1.0 |
|---|---|---|
| Sphere-Haversine distance | `ST_DistanceSphere(g1, g2)` | `ST_Distance(g1, g2, 0)` (3-arg form, `use_ellipsoid = 0`) |
| Ellipsoid distance | `ST_DistanceSpheroid(g1, g2)` | `ST_Distance(g1, g2, 1)` (3-arg form, `use_ellipsoid = 1`) |
| Axis-aligned envelope from bounds | `ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid)` | not present; use `BuildMbr` or hand-rolled `ST_GeomFromText('POLYGON((...))')` |

SpatiaLite 5.1.0 removed `GreatCircleDistance` (present in earlier 4.x releases). Tests targeting both libraries on geodesic workloads must branch by library or use only the 3-arg `ST_Distance` form.
