# Known sqlitegis limitations

A running list of API gaps and incompatibilities surfaced during development. Each entry names where it was discovered and what would close it.

## Naming differences vs SpatiaLite (not sqlitegis bugs, but worth noting)

These are SpatiaLite's own naming quirks vs the PostGIS surface sqlitegis follows. They affect bench portability and any user porting queries between the two libraries.

| Operation | PostGIS / sqlitegis | SpatiaLite 5.1.0 |
|---|---|---|
| Sphere-Haversine distance | `ST_DistanceSphere(g1, g2)` | `ST_Distance(g1, g2, 0)` (3-arg form, `use_ellipsoid = 0`) |
| Ellipsoid distance | `ST_DistanceSpheroid(g1, g2)` | `ST_Distance(g1, g2, 1)` (3-arg form, `use_ellipsoid = 1`) |
| Axis-aligned envelope from bounds | `ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid)` | not present; use `BuildMbr` or hand-rolled `ST_GeomFromText('POLYGON((...))')` |

SpatiaLite 5.1.0 removed `GreatCircleDistance` (present in earlier 4.x releases). Tests targeting both libraries on geodesic workloads must branch by library or use only the 3-arg `ST_Distance` form.
