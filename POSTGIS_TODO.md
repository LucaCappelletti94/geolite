# PostGIS Alignment TODO (Research-Only)

Date: 2026-03-04

Scope:
- Document how PostGIS/PostgreSQL handle the 6 review findings.
- List possible solution directions for geolite.

Non-goals:
- No code changes.
- No migrations.
- No implementation details beyond option-level design.

## Brief Context In geolite

1. `ST_DWithin` negative-distance behavior is aligned across planar/geodesic variants.
Relevant files:
- `geolite-core/src/functions/predicates.rs` (planar `st_dwithin`, geodesic `st_dwithin_sphere`, `st_dwithin_spheroid`)
- `geolite-sqlite/tests/support/shared_cases.rs` (planar/geodesic negative-distance rejection tests)

2. Geodesic distance functions are currently Point-only and strict on SRID 4326.
Relevant files:
- `geolite-core/src/functions/measurement.rs` (`parse_two_geographic_points`, `st_distance_sphere`, `st_distance_spheroid`)
- `geolite-core/src/functions/predicates.rs` (geodesic `dwithin` wrappers and tests)

3. Wall-clock speed assertions are in normal test flows.
Relevant files:
- `geolite-sqlite/tests/support/shared_cases.rs` (index speed test section)
- `geolite-diesel/tests/diesel_test_helpers.rs` (index speed test section)
- `geolite-diesel/tests/sqlite_integration.rs` (deterministic query-plan style tests already present)

4. Extension loading confidence currently includes a symbol-export inspection test.
Relevant files:
- `geolite-sqlite/tests/integration.rs` (`nm -D` symbol checks)
- `geolite-sqlite/src/ffi.rs` (SQLite entrypoints and registration)

5. Function declarations are repeated across multiple surfaces.
Relevant files:
- `geolite-core/src/function_catalog.rs` (canonical catalog metadata)
- `geolite-sqlite/src/ffi.rs` (callback arrays + parity asserts)
- `geolite-diesel/src/functions.rs` (many `define_sql_function!` declarations)
- `geolite-diesel/tests/expression_methods.rs` (surface parity checks)

6. Structural parity checks are strong, but semantic parity is partial by surface.
Relevant files:
- `geolite-sqlite/src/ffi.rs` (semantic smoke over catalog SQL)
- `geolite-diesel/tests/expression_methods.rs` (signature/method parity checks)

---

## Case 1: `ST_DWithin` Negative Distance Semantics

### Status
- Completed on 2026-03-04.

### Current geolite behavior
- Planar `ST_DWithin` rejects non-finite distances and rejects negative finite distances.
- Geodesic `ST_DWithinSphere` and `ST_DWithinSpheroid` reject negative distances.

### How PostGIS handles it
- PostGIS public docs define `ST_DWithin` signatures and units for geometry/geography.
- PostGIS geometry implementation (`LWGEOM_dwithin`) explicitly errors on negative tolerance (`tolerance < 0`).
- Inference: geometry `ST_DWithin` treats negative distance as invalid input, not as a valid false predicate.

### References
- PostGIS `ST_DWithin` docs: https://www.postgis.net/docs/ST_DWithin.html
- PostGIS source (`LWGEOM_dwithin`, negative tolerance check): https://postgis.net/docs/doxygen/3.5/d3/dcc/lwgeom__functions__basic_8c_source.html

### Considered solutions
- Option A (compat-first): reject negative distance in geolite planar `ST_DWithin` to align with PostGIS geometry behavior.
- Option B (strict SQL semantics doc): keep current planar behavior but document deliberate divergence and add explicit compatibility mode toggle.
- Option C (uniform policy): enforce one shared distance validator across planar/geodesic variants.

### Decision (locked)
- Strict PostGIS parity is required by default (no compatibility flag for negative planar distance).

### TODO
- [x] Decide default negative-distance policy for planar `ST_DWithin`.
- [x] Record policy in docs and test matrix before implementation.
- [x] Implement and validate planar negative-distance rejection across core, SQLite, and Diesel tests.

---

## Case 2: Geodesic Distance Type Support (`Point`-only vs broader geometry support)

### Status
- Completed on 2026-03-04.

### Current geolite behavior
- `ST_DistanceSphere` and `ST_DistanceSpheroid` require Point inputs and SRID 4326.
- `ST_DWithinSphere`/`ST_DWithinSpheroid` are implemented as wrappers over those Point-only measurements.

### How PostGIS handles it
- PostGIS `ST_DistanceSphere` and `ST_DistanceSpheroid` docs note support for geometry types beyond points (since 1.5).
- PostGIS distance-radius predicate is represented by `ST_DWithin(..., use_spheroid)` on geography, instead of separate `ST_DWithinSphere`/`ST_DWithinSpheroid` function names.
- Inference: PostGIS favors broader geodesic coverage and a unified predicate API for sphere/spheroid choice in geography mode.

### References
- PostGIS `ST_DistanceSphere`: https://postgis.net/docs/ST_DistanceSphere.html
- PostGIS `ST_DistanceSpheroid`: https://postgis.net/docs/ST_Distance_Spheroid.html
- PostGIS `ST_DWithin` (geography signature with `use_spheroid`): https://www.postgis.net/docs/ST_DWithin.html
- PostGIS special functions index (function naming surface): https://postgis.net/docs/en/PostGIS_Special_Functions_Index.html

### Considered solutions
- Option A (parity expansion): extend geodesic distance internals to non-Point geometry combinations where practical.
- Option B (API normalization): add a unified geodesic `dwithin` form aligned to `use_spheroid` semantics, keep current names as aliases.
- Option C (scope lock): explicitly keep Point-only scope and document it as a deliberate subset with clear errors.

### Decision (locked)
- geolite keeps the current Point-only behavior as a documented compatibility subset for now, but treats broader geodesic geometry support as planned PostGIS-parity work.
- API direction is normalization-first: target one unified geodesic `dwithin` form with a spheroid selector, and preserve `ST_DWithinSphere`/`ST_DWithinSpheroid` as compatibility aliases once that form exists.

### Geodesic Input Support Matrix (Policy)

| Input pair | Current support (2026-03-04) | Policy target |
|---|---|---|
| Point ↔ Point | Supported | Keep supported |
| Point ↔ LineString | Not supported (`requires Point` error) | Phase 1 parity expansion |
| Point ↔ Polygon | Not supported (`requires Point` error) | Phase 1 parity expansion |
| LineString ↔ LineString | Not supported (`requires Point` error) | Phase 2 parity expansion |
| LineString ↔ Polygon | Not supported (`requires Point` error) | Phase 2 parity expansion |
| Polygon ↔ Polygon | Not supported (`requires Point` error) | Phase 2 parity expansion |

Notes:
- Matrix rows are symmetric; reverse-order input pairs share the same status.
- Expansion phases are policy-level commitments only; implementation sequencing stays out of scope for this document.

### TODO
- [x] Define supported geodesic input type matrix (Point/Line/Polygon combinations).
- [x] Decide whether to preserve current function names as primary API or aliases.

---

## Case 3: Timing Assertions Inside Regular Tests

### Current geolite behavior
- Some tests assert indexed query time is faster than full scan based on wall-clock timing.
- Deterministic query-plan tests also exist (good baseline style).

### How PostGIS/PostgreSQL handle it
- PostgreSQL regression guidance emphasizes deterministic expected output and warns about environment-sensitive differences.
- PostGIS build/test guidance uses `make check` and extension test flows, aligned with deterministic SQL behavior verification.
- Inference: ecosystem standard is correctness/regression determinism in regular CI; performance is usually isolated into benchmark or specialized test lanes.

### References
- PostgreSQL regression tests: https://www.postgresql.org/docs/current/regress.html
- PostgreSQL test evaluation (platform variance and plan/config sensitivity): https://www.postgresql.org/docs/current/regress-evaluation.html
- PostGIS manual (`make check`, extension test flow): https://postgis.net/docs/manual-3.5/postgis-en.html

### Considered solutions
- Option A (recommended): move wall-clock assertions to ignored/perf-only test target; keep regular tests deterministic.
- Option B: replace timing assertions in normal tests with structural plan assertions and candidate-count checks.
- Option C: keep timing tests but gate by environment marker and make non-blocking in default CI.

### Decision (2026-03-04)
- Adopt Option A as baseline and keep Option B-style deterministic assertions in default CI.
- Performance regression checks run in a dedicated `Performance` workflow lane, not in the default PR-gating `CI` workflow.
- Perf lane enforcement scope: runs on `main` pushes, scheduled weekday runs, and manual dispatch.
- Stability constraints for perf assertions:
  - isolate to a dedicated lane and fixed runner class (`ubuntu-latest`);
  - run one perf assertion per test invocation with single-threaded test execution;
  - keep wall-clock checks out of deterministic PR-gating lanes.

### TODO
- [x] Define a deterministic-vs-performance test policy.
- [x] Assign timing checks to explicit perf lane or benchmark tooling.

---

## Case 4: Extension Loading Model vs Symbol-Export Test

### Current geolite behavior
- Native test inspects compiled shared object symbols using `nm -D` and hardcoded debug-path assumptions.

### How PostGIS/PostgreSQL handle it
- PostgreSQL extension model centers on `CREATE EXTENSION` and extension control/script packaging.
- PostGIS operational docs center on enabling and upgrading through SQL extension mechanisms.
- Inference: runtime SQL-level extension loading/behavior validation is the primary compatibility contract; raw symbol table inspection is a low-level packaging check, not the core behavior contract.

### References
- PostgreSQL `CREATE EXTENSION`: https://www.postgresql.org/docs/current/sql-createextension.html
- PostgreSQL extension packaging model: https://www.postgresql.org/docs/current/extend-extensions.html
- PostGIS spatial enable via extension: https://postgis.net/docs/manual-3.5/postgis-en.html

### Decision (2026-03-04)
- Public extension compatibility contract is SQL runtime load-and-call behavior.
- Symbol visibility checks are retained only as a packaging sanity signal.
- Symbol checks run in a dedicated non-gating Linux packaging workflow, not in PR-gating CI tests.

### TODO
- [x] Define extension-contract test boundary (SQL behavior vs binary export surface).
- [x] Decide if symbol checks remain and under what CI scope.

---

## Case 5: Multi-Surface Declaration Duplication (Catalog/FFI/Diesel)

### Current geolite behavior
- Function signatures live in several places with parity checks to reduce drift.
- Duplication still increases maintenance overhead and drift risk.

### How PostGIS/PostgreSQL handle it
- PostgreSQL extension system uses versioned control/scripts as install/update source of truth.
- PostGIS follows extension packaging and update path model (single extension lifecycle contract per version).
- Inference: one authoritative declaration layer plus generated/validated derivatives is the dominant model.

### References
- PostgreSQL extension packaging and update scripts: https://www.postgresql.org/docs/current/extend-extensions.html
- PostgreSQL `CREATE EXTENSION` behavior: https://www.postgresql.org/docs/current/sql-createextension.html
- PostGIS extension upgrade lifecycle: https://postgis.net/docs/PostGIS_Extensions_Upgrade.html

### Possible solutions (do not implement yet)
- Option A (recommended): keep `function_catalog` as canonical source; generate FFI callback specs and Diesel declarations from it.
- Option B: move to macro-based single declaration DSL consumed by all surfaces at compile time.
- Option C: retain current duplication but enforce stricter generated parity manifests in CI.

### Open decision
- Preferred generation boundary: build-time codegen, macro expansion, or runtime metadata checks only.

### TODO
- [ ] Select canonical declaration mechanism and ownership.
- [ ] Define generated artifact policy and CI drift checks.

---

## Case 6: Structural Parity vs Semantic Parity Coverage

### Current geolite behavior
- Structural parity checks exist (signatures, callback ordering, Diesel method/function mapping).
- Semantic smoke exists in SQLite registration tests, but cross-surface semantic equivalence is not fully centralized.

### How PostGIS/PostgreSQL handle it
- PostgreSQL regression model is expected-output based and semantics-oriented.
- PostGIS test workflows emphasize SQL behavior validation through extension-enabled runs.
- Inference: structural checks are useful guardrails, but semantic golden tests are the higher-confidence contract for compatibility.

### References
- PostgreSQL regression chapter: https://www.postgresql.org/docs/current/regress.html
- PostgreSQL test evaluation guidance: https://www.postgresql.org/docs/current/regress-evaluation.html
- PostGIS extension testing workflow (`make check`, extension mode): https://postgis.net/docs/manual-3.5/postgis-en.html

### Possible solutions (do not implement yet)
- Option A (recommended): define a shared semantic-golden suite keyed by canonical function catalog entries.
- Option B: enforce per-function semantic checks across core/SQLite/Diesel using shared fixtures for normal and error cases.
- Option C: keep structural checks as fast guardrail and add targeted semantic packs for high-risk functions first.

### Open decision
- Target depth of semantic parity: full catalog now vs phased high-risk rollout.

### TODO
- [ ] Define semantic-golden schema (inputs, expected class/value/error class).
- [ ] Choose phased rollout order for semantic parity enforcement.

---

## Decision Matrix (Research Summary)

| Case | PostGIS baseline | Recommended default for geolite | Alternative | Main risk if deferred |
|---|---|---|---|---|
| 1 | Negative tolerance invalid for geometry `ST_DWithin` | Reject negative distance consistently | Compatibility toggle | Silent behavior mismatch |
| 2 | Geodesic distance supports broader geometry scope; unified geography `ST_DWithin` options | Keep documented Point-only subset now, then phase in broader geometry support and normalize to unified `dwithin` + aliases | Keep Point-only subset indefinitely | User confusion and portability gaps |
| 3 | Regression style is deterministic, not wall-clock | Move timing checks to perf lane | Keep timing checks as non-gating | CI flakes and non-actionable failures |
| 4 | SQL extension loading is primary contract | Validate load+call contract in tests | Optional symbol-export smoke | Brittle platform/path failures |
| 5 | Extension scripts/control form source-of-truth lifecycle | Single canonical declaration + generation | Stronger drift CI only | Ongoing duplication debt |
| 6 | Semantics-first regression culture | Shared semantic-golden suite | Structural checks plus partial semantic packs | False confidence from shape-only parity |

---

## Suggested Adoption Order (Non-Implementation)

1. Case 2 policy is locked (2026-03-04): documented subset now, phased parity expansion, unified `dwithin` direction with compatibility aliases.
2. Stabilize test strategy for Case 3 and Case 6 (highest CI signal quality impact).
3. Clarify extension contract scope for Case 4 (reduces brittle native-only failures).
4. Choose declaration architecture for Case 5 (larger refactor, best after semantic policy is fixed).

---

## Assumptions And Defaults Used In This Research

- PostGIS docs and PostgreSQL docs are the primary sources of behavioral intent.
- Where PostGIS behavior is taken from C source, it is marked as inference from implementation details.
- Recommendations are option-level only; this document intentionally omits implementation tasks.
