# Geolite Audit TODO

Date: 2026-03-02

This file tracks confirmed issues from the crate audit and the plan to fix them.

## Work Order

1. P0: spatial index name collision
2. P0: rowid update desync
3. P1: `ST_Project` non-finite input handling
4. P2: function catalog duplication
5. P2: Diesel API duplication maintenance cost

---

## P0: Spatial Index Name Collision

Status: [x]

Problem:
- `CreateSpatialIndex` names index objects as `{table}_{column}_...`.
- Different `(table, column)` pairs can collide (example: `("a_b","c")` vs `("a","b_c")`).
- This can cause one table to reuse another table's R-tree/triggers incorrectly.

Tasks:
- [x] Add fail-fast collision detection for trigger/index object names (compatibility-preserving).
- [x] Keep current naming pattern for existing query templates.
- [x] Add regression test for colliding name pairs.
- [x] Add migration/compat behavior notes for previously created indexes.

Compatibility notes:
- Existing valid indexes created with `{table}_{column}_...` names continue to work unchanged.
- A second index whose generated object names would collide now fails fast with an explicit error.
- To resolve collisions, rename one conflicting table/column pair and recreate that index.

Acceptance:
- A colliding second index creation fails with a clear error (no silent reuse).
- Existing index objects remain intact and associated with the original table.

---

## P0: Spatial Index Not Updated on `rowid` Change

Status: [x]

Problem:
- Update trigger is `AFTER UPDATE OF [column]`, so updates that change `rowid` only are not captured.
- Result: stale `id` entries in R-tree and broken index lookups.

Tasks:
- [x] Update trigger logic to handle rowid changes safely.
- [x] Add regression test: `UPDATE table SET rowid = ...` keeps R-tree in sync.
- [x] Verify index-backed query patterns still return exact row sets.

Acceptance:
- After rowid-only updates, R-tree `id` values match table rowids.
- No stale rows remain in `<table>_<column>_rtree`.

---

## P1: `ST_Project` Accepts Non-Finite Inputs

Status: [x]

Problem:
- `ST_Project` currently accepts non-finite numeric inputs (`INF`/`NaN`), which can produce `POINT EMPTY` silently.
- This behavior is surprising and hides invalid input.

Tasks:
- [x] Validate `distance` and `azimuth` are finite in core (`geolite-core`).
- [x] Return explicit `InvalidInput` errors for non-finite values.
- [x] Add tests in core and sqlite adapter for non-finite inputs.

Acceptance:
- Non-finite inputs return clear errors.
- No silent conversion to `POINT EMPTY`.

---

## P2: Function Catalog Duplication

Status: [x]

Problem:
- SQLite function list is defined in `geolite-core/src/function_catalog.rs`.
- Callback mapping list is duplicated in `geolite-sqlite/src/ffi.rs`.
- Current runtime checks reduce risk, but maintenance cost is high.

Tasks:
- [x] Reduce duplication by deriving registration mapping from a single source of truth.
- [x] Keep compile-time or startup-time coverage checks.
- [x] Preserve behavior and function names/arity.

Notes:
- `geolite-sqlite` now uses `geolite_core::function_catalog` as canonical source for function names/arity.
- SQLite keeps only ordered callback lists, and registration zips catalog signatures to callbacks.
- Unit test `register_functions_covers_full_catalog` exercises all catalog signatures to catch drift.

Acceptance:
- Adding/removing a function requires one canonical declaration update.
- Coverage tests still fail fast on missing registrations.

---

## P2: Diesel Surface Duplication / Maintenance Overhead

Status: [x]

Problem:
- Function declarations, method wrappers, and many test paths duplicate the same API surface.
- Not a functional bug, but expensive to evolve and easy to drift.

Tasks:
- [x] Identify codegen/macro options to reduce repeated declarations.
- [x] Consolidate repeated test patterns where possible.
- [x] Keep API and docs stable.

Notes:
- `geolite-diesel/tests/expression_methods.rs` now uses grouped assertion macros instead of dozens of hand-written near-identical tests.
- Coverage now also includes operation and predicate wrappers that were previously missing from method-vs-function SQL parity checks.
- Public API/docs stayed unchanged; full `cargo test -p geolite-diesel --features sqlite` remains green.

Acceptance:
- Meaningfully fewer manually duplicated entries.
- Existing behavior and test coverage remain unchanged.
