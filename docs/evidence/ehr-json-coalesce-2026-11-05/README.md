# JSON COALESCE fallbacks retain their JSON shape

Base: latest fetched main `c99665d97fa1ed0ca5676ba41807e20709f6cf18`, including
the November 5 persistent BLOB changes and PostgreSQL catalog preservation
for the Oracle driver. The earlier repeated-summary AST fix `0f62888` is included.

The hosted EHR returned empty practice/factor/program/group histories as the
JSON string `"[]"`. SQL correctly inferred a JSON/JSONB COALESCE result, but
the selected unknown text fallback reached `jsonb_build_object` without being
coerced to that inferred type. Populated histories already contained JSON,
making the API's shape depend on whether data existed.

A shared JSON-only coercion helper now applies that inferred JSON/JSONB type
to the selected value in constant, row, aggregate, target-record and routine
evaluators. COALESCE still evaluates only the selected branch. Other result
types retain their existing value conversion behavior; this is not a claim
of complete PostgreSQL conditional-expression coercion parity.

The new integration regression covers ten expressions: empty aggregates,
scalar subqueries, nullable/non-null JSON columns, stored routines, JSON and
JSONB, object fallback values, a failing unselected branch, and UPDATE RETURNING.

- `negative.log`: after restoring unchanged `c99665d` source, the new test fails
  because `{"items":"[]"}` is returned instead of `{"items":[]}` (exit 101).
- `positive.log`: after restoring the repair, all 85 focused integration tests
  pass across JSON storage/SRF/filter/composite, Carrier workflows, type
  consistency, null arithmetic and typed comparisons.
- `unit.log`: 676 SQL library tests pass; one existing benchmark is ignored.
  Command: `cargo test --locked -p bicdb-sql --no-default-features --features extension-host --lib`.
- `protocol.log`: all 180 pgwire protocol tests pass.
- `rustfmt --check` and `git diff --check` pass.

The initial library/queued integration runs on the preceding upstream revision
were deliberately interrupted (exit 143) when `c99665d` arrived. They are not
passing evidence. The retained logs above use the integrated latest base.

Builds used two jobs, no incremental compilation, active RAM/disk monitoring,
and stripped test debug information on the task-owned host. Superseded test
executables and orphaned task cache were retired; reusable dependency artifacts
were preserved in RAM with their original timestamps. The RAM cache was about
18.56 GB after cleanup. Frozen native `4dc286e` binaries remain available for
baseline comparison. No production data or services were changed.

A new exact-source binary and the hosted EHR clinical checks remain required.
Program creation, duplicate-goal verification and production cutover are still
open; this engine test result does not claim clinical acceptance.
