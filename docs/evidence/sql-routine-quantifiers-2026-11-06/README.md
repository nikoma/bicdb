# Procedural quantifiers and the actual monitoring validator

Source: `67ff6c5c79574508f98c579c769b5b25cf4e2d0b`.

WalkNorth monitoring validation progressed past scalar query loops and exposed
procedural `ANY`/`ALL` operands falling through to constant-only evaluation.
The session evaluator now resolves their routine variables, parameters, JSON
extractions, arrays and subqueries from the active frame. Existing three-valued
comparison logic is reused. Known scalar right operands remain invalid; catalog
vectors use their declared element types instead of treating ordinary text as a
vector.

Related cases are covered in the same batch:

- Query-loop OID aliases retain OID identity for scalar targets, while text,
  text domains and records retain their expected display values.
- `generate_series` arguments resolve routine inputs at execution, without
  invoking dynamic functions during cardinality estimation. Mutating per-row
  functions remain explicitly unsupported and leave data unchanged; this change
  does not broaden that boundary.
- JSONB numbers cast directly to exact NUMERIC values. Non-number JSON kinds
  produce PostgreSQL-16-compatible `22023` errors. The constants-only PostgreSQL
  reference observations are retained; no clinical rows were queried.
- The full real application validator, its public synthetic configuration and
  source hashes are retained as fixtures. It checks valid configuration,
  unknown-measurement rejection and serialized UTF-8 size bounds.

Once the earlier semantic failures were fixed, that full validator exposed an
unoptimized interpreter stack overflow. Outlining expression arms and numeric
conversion removes large temporaries from recursive evaluation. **The test stack
was not enlarged**: the complete application regression explicitly uses a
2 MiB thread, independent of `RUST_MIN_STACK`.

Actual local evidence:

- Unfixed runtime with the then-current regressions: **43 passed, eight failed**.
- Intermediate semantic fixes without stack outlining: retained stack-overflow
  failures in the v3/v4 logs. These runs did not pass.
- Focused final behavior: **52 passed**, zero failures, under the ordinary test
  stack configuration (v5).
- Final source, including the explicit 2 MiB regression:
  `cargo --config profile.dev.debug=0 test -p bicdb-sql --lib -- --test-threads=2`:
  **757 passed, zero failed, four ignored**, 160.73 seconds. Ignored tests were not
  executed. `cargo fmt --all -- --check` passed.

The batch used one Cargo job, disabled incremental compilation and an active
12 GiB available-RAM / 8 GiB free-disk guard. No test assertions from the actual
application validator were weakened. The public application fixture's size
guard comes from the pending EHR operator migration identified in its source
manifest; it does not pretend PostgreSQL physical-storage sizing exists here.

This establishes interpreter behavior and its regression coverage. Production
runtime builds, complete EHR workflows, clinical-data reconciliation, physical
HA failure drills and production admission remain separate gates.

Latest-main integration `3c2c40d639716d9b7d07547f2cadd358e4cf4d0c` includes the concurrent residual correlated EXISTS changes and national-character reference observations from `cbc5d9f`. The complete SQL library passes again: **760 passed, zero failed, four ignored**, in 162.43 seconds. The actual integration log is retained alongside the original causal evidence. Formatting also passes. Runtime build and production application acceptance remain separate.
