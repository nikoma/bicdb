# Keep unresolved row types out of the routine-variable text fallback

The optimized f8225b8 native EHR candidate passed the 163-migration schema/API
gate, but its hosted therapeutic-content workflow failed with 42804:
`CASE types int4 and text cannot be matched`. The same query with constant
arguments outside a SQL function worked.

A minimal reproduction is a SQL function with a CTE containing an integer `n`
and a CASE returning `LEAST(t.n,t.n)` or zero. The row typer correctly leaves
unresolved CTE columns unknown; the routine-variable fallback then called the
context-free function typer, which interpreted all-unknown inputs as text.
CASE consequently rejected a valid numeric branch.

BoundExprScope now resolves common-type function arguments recursively. An
unresolved row expression is kept unknown instead of manufactured text. Unknown
literal-only calls still resolve to text. Explicit incompatible CASE branches
are still rejected. LEAST, GREATEST, COALESCE, NULLIF and IFNULL share this rule.

Three regressions fail on the old engine and pass with the repair: common-type
functions over CTE columns; dated adherence calculation inside a SQL routine;
and the actual EHR therapeutic-status function, including two completions out of
four expected days, 50% adherence, and today's completion state. Validation also
passes the SQL unit suite, eight focused typing/routine/aggregate/sequence
suites and the full 183-test wire-protocol suite. Exact output, compiler
completion, source and artifact hashes are attached in validation.json.

Two test-authoring diagnostics are not counted as completed regression proof:
the initial Rust fixture needed the existing Arc conversion for SqlValue::Json;
the first repaired run then reached a previously unreachable fixture INSERT
whose synthetic event IDs were missing. Explicit IDs were added. The CASE
queries and their old-engine failures were unchanged; the final repaired batch
passes all three tests, including the subsequent completion readback.

Attached text starts at the running-tests marker; full original Cargo logs
remain on the qualification host and their hashes are recorded. Rebuilt test
executables were hashed before retirement; reusable dependencies and frozen
native candidates were retained. No application retry or conditional type
validation was disabled. Full hosted/load/worker qualification and production
cutover remain separate gates. Production EHR remains on PostgreSQL.
