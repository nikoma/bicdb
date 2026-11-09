# Selective filters and preparation metadata

A narrow catalog query with several inner joins and a LEFT JOIN previously
materialized catalog-wide intermediate rows before applying its base OID filter.
The unchanged financial-report application test exposed this by hitting the
512 MiB join-work guard. The guard remains enabled at the same limit.

The executor now pushes qualified, simple column/literal predicates into a known
virtual base table when subsequent joins preserve its rows (inner/cross/left).
Functions, casts, custom operators, subqueries and ambiguous unqualified names
stay on the existing path. Final WHERE evaluation remains in place.

RIGHT and FULL joins can synthesize NULL base rows. Their base WHERE filters are
kept after matching: filtering beforehand can discard a matching left row and
then invent an unmatched right row that passes `left.value IS NULL`. This also
repairs the previously existing physical-table path.

Focused source 9cb2dfa4 reproduced two failures: a four-row catalog result exceeded
a 2 MiB envelope, and a RIGHT JOIN returned an extra synthetic NULL row. Two
other outer-join controls already passed. Source 376f8521 passed all four. The
native PostgreSQL 16.13 control transaction, rolled back without changing the
reference database, is retained in
`crates/bicdb-sql/tests/fixtures/postgres/selective_outer_join_filters_pg16.json`.
The integrated tests additionally assert its nullability and identity results.

This is general predicate handling, not an application/table-name exemption or
a larger memory allowance. The original background-job 10-second wait remains
unchanged and requires an application rerun before any timeout-resolution claim.

The broad 2f05e39f SQL run also reached an older engine test whose name explicitly
stated that it documented the incorrect RIGHT JOIN null-extension behavior.
Its expected extra matched row is replaced with PostgreSQL's genuinely
unmatched row; the original failure and source remain retained. The native
control includes that exact fixture. This does not change any application test.

Early filtering must also distinguish quoted aliases such as "c" and "C".
Virtual filter eligibility uses exact bound column spelling, and materialized
column lookup preserves a complete exact identity before its legacy folded
fallback. A separate LEFT JOIN control checks that a nullable-side predicate
cannot be captured by a case-distinct base alias.

The broad run at b6af29cd exposed two existing lookup regressions:
legacy UPDATE layouts can retain an uppercase qualifier even though an unquoted
reference has already been folded. Keep whole-name identity first, then a
separate unambiguous qualifier-folded lookup that preserves column spelling.
The original quoted-column and UPDATE FROM assertions remain unchanged, with an
additional control covering case-distinct aliases and columns together.

Source 0875a692 passed the complete 232-target SQL run (2,368 tests, four
measurement ignores), the 120-target core run (1,289 tests and 11 recorded
helper/manual/optional-model ignores), and locked workspace/all-target
compilation. Its standard optimized build passed the unchanged financial report
test that previously rejected the catalog join. The background report still
exceeded its original 10-second wait; AuditTraceContext passed all three tests.

A separate profile of the retained f0976fd4 diagnostic attributed 38.84% of
sampled backend CPU in a 25-second report window to `load_schema_raw_shared`
(cumulative samples, overlapping its descendants). Preparation helpers ran
without the existing execution cache scope. The tests-only 4613dc20 regression
reproduced 64 decodes of one table definition during a single Oracle lowering
operation, and two decodes during origin inference. Three controls already
passed. The earlier f7ed59bf attempt stopped at a missing test import and is
retained as a compilation failure, not runtime defect evidence.

Preparation now enters the existing nested `SqlSchemaCacheScope`: inferred
parameters, output types, origins and Oracle rewrites share each loaded schema
within their operation. An existing outer execution scope keeps ownership;
otherwise the cache is released when preparation returns. Later preparation
observes DDL and rollback through a fresh scope. No storage format or protocol
contract changes.

All five controls pass at 032ab77b with the original assertions retained. Four
preparation controls run in both storage modes; a further ServerPaged control
checks description freshness across ALTER and ROLLBACK. These focused results
do not establish that the original background timeout is resolved. The combined
version 452 source also includes main 5aa8a5ff's authenticated bounded reads and
requires its own broad checks and optimized application rerun.
