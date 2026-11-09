# Selective filters across outer joins

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
