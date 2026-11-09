# NULL scalar index bounds and commit metadata

An equality index probe evaluated an independent scalar query to SQL NULL, then
fell through to a full table scan. SQL equality cannot match that key. The
locator now returns an empty candidate set after the existing evaluation, when
the scalar query is pure and there is no outer row. Outer-row placeholders keep
the existing deferred path. The scalar query is evaluated once at the probe;
its errors and purity checks retain their existing behavior.

The original iDempiere report exposed this through a field lookup whose MIN
subquery has no matching tab. On the owned analysis clone, the equivalent literal
NULL query took about 3 ms; the scalar-bound query took 1.1–1.4 seconds. The
non-NULL control already used its index. The tests-only 39fb5c51 source returned
the correct empty result but materialized 5,000 outer rows for both NULL controls.
All three controls pass at 570c88ee, including the non-NULL 50-row result. Native
PostgreSQL 16.13 confirms the 50/0/0 results in a rolled-back temporary fixture;
its execution plan is retained in `crates/bicdb-sql/tests/fixtures/postgres/scalar_null_index_pg16.json`.
A correlated NULL-first control also checks subsequent non-NULL bindings and
LEFT JOIN/LATERAL row preservation.

The same report's exact ten-second Future wait contains substantial commit work.
A same-run Java/native trace records the boundary; the existing query logger's
interior whole seconds contain 136 commits and roughly 5.2 seconds of their
execution time. Commit-time deferred foreign-key checks loaded schemas outside
the connection's existing cache scope. The tests-only 49585066 control passes
referential-error/rollback checks but decodes two warmed schemas (1,775 bytes)
on the next drain. Deferred drains now borrow and restore the existing session
metadata cache. Simple-query Oracle lowering also borrows the connection cache,
following the Parse/Describe path. Commit durability, sync ordering, transaction
boundaries and original application assertions/timeouts remain unchanged.

This batch requires broad SQL and pgwire validation, its own versioned-head CI,
and an optimized original-application rerun. The 77403dca predecessor still fails
the report's unchanged ten-second wait despite passing the financial report and
all audit tests. Focused regressions do not establish application acceptance.
