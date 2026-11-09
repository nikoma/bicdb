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
on the next drain. Each deferred foreign-key check now borrows and restores the
existing session metadata cache. Custom trigger bodies retain their existing
execution scopes. Simple-query Oracle lowering also borrows the connection cache,
following the Parse/Describe path. Commit durability, sync ordering, transaction
boundaries and original application assertions/timeouts remain unchanged.

An initial cache scope around the whole drain incorrectly retained policy
scalar results across custom trigger calls. The direct-protocol af34d117 control
read one protected row twice after the first trigger changed the policy's data;
the merged predecessor and native PostgreSQL read one row, then zero. The
tests-only 32c669ca regression reproduces the failure. Restricting the new scope
to each foreign-key check preserves both metadata reuse and policy visibility.
The native transaction is retained in
`crates/bicdb-sql/tests/fixtures/postgres/deferred_policy_visibility_pg16.json`.

This batch requires broad SQL and pgwire validation, its own versioned-head CI,
and an optimized original-application rerun. The 77403dca predecessor still fails
the report's unchanged ten-second wait despite passing the financial report and
all audit tests. Focused regressions do not establish application acceptance.
The af34d117 optimized rerun also passes seven of eight original methods: the
NULL-bound field lookup falls from 1.1–1.4 seconds to about 2–3 ms, but the
report's original ten-second wait still fails. Further latency work remains.

## Qualified binary at 7dbfc8b1 (1.0.453-beta)

The stable candidate passes 2,387 SQL tests across 232 targets, with four existing
ignores, and the locked workspace/all-target compilation check. Independent
candidate CI reports the same SQL counts. The standard Rust 1.96 release build
uses the repository's thin-LTO and line-table settings. Its unstripped SHA-256 is
`49d2f7a52a7842c0baf5c63fe9e5ef383ecbad20c9771df9b14531eafb143a6d`.

The canonical stripped artifact, SHA-256
`191e9378c02517fa8f3de12fecc6b4702b49fc32985c0a0ef1434796a415dfd8`,
passes 14 full and seven large-transaction hard VM reset cases. Independent
read-only verification passes for all 21 saved crash images. The large campaign
initially stopped before starting because the launcher used a filename outside
its `large-tx-` safety prefix; that wrapper failure is retained and the campaign
passes after correcting the launcher. These are VM reset tests, not physical
power-loss or production deployment evidence. The same canonical artifact passes
four live JDBC modes: 89 checks each for extended, simple and extended-cache
queries, plus the expected PostgreSQL-profile rejection check.

The unchanged original eight report/audit methods still pass seven of eight on
the older pristine database. A separate controlled comparison of all 915
business tables, every column and complete row multisets finds identical values
after the newer engine opens the database. Actual relation/routine ownership and
222 stored routine/view definitions also match. The only reported column-metadata
differences are the documented #49 timestamp type-name labels: 3,364 `timestamp`
and three `timestamptz` labels become their full PostgreSQL names. A fresh-copy
application control after this preparation passes all eight methods once, then
fails the report deadline on repetition. Preparation does not reliably resolve
the deadline, and the original first-open failure remains valid evidence.

## Read-only schema borrowing follow-up

A same-run profile of the original report Future wait at 7dbfc8b1 records
10.000060511 seconds and 411 native CPU samples, with no lost samples. Samples
under schema cloning include Oracle national/temporal column resolution and
foreign-key parent validation. Four read-only call sites now use
`load_schema_shared`, preserving the existing cache generation checks,
partition reconciliation, validation and errors while avoiding deep copies of
the complete table definition. This follow-up does not change transaction
durability or the original application assertions and ten-second waits. Earlier
7dbfc8b1 qualification belongs to that binary; the follow-up requires its own
source-pinned checks and application result.
