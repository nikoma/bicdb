# Recursive query analysis on ordinary worker stacks

A correlated query with nested predicates could exhaust the normal 2 MiB
protocol worker stack during Oracle type analysis. The problem appeared after
structured constraint diagnostics increased the payload carried by recursive
AST visitors. The previous binary executed the query; the diagnostic candidate
aborted before executing its rows. Default PostgreSQL execution remained intact.

The arithmetic/type resolver and lock-policy scanner now carry boxed diagnostics
as their visitor break values. Successful traversal carries a pointer-sized
control value through generated recursive frames. An actual error is unboxed at
the existing API boundary, preserving its SQLSTATE, message and structured fields.
Worker stack sizes, query semantics and authorization policies are unchanged.

The [neutral regression fixture](../../../crates/bicdb-sql/tests/fixtures/correlated_predicate.sql)
uses task ownership, user/role responsibility, approvers and private-access
exclusions. It reproduces the failure without application-specific engine rules.
Its [two profile controls](../../../crates/bicdb-sql/tests/recursive_query_stack.rs)
explicitly run with a 2 MiB thread stack and cover lowering, Describe metadata
and execution. Both pass. The corrected protocol server also executes the
original failing prepared query on a disposable database clone.

Independent Oracle 23.26.3 and PostgreSQL 18.4 controls both return two matching
rows initially and one after a private-access exclusion. The corrected server
matches these results in Oracle preview and default PostgreSQL profiles. Native
Oracle controls use nonce objects that are removed; native PostgreSQL controls
run in a disposable resource-limited container.

The [engine validation record](engine-validation.json) contains 1,118 passing
regression tests, all 13 retained independent reference families and the stock
PostgreSQL JDBC optimistic-update check. Workspace-wide all-target compilation
and formatting checks pass. Three pre-existing SQL benchmarks are ignored; the
new tests are not skipped. Actual previous-binary view reads, OIDs and positive
and negative grants also survive the [upgrade controls](legacy-upgrade.json).

The [application validation record](validation.json) includes the nine selected original classes: 168 of 170 tests pass. The two recorded failures remain the Oracle BLOB export block and the Oracle-mode savepoint assertion; neither is excluded or rewritten. After clean shutdown, the disposable test database retains 67 prior workflow observations, 27 business queries, original object fingerprints, seven price rows and four product rows with source timestamps and update users. The primary database is not used for test mutations.
The rejected application upgrade was stopped and the previous runtime restored.
It was verified at HTTP 200 with browser login and process navigation, and all 77 selected
workflow observations, 27 business queries, routine/view fingerprints and 141
saved report rows preserved. The failed runtime is not an accepted deployment.

These controls qualify the recorded query shapes; they do not guarantee arbitrary
query depth, full Oracle conformance or complete application acceptance.

## Subsequent application upgrade

The [clean runtime upgrade](application-upgrade.json) opens a copy of the stopped
current application database without recreating original objects. All 77
selected workflow observations and 27 settled business queries retain their
pre-upgrade values, including the completed project issue and reversal. The 62
routine and 160 view fingerprints are unchanged; all 11,983 projected view labels
and type families match the source. All 141 rows of three saved Trial Balance
reports retain their fingerprints. Their PDFs were not regenerated.

The actual application JVM serves HTTP 200 and the browser reaches the GardenAdmin
desktop and standard Issue to Project process. This qualifies the recorded
upgrade and navigation; the remaining upstream failures, broader workflows and
full application acceptance remain open.
