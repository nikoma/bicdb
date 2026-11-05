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

Full upstream application checks are in progress.
The rejected application upgrade was stopped and the previous runtime restored.
It serves HTTP 200, browser login and process navigation, with all 77 selected
workflow observations, 27 business queries, routine/view fingerprints and 141
saved report rows preserved. The failed runtime is not an accepted deployment.

These controls qualify the recorded query shapes; they do not guarantee arbitrary
query depth, full Oracle conformance or complete application acceptance.
