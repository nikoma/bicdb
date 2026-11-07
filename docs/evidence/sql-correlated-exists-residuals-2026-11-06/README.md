# Correlated EXISTS with safe residual filters

Bounded correlated equality candidate maps now accept additional inner filters
made from ordinary character equality/inequality, column nullness, boolean
literals and AND/OR/NOT. These filters remain in the original subquery; the map
only adds a necessary primary-key restriction. No application, schema or table
name selects this behavior.

Independent PostgreSQL and Oracle-preview controls cover duplicate correlation
keys, NULLs, nested disjunctions, predicate order, a second subquery that shadows
an alias, pending updates and savepoint rollback. A native PostgreSQL-transport
control returned the correct rows on the retained previous backend but scanned
26 times in each profile. The unchanged control passes the at-most-three-scans
assertion on this implementation. This is a bounded scan-count regression, not
a production latency or throughput benchmark.

Eligibility is conservative. Residual conversions, character comparisons with
custom types or collations, hidden residual columns and unsupported expressions
decline the optimization.
Parent-query custom operators and custom-type casts also decline because they
may invoke callbacks. Existing restrictions on policies, views, functions,
locking and modifying queries remain. The candidate map has the same statement
scope and row, bucket and memory-estimate limits as the earlier equality-only
implementation. These limits do not bound the ordinary scan executor's memory.

An original client-setup run with the earlier backend completed 384 period-control
inserts, then reached a print-format query containing an inner disjunction.
Repeated 30-second database-writer timeouts made that run operationally invalid;
it was explicitly interrupted, its database/logs were retained, and no natural
JUnit pass or failure is claimed.

A subsequent run on this implementation completed all nine corresponding
print-format queries in 3,984–4,351 ms each, returned 368 rows across those
queries, and recorded 736 print-item INSERT statement events after completing all
384 period-control INSERT statement events. No database-writer timeout was observed in that run.
The [application-query evidence](client-setup-query-progress.json) records exact
backend, upstream source, original test-bytecode and loaded-driver identities,
as well as individual completed query-event hashes. Insert-event counts do not
prove that the application's final transactions committed. The host memory
guard interrupted the run before the original JUnit result; this is query
progress evidence, not a passing client-setup test or full application
qualification. The diagnostic database was retained. No upstream assertion or
application seed was changed. A fresh application acceptance run remains
required.

See [the check provenance](summary.json) for source revisions, backend and log
hashes, exact check counts and the previous-backend control. Full application
acceptance, the remaining Oracle programme, production qualification, releases
and deployments remain separate gates.

Validation on the recorded implementation revision: 751 SQL library tests pass with 4 existing ignores; 259 native tooling tests pass with 4 external-reference skips. All 183 protocol, 48 Oracle integration, 44 authorization/JSON, 25 JDBC unit and 250 live JDBC checks pass. The adapter's ten production class files and driver-service resource match the staged application SDK.

Focused checks:

```sh
cargo test --locked -p bicdb-sql --lib exists_candidate_probes_ -- --test-threads=1
BICDB_ORACLE_SERVER=/path/to/oracle_server python3 -m unittest discover \
  -s examples/oracle/idempiere -p test_exists_residual_probe_native.py -v
```
