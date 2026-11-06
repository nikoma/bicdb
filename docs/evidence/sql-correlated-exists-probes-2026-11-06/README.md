# Correlated EXISTS candidate probes

Pure correlated equality EXISTS queries can now build a statement-local candidate
map over a bounded inner relation. Each outer row still executes its original
subquery, with an added necessary primary-key filter. Projection, authorization,
SQL comparisons and current transaction visibility retain their ordinary paths.
The implementation does not depend on application or table names.

The regression combines a full outer join, DISTINCT, two correlated equality
columns, duplicate requests and duplicate inner keys, and a NULL correlation key.
It verifies the expected 34 result rows and at most three full scans in both the
PostgreSQL and Oracle-preview profiles. Pending inserts change the result and
savepoint rollback restores it. This is a reproducible scan-count control, not a
production throughput or latency benchmark.

The general row-fetch path also handles bounded constant primary-key IN lists
through existing exact-key lookups. Independent integer and NUMERIC controls
cover repeated values, scaled numeric identity, NULLs, absent keys, pending
inserts/deletes, full rollback and NOT IN semantics.

Candidate maps are scoped to one statement and outer type environment. The
supported shape uses one base inner table, conjunctions of column equality,
a bounded numeric correlation key and one numeric primary key. Views, row
policies, volatile/user functions, locking reads and modifying CTEs decline the
optimization. Unsupported query shapes, oversized maps or large candidate
buckets use the ordinary evaluator. The map retains at most four entries,
65,536 scanned result rows and an estimated eight MiB per accepted map; this
is not a bound on the existing scan executor's peak memory.

The provenance record identifies the implementation revision, exact test-server
hash and check log hashes. Full original application acceptance, other Oracle
programme workstreams, production qualification, a published release and a
verified deployment remain separate gates.

Validation: 742 SQL library tests pass with four existing ignores; 257 native
tooling tests pass with four external-reference skips. All 183 protocol, 48
Oracle-profile integration and 44 authorization/JSON checks pass. The adapter
passes 25 unit tests and 250 live PostgreSQL-driver checks across three Oracle
execution modes and default-profile rejection. Its ten production class files
and driver-service resource match the staged application adapter.

Run the focused regressions from the repository root:

```sh
cargo test --locked -p bicdb-sql --lib exists_candidate_probes_ -- --test-threads=1
cargo test --locked -p bicdb-sql --lib primary_key_lists_ -- --test-threads=1
```

See [the check provenance](summary.json) for exact scope. Earlier attempts
returned correct rows but failed the scan-count assertion; those results were
retained, and the final batch passed without weakening that assertion.
