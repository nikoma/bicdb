# Fixed-day expression indexes and driver savepoint release

B-tree indexes now execute the bounded timestamp-without-time-zone expressions
`TRUNC(column)`, `TRUNC(column, 'DD')` and PostgreSQL
`date_trunc('day', column)`. Physical keys use integer day flooring, including
negative epoch values, with NULL and infinity handling. Backfill, uniqueness,
insert/update/delete, rollback and reopen have focused core regressions in both
memory and paged storage. Unsupported stored envelopes fail before publishing
index entries or writes.

SQL regressions independently exercise PostgreSQL and Oracle profiles. They
assert the physical index used by equality and range plans, retained strict
range boundaries and correct values. A precision-changing cast that rounds
across midnight remains part of the predicate and cannot probe an index on the
unrounded input. Numeric TRUNC, other format masks, zoned timestamps and arbitrary
user functions are outside this implementation.

Independent source comparisons through the documented CLI match all three
day-index cases on Oracle and all three on PostgreSQL: nonunique backfill and
mutation, unique-build rejection, and unique NULL/savepoint rollback behavior. Source and target each satisfy explicit
expected values and executable-index checks. A CLI negative control against the
older target fails while its PostgreSQL source still satisfies all three
expectations. This verifies the command reports
an unsupported target with a nonzero exit instead of silently succeeding.
The frozen older default-profile reader refuses the new `TimestampDay` catalog
variant. This checks refusal; it does not qualify mixed-version replication,
downgrade or migration rollback.

The PostgreSQL-driver `RELEASE SAVEPOINT` adaptation resolves names consistently
with Oracle-profile SAVEPOINT and ROLLBACK TO. Unquoted names are Oracle-folded;
quoted driver names retain their exact spelling and escaped quotes. Release
retains pending writes and the outer transaction can still roll back. It does
not reserve RELEASE as an ordinary application identifier or claim Oracle SQL
itself supports this transport command. An initial native-tool run exposed the
unquoted-name mismatch; that assertion remains unchanged and the combined-source
validation includes its rerun. The failed run's source comparison was refused
at its prerequisite before source fixtures were created.

The [validation record](validation.json) binds the tested combined engine,
frozen binary, reference fixture, per-suite counts and logs, actual skips,
source results, older-reader refusal and cleanup. Heavy checks run serially with
one compiler job, incremental compilation disabled and active memory monitoring.
No GitHub workflows were run. The binary is a compatibility example with the
system allocator, not a qualified production CLI.

This checkpoint does not establish completion of the original-schema import,
all original application tests, ownership and security, full temporal/index
conformance, workload performance, production operation or the Oracle programme.

## Repeating the focused checks

Run with bounded compiler concurrency and monitor available memory. The evidence
records single-threaded test execution; the full SQL and protocol suites are
separate from these focused commands.

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test -p bicdb-core --test timestamp_day_indexes -- --test-threads=1
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test -p bicdb-sql --lib session::timestamp_day_indexes:: -- --test-threads=1
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test -p bicdb-sql --lib oracle::transport_savepoints:: -- --test-threads=1
```

The [reference instructions](../../../../examples/oracle/reference/README.md)
describe the Oracle and PostgreSQL CLI invocations and connection configuration.
A native savepoint transport check uses `test_savepoint_recovery_native.py` in
the [application tooling](../../../../examples/oracle/idempiere/README.md), with
`BICDB_ORACLE_SERVER` selecting a built compatibility-example executable.
