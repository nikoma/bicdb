# Foreign-key parent point reads and cross-width validation timing

An eligible foreign key referencing the parent's ordered primary key first
checks the physical row identifier, then rechecks typed key equality. Successful
matches avoid materializing the whole parent table. This includes inline and
composite primary keys. Non-primary unique references, unsupported identities,
misses and unequal candidates retain the authoritative scan. No application
names or table-specific rules select the optimization.

The read uses the latest committed parent with the current transaction's pending
write overlaid. It deliberately does not use the snapshot-visible integrity-read
helper. Parent-key reservations still precede the lookup, and the same typed
comparison controls acceptance. Regression checks cover parent insertion/deletion
after transaction start, pending insert/delete and savepoint restoration.

`BICDB_TRACE_PLAN=1` now reports `foreign_key_parent_lookups` and
`foreign_key_parent_scans`. Focused checks require an immediate primary-key match
to use one point read and zero parent scans, and deferred composite validation
to avoid parent scans. These counts establish the selected path, not a general
throughput or production-scale qualification.

## Cross-width foreign keys

A child integer outside the parent's numeric domain cannot match any parent.
Immediate enforcement reports SQLSTATE `23503`, rather than leaking the
reservation-key conversion's `22003`. Deferred enforcement queues the row check
without reserving an impossible key and fails at validation, or succeeds if the
invalid child has been deleted before commit. Other conversion errors retain
their existing behavior. Validation of legacy rows has independent coverage.

The public [reference fixture](../../../examples/oracle/reference/foreign_key_timing.py)
has 17 Oracle cases and 21 PostgreSQL cases; the additional PostgreSQL cases
cover cross-width diagnostics and timing. Source SMALLINT declarations in
Oracle and PostgreSQL have different numeric domains, so the PostgreSQL cases
are not advertised as Oracle equivalents.

All exported application foreign keys still require source-faithful migration,
orphan inventory and business-workflow qualification. Secondary-key indexing,
all key types, recovery/concurrency schedules and complete Oracle conformance
remain broader programme work.

## Recorded validation

The [live comparison](reference-results.json) passes **17/17 Oracle cases and
21/21 PostgreSQL cases**, with each source satisfying its own explicit
expectations. The [previous binary](baseline-results.json) matched 18/21
PostgreSQL cases; its three new immediate/deferred cross-width failures are
recorded separately. PostgreSQL SQLSTATEs are compared; Oracle error-number
equivalence is not claimed.

The same live comparison records 17 Oracle-profile and 19 PostgreSQL parent
point lookups, with six fallback scans in each profile. These are aggregate
counts for this fixture, including expected invalid references, rather than a
workload benchmark. The [source inventory](parent-key-inventory.json) has 3,823
relationships referencing primary keys and three referencing other unique keys;
this does not prove that every source relationship has migrated or used a
particular runtime path.

The [validation record](validation-results.json) includes 706 passed SQL library
tests, three ignored, all selected integration suites, 181 PostgreSQL and 48
Oracle-profile protocol tests, and workspace/all-target checks. The first final
build was interrupted when the memory guard's 12 GiB floor was crossed. The
build resumed successfully with SQL development code-generation units set to
512 and the same memory floor. The record preserves both resource outcomes.

All six [executable upgrade/fencing checks](writer-fence-results.json) passed
again. The accepted application deployment remains separate from these candidate
results; its exported foreign keys are not counted as installed here.
