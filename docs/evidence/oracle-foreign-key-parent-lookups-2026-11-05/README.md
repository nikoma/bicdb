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
