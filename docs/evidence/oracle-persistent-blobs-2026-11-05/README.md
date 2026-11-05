# Selected persistent BLOB execution

[Validation results](validation-results.json) identify the tested source and
924 passing local Rust tests: 688 SQL library, 9 transaction receipt, 48 Oracle
protocol-suite and 179 general protocol tests. Three existing SQL benchmarks
remain ignored. Workspace/all-target checking passed. This is development
regression evidence, not application or production qualification.

The tests cover stored APPEND/self-APPEND, alias snapshots and origin commit,
savepoints, ordinary versus locked acquisition, deletion/replacement errors,
primary-key movement, RETURNING identity, later-transaction rollback baselines,
current SQL privileges/RLS, cross-instance refusal even with the same UUID,
explicit binary reconciliation, and binary transport. An 8,193-byte anonymous
chunked append round-trips through both native SQL and the PostgreSQL driver;
rollback restores the original bytes. Ordinary PostgreSQL BYTEA and protocol
regressions run independently of the Oracle profile cases.

The earlier [ownership](../oracle-persistent-blob-ownership-2026-11-05/README.md)
and [acquisition](../oracle-lob-read-versions-2026-11-05/README.md) observations
supply native Oracle expectations for the covered lifecycle cases. They were
not rerun here, and not every source observation is an end-to-end differential
case in this checkpoint. See the [contract](../../oracle-blob-storage.md) for
implemented boundaries and remaining integration.

Functional fixtures generally disable fsync. Their reopen/rollback results do
not establish power-loss behavior. The record distinguishes corrected initial
failures from passing final checks. No standalone binary, application deployment,
complete test-class result, unchanged Oracle driver support or full Oracle
programme completion is implied by this source checkpoint.
