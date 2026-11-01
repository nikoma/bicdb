# Optimized online-backup qualification, 2026-11-01

The corrected build passed a 30-minute concurrent workload followed by independent
restores, actual injected I/O failures, process crashes and artifact corruption.
The earlier campaign found a real partial commit; its
[failure evidence](../online-backup-qualification-failure-2026-11-01/finding.json)
is retained separately.

| Check | Measured result |
| --- | --- |
| Concurrent SQL clients | 8; peak 8 transactions in flight; every writer progressed |
| Sustained write phase | 1,827.18 seconds |
| Known acknowledged transactions | 19,007; none lost |
| Backups during writes | 41, alternating full/incremental; each observed successful commits during copying |
| Total snapshots recorded | 43, including fault-baseline and after-uncertain snapshots |
| Independent restored-server checks | 47; all row and indexed-query comparisons passed |
| External model checks | 60, each comparing 14 queries |
| Process/I/O fault cases | 12; all passed |
| Corruption cases | 84: four mutations of each of all 20 referenced objects, plus four manifest mutations |
| Explicit rollbacks / serialization aborts | 1,844 / 32,810; excluded from committed expectations |
| Owned child processes after completion | All stopped |

The workload inserts immutable transaction IDs and updates account heads/totals,
deleted/reinserted indexed slots and a shared aggregate in each transaction.
It also checks an independently generated 8 MiB payload fixture. Expected results
come from SQLite intent history rather than BicDB checksums. Snapshot comparisons
use capture bounds and observed commit-order dependencies.

## Non-atomic acknowledgements

SQLite uses WAL mode and FULL synchronization. Intent and a unique transaction ID
are persisted before submitting `COMMIT`; its acknowledgement is recorded
separately. Only uncertain outcomes are reconciled against recovered immutable
ledger IDs. Known acknowledgements can never be reclassified as permitted loss.
The exported transcript contains 19,007 acknowledged attempts, 34,654 aborted
attempts, two resolved committed attempts and two resolved aborted attempts.

The forced crash after BicDB returned from `COMMIT` but before SQLite recorded
its acknowledgement recovered that transaction as committed. A transaction that
never submitted `COMMIT` remained absent. The core-log append-error transaction
reconciled as aborted; the page-WAL sync-error transaction reconciled as committed
with all of its tables and indexes consistent. Both failures blocked data access
through a fresh SQL connection until restart completed recovery.

## Fault coverage

The campaign injected ENOSPC during object creation; file-sync, directory-sync,
post-publication directory-sync and manifest-publication errors; SIGKILL during
object copying, before publication, after publication and during restore
installation; the missing-acknowledgement crash; and separate core-log durable
append and native page-WAL sync errors. The fault trace proves the real I/O
operations were reached. A manifest visible after an unacknowledged publication
fully verified and restored. Interrupted restore targets refused to open and
were repaired by a fresh forced restore.

Each referenced encrypted object was bit-flipped, truncated, given trailing bytes
and removed. Manifests were damaged in four ways. Verification and forced restore
rejected every corrupted artifact before modifying the destination payload.

## Provenance and limits

[release-build.json](release-build.json) identifies runtime source commit
`a6fc1c31e2cf2b4cc19ef34bf5958a445fc16dcb`, qualification source commit
`a8fb422dcf275135c7b91fe283aaa65082eefcf0`, the build command and executable SHA-256
`d96836c9f92a47d96eea335f206c24adf83a49bea852585b8070275ccac4ddbb`.
Both commits have the same runtime source tree. This is the default Cargo release
profile with thin LTO; debug sections were stripped from the executable.

The [full source CI run](https://github.com/nikoma/bicdb/actions/runs/41271849946)
for qualification commit `a8fb422` completed with all eight jobs green: 3,647 Rust
tests passed, 16 explicitly ignored, across 350 test targets. The platform checks
also passed 12 Python tests and two JavaScript tests.
[ci.json](ci.json) records the tested commit and completed jobs;
[ci-summary.json](ci-summary.json) records the Rust suite totals. This evidence
commit only adds documentation, recorded artifacts and the standalone replay
check; it does not change the qualified runtime or harness.

[release-report.json](release-report.json) contains individual results and harness
digests; [transaction-intents.jsonl.gz](transaction-intents.jsonl.gz) contains the
external transcript; [fault-trace.txt](fault-trace.txt) records injection hits;
[oracle-tests.txt](oracle-tests.txt) records negative controls for the oracle.
[evidence.json](evidence.json) binds evidence files to their SHA-256 digests and
records the local raw-artifact directory. Running
`python3 docs/evidence/online-backup-qualification-2026-11-01/replay_evidence.py`
checks those files and reconstructs all 840 query-result digests across the 60
oracle checks from 53,665 exported attempts, without BicDB, SQLite or the harness.
The [replay receipt](evidence-replay.json) records that successful check. This
validates the exported expectation history; the restored-server comparisons
themselves ran during the original campaign. Reproduce the full campaign with
`scripts/platform/qualify_online_backups.py --duration 1800 --minimum-backups 10 --corrupt-objects 1000`
and the required `--binary` and fresh `--work-dir` arguments.

This campaign covers one Linux host, eight clients, local encrypted repositories,
process crashes and the recorded libc I/O errors. Hardware power cuts, storage
controllers, multi-region failure and longer deployment-specific workloads need
separate campaigns. The executable was built and tested, not deployed or
published as a release.
