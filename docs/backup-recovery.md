# BicDB Backup, PITR, And Restore Drills

The desired engine contract for extent-level incremental online backups,
including regular and FTS indexes, multi-store backup sets, direct repository
uploads, retention, and acceptance criteria is documented in
[`incremental-online-backup-contract.md`](incremental-online-backup-contract.md).

BicDB backups are encrypted archives with a manifest checksum, per-file
checksums, encryption metadata, recovery metadata, and source on-disk format
metadata. A backup captures an online checkpoint after flushing database
format metadata, catalogs, record segments, transactions, events, sync logs,
indexes, planner statistics, and `large_values/` sidecar attachments. Record
audit events are the point-in-time
recovery log for BicDB-first transactional deployments, so PITR requires databases to run
with audit events enabled before the backup is taken. Schema migration metadata
in `__bicdb_schema_version` and
`__bicdb_migration_history` is stored as durable records and is included in
the same backup/restore flow.

New backups use the streaming `BICBAK03` container. BicDB walks and hashes the
source through fixed buffers, encrypts an authenticated manifest separately,
then reads each included file in independently authenticated 1 MiB frames.
Each frame is compressed with zstd only when that makes it smaller. Nonces and
associated data bind every frame to the backup header and its exact sequence,
so truncation, insertion, or reordering fails authentication. Create, verify,
and restore therefore keep payload memory constant as database size grows.

The encrypted manifest and file list are capped at 64 MiB/one million files,
and source-manifest accumulation has its own conservative 64 MiB charge limit;
individual paths are capped at 16 KiB; chunk sizes and ciphertext lengths are
validated before allocation; and decompression is output-bounded. Recovery
event bounds are reduced in place rather than cloning the event archive. Source files
are hashed before streaming and hashed again while copied, so a file that
changes during the protected checkpoint fails the backup instead of producing
a mismatched archive. New full and incremental artifacts are written to a
temporary file, flushed, and atomically renamed only after completion. Failure
removes the incomplete temporary file and preserves any previously published
backup at the destination. The online checkpoint marker is protected by an
OS-owned exclusive lock, so two backups cannot claim the same checkpoint; a
marker left by a crashed process is detected as ownerless and removed before
the next storage write. Source and restore-path symbolic links are rejected so
an archive cannot escape the database or restore root.

Incremental v3 archives also carry authenticated deletion entries, so applying
an incremental removes files absent from its final manifest instead of leaving
stale sidecars from the base. Restore verifies the complete artifact before it
changes the target, writes each restored file through a flushed temporary, and
then verifies the final manifest with streaming hashes.

Restore and verify remain backward compatible with encrypted v1 JSON and
`BICBAK02` archives. Those historical formats were monolithic, so reading an
old artifact may still require memory proportional to that artifact; creating
all new backups in v3 and rotating old chains after a verified restore drill is
recommended.

Database format compatibility policy, migration crash recovery, and failed
upgrade rollback are documented in [`on-disk-format.md`](on-disk-format.md).

## Schedule

Recommended high-throughput business-system baseline:

- take one full backup daily;
- take incremental backups every 15 minutes during business hours;
- run `bicdb backup verify` on every artifact after upload;
- run `bicdb backup verify <full> <incremental> ...` after each incremental to
  validate the ordered chain;
- run `bicdb backup drill` at least daily against the newest restorable chain
  and retain the JSON report with release evidence.

For a 100GB deployment, a reasonable starting SLO is: full backup creation
within 90 minutes, incremental backup within 15 minutes, and full restore
within 90 minutes. Measure and tighten these limits for the target hardware.

## Commands

```bash
export BICDB_BACKUP_KEY='use-a-real-secret'

cargo run -p bicdb-cli -- backup create ./appdb ./app-full.bicbackup
cargo run -p bicdb-cli -- backup create ./appdb ./app-inc-001.bicbackup \
  --base ./app-full.bicbackup

cargo run -p bicdb-cli -- backup verify ./app-full.bicbackup
cargo run -p bicdb-cli -- backup verify ./app-full.bicbackup ./app-inc-001.bicbackup \
  --json
```

Restore the latest full backup:

```bash
cargo run -p bicdb-cli -- backup restore ./app-full.bicbackup ./restore --force
```

Restore to a Unix timestamp using audit-event PITR:

```bash
cargo run -p bicdb-cli -- backup restore ./app-full.bicbackup ./restore-pitr \
  --force \
  --target-timestamp 1781913600 \
  --pitr-record-batch 1024 \
  --pitr-event-batch 1024 \
  --pitr-event-bytes 8388608
```

PITR clears affected collections and replays the durable record-audit stream in
bounded transactions. The three limits cap rows deleted per transaction,
source events scanned per pass, and serialized target-event bytes retained by a
pass. Their defaults are 1,024 rows, 1,024 events, and 8 MiB. A single event
larger than the byte limit fails explicitly; increase the limit deliberately
after inspecting that event rather than allowing an unbounded allocation.

Run a fire drill and write machine-readable evidence:

```bash
cargo run -p bicdb-cli -- backup drill ./app-full.bicbackup \
  --target ./drill-restore \
  --target-timestamp 1781913600 \
  --json-out ./backup-drill.json
```

The drill restores into an isolated path, verifies the restored database with
the strict integrity checker, opens it, runs smoke stats, and reports:

- backup id and manifest hash;
- target timestamp when PITR is requested;
- restored file count;
- integrity check status, including storage frames and SQL integrity checks;
- large-value sidecar blob counts, bytes checked, orphan temp files, and
  checksum failures;
- smoke collection and record counts;
- archived event count;
- measured RTO in milliseconds;
- RPO evidence in seconds when the backup contains event timestamps.

Reports must not contain backup keys, database keys, decrypted protected data, or record
payloads.

## Failure Modes

Verification fails closed when:

- encrypted archive checksums do not match;
- the key or KDF metadata is wrong;
- archive or manifest versions are unsupported;
- the backup source database format version or required feature flags are
  unsupported by the restoring binary;
- a file listed in the manifest is missing from the archive;
- a file checksum does not match;
- a source or restore path traverses a symbolic link;
- an incremental does not reference the previous backup id and manifest hash;
- the restored target does not match the final manifest.

Interrupted backup writes use an atomic temporary file and either leave the
previous artifact intact or fail without a complete output. Interrupted restores
write files atomically and must be retried into a clean path or with `--force`
for a full restore. Do not serve traffic from a path that did not finish
restore and integrity verification.

## Key Handling

Provide backup keys with `--key`, `--key-env`, or `BICDB_BACKUP_KEY`. Prefer
`--key-env` or `BICDB_BACKUP_KEY` in operator automation so keys do not appear
in process arguments or shell history. Keep database encryption keys separate
from backup encryption keys and rotate them independently. Drill and verify
reports intentionally omit all secret material.

## Current Boundaries

PITR is timestamp-based and uses BicDB record audit events. It is intended for
greenfield BicDB transactional deployments that enable audit events; it is not a general
PostgreSQL WAL compatibility claim. Restoring encrypted database contents and
then opening them for PITR or drill smoke checks also requires the database key
to be available to the operator path that performs the open. Restore-based
rollback requires a pre-upgrade backup whose source format is supported by the
binary used for the rollback.

Backup archive I/O is bounded in v3, and timestamp PITR no longer materializes
a second whole-database `DbSnapshot`: it clears and reapplies audited records in
explicit row/event/byte-bounded batches. The durable event catalog itself is
still resident at database open, so end-to-end event-log memory is not yet
independent of history length. Cluster-wide restore also remains an
operator-coordinated set of node/range backups rather than one quorum-bound
cluster snapshot. Those are separate requirements from the now-streaming file
archive and must be certified before claiming cluster-wide PB-scale PITR.
