# BicDB Backup, PITR, And Restore Drills

For scheduled restore and promotion drills, bounded region-local reads, planned
switchover, and role/tenant budgets, see
[verification and workload protection](verification-and-workload-protection.md).

The desired engine contract for extent-level incremental online backups,
including regular and FTS indexes, multi-store backup sets, direct repository
uploads, retention, and acceptance criteria is documented in
[`incremental-online-backup-contract.md`](incremental-online-backup-contract.md).

The running server supports full online database snapshots and changed-extent
incrementals in an encrypted `BICBAK04` object repository. Ordinary SQL writes,
reads and DDL continue during payload copying. A short coordination interval
establishes one committed boundary; it does not flush or copy the entire store.

## Online full and incremental backups

Configure the server with a repository outside its database directory:

```sh
export BACKUP_KEY='use-a-secret-from-your-secret-manager'
bicdb serve /data/bicdb --backup-repository /backups/bicdb --backup-key-env BACKUP_KEY
bicdb backup start daily --wait
bicdb backup start quarter-hour --base daily --wait
bicdb backup status
# bicdb backup cancel <job-uuid>
```

These commands connect to the running SQL listener at `127.0.0.1:5433`; use
`--server`, `--user`, `--dbname` and `--password-env` as needed. The CLI accepts
loopback or SSH-forwarded connections. A remote TLS-capable PostgreSQL client
can invoke the same standalone superuser functions:

```sql
SELECT bicdb_backup_start('daily');
SELECT bicdb_backup_start('quarter-hour', 'daily');
SELECT bicdb_backup_status();
SELECT bicdb_backup_cancel('job-uuid');
```

SQL arguments name artifacts within the host-configured repository. They never
supply paths or encryption keys. Calls require an effective superuser, outside
a transaction and delegation. One job runs per server database; cancellation
and shutdown release the capture. Status includes the recovery commit sequence,
phase, copied/source/uploaded/reused bytes and failures. `bicdb_backup_*` metrics
also expose progress and success. The backup uses the node governor's
`backup_restore` lane with a bounded memory/I/O reservation and CPU slot.

At capture, core append-log prefixes, catalogs and atomic-replacement sidecars
retain independent file descriptors. The page store pins its WAL prefix and
preserves the first before-image of each uncopied physical page. Required WAL
is copied first, allowing checkpoint and WAL recycling to resume during the
remaining upload. The restored disk pages plus this exact WAL prefix recover
the declared committed boundary, including native indexes and FTS. Large-value
attachments retain their captured inodes if later replaced or deleted. No
outer database read guard is held across compression, encryption or copying.

Each physical page extent becomes an immutable content-addressed object.
Incrementals use a durable, checksummed extent-generation journal: the first
physical change in an epoch is recorded and synced **before** the page write.
Capture advances the epoch before foreground writes resume. Matching source,
tracking identity, layout, extent generation and length allow unchanged source
extents to be skipped. Tracking survives restart; corrupt/missing tracking or
bounded journal rollover changes identity and safely forces recopying. Relayout
invalidates tracking before replacing files. Branches and restored databases
start with fresh backup identities. Native paged FTS follows the same tracker.
The first paged capture enables an on-disk compatibility guard: earlier core
binaries and page tools refuse the tracked database. Use this version or a newer
compatible version for later opens, restores and branches; see
[format compatibility](on-disk-format.md#online-extent-tracking-compatibility).
Other metadata, append streams and blob files are safely recaptured and
content-deduplicated; source-read avoidance currently applies to page extents,
not all nonpaged components or embedded-memory stores.

Objects use independently authenticated, optionally compressed 1 MiB frames;
plaintext hashes bind their identity. Files and newly created directories are
synced before the authenticated encrypted manifest is published without
replacing an existing name. Each manifest lists the **complete** required
object set, so the newest incremental restores directly even after parent
manifest files are retired. Objects referenced by retained manifests must stay
in the repository. Copy the repository, including `objects/`, when transporting
backups; a `.bicbackup` file alone is not a portable v4 backup.

```sh
bicdb backup verify /backups/bicdb/quarter-hour.bicbackup --key-env BACKUP_KEY
bicdb backup restore /backups/bicdb/quarter-hour.bicbackup /restore/bicdb --key-env BACKUP_KEY
bicdb backup drill /backups/bicdb/quarter-hour.bicbackup --key-env BACKUP_KEY
bicdb backup repository-gc /backups/bicdb --key-env BACKUP_KEY --dry-run
```

Verification authenticates and hashes every referenced object. Publication
checks every reference and fully verifies newly uploaded objects; reuse does
not reread every old payload on each incremental. Schedule full verification
and actual restore drills to detect storage bit rot. A supplied ordered chain
is checked before CLI restore, which installs its final v4 snapshot once.
Restore authenticates payloads in an inactive staging directory before acquiring
the destination's stable ownership lock and installing them. An interrupted
install leaves a durable pending marker; opening that partial database fails
until a forced restore succeeds. Active databases cannot be overwritten, even
with `--force`.

Remove only retired manifest files, then run `repository-gc` to sweep objects
unreachable from **all** authenticated retained manifests. The default grace is
one hour; `--min-age-seconds` changes it. Missing references, wrong keys and
corrupt manifests abort collection before deletion. Dry-run reports candidates.
Publication, verification, restore and collection share a repository lock.
Automatic log archival takes an initial full v4 snapshot, then incrementals from
the latest retained v4 base, and performs grace-protected object collection.
After the first published base anchors a fresh archive, periodic base copying
runs on its own registered worker so commit archival continues during long
copies. Pruning and scheduled restore drills remain on the archive worker;
backup/drill admission prevents repository ownership conflicts. Shutdown cancels
and waits for the copy worker to release its pins.

`--backup-limits <json-file>` overrides `OnlineBackupLimits`. Defaults include
65,536 files, a 64 MiB manifest, a 1 GiB/262,144-page before-image journal,
1 second of cooperative capture coordination, 8 GiB retained WAL, and a six-hour
job duration. `bytes_per_second` defaults to unlimited. Budgets and cancellation
are checked between bounded frames; operating-system I/O itself cannot be
preempted by a deadline. Exceeding a before-image budget fails the backup while
allowing foreground page writes. A tracking-journal write error refuses the
physical page write rather than falsely marking it unchanged; reopen after
resolving the underlying I/O problem. Logs and before-image files left by killed
uploaders are reclaimed under ownership locks.

## Qualification with an external restore oracle

The initial optimized campaign reproduced a real atomicity failure: after a
page-WAL sync error, an account update survived restart while its ledger insert
did not. The transaction's uncertain outcome had already been reconciled by ID;
this was a partial commit, not a missing external acknowledgement. The
[failure evidence](evidence/online-backup-qualification-failure-2026-11-01/finding.json)
includes the original executable digest, frozen harness, fault trace and intent
history.

Transactions retain complete logical redo before changing physical or resident
data. All tables and their native indexes share one paged transaction. A
materialization acknowledgement is appended only after the whole apply succeeds;
normal paged startup can then keep those collections lazy. If durability or
materialization fails, the live handle refuses data access, checkpoints and
backup capture until reopening completes recovery. The unacknowledged transaction
can recover as committed; clients must reconcile by transaction ID. This restores
the full logical WAL I/O that the earlier marker-only optimization omitted.

Run the built CLI in isolated directories with sustained concurrent SQL,
controlled process crashes, actual injected filesystem errors and artifact
corruption:

```sh
python3 -m pip install 'psycopg[binary]>=3,<4'
python3 scripts/platform/qualify_online_backups.py \
  --binary /absolute/path/to/bicdb \
  --work-dir /absolute/path/to/new-qualification-directory
```

The default workload runs eight clients for at least five minutes, alternating
full and incremental backups. Transactions update an immutable ledger, account
heads, deleted/reinserted indexed slots and a shared aggregate; deliberate rollbacks and
serialization aborts are recorded separately. Each completed backup must have
observed successful commits while its payload was copying. Separate restored
server processes are checked row by row and through indexed predicates against
expected values computed with SQLite, including cross-table atomicity and
observed commit-order dependencies. The oracle does not use BicDB's own hashes
to decide whether the restored data is correct.

The two databases cannot commit atomically. The external SQLite journal syncs
transaction IDs and intent before sending `COMMIT`, then records known
acknowledgements separately. Unresolved outcomes are reconciled by transaction
ID after restart; a known acknowledgement is never downgraded to an allowable
abort. A controlled crash explicitly covers a BicDB commit whose SQLite
acknowledgement was never written, alongside a transaction that never committed.
Backup checks use independently observed lower/upper capture bounds rather than
comparing an old snapshot against later source acknowledgements.

A Linux-only `LD_PRELOAD` helper injects ENOSPC, file/directory sync errors and
manifest-publication errors into the actual executable. It stalls at object
copying, manifest publication and restore installation so the controller can
send SIGKILL at those exact points. Every injected operation is traced; a case
cannot pass without reaching its fault. Corruption cases mutate, truncate,
append to and remove referenced encrypted objects, and damage manifests; verify
and restore must reject each case before changing the destination payload.
Core-log durable-append and page-WAL sync failures are injected separately and their uncertain
transaction IDs are reconciled after restart before the row-level oracle runs.
If directory fsync fails after manifest linking, publication has an uncertain
outcome: a surviving artifact must fully verify and restore, while an absent
artifact is also valid. The failed operation does not promise durable publication.

The new work directory retains `report.json`, the external SQLite transcript,
fault traces, repositories and process logs, including failures. The report
binds results to the executable and harness digests. This campaign measures its
configured workload and Linux process/I/O fault model; machine power cuts,
storage-controller behavior and geographically distributed outages need their
own campaigns. Increase duration, data volume and client count for qualification
on the intended deployment hardware.

## Legacy archives and compatibility

`backup create`, the older paged `backup online` base/WAL-tail interface, and
existing v1-v3 readers remain available. New live server work should use
`backup start`; offline legacy creation may pause writes and is not the new
whole-database capture interface. Streaming `BICBAK03` archives own their
payloads and apply deletion-aware incrementals in order. Historical v1/v2
readers may use memory proportional to the archive size. Stable OS-owned lock
inodes are retained across releases; an unlocked file left by a crash does not
block normal writes or the next backup.

Core audit-event PITR and commit-log archive PITR are distinct recovery paths.
Audit-event PITR requires audit events to have been enabled before capture.
Schema migration records are included with the database's durable records.
Format compatibility and upgrade recovery are documented in
[`on-disk-format.md`](on-disk-format.md).

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

## Point-in-time recovery from the commit log archive

A server started with a commit log archive writes every commit to it as it
becomes durable, and takes encrypted base backups into the same directory:

```bash
export ARCHIVE_KEY=...        # base backup encryption key
bicdb serve /srv/db --log-archive /backup/archive \
  --log-archive-key-env ARCHIVE_KEY \
  --log-archive-base-hours 24 --log-archive-keep 7
```

Restore the whole database as it was at a moment, for example just before an
accidental `DELETE`:

```bash
bicdb backup pitr /backup/archive /srv/restored \
  --target-timestamp 1791654946 --key-env ARCHIVE_KEY
# or an exact commit:   --target-commit 81234
# or everything:        (no target)
```

`pitr` picks the newest base backup that precedes the target, restores it,
and replays the archived commits up to the target, schema changes included.
The result is an ordinary database. To replay onto a base backup taken some
other way, use `bicdb backup restore <backup> <target> --log-archive DIR
[--target-commit N] [--target-timestamp T]`.

- Archiving needs no audit events and no settings on the tables: it archives
  the same commit frames streaming replication sends to standbys.
- A commit is archived once it is durable on the server; an archive append is
  fsynced. The server keeps its transaction log until a commit is archived,
  also across restarts, so a stalled archive directory cannot lose commits
  (watch its free space: the log grows while archiving is stalled).
- Server-managed v4 bases coordinate briefly, then copy while commits continue. Old base backups
  beyond `--log-archive-keep`, and log segments only they needed, are removed.
- Targets are commit sequence numbers or Unix seconds; commits that wrote
  nothing do not stop a time-based replay.
- In cluster mode each database archives into its own subdirectory.

## Audit-event PITR

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
