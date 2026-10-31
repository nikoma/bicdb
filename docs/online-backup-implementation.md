# Online backup implementation work log

Goal: ship online full database backups with WAL recovery first, then efficient
changed-extent incrementals. Deliver both on GitHub main; leave the canonical
checkout clean and remove only our temporary branches/worktrees.

Baseline: 2dde68d. No other agents are delegated. Existing running services and
other projects' checkouts must not be changed.

## Stage one

Implement a short, coherent physical capture at an exact committed boundary,
then release the database/commit gates before copying payloads. The captured
page files are read through a bounded before-image journal: the first overwrite
of an uncopied page preserves its old physical bytes. Required page WAL is pinned
and captured at the same boundary, so dirty buffered pages recover normally.
Core append streams are captured as immutable inode references with fixed
lengths; atomic-replacement sidecars and blobs retain their captured inodes.
Catalogs, ordinary indexes and FTS participate in this same capture. Refuse
unsupported unsafe source components rather than silently omit them.

Copy to an encrypted content-addressed backup repository with authenticated,
complete manifests published last. The repository avoids a second full local
staging archive and is the foundation for stage two. Existing v1-v3 artifacts
remain readable. Integrate server archival and live start/status/cancel access;
restore/verify/drill consume the new format. Copying holds no outer database
guard. Cancellation, bounded I/O and journal-space limits abort backup work
without invalidating the live store or older backups.

Stage-one evidence must force SQL commits to occur during a deliberately slow
copy, exercise metadata/index/FTS/blob changes, and compare restored contents and
indexes at the declared recovery boundary. Existing raw-page writer tests are
insufficient: they bypass core transaction-log/catalog persistence.

## Stage two

Add durable, fail-closed page-extent change tracking. Before the first physical
write to an extent in a tracking epoch, persist its dirty identity. Advance the
epoch at capture, so writes after the cut cannot disappear when a backup
publishes. Parent manifests bind source/tracker identity, epoch, file layout,
object identities and lengths. Reuse unchanged objects without reading their
source extents. Restart, interrupted tracking publication, compaction, extent
growth/shrink and wrong-parent checks must never turn uncertainty into reuse.
FTS data in the paged store follows the same tracking; nonpaged components need
explicit immutable/append-generation reuse or safe bounded recapture.

Manifests describe the complete referenced object set, supporting synthetic
full publication and reachability-safe retention. Verify references before
success and perform payload verification and actual restore drills separately.
Missing/corrupt objects, gaps, foreign identities and unsupported formats fail
before target activation. Include no-write/one-extent-change/restart reuse tests,
resource accounting, and scale evidence.

## Completion checks

Run meaningful page/core/SQL/pgwire/CLI correctness and crash/restore coverage,
workspace/all-target checks, formatting/licensing/dependency gates and full CI.
Build an accurately labelled binary, push main, verify remote SHA and cleanup.
Source merge, binary build and production deployment remain separate states.

## Implemented result

The full stage passed physical-cut, ordinary-commit, catalog/DDL, B-tree, FTS
ranking, attachment replacement, WAL recovery and protocol tests before the
incremental tracker was added. Both stages now use the same owned capture and
v4 object repository. Publication is encrypted and authenticated; complete
manifests are installed last without overwriting an existing artifact.

The incremental stage persists one checksummed dirty record before the first
physical write to an extent in each epoch. Epoch sealing, journal rollover,
restart recovery, layout invalidation and source identity are part of reuse
validation. A multi-extent test requires a no-write incremental to copy zero page
extents, reuse every prior page extent, and read less than half the full backup's
source bytes. A later incremental restores after restart and after deleting its
parent manifest files. Reports distinguish copied/reused page extents and reused
extent bytes from ordinary content deduplication.

Retention scans every retained authenticated manifest under the repository
ownership lock. Missing references and corrupted magic abort collection.
Observed manifest retirements are synced before any payload deletion, preventing
power-loss resurrection of references to deleted objects. The repository can
adopt an archive directory containing legacy v3 backups without replacing them.
Interrupted uploads and before-image journals are reclaimed under ownership.

Additional correctness fixes found during implementation:

- Checkpoint replacement of `transactions.log` now flushes buffered markers and
  invalidates cached append descriptors. Otherwise the next commit could append
  to an unlinked inode and lose its recovery sequence marker.
- B-tree splits account for actual entry bytes, compact fragmented leaves, and
  check rebuild capacity in release builds. Count-based splits could overflow a
  node; the release build could silently omit entries. Separator capacity and
  nonempty interior halves are checked as well.
- Backup owner locks retain a stable inode, closing an unlock/unlink race.
- Restores retain stable destination ownership and fail closed on interrupted
  installation; format migrations run only after database ownership is acquired.
- Owned captures retain the database directory lock even if the originating
  handle is dropped. Legacy two-artifact APIs document their narrower authority;
  the BicDb compatibility entry holds core commits fixed across its two files.
- Standalone SQL startup reads the persisted storage mode, allowing a paged
  database to use the real server startup and backup-control path.
- Legacy consistent backups and branches flush pending core commit markers
  before copying their declared recovery boundary.

Validation evidence is recorded outside the repository in
`/root/bicdb-audit-artifacts/online-backups/`. The real CLI exercise starts isolated
servers, creates a full and incremental, verifies the chain, restores 41 rows at
the incremental cut, then replays the commit archive to 42 rows. It also exercises
status, cancellation and GC dry-run. A separate SIGKILL regression kills an active
incremental after an acknowledged concurrent commit, reopens the source, checks
that commit, restores the earlier backup, and takes/restores a new incremental.
Page tests cover before-image exhaustion without rejecting source writes,
tracking I/O failure, torn tracking records, and bounded journal rollover.

This implements one database per manifest. Multi-store atomic backup sets and
remote object-store transports remain the broader contract. Source-read avoidance
covers native page extents (including their native indexes/FTS); other files are
safely recaptured and deduplicated. Full payload verification and restore drills
remain explicit operations; publication verifies new objects and reference
presence without rereading every old payload on every incremental.

The final multi-extent fixture (700 padded records plus native B-tree/FTS data)
read 7,264,850 source bytes for its full snapshot and 6,738 for its unchanged
incremental, reusing 7,258,112 bytes across seven extents with zero extent copies.
The incremental cut took 25 ms in this run. This is an acceptance fixture, not a
multi-terabyte throughput claim. Restart reuse and exact restored contents are
checked separately in the same test.

Compatibility validation also checks the durable database feature guard and
tracked page magics (`BICDBPG3`/`BICDBPG4`). Old writers and tools must refuse these
stores; silently running an engine that ignores tracking would invalidate reuse.
Routine metadata rewrites, reopening and offline relayout preserve the guard.

The final ownership review extended restore locking to the database root and an
existing paged child engine. Both formats preserve the stable directory/lock
inodes during forced replacement. Legacy archives no longer read source lock
files (closing another descriptor can release a Unix process lock), and restore
consumes/authenticates historical lock entries without installing them. A real
second-process opener proves ownership remains held after legacy backup.
Legacy restores now also retain an interrupted-install marker until successful
verification, and create nested directories durably. All 14 repository tests,
legacy backup/online/paged restore suites, and the source/child-ownership
regression passed. The complete workspace audit and remote CI are separate final
delivery checks.
