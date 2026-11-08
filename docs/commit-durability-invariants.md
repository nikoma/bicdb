# Commit durability and recovery invariants (server_paged)

Status: investigation, 2026-11-07. This document records how a COMMIT becomes
durable in `server_paged` storage today, the invariants recovery depends on, and
the design of an experimental mode in which the transaction log is the only
synchronous write before acknowledgment. The experimental mode is off by
default. The production commit path is unchanged.

Code citations are to `main` at `4fd762c`.

## 1. The files

| File | Content | Sync primitive | Group commit |
|---|---|---|---|
| `transactions.log` | Logical redo: `Begin`, `Write` (full row image), `Patch`, `Truncate`, outbox frames, `Commit{tx_id, commit_seq}`, `CommitMaterialized{tx_id, commit_seq}`, `Abort`. CRC32-framed (`storage.rs:1391`). | `O_DSYNC` handle (`storage.rs:310-324`) | Yes. Whoever holds `write_state` writes the whole contiguous pending prefix in one write (`db.rs:2709-2741`) |
| `paged/store.wal` (+ sealed `store.wal.<lsn>`) | Physical redo: full page after-images, `Commit(xid)`, `Abort(xid)`, `Checkpoint(redo_lsn)`. LSN-continuous, CRC32 (`wal.rs:59-104`). | `fdatasync` in `sync_through` (`wal.rs:657-716`) | In principle, but every core commit holds the store-wide `write_lock` across its fsync (`paged.rs:2123`), so commits never share one |
| `paged/store.pages` (+ superblock) | Pages, written by eviction (after the writeback barrier syncs the WAL) and by checkpoints | `fsync` at checkpoint | n/a |
| `sync.log` | Change-data-capture outbox | open + `fdatasync` per commit (`storage.rs:246-272`) | No |
| Sidecars (collection and index catalogs) | DDL state. Never in either log (`db.rs:19052-19055`) | write + fsync per change (`commit_snapshot.rs:1578-1607`) | n/a |

The two logs share no identifier. Page WAL records carry a page-store xid and
an LSN. `CommitMaterialized` carries only `tx_id` and `commit_seq`.

## 2. COMMIT today, in order

For a transaction with row writes (`spatial_paged.rs:2184-2505`):

1. Row write locks were taken at write time. Commit admission is shared, or
   exclusive for SERIALIZABLE. Unique and exclusion claims are taken.
2. **`sync.log` append + fdatasync**, when the outbox is on. This happens before
   the commit decision (TODO at `spatial_paged.rs:2328`).
3. `commit_seq` is reserved, the logical frames are encoded and enqueued.
4. **`transactions.log` write (O_DSYNC). This is the commit decision.** A failure
   here sets `recovery_required`.
5. Page apply under the store-wide `write_lock`: begin, puts and deletes,
   durable index entries (B-tree, unique, FTS, packed spatial). Then
   `PagedStore::commit`: page images appended, a `Commit` record appended, and
   **`store.wal` fdatasync while still holding `write_lock`**. Only then are the
   rows marked committed in page MVCC.
6. Resident structures are updated (shards, vectors, HNSW, in-memory indexes).
7. **`CommitMaterialized` appended to `transactions.log` (O_DSYNC)**, one per
   commit and not batched.
8. The applied watermark advances, `wait_until_visible`, row locks released.
9. Synchronous standbys: `sync_quorum.wait_for(commit_seq)`.
10. Acknowledgment to the client.

That is four synchronous writes per commit (three with `BICDB_SYNC_OUTBOX=off`).
Two of them, steps 5 and 7, serialize all committers: step 5 under
`write_lock`, step 7 under `write_state`.

Writes that are **not** row commits each run their own paged transaction and
rely solely on the step-5 fsync, because they have no redo in
`transactions.log`:
- `bulk_load_insert`
- index builds and rekeys
- FTS builds and folds
- spatial pack and delta writes
- entry-format records

## 3. Recovery today, in order (`db.rs:9530-10290`)

1. `PagedStore::open` replays `store.wal` before any page is read. Pass 1 scans
   sealed segments and the active file, truncates a torn active tail, and
   collects commit and abort outcomes. Pass 2 applies committed page images
   from `redo_lsn` onward.
2. The whole of `transactions.log` is read. Writes of any transaction that has
   a `CommitMaterialized` frame are discarded.
3. Catalogs load. Collections whose indexes are all durable kinds open **lazy**:
   their rows live only in the page store.
4. `apply_unmaterialized_transaction_log`: every committed transaction without
   a marker is re-applied in `commit_seq` order. Each write becomes its own
   paged transaction, with index maintenance. A re-put of an already-durable row
   leaves a duplicate version, which the code treats as benign
   (`db.rs:19009-19016`).
5. Paged checkpoint, then `sync.log` and outbox replay, then `transactions.log`
   is cut to empty. When replication is enabled the log is retained instead.

## 4. Invariants

Recovery is correct only while all of these hold.

- **I1, decision durability.** A commit is acknowledged only after its complete
  logical redo (all `Write`/`Patch`/`Truncate` frames and `Commit`) is durable
  in `transactions.log`.
- **I2, prefix durability.** The durable part of `transactions.log` is a prefix
  in `commit_seq` order. If commit *n* is durable, every *m < n* is durable or
  was enqueued empty.
- **I3, marker truth.** A durable `CommitMaterialized` for commit *c* implies
  that *c*'s page images and page `Commit` record are durable in `store.wal`, or
  in pages covered by a completed checkpoint. Recovery skips redo on the
  strength of the marker, so a marker that lies is data loss.
- **I4, marker prefix closure.** If a marker for *c* is durable, then markers
  are durable for every commit *b* whose page commit precedes *c*'s in WAL order.
  Otherwise replay could re-put *b*'s older row image over *c*'s newer one.
  (Today I4 follows from I3 plus row locks being held until the marker is
  written.)
- **I5, WAL before data.** No page reaches `store.pages` before the WAL records
  that describe it are durable. Writeback barrier: `paged.rs:5298-5302`.
- **I6, page-only durability.** A paged transaction with no redo in
  `transactions.log` (section 2, last list) returns only after its `Commit`
  record is durable. Because `store.wal` is sequential, that fsync also covers
  every earlier record.
- **I7, cut safety.** `transactions.log` is cut through commit *n* only when
  every commit up to *n* is page-durable: its page commit is durable in
  `store.wal`, or covered by a checkpoint.
- **I8, replay idempotence.** Re-applying a committed transaction whose pages
  are already durable produces the same logical state: same rows, same index
  answers, unique constraints intact.
- **I9, DDL ordering.** A sidecar change (create/drop collection or index) does
  not overtake a commit whose replay depends on the catalog before the change.
- **I10, replication.** Standbys receive frames only up to
  `min(written_seq, last_committed_tx)`, so only commits that are durable and
  applied (`replication_live.rs:156-160`). The synchronous quorum waits after
  every local sync and before the acknowledgment. The commit-log archive tails
  `transactions.log` asynchronously. Page-WAL archives seal and fsync segments
  independently of commit.
- **I11, checkpoint truncation.** `store.wal` is reset only after every page
  dirtied by the discarded records is durable, and under `write_lock`, so no
  record can be appended between the sync and the reset (`paged.rs:2373-2428`,
  `wal.rs:933-980`).

## 5. Experimental mode: the transaction log as the only synchronous point

The flag is `BICDB_COMMIT_DURABILITY=transaction-log`
(`DbConfig::with_commit_durability`). It is off by default and has an effect
only in `server_paged` with fsync on.

What changes, only for row commits that have redo in `transactions.log`:

- **Step 5:** the page `Commit` record is appended but **not fsynced**
  (`PagedStore::commit_deferred`). Rows become visible in page MVCC
  immediately. This is safe under I1: the logical redo is already durable, so a
  crash that loses the WAL tail is repaired by replay. The fallback is a full
  synced commit when the transaction frees pages (free-segment adoption).
- **Step 7:** the marker is queued with the commit's page `Commit` LSN instead
  of being written.
- **Materializer:** a background thread, every `BICDB_DEFERRED_MATERIALIZE_MS`
  (default 10 ms), takes the queue, calls one `store.wal` `sync_through(max
  lsn)`, and only then appends the queued markers in LSN order as one write.
  This satisfies I3 because the markers follow the sync, and I4 because the
  batch is LSN-ordered and a torn write leaves a prefix.
- **Barriers:** `cut_transaction_log` (I7), sidecar writes (I9), checkpoints,
  close and `checkpoint_for_resume` first run the materializer synchronously.
- **Steps 1-4 and 8-10 are unchanged:** I1, I2, I10. Page-only transactions keep
  the synced commit (I6). Eviction keeps the writeback barrier (I5).

What a crash costs in this mode: the WAL tail that was appended but not
synced, at most one materializer interval of commits, is lost from
`store.wal`. Their markers are absent too, so recovery replays them from
`transactions.log` (I8). Recovery time grows by that replay, and WAL growth is
unchanged.

What this mode must still demonstrate (section 6) before any rollout:
- I8 holds under real workloads, including unique indexes, lazy tables,
  updates and deletes of the same row across replayed and durable commits, and
  TRUNCATE.
- I9 holds with DDL concurrent with deferred commits.
- No acknowledged commit is lost at any injected crash point, and an
  interrupted recovery replays again correctly.

## 6. Verification plan

The test child (`crates/bicdb-core/examples/durability_child.rs`) runs a
concurrent workload against `server_paged`:
- Several writer threads, unique keys, hot-row updates with monotonic values,
  deletes, a unique secondary index, a B-tree secondary index.
- Periodic DDL.
- Each acknowledged commit is appended to an fsynced ledger only after commit
  returns.

The parent kills it at a named boundary with `BICDB_TEST_CRASH_AT=<point>:<n>`
(an `abort()` at the n-th hit). Power loss of the unsynced page-WAL tail is
emulated with `BICDB_TEST_WAL_VOLATILE_TAIL=1`: WAL appends stay in memory until
`sync_through`, so a kill discards exactly the bytes a power cut could discard.
After reopening, an independent reconciler checks:
1. every acknowledged commit's effects are present;
2. every transaction is all-or-nothing;
3. index lookups equal full scans;
4. unique constraints hold;
5. a second reopen changes nothing (idempotence).

Crash points:
- transaction-log write, before and after the page apply
- after the page commit append, before the materializer sync
- between the materializer sync and the marker write
- a torn marker batch
- each checkpoint phase, before and after `store.wal` reset
- `transactions.log` cut, before and after the floor
- sidecar write
- mid-replay during recovery

Both modes run the same matrix.

## 7. Results (2026-11-08)

All results are on branch `durability/txlog-only-commit`. Nothing here changes
the default commit path.

### Crash and fault injection

| Check | Production (`all-logs`) | Experimental (`transaction-log`) |
|---|---|---|
| Full matrix: 15 boundaries (commit, checkpoint, log cut, page writeback), interrupted replay | pass | pass, plus 4 materializer boundaries |
| Same matrix with a 256 KiB buffer pool (eviction and writeback during the workload) | pass | pass |
| Repeats of the hardest case (crash just after the transaction-log write) | 310/310 | 610/610 |
| Repeats: crash right after a page writeback, small pool | not run | 100/100 |
| Existing recovery, replication, PITR, backup and HA suites, forced `server_paged`, flag off vs on | pass | pass; one observability test made monotonic (below) |
| `bicdb-page` tests | 380/380 | (same binary) |

The reconciler checks every acknowledged commit, per-commit atomicity, index
lookups against a scan, unique-key resolution, collections recreated under a
reused name, and identical state after a second reopen.

What the matrix found, and how each finding was resolved:

1. **Test emulation flaw.** The first power-loss emulation truncated the page
   log without holding its file lock. Concurrent threads could then append
   after the cut or sync past it. That produced impossible logs: records
   missing from the middle, later ones kept. Fixed. The kept crash states
   were decoded to confirm the cause.
2. **Real defect in the experimental mode (I5).** A page logged by a deferred
   commit stays dirty with no pending log record. Writeback then skipped the
   barrier and could write the page ahead of its unsynced log records. Fixed:
   writeback first syncs through the latest deferred commit record. This only
   adds a sync. A deterministic page-level test reproduces the defect (with
   the fix reverted, a lookup fails with `OutOfBounds`) and passes with the
   fix. The end-to-end matrix did NOT detect this defect when the fix was
   reverted: the replay from the transaction log masks it unless a split
   leaves a dangling page. The page-level test is the regression guard.
3. **Behaviour change visible to observability.** With the flag on, a
   read-only scan that evicts a page logged by a deferred commit now syncs
   the page log first. One test compared WAL-sync counters across two
   samples for equality; it now checks that they never go backwards.

### Performance

Host s3ny: 24 cores, consumer NVMe, ext4, fsync on, shared at load 13-27.
Median of 3 trials of 30 s each. The spread between trials is about ±30%.
"Old" is the code in this branch. "New" adds a change that syncs the page log
without holding its append lock. That change is NOT part of this branch: it
is being qualified separately (section 8, step 2).

| Threads | Mode (code version) | p50 µs | p95 µs | p99 µs | Commits/s |
|---|---|---|---|---|---|
| 1 | all-logs (old) | 2655 | 8442 | 20520 | 267 |
| 1 | transaction-log (old) | 1744 | 4747 | 11024 | 458 |
| 4 | all-logs (new) | 6765 | 11278 | 22615 | 522 |
| 4 | transaction-log (new) | 5714 | 12628 | 16322 | 666 |
| 16 | all-logs (new) | 30634 | 125375 | 176765 | 383 |
| 16 | transaction-log (new) | 26427 | 55934 | 155778 | 620 |
| 64 | all-logs (new) | 120726 | 376428 | 507720 | 409 |
| 64 | transaction-log (new) | 128378 | 569001 | 741110 | 411 |

- **Where the experimental mode helps:** 1 to 4 concurrent writers, with 1.3 to 1.9× throughput and lower tails.
- **Where it doesn't:** it is roughly even at 16 writers, where the trial ranges overlap. At 64 writers it is even on throughput with worse tails. With 20 inserts per commit it gains little and is worse at 64 writers.
- **The ceiling is not the sync count.** Both modes stay at roughly 400 to 700 commits/s from 4 to 64 writers. A one-row commit writes about 70 KB of full page images (about 90× the logical record), under the store-wide write lock. That serial work bounds throughput no matter how many fsyncs are removed.
- **The lock change helps the production path by itself:** at 4 writers, 368 → 522 commits/s and p95 35 → 11 ms. It is a change to the production commit path, so it is held for a separate decision.

### Recovery and log growth

- **Recovery opens:** 0.3 to 1.0 s after a 16-writer crash, in either mode, driven by page-log size. No acknowledged commit was lost in any trial.
- **What the benchmark cannot measure:** its crash keeps unsynced bytes in the OS cache, so it does not measure the replay cost after a real power loss. The matrix shows that replay is correct, not how long it takes.
- **Log growth:** the transaction log grows by about 760 bytes per commit in both modes. Page-log size depends on checkpoint timing; cumulative growth needs a counter that does not exist yet.

### Replication and failover

- **Test results:** live replication, synchronous quorum, failover/promotion, standby bootstrap, PITR and online backup suites give the same results with the flag off and on.
- **Why the flag does not change replication:** standbys still receive frames only after the transaction log is durable and the commit applied (I10). The standby import path does not use deferral.

## 8. Rollout proposal

The experimental mode meets the tested guarantees, but its benefit is limited
to low concurrency, and it worsens tails at high concurrency. It should not
become the default. Proposed steps, each a separate decision:

1. **Merge the investigation with the flag off** (this branch). This adds:
   - this document;
   - the crash-injection points (one relaxed load each when unset);
   - the power-loss emulation;
   - the crash matrix and reconciler;
   - the benchmark;
   - the experimental mode behind `BICDB_COMMIT_DURABILITY=transaction-log`.

   Default behaviour is unchanged. Costs on the default path:
   - one relaxed load per crash point;
   - one atomic store per page-log sync;
   - one mutex lock per page writeback, for the deferred-commit hook. That
     hook does nothing unless the flag is on, because no deferred commit LSN
     is ever recorded.

   `tests/commit_durability_isolation.rs` pins this down:
   - the flag is off by default;
   - only the exact value `transaction-log` enables it;
   - it never engages outside `server_paged`;
   - the materializer never starts on the default path.

   A page-level test checks that synced commits never record a deferred
   commit LSN.
2. **The append-lock change ships on its own, separately.** It showed the
   clearest win (4 writers: +42% throughput, p95 −68%). It will be gated off
   until it passes qualification on:
   - concurrent commit ordering;
   - crash recovery;
   - checkpoint interaction;
   - fsync failures;
   - visibility;
   - replication.
3. **Opt-in only, for single- and low-concurrency write workloads**, e.g. a
   serial application job that commits many small transactions. Before
   anyone enables it in production:
   - repeat the crash matrix on the target hardware;
   - measure replay time after a real power cut (a VM hard reset, not a
     process kill);
   - watch materializer health (`deferred_materialize_stats`).
4. **Biggest lever, next:** reduce page-log volume per commit (page deltas
   instead of full page images, or a smaller logged footprint per row).
   That bounds both modes above 4 writers.
5. **Production recovery hardening found along the way**, for separate review:
   - **First-record check:** fail recovery when the first record of the active
     page log does not carry the LSN the last checkpoint published. That
     mismatch can only mean corruption, and checking it would have caught
     the emulation flaw directly.
   - **Possible mid-transaction eviction window:** a page evicted right after
     a leaf split, followed by a crash before commit. This was found by
     reasoning only and is untested.
