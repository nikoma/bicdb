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
