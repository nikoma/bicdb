# Commit page-log sync outside the store write lock (design)

Status: **prototype under qualification, gate off by default.** The owner
approved the prototype (contract items in section 10) once P0's fail-closed
semantics were settled (PR #55). Enabling it beyond tests needs the owner's
explicit approval. Code: `crates/bicdb-page/src/paged_commit_outside_lock.rs`,
`PagedStore::{commit_append, await_commit}`, and
`row_commit_with_sync_outside_lock` in `crates/bicdb-core/src/db/spatial_paged.rs`.

Code citations are to `main` at `decb9d0`.

Related documents:
- `docs/commit-durability-invariants.md` defines I1-I11, the commit steps (its
  section 2) and the crash matrix.
- `docs/wal-unlocked-sync.md` (draft PR #43) covers running `fdatasync` without
  the page-log *file* mutex.
- `docs/page-wal-write-amplification.md` (PR #40) measured the lock hold time.
- `docs/durability-failure-handling.md` (branch `durability-fail-closed`, P0)
  will define fsync-failure semantics. That branch has no commits yet. Section 8
  states what this design assumes from P0. Any difference must be resolved in
  P0's favour before the prototype is qualified.

## 1. Problem

Every core row commit in `server_paged` runs as one page transaction under the
store-wide `write_lock`:
- the outer guard is taken in `in_paged_transaction_inner`
  (`db/spatial_paged.rs:1508`);
- it is held across `begin`, every `put`/`delete` and `PagedStore::commit`;
- `commit_with_sync` (`paged.rs:2128-2200`) appends the dirty page images and
  the `Commit` record, then runs `Wal::commit`, which does `fdatasync`, while
  still holding the lock.

PR #40 measured that this fsync is 76-89% of the time the lock is held. Small
commits are therefore capped near 1/fsync-latency, about 840/s on s3ny,
whatever the number of writers. No two commits can share an fsync:
`group_commit_savings` stays 0 (`docs/wal-unlocked-sync.md` section 4).

## 2. Proposal in one paragraph

Under `write_lock`, a row commit appends its page images and its `Commit`
record exactly as today, then registers itself as **committing** (record LSN
known, not durable, not visible) and releases the lock. Outside the lock it
waits until the page log is durable through its LSN, sharing one `fdatasync`
with every other committer queued behind the same sync. Only then is it
**published**: marked committed in page MVCC, in commit-record LSN order. The
core commit path continues as today from the point where `PagedStore::commit`
returns: resident apply, `CommitMaterialized`, watermark, row-lock release,
quorum wait, acknowledgment. Nothing becomes visible, marked or acknowledged
earlier than today. The only thing that changes is that the next writer may
start its page work while this commit's fsync is in flight.

## 3. What `write_lock` protects today, and how each stays correct

| # | Protected today | With the sync outside the lock |
|---|---|---|
| P1 | **Single-writer B-trees.** Structure changes (splits, root publication in `sync_roots`) are not safe for concurrent writers (`paged.rs:735-758`). | Unchanged. All tree mutation, including `log_dirty_pages` and the commit-record append, still happens under the lock. The released window contains no tree mutation, only `sync_through` and publication, which touch the WAL sync state, the transaction table and the snapshot map. |
| P2 | **Commit attribution of dirty pages** (`log_dirty_pages`, `paged.rs:3224`). It takes every page "needing log", so it must not interleave with another transaction's writes. | Unchanged. The images are taken and logged before the `Commit` record, under the lock. A page that the next writer dirties after the release is logged again by that writer's commit (or by the writeback barrier), with that writer's xid. Recovery applies the newest committed image of a page. If the next writer never commits, its image is skipped and this commit's earlier image is used. Both cases are the same as today, where the next writer also dirties pages right after this commit. |
| P3 | **Heap/FSM placement and free-page adoption.** A commit that frees pages rewrites the free map, flushes and adopts segments after a synced commit (`paged.rs:2142-2152`). | Not moved. A transaction with pending frees takes today's synced path under the lock (the same fallback `commit_deferred` uses). Ordinary heap placement happens in `put`, under the lock, before the release. |
| P4 | **Checkpoint sync-to-reset window** (I11). A checkpoint syncs, flushes, publishes and resets the log under the lock, so no record is appended between the sync and the reset (`paged.rs:2397-2449`, `wal.rs:1005-1052`). | **New hazard, closed by the committing barrier (section 4.3).** Without it, a checkpoint could truncate the log while a commit is still waiting: its `Commit` record would be discarded, and `reset()` sets `durable_lsn = next_lsn - 1`, so the waiter returns success. Its status would be in neither the log, the watermark nor the spill (it is still in progress when `persist_status_spill` runs). After a crash, its rows are judged uncommitted, while the marker written afterwards tells recovery not to replay them. That would be data loss. The barrier makes every checkpoint first resolve all committing transactions, under the lock, before it freezes, spills or truncates. |
| P5 | **Visibility after durability.** `transactions.commit(xid)` runs only after `wal.commit` returned (`paged.rs:2153-2159`). | Preserved. Publication runs only for entries whose LSN is at or below `durable_lsn`, and only after a successful sync. A committing xid is `InProgress` in the transaction table, so `xid_visible` (`mvcc.rs:937-951`) returns false for every snapshot until publication. |
| P6 | **Visibility order = commit-record order.** Commits publish one at a time under the lock, so no snapshot sees commit B without commit A when A's record precedes B's. | Preserved by publishing from an LSN-ordered queue, as a prefix (section 4.2). |
| P7 | **Writer snapshot freshness.** A page transaction begins under the lock, after the previous one is published, so it never has a committed-but-unpublished transaction in its `in_flight` set. | **Changes**, see section 5. A row commit that begins while another is committing has it in `in_flight`. This is harmless for keys protected by core locks, and handled by an explicit wait-and-refresh for any other key. |
| P8 | **Other single-writer users:** `abort`, vacuum, orphan reclaim, dead-entry sweep, transaction-floor advance, header repair, `replace_faulty_version_chain`, backup capture (`try_lock_for`), non-row page transactions (bulk load, index and FTS builds and folds, spatial pack, entry-format records), replay at open. | Each of these first passes through the committing barrier, so it sees exactly the state it sees today: no committing transactions. The two exceptions are `abort` (it changes only its own xid and appends an abort record, which commit-wins already handles) and the reentrant `put`/`delete`/`commit` calls inside a row commit's own guard. |

## 4. Mechanism

### 4.1 The gate

- `PagedStoreOptions::with_commit_sync_outside_lock(bool)`, set from
  `DbConfig::paged_commit_sync_outside_lock`.
- That field defaults from `BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK`. Only the
  exact value `1` enables it. Default: off.
- Off means the original `commit_with_sync` and `in_paged_transaction_inner`
  code, byte for byte. The committing queue then stays empty, and the barrier
  is one relaxed load that finds it empty.
- It engages only for core row commits that would sync: production
  `all-logs` durability, fsync on. The experimental `transaction-log` mode
  already defers the sync, so the gate changes nothing there. Page-only
  transactions (I6) are not affected.

### 4.2 Committing queue and publication

The page store gains a `committing: Mutex<CommittingQueue>`. It is an
LSN-ordered map `Lsn -> Xid` plus a `failed` flag.

The commit is split in two.

`PagedStore::commit_append(xid) -> CommitTicket`, under `write_lock`:
1. Prepare free adoption. If any page is pending free, take the original
   synced path and return an already-published ticket.
2. `log_dirty_pages(xid)`.
3. `lsn = wal.commit_unsynced(xid)`. This also records `deferred_commit_lsn`,
   so the writeback barrier syncs through it before writing any page this
   commit logged (I5, the hook added by PR #36, `paged.rs:5329-5335`).
4. Insert `(lsn, xid)` into `committing`.
5. Return `CommitTicket { xid, lsn }`.

`PagedStore::await_commit(ticket)`, with no store lock held:
1. `wal.sync_through(lsn)`. With #43's unlocked sync this shares one
   `fdatasync` across every appender, which is group commit. With the locked
   sync it still works, but appends stall behind the in-flight fsync.
2. On success, `publish_durable()`: under the `committing` mutex, pop every
   entry with `lsn <= wal.durable_lsn()` in ascending LSN order. For each,
   run `transactions.commit(xid)` and remove its snapshot. Whoever syncs
   publishes the whole durable prefix, so a follower whose LSN a leader's sync
   covered is normally already published when it looks.
3. Return once the ticket's own xid is published.

The core side (`in_paged_transaction_inner`, gate on, row-commit call only):
take the guard, `begin`, apply, `commit_append`, drop the guard,
`await_commit`. An error from `await_commit` is handled exactly like an error
from `paged.commit` today: `recovery_required = true`, error returned. The
rest of `commit_transaction_inner` is unchanged and still runs after the page
commit returns.

Lock order: `await_commit` takes only leaf locks (`sync_state`, the WAL file
mutex on the locked sync path, `committing`, the transaction table, the
snapshot map). It never takes `write_lock`, a row lock or an index lock, so a
holder of `write_lock` that waits on it cannot deadlock.

### 4.3 The committing barrier

`write_lock` acquisition is split:
- `writer_guard_for_row_commit()` is used only by the core row-commit call.
- Everything else uses `writer_guard()`. After acquiring the lock, at
  outermost depth only, it calls `drain_committing()`: `sync_through(max
  committing lsn)`, then `publish_durable()`, and loops until the queue is
  empty. No new entry can appear while the lock is held, because entries are
  created only under it.

Reentrant acquisitions inside a row commit (its own `put`, `delete`, `commit`)
must not drain. Otherwise every contended commit would wait for the previous
commit's fsync again, under the lock. Depth is tracked per thread.

Checkpoints (`checkpoint`, both `write_lock` phases of `checkpoint_step`,
`finalize_structural_frees`) and every `Wal::reset` caller therefore run with
an empty queue. As a second guard, every truncation decision becomes
`!has_unfrozen_commits() && !backup_pinned() && committing.is_empty()`: with a
non-empty queue the checkpoint still flushes, but keeps the log. With the
barrier in place this guard is unreachable (the only way the barrier leaves
entries is a durability failure, and P0 then refuses the reset anyway), so a
mutant removing only this guard survives by construction; the barrier
mutant is the one the tests must kill.

Implementation: `writer_lock()` replaces every `write_lock.lock()` in the
page store. Gate off it is exactly `write_lock.lock()`. Gate on it drains when
`!write_lock.is_owned_by_current_thread()` (outermost depth). The row commit
takes `row_commit_guard()`, which never drains. Backup capture
(`try_lock_for`) drains after acquiring.

### 4.4 Crash points

These are new, and exist only on the gated path:
- `commit.page.appended_unlocked`: after the guard is released, before the
  sync;
- `commit.page.synced_unpublished`: after `sync_through` returned, before
  publication;
- `commit.page.published`: after publication, before returning to the core;
- `commit.barrier.drained`: after a writer-lock barrier emptied the queue,
  before the holder (often a checkpoint) proceeds.

## 5. Conflict and visibility rules with a committing transaction

Definitions: a committing transaction C has its `Commit` record appended, is
not yet published, and is `InProgress` in the transaction table.

- **Readers.** No snapshot sees C's writes until C is published. Snapshots
  taken while C is committing keep C in `in_flight` forever. This is the same
  as a snapshot taken while C was mid-apply today, and is correct snapshot
  isolation.
- **Core row commits (the gated path).** A later row commit W that touches a
  row, unique claim or index key C also touched cannot reach page apply while
  C is committing:
  - row write locks are released only after `PagedStore::commit` returns
    (`spatial_paged.rs:2516-2521`);
  - the page keys a row commit writes are all keyed by the row's primary key:
    the record key, B-tree entry `index_entry_key(index, key, pk)`, FTS posting,
    tombstone and v2 twin keys, doc-terms. So they are covered by the row lock.

  So W's `in_flight` may contain C, but W never reads or overwrites a version
  C wrote. W's commit record has a higher LSN than C's, so W publishes after C
  (section 4.2). If C's sync fails, W cannot be published either (section 8).
- **Safety net for an unexpected overlap.** Raising `WriteConflict` against a
  committing writer would be spurious, and on this path fatal: a page-apply
  error after the durable decision fences the handle. As implemented:
  1. `begin_row_commit` removes every transaction committing at that moment
     from the new row commit's snapshot `in_flight` set. The host ordered them
     before it (their decisions and page work came first, under the same
     lock); with the locked commit this transaction would only have begun
     after their publication. They still count as committed only once
     published (`TxStatus::Committed`).
  2. Each row-commit `put`/`delete` records a hash of its key. If the key was
     written by a committing transaction, the writer waits for that
     transaction's publication (`sync_through` its LSN, publish the durable
     prefix), still holding `write_lock`. This is correct, only slow, and it
     needs no lock the committing transaction holds.
  3. A "decided before xmax" read (`xid == 0`, empty `in_flight`: the host's
     pre-image read, `pre_snapshot` in `apply_record_writes_to_paged`) of a key
     a committing transaction wrote also waits for its publication first.

  `CommittingStats::overlap_waits` counts these waits. Core row locks make
  them unreachable from the core commit path. The core workload test asserts
  0, so a missing core lock shows up as a failure rather than being masked.
- **Non-row writers.** They see an empty queue (section 4.3), so their
  conflict and visibility behaviour is unchanged.

## 6. Group commit

- With #43 on (unlocked sync), appends proceed during an `fdatasync`. The
  first committer to call `sync_through` leads. Committers that append during
  its sync wait on `sync_signal` and are covered by the next sync. With N
  writers the expected cost is about two fsyncs per batch rather than one per
  commit.
- With #43 off, the leader holds the WAL file mutex across `fdatasync`. The
  next committer can do its puts in memory, but blocks in `log_dirty_pages`
  until the sync ends. The overlap is limited to the page-apply phase.

The prototype will be measured both ways. The recommendation will be to ship
this gate only together with, or after, #43. No timer or extra delay is added:
the batch is whatever accumulates during one fsync.

## 7. Order of durability, visibility, shipping and acknowledgment

Per commit, after this change (the numbers refer to the steps in section 2 of
the invariants document):

1. Step 4: the `transactions.log` decision is durable (unchanged, I1, I2).
2. Step 5a: page images and `Commit` record appended under `write_lock`
   (unchanged); committing entry registered; lock released (**new**).
3. Step 5b: page log durable through the record, possibly by another thread's
   fsync (**new owner, same condition**).
4. Step 5c: page MVCC publication in LSN order, possibly by another thread
   (**new owner, same condition**).
5. Steps 6-10: resident apply, `CommitMaterialized` (I3 holds: it follows 5b),
   applied watermark, row-lock release, synchronous quorum, acknowledgment
   (unchanged).

Replication shipping (I10) is unchanged. Standbys receive frames only up to
`min(written_seq, last_committed_tx)`, and `last_committed_tx` advances in
`mark_committed_seq`, after step 5c. The synchronous quorum still waits after
every local sync. Page-log archive segments may now contain a `Commit` record
that is not yet published locally. That is the same as a sealed segment
containing a record whose sync is in flight today, and the transaction-log
decision for it is already durable.

I4 (marker prefix closure) relies on row locks held until the marker is
written. That is unchanged: two commits whose markers could be reordered still
cannot touch the same row.

## 8. Fsync failure (aligned with P0)

Assumed from P0: a failed page-log sync puts the store into an explicit
unhealthy state, with no later sync publishing anything, and every commit past
its durable decision reports *outcome unknown*.

With the gate on:
- The thread whose `sync_through` fails sets `committing.failed`. No entry is
  ever published after that, even if a later fsync succeeds. The kernel may
  have dropped the dirty pages and cleared the error (#43, risk 4). The
  existing path has the same exposure for its single committer. Here it would
  extend to every queued commit, so the flag closes it for this mechanism
  only.
- Every queued committer gets an error that the core maps exactly as today:
  `recovery_required = true`, receipt "recovery required". When P0 lands, this
  becomes P0's outcome-unknown error and unhealthy transition. The flag is a
  local interlock, not a second failure policy.
- No abort record is written for a committing xid. The commit-wins rule in
  `Wal::abort` (`wal.rs:718-723`) already forbids it, because the record may
  be on disk.
- On restart each queued commit's logical redo is durable (step 4), so
  recovery replays it from `transactions.log` (no marker). The outcome is
  committed after restart, as for the single failed commit today.
- The barrier on a failed queue returns the failure, so checkpoints, DDL and
  maintenance fail closed instead of truncating.

What changes: today one failed fsync makes one in-flight commit's outcome
unknown. Commits behind it fail at `check_recovery_state`, but those already
past step 4 have a durable decision too, so they are also outcome-unknown.
With the gate on, the set in flight at the moment of failure is larger: every
committing entry. The semantics per commit are the same. Section 10, item C2.

## 9. Qualification plan

All runs on s3ny, gate off vs on, and on top of #43 where noted.

1. **Gate isolation.** Off by default; only `1` enables it; near misses
   rejected; builder both ways. With the gate off, the committing queue is
   never written (counter = 0), and the crash matrix and suites match `main`.
2. **Concurrent commit ordering** (page tests):
   - N writers on disjoint keys, a monitor thread checking that published
     order equals commit-record LSN order and that `durable_lsn >= lsn` at
     every publication;
   - a paused-sync hook: a second commit appends and registers while the
     first one's sync is paused; a fresh snapshot sees neither; both publish
     in LSN order after release;
   - followers covered by a leader's sync are published by the leader.
3. **Crash matrix** (`tests/commit_durability_crash.rs` and
   `examples/durability_child.rs`), gate on:
   - `all-logs` and `all-logs+small-pool`, plus the four new crash points
     (section 4.4) at 3 hit counts each, plus interrupted replay;
   - the reconciler checks every acknowledged commit, atomicity, index = scan,
     unique keys and second-reopen idempotence;
   - repeats: 50 rounds at the new points and at `commit.after_txlog_durable`;
   - the same with #43 on.
4. **Checkpoint races:**
   - a checkpoint, `checkpoint_step` Finalize, and backup capture each racing
     a paused committer: the barrier must publish it first and the log must
     keep its record until then;
   - a `reset()` attempted with a non-empty queue fails with an error;
   - a mutant that removes the barrier must be killed by these tests (and by
     the crash matrix).
5. **Fsync failure,** using `SyncTestHooks` from #43 now and P0's injection
   once available:
   - one failure with K queued committers: all K error, none published, the
     queue is failed, a later successful sync publishes nothing, checkpoint
     refuses;
   - after reopen, each one's outcome matches what recovery reconstructs.
6. **Visibility.** A snapshot taken between append and publication sees no
   row of the committing transaction, gate on, paused at each phase.
   `commit_visibility`, `sync_transaction_visibility` and `mvcc_chain_recovery`
   (core, forced `server_paged`), gate off vs on.
7. **Overlap safety net.** A constructed page-level overlap (a second row
   commit writing a key the committing one wrote) waits and succeeds instead of
   conflicting. `committing_overlap_waits` = 0 on every core workload.
8. **Replication,** forced `server_paged`, gate off vs on:
   - core: `live_replication`, `log_archive_pitr`, `backup_online`,
     `backup_online_wal_rotation`, `backup_paged`, `replication_journal`;
   - pgwire: `embedded_ha`, `online_backups`.
9. **Regression.** `bicdb-page` full suite, and `paged_concurrency`,
   `paged_checkpoint_maintenance` and `paged_observability`, with the gate on.
10. **Benchmark.** `commit_durability_bench run <dir> all-logs T 30`, release,
    fsync on, s3ny NVMe, T in 1/4/16/64, 3 trials, same host, back to back.
    Arms: off, P2b, #43, P2b+#43. Reported: commits/s median and range,
    p50/p95/p99, syncs per commit, `group_commit_savings`.

Mutants to kill:
- publish before the sync;
- publish out of LSN order;
- no barrier in checkpoint;
- the barrier drains at reentrant depth (performance only, measured);
- a publish allowed after a failure;
- conflict instead of wait in the safety net.

## 9a. Qualification results (2026-11-08)

All runs on s3ny (24 cores, ext4 on NVMe, fsync on), on `main` at `e4647d8`
(fail-closed, PR #55) plus this change. "Gate on" means
`BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK=1`. "Both" adds #43's
`BICDB_PAGED_WAL_SYNC_UNLOCKED=1`, on a bench-only branch that combines the
two changes (`p2b-combo-bench3`, not proposed for merge).

| Area | Test | Result |
|---|---|---|
| Gate | `commit_sync_outside_lock_is_strictly_opt_in` (core): default off; only `1` enables it, 11 near misses rejected; builder both ways; off means no row commit enters the committing queue (counter 0), on means all 10 do; no page store outside `server_paged` | pass |
| Gate | `the_gate_is_off_by_default_and_off_means_the_locked_commit` (page): off, `commit_append` is the synced commit, visible on return; queue counters stay zero; no deferred commit LSN recorded | pass |
| Ordering | `publication_is_durable_first_and_in_commit_record_order`: 8 writers × 60 row commits, a monitor checks every snapshot (visible ⇒ LSN ≤ `durable_lsn`; invisible ⇒ above every visible LSN); all present after reopen | pass |
| Ordering | `publication_never_covers_a_record_past_the_durable_lsn`: a durable commit publishes while a later appended commit stays invisible | pass |
| Ordering | `concurrent_row_commits_with_the_gate_on/off` (core): 8 writers × 80 commits (insert, own hot row, deletes, unique index) plus a contended shared row; a reader never sees a hot row go backwards; `overlap_waits == 0`; published == enqueued; state identical after reopen | pass |
| Lock released | `a_writer_proceeds_while_a_commit_sync_is_in_flight_and_readers_do_not_see_it`: with the sync paused (#55's I/O hook), another writer takes the writer lock and does page work; a reader does not see the committing row | pass |
| Crash matrix | `outside_lock_path_survives_every_crash_point` and `_with_eviction` (256 KiB pool): 15 common points + 9 new-window points (`commit.page.appended_unlocked`, `commit.page.synced_unpublished`, `commit.page.published`, each at hits 4/60/250) + 2 interrupted replays; reconciler: every acknowledged commit, atomicity, index = scan, unique keys, no resurrected generation, identical second reopen | pass |
| Crash matrix | `outside_lock_path_survives_crashes_inside_the_checkpoint_barrier` and `_with_eviction`: `+small-wal` (256 KiB page-log trigger, so commits run checkpoint steps under the shared admission and meet committing transactions); `commit.barrier.drained` at hits 1/5/20 + the 9 window points + interrupted replay | pass (the points are reached) |
| Crash matrix | Production (`all-logs`, ±small pool) and transaction-log matrices, gate off | pass (regression) |
| Crash matrix | Combined with #43 (bench branch): every production, transaction-log, unlocked-sync, outside-lock and both-gates matrix (20 tests) | pass |
| Repeats | 50 rounds each: `commit.page.synced_unpublished:60`, `commit.page.appended_unlocked:60`, `commit.after_txlog_durable:150`; small pool + small WAL `commit.barrier.drained:5`; small pool `pool.after_writeback:120` | 250/250 |
| Checkpoint | `a_checkpoint_resolves_committing_transactions_before_it_truncates` (a committing transaction across `checkpoint()`, then reopen), the same for bounded `checkpoint_step`, `a_checkpoint_waits_for_a_paused_commit_sync`, `non_row_writers_never_coexist_with_a_committing_transaction` (write guard, vacuum) | pass |
| Eviction (I5) | `eviction_of_a_committing_page_syncs_the_log_first`: 200 committing transactions with a 16-page pool, all present after reopen | pass |
| Fsync failure | `one_failed_sync_fails_every_queued_commit_and_publishes_nothing` (1, 4, 16 queued): exactly one fsync attempted; every waiter gets `WriteFailed`; none visible; the queue stays; later syncs and checkpoints refuse; earlier commits intact | pass |
| Fsync failure | `a_covered_but_unpublished_commit_is_not_reported_committed_after_a_failure`: a commit a successful sync covered, not yet published when a later sync fails, returns an error and stays invisible (P0: never reported committed after the failure) | pass |
| Fsync failure | `outside_lock_path_fails_closed_at_every_io_failure_site` and `_with_eviction`: #55's `BICDB_TEST_FAIL_IO` specs (transaction-log write/sync/repair, page-log sync/write/reset, page-file sync, log-cut sync); the child reports any acknowledgment after the failure and any non-durability error; marker required; reconciled twice against the acknowledged-commit ledger | pass |
| Overlap safety net | `an_overlapping_row_commit_waits_instead_of_conflicting` (pre-image read and put) and `an_overlapping_write_without_a_pre_image_read_waits` (put and delete) | pass |
| Visibility | `commit_visibility`, `sync_transaction_visibility`, `mvcc_chain_recovery`, forced `server_paged`, gate off vs on | identical, pass |
| Replication / failover | Core, forced `server_paged`, gate off vs on: `live_replication` 31, `log_archive_pitr` 8, `backup_online` 3, `backup_online_wal_rotation` 1, `backup_paged` 4, `replication_journal` 2, `durability_fail_closed` 16 (includes quorum `08007` after fencing and failover after a primary failure). pgwire: `embedded_ha` 6, `online_backups` 4 | identical, pass |
| Regression | `paged_concurrency` 13, `paged_checkpoint_maintenance` 4, `paged_observability` 4, gate off vs on; `bicdb-page` full suite (326 lib + integration), gate default | pass |

The replication and visibility suites ran on the #55 branch tip (`28ffd8c`)
before it merged. `main` differs from it only in `bicdb-sql`. The page suite,
the crash matrices and the repeats ran on the rebased branch.

### Test strength (mutants)

| Mutant | Page tests | Crash / core tests |
|---|---|---|
| Publish past `durable_lsn` | killed (`publication_never_covers_a_record_past_the_durable_lsn`) | — |
| Publish in reverse LSN order | killed (2 tests) | — |
| No committing barrier (writer lock and backup capture never drain) | killed (3 checkpoint/barrier tests) | killed (both barrier crash matrices: acknowledged commits lost) |
| Publish after a durability failure | killed | — |
| Report committed after a failure | killed | — |
| Skip the sync before publication | killed (7 tests) | killed (children never publish a commit) |
| Conflict instead of wait (safety net off) | killed | survives: core row locks make the overlap unreachable |
| Committing transactions left in a new row commit's `in_flight` | killed (2 tests) | survives, same reason |
| No truncation guard (barrier intact) | survives | survives: unreachable by construction (section 4.3) |

The two safety-net mutants survive the core tests, which is the expected
outcome: they confirm that the core never reaches the safety net, and the
page tests exercise it directly. The truncation guard is defence in depth
behind the barrier.

### Measurements

Command: `commit_durability_bench run <dir> all-logs <writers> 30` (production
commit path, one-row commits with a unique and a B-tree index), release
build, fsync on, s3ny NVMe. The four arms come from one binary built on the
bench branch:
- `off`: both gates off, which is `main`'s commit path;
- `p2b`: this change;
- `u43`: #43;
- `both`: the two together.

Each trial ran every arm at every writer count, with the arm order rotated
per trial and per writer count. There were two batches of 5 trials (13:12 to
13:54 and 13:54 to 14:35), so 10 trials per cell. The host is a shared warm
standby: the 1-minute load average at run start ranged from 4 to 30.
"syncs/commit" is page-log `fdatasync` calls per commit.

| Writers | Arm | Commits/s median (range) | p50 ms | p95 ms | p99 ms | syncs/commit | load (median) | n |
|---|---|---|---|---|---|---|---|---|
| 1 | off | 224 (57-559) | 3.1 | 14.7 | 26.9 | 1.01 | 12.1 | 10 |
| 1 | p2b | 280 (+25%) (68-528) | 2.7 | 9.6 | 26.6 | 1.01 | 10.9 | 10 |
| 1 | u43 | 243 (+9%) (47-545) | 3.4 | 8.2 | 21.4 | 1.01 | 13.0 | 10 |
| 1 | both | 205 (-8%) (12-614) | 2.8 | 18.2 | 25.2 | 1.01 | 11.7 | 10 |
| 4 | off | 510 (75-779) | 6.6 | 10.1 | 19.6 | 1.01 | 12.1 | 10 |
| 4 | p2b | 504 (-1%) (178-820) | 6.0 | 13.7 | 27.7 | 1.01 | 11.5 | 10 |
| 4 | u43 | 361 (-29%) (201-821) | 8.0 | 28.6 | 76.0 | 1.01 | 10.5 | 10 |
| 4 | both | 350 (-31%) (77-504) | 7.9 | 40.2 | 81.2 | 0.99 | 10.7 | 10 |
| 16 | off | 438 (268-637) | 25.6 | 53.1 | 218.8 | 1.01 | 7.0 | 10 |
| 16 | p2b | 534 (+22%) (316-709) | 19.5 | 108.2 | 213.2 | 1.01 | 7.4 | 10 |
| 16 | u43 | 528 (+20%) (251-875) | 23.6 | 57.7 | 186.1 | 1.01 | 10.9 | 10 |
| 16 | both | 664 (+52%) (386-1301) | 18.0 | 61.9 | 143.0 | 0.45 | 8.2 | 10 |
| 64 | off | 627 (369-777) | 99.2 | 166.0 | 487.0 | 1.01 | 16.2 | 10 |
| 64 | p2b | 534 (-15%) (327-1112) | 62.2 | 395.6 | 811.7 | 1.01 | 9.0 | 10 |
| 64 | u43 | 411 (-34%) (192-858) | 92.6 | 549.5 | 805.5 | 1.01 | 7.5 | 10 |
| 64 | both | 550 (-12%) (147-1216) | 84.8 | 435.7 | 592.8 | 0.22 | 16.5 | 10 |

How to read this:

- **The noise is larger than most of the effects.** Within a cell, trials
  span 2 to 40 times. At 1 writer, where none of the arms can change anything,
  the medians still differ by up to 25%. Only a dedicated host can settle the
  throughput figures.
- **Group commit works only with both changes.** With `both`, page-log syncs
  per commit fall to 0.45 at 16 writers and 0.22 at 64. The queue reaches the
  full writer count, and most publications are done by another committer's
  sync (`published_by_other`, in the result JSON). With `p2b` alone, syncs
  stay at 1.01 per commit and the queue depth stays at 1 to 8. The locked
  page-log sync still holds the log's file mutex across `fdatasync`, so the
  next committer cannot append its commit record until the sync ends.
- **16 writers is the one consistent gain.** `both` is +52% over `off`
  (median of 10). The two batches separately gave +38% and +62%, and p99 was
  143 ms against 219 ms.
- **64 writers: no throughput gain, and worse tails.** `both` does five times
  fewer syncs, yet throughput is not higher, and p95 is worse than `off` in
  both batches. The remaining serial work under the writer lock caps it: the
  full page images (about 70 KB per one-row commit,
  `docs/commit-durability-invariants.md` section 7), plus the committing
  barrier. The commit-triggered checkpoint steps drained the queue under the
  lock 7 to 161 times per 30-second run. This matches PR #40's conclusion:
  above a few writers, the volume of page-log data per commit is the next
  bound.
- **4 writers is negative for `u43` and `both`.** The figures are -29% and
  -31%, with wide ranges. #43's own earlier run on the same host showed +25% at
  4 writers. This is not resolved here.
- **Correctness counters:** `overlap_waits` was 0 in every run.

## 9b. Remaining risks

1. **Benchmark noise.** No throughput claim except "group commit happens
   with both gates" survives the noise on s3ny. A dedicated host is needed
   before any enablement decision, especially for 4 and 64 writers.
2. **Tails at 64 writers.** p95/p99 are worse with the gate on. A likely
   cause is the barrier: commit-triggered checkpoint steps drain the queue
   while holding the lock, so every committer waits for one more fsync. A
   remedy, for example the supervisor doing checkpoint steps instead of
   committers, needs its own measurement.
3. **Real power loss** is not tested. The emulation cuts the active page log
   to the last published sync offset (as in #43). A VM hard-reset test on the
   target hardware is needed before enabling.
4. **The overlap safety net is unreachable from the core path.** That is by
   design and proved by the mutants. Any future core path that writes shared
   page keys in a row commit without a core lock would reach it; it would
   then be correct, but slow (a sync under the lock).
5. **`p2b` without #43 does little:** no group commit. The value is in the
   combination, which needs #43 merged, or rebased onto `main` with #55's
   fail-closed sync (done on the bench branch, not on #43 itself).
6. **The waiting moves.** DDL, index builds, vacuum and backup capture now
   wait under the lock for in-flight commit syncs (item C4). This was not
   measured under a mixed DDL load.

## 10. Contract and semantics changes needing approval

None of the durability invariants I1-I11 is weakened. A commit is still
acknowledged only after both logs are durable through it. The following are
nonetheless changes to what the system does, and need approval before the
prototype is built:

- **C1. Visibility owner and timing.**
  - A page transaction's commit can be published by another thread's sync,
    after its own lock is released.
  - Publication stays durable-first and in LSN order.
  - Between append and publication, the transaction is an in-progress xid
    whose record is already in the log.

  No snapshot sees anything earlier than today. *Approval asked for:* the
  existence of the committing state.
- **C2. Fsync-failure blast radius.** One failed page-log sync makes every
  committing transaction outcome-unknown, not only the leader's, and the queue
  stays failed until restart. Recovery commits each of them from
  `transactions.log`. The per-commit semantics are unchanged, but more commits
  can be in that state at once. This must match P0's unhealthy state. *Approval
  asked for, jointly with P0.*
- **C3. Page-level write rule for row commits.** A row-commit page transaction
  that meets a committing writer's version waits for publication and then
  treats it as committed before its own snapshot, instead of raising
  `WriteConflict`. Core-level isolation (row locks, unique claims,
  SERIALIZABLE admission) is unchanged and decides every logical conflict. This
  is expected never to trigger (section 5). *Approval asked for:* the snapshot
  refresh, which is a deliberate exception to the page store's
  first-updater-wins rule for this one caller.
- **C4. Latency moves to non-row writers.** DDL, index and FTS builds, vacuum,
  checkpoints and backup capture now wait, under the lock, for the in-flight
  commits' fsync before they start. There is no semantic change. Backup
  capture's coordination budget (`try_lock_for`) now includes that wait.
  *Notice, not a contract change.*
- **C5. Page-log contents.** At any instant the log can hold several commit
  records whose sync is in flight, not at most one. Recovery already treats an
  unsynced tail generically (torn tail truncated, a commit counts only if its
  record survived), so the format and recovery code are unchanged. *Notice.*

Not changed:
- the `transactions.log` protocol;
- marker semantics;
- the replication watermark;
- quorum ordering;
- the checkpoint truncation condition (it is only strengthened);
- page-only transaction durability;
- the defaults.

## 11. Expected effect and limits

- PR #40 measured the fsync at 76-89% of the lock hold. With the sync outside
  the lock and group commit (#43), the lock hold for a small commit drops to
  the remaining 11-24%. That puts the serial ceiling at roughly 4-9 times
  today's for small commits.
- That ceiling is then set by page-image volume under the lock, about 70 KB
  per one-row commit (`docs/commit-durability-invariants.md` section 7), and by
  how many commits one fsync can batch.
- Hot-row workloads do not gain on the hot row. Row locks are held until
  publication, so the same row commits at most once per fsync, as with any
  synchronous-commit database.
- A single writer gains nothing.
