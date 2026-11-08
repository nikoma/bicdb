# Commit page-log sync outside the store write lock (design)

Status: **design, awaiting approval of the contract items in section 10.
No code yet.** The prototype will sit behind a gate that is off by default.

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
an empty queue. As a second guard, the truncation decision becomes
`!has_unfrozen_commits() && !backup_pinned() && committing.is_empty()`, and
`reset()` refuses to run while the queue is non-empty. The refusal is an error,
not a silent skip, and it is covered by a test.

### 4.4 Crash points

These are new, and exist only on the gated path:
- `commit.page.appended_unlocked`: after the guard is released, before the
  sync;
- `commit.page.synced_unpublished`: after `sync_through` returned, before
  publication;
- `commit.page.published`: after publication, before returning to the core;
- `checkpoint.barrier.drained`: after the barrier emptied the queue, before
  the freeze.

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
- **Safety net for an unexpected overlap.** If `write_base` or
  `check_write_conflict` meets an `xmin`/`xmax` that is in the committing
  queue, raising `WriteConflict` would be spurious, and on this path fatal: a
  page-apply error after the durable decision sets `recovery_required` and
  fences the handle (`spatial_paged.rs:1510-1527`). So instead, the writer:
  1. calls `sync_through(C.lsn)` and `publish_durable()`, still holding
     `write_lock`. This is correct, only slow. It needs no lock that C holds;
  2. refreshes its own stored snapshot so C counts as committed before it.
     This is allowed only for row-commit page transactions, whose logical
     isolation was already decided by the core, and is what today's ordering
     would have produced;
  3. re-reads the chain head and continues.

  The pre-image read uses `pre_snapshot { xmax: xid, in_flight: {} }` at
  `spatial_paged.rs:1367-1371`, which judges C uncommitted while it is
  committing. It goes through the same check: if the version it would return
  was written by a committing xid, it waits for publication first. A counter,
  `committing_overlap_waits`, counts these. The qualification expects 0 on
  every workload, and a debug assertion flags any non-zero value in tests, so
  that a missing core lock is found rather than masked.
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
