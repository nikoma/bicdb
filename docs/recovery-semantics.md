# Recovery semantics (server_paged)

Status: specification of the code on `main` at `decb9d0` plus the crash points
added with this document. It builds on
[commit-durability-invariants.md](commit-durability-invariants.md), which
describes the commit order (section 2), the open-time order in outline
(section 3) and invariants I1-I11 (section 4). This document does not repeat
them. It defines what recovery does, exactly, and marks with **GAP** every
place where the code does not currently guarantee the rule a reader would
expect. The GAP list is in section 9.

Citations are `file:line` in this tree, relative to `crates/`. Short names
mean: `db.rs`, `storage.rs` = `bicdb-core/src/`; `wal.rs`, `paged.rs`,
`pool.rs`, `btree.rs`, `manager.rs` = `bicdb-page/src/`.

## 1. Which log is authoritative for what

| Question | Authority | Why |
|---|---|---|
| Did a row commit happen? | `transactions.log`: a durable `Commit{tx_id, commit_seq}` frame | The commit decision is the `O_DSYNC` write of the frames (`bicdb-core/src/db/spatial_paged.rs:2414`), before any page is touched (I1). |
| In what order do row commits apply? | `commit_seq` in `transactions.log` | Replay sorts by it (`bicdb-core/src/db.rs:19082`). File order is not the order. |
| Is a row commit already in the page store? | A durable `CommitMaterialized` frame for it (I3) | Its writes are discarded at read time (`db.rs:18991-19003`). Without a marker the commit is replayed, whether or not its pages made it. |
| What bytes does a page hold? | The page store: `store.pages` up to its published horizon, then committed images in `store.wal` from the redo point | Section 3. |
| Did a page-store transaction (xid) commit? | A `Commit(xid)` record in `store.wal`; after truncation, the meta page's `frozen_xid` watermark, its abort-exception list, and the status spill | `wal.rs:1270-1296`, `paged.rs:1043-1071`, and the truncation gate at `paged.rs:2435`. |
| Which collections and indexes exist? | The sidecar catalogs | Never in either log (I9). A replayed write to a collection that is not in the catalog is skipped (`db.rs:19105`). |
| Is a page-only write durable (bulk load, index and FTS builds, spatial pack)? | Only `store.wal` (I6) | These have no frames in `transactions.log`. |

The two logs share no identifier. Recovery never correlates a page xid with a
`tx_id`. It relies on the marker (I3/I4) and on replay being idempotent (I8).

## 2. Recovery order

`BicDb::open_with_config` (`bicdb-core/src/db.rs:9473`), in `server_paged`
mode:

1. **Refuse a missing page store.** If `paged/store.pages` is absent while a
   `commit_seq` floor file or a non-empty `transactions.log` exists, open fails
   (`db.rs:9571-9580`).
2. **Page store** (`PagedRecords::open` → `PagedStore::open`,
   `bicdb-page/src/paged.rs:880`):
   1. take the directory lock (`paged.rs:917`), so two processes can never
      replay the same log;
   2. open `store.pages` and its superblock: root page, page count, free list,
      checkpoint horizon `H` (`checkpoint_lsn`);
   3. open `store.wal` with `H` as the LSN floor (`paged.rs:936`), discovering
      sealed `store.wal.<first-lsn>` segments (`wal.rs:1164`);
   4. **page recovery**, `wal::recover_with_outcomes` (`wal.rs:1246`, section
      3), before any page is read;
   5. read the meta page: tree roots, `frozen_xid`, `next_xid`, abort
      exceptions, status-spill descriptor;
   6. rebuild the transaction table: watermark, then spill, then the outcomes
      from the log (`paged.rs:1043-1050`). `next_xid` advances past every xid
      named by any record, including page images of transactions that never
      reached a terminal record (`paged.rs:1053`). Every xid issued by an
      earlier process without a terminal record is then aborted
      (`seal_previous_incarnations`, `paged.rs:1058`), and an open-time freeze
      absorbs what it can (`paged.rs:1069`).
3. **Transaction log read** (`recover_transaction_log`, `db.rs:18918`): the
   whole file is streamed, a torn tail is truncated and synced
   (`storage.rs:700`, section 4.1), and the writes of every transaction with a
   `CommitMaterialized` frame are discarded as soon as the marker is read.
   The `commit_seq` floor file raises `last_commit_seq` (`db.rs:9617`).
4. **Catalogs** load. Collections whose indexes are all durable kinds open
   lazy (`db.rs:9852`).
5. **Logical replay** (`apply_unmaterialized_transaction_log`,
   `db.rs:19045`, section 5).
6. **Recovery checkpoint**: a full paged checkpoint (`db.rs:10179`), which
   makes the replay durable and normally truncates `store.wal`.
7. `sync.log` / outbox / audit-outbox replay (`db.rs:10238-10297`).
8. **Transaction-log cut at open**, unless replication is enabled or a broker
   outbox message could not be flushed: persist the `commit_seq` floor, then
   replace the log with an empty one (`db.rs:10300-10313`, section 4.5).

Steps 2-8 are each restartable: a crash anywhere in them leaves a state that
the same sequence recovers (section 7).

## 3. Page recovery: which records are replayed, in what order

`wal::recover_with_outcomes` (`bicdb-page/src/wal.rs:1246`).

### 3.1 The record chain

- Record kinds (`wal.rs:71`): `PageImage` (a full after-image of one page),
  `Commit(xid)`, `Abort(xid)`, `Checkpoint(redo_lsn)`.
- LSNs are assigned under the file lock together with the write, so file order
  is LSN order (`wal.rs:564`, comment at `wal.rs:576-587`).
- The chain is the sealed segments in ascending name order, then the active
  file (`wal.rs:928-958`). A record never spans files (`wal.rs:589-590`).
- Decoding stops at the first record that has a bad magic or reserved bytes,
  an unknown kind, an impossible length, a CRC mismatch, or an LSN that does
  not continue the previous one (`wal.rs:1089-1150`, the LSN rule at
  `wal.rs:1130`). LSN continuity is checked across file boundaries.
- In a **sealed segment**, stopping before its end is corruption and open fails
  (`wal.rs:938-956`). In the **active file**, everything after the stop is a
  torn tail. Pass 1 truncates it under the file lock (`wal.rs:984-998`).

### 3.2 Pass 1: outcomes and the redo point

One scan over the chain (`wal.rs:1260-1296`):

- `Commit` and `Abort` records give each xid its outcome. An xid with both is
  corruption and open fails (`wal.rs:1297-1303`). `Wal::abort` refuses to
  write an abort after a commit in the same generation, so the pair is
  unwritable (`wal.rs:723`).
- The redo point is the `redo_lsn` of the **last** `Checkpoint` record in the
  chain (`wal.rs:1290`).
- The redo point is used only if the page file's own horizon vouches for it,
  that is `H >= redo_lsn`. Otherwise recovery replays from LSN 0
  (`wal.rs:1304-1317`). This covers a restored base whose superblock predates
  later checkpoints in an archived log.
- The highest xid named by any record is kept (`wal.rs:1263-1268`).

### 3.3 Pass 2: what is applied

A second scan applies, in chain order, every record that satisfies all of the
following (`wal.rs:1339-1381`):

1. it is a `PageImage`;
2. its LSN is at or above the redo point;
3. its xid is `0` (structural: status-spill pages logged by
   `PagedOutcomeSpill`, `paged.rs:5110`), or its xid has a `Commit` outcome
   from pass 1.

Images of aborted xids and of xids with no terminal record are skipped and
counted in `uncommitted_skipped`. Before the first applied image the
superblock free list is detached and synced (`wal.rs:1363`,
`manager.rs:1144`). This makes formerly free pages orphans that a later scan
reclaims, so a replayed allocation can never also be reachable from the free
list. Each applied image may extend the page count (`wal.rs:1374`).

Because records are full images applied in LSN order, the last committed image
of each page at or after the redo point wins. Pass 2 must see exactly the
records pass 1 saw, or open fails (`wal.rs:1383-1391`). The page file is then
synced (`wal.rs:1392`).

**GAP-1 (steal without undo).** "Skipped" means "not rewritten". It does not
mean "undone". An uncommitted transaction's page can already be in
`store.pages`: it got there by eviction, by a bounded checkpoint drain, by the
B-tree root-split flush, or by a full checkpoint taken between that
transaction's operations. That writeback logs the image first
(`paged.rs:5334`), but recovery then skips the image and leaves the page file
holding the uncommitted bytes. The store keeps no before-images
(`paged.rs:5319-5332` records this as a known limitation). For row versions
this is harmless, because MVCC hides versions whose xid did not commit. For
**structure** it is not; see section 8 (Q1).

### 3.4 What a checkpoint publishes

A checkpoint (full: `PagedStore::checkpoint`, `paged.rs:2397`; bounded:
`PagedStore::checkpoint_step`, `paged.rs:2463`) ends in
`Checkpointer::finish_checkpoint` (`wal.rs:1477`):

1. log sync, page-file sync;
2. append and sync `Checkpoint(redo_lsn)`, where `redo_lsn` is `next_lsn`
   before the flush started (full) or at Finalize (bounded);
3. `publish_root(meta_page, H = next_lsn)` writes and syncs the superblock
   (`wal.rs:1494`, `manager.rs:1176`): the checkpoint record has LSN `H - 1`;
4. if allowed, `Wal::reset` (`wal.rs:1010`). Reset is allowed only when every
   commit since the last truncation is frozen into the meta-page watermark and
   no backup pin is held (`paged.rs:2435`, `paged.rs:2667`).

The horizon `H` means: every page image with LSN `< H` is reflected in this
page file. The full checkpoint holds the writer gate and flushes every dirty
page before step 2. The bounded checkpoint publishes only after a writeback
under the gate observed zero dirty pages (`paged.rs:2643-2672`). `Wal::reset`
(`wal.rs:1010`) seals the active file under archive retention, then truncates
the active file, then deletes the sealed segments, oldest first, except the
ones that archive retention still needs.

Consequences used below:

- After a completed reset, the next record appended has LSN exactly `H`
  (`wal.rs:1057-1058`, and the floor at open, `wal.rs:385`).
- Without a reset (pinned, unfrozen commits, or a crash before the reset), the
  chain still contains the checkpoint record `H - 1`.
- A record with LSN `>= H` is never deleted unless a later checkpoint first
  publishes a larger horizon.

## 4. Incomplete operations

### 4.1 Torn tails

- **`transactions.log`.** The first frame whose header, kind, length or CRC
  does not verify ends the log. The file is truncated there and synced
  (`storage.rs:700-790`). A torn tail is always a suffix of the last group
  write, so the commits it held were never acknowledged (I1, I2).
- **`store.wal`, active file.** The same rule, per record, plus LSN
  continuity (section 3.1).
- **`store.wal`, sealed segments.** Never torn. A short decode fails open.

**GAP-3 (corruption reads as a torn tail).** Both logs treat any undecodable
record in the active file as the end of the log, wherever it is. Bit rot in
the middle of `transactions.log`, or a record missing from the middle of the
active `store.wal`, silently drops every later record, including acknowledged
commits. Neither log has a durable end marker or a recorded length that would
tell a crash tail from damage. The Q2 check (section 8) closes the
lost-prefix case only.

### 4.2 A partial row transaction

- **No `Commit` frame durable.** Dropped: only `Committed` states survive the
  read (`db.rs:19030`). Its page xid, if page apply had started, has no
  `Commit` record, so its images are skipped and the xid is aborted at open
  (section 2, step 2.6).
- **`Commit` durable, page apply incomplete or page commit not durable.** No
  marker, so it is replayed (section 5). Its own page images are skipped
  because its xid has no commit record. GAP-1 applies to whatever it stole.
- **Page commit durable, marker not durable.** Replayed over pages that
  already hold it: duplicate versions, I8 (section 6.1).
- **Savepoint rollback** (`Truncate{len}`) cuts the transaction's recovered
  write list to `len` before its commit (`db.rs:18960`).

### 4.3 A partial page-only transaction

Index builds, FTS builds and folds, bulk load and spatial pack have no logical
redo. Without a durable `Commit(xid)` their images are skipped and the xid is
aborted. Whatever they stole stays in the page file (GAP-1). The caller-level
state (for example a build marked complete in a sidecar) is not part of this
specification. I6 requires that the caller trusts the build only after its
commit returned.

### 4.4 Interrupted checkpoint phases

Crash points: `checkpoint.*`, `checkpoint_step.*`, `wal_reset.*`,
`wal_seal.after_rename`. Section 12 lists the cases run.

| Interrupted at | State on disk | What recovery does |
|---|---|---|
| Freeze, status spill, meta write (full or bounded) | Spill pages synced, descriptor not yet in a durable meta page; log intact | Old meta page and spill descriptor; the log still holds every outcome. The new spill pages are unreferenced: at worst leaked space, never reachable data. |
| After log sync / page flush, before the checkpoint record | Pages newer than the last horizon; log intact | Replays from the previous redo point. Full images in LSN order reconverge. |
| Bounded Drain or Finalize writeback | Some dirty pages written | Same. Every written page's image was synced first (I5, `pool.rs:1661`). GAP-1 applies when a page belongs to an in-flight transaction. |
| After the checkpoint record, before `publish_root` | Record durable, horizon old | The record's redo point is above `H`, so it is distrusted and replay starts at LSN 0 (`wal.rs:1313`). Correct, slower. |
| After `publish_root`, before or during `reset` with no sealed segments | Horizon `H`, log still full or truncated | Full log: honours the redo point. Truncated: an empty log, nothing to replay. Both are correct. |
| During `reset` **with sealed segments** | Active truncated, some newer sealed segments left | **GAP-2**, below. |
| After `reset`, during tail reclaim or structural frees | Log empty | Nothing to replay. A trailing-page truncation is idempotent. Frees not yet finalized are at worst leaked space. |
| `wal_seal.after_rename` | Active renamed into a sealed segment, no new active file | `Wal::open` creates an empty active file. The chain is the sealed segments. |

**GAP-2 (an interrupted reset replays stale segments).** `Wal::reset`
truncates the active file, which holds the checkpoint record, **before** it
deletes the sealed segments (`wal.rs:1025` then `wal.rs:1047`). Neither step
syncs the directory. A crash between them leaves a chain made only of records
older than `H`, with no checkpoint record. A power cut before the next
journal commit can do the same. Recovery then:

1. finds no usable redo point and replays the stale chain over pages that are
   already newer, so committed updates regress
   (`recovery_semantics.rs::an_interrupted_log_reset_neither_regresses_pages_nor_loses_later_commits`
   reads `v8` where `v10` was committed);
2. sets `next_lsn = max(last + 1, H) = H` (`wal.rs:987-990`), so the next
   record does not continue the stale chain. If no truncating checkpoint runs
   first, the next recovery stops at that discontinuity and truncates the
   whole active file as a torn tail, and commits made after the reopen are
   lost (same test).

In the core, open's recovery checkpoint usually truncates the log and deletes
the stale segments (step 6). That hides the second effect but not the first.
End to end, a crash at `wal_reset.after_active_truncate` or
`wal_reset.after_segment_delete` with sealed segments present loses
acknowledged commits: 3 of 4 cases failed, for example "writer 2
acknowledged commit 10 but only 9 survived"
(`gap2_production_path_survives_an_interrupted_log_reset`).

### 4.5 Interrupted log cut

`cut_transaction_log` (`bicdb-core/src/db/replication_live.rs:312`): drain
pending frames, run the deferred-materialization barrier
(`txlog_cut.after_barrier`), persist standby slots and the `commit_seq` floor
(`txlog_cut.after_floor`), then replace the log with its retained tail by
temp file, fsync and rename (`db.rs:18694`, `db.rs:18721`). A crash leaves the
old log or the new one, never a mixture. Every caller excludes committers
(commit admission or `&mut self`). With the old log, its commits all have
markers or are page-durable through the checkpoint that preceded the cut
(I7), so replay is discarded or duplicates (I8). The floor only rises, and
it keeps `commit_seq` monotonic across a log that no longer holds the newest
commit (`db.rs:18646`). The open-time cut has the same shape
(`open.after_txlog_floor`).

### 4.6 Interrupted recovery

See section 7.

## 5. Logical replay: order and rules

`apply_unmaterialized_transaction_log` (`db.rs:19045`):

1. Candidates: every `Committed` transaction without a marker that has a
   `commit_seq` (`db.rs:19069-19081`), sorted by `commit_seq`
   (`db.rs:19082`).
2. Each write of each transaction, in frame order:
   - skipped if its collection is not in the catalog (dropped,
     `db.rs:19105`). The skip is counted and reported;
   - a legacy frame (`commit_seq == 0`) is skipped if a checkpointed segment
     already superseded the row (`db.rs:19130`). Modern frames are never
     skipped this way;
   - applied as **its own paged transaction** under the writer gate: `begin`,
     `put` or `delete`, durable index maintenance from the definitions,
     `commit` (`db.rs:19140-19192`). Crash point: `replay.after_write`;
   - then applied to the resident collection state, stamped with the
     transaction's `commit_seq`.
3. Patch frames are not produced in `server_paged` mode (`db.rs:14971`), so
   replay only sees full row images there.
4. Replay writes no `CommitMaterialized` marker. The replayed transactions
   stop being candidates only when the open-time cut (section 2, step 8)
   removes them, after the recovery checkpoint made them page-durable.

A replayed transaction is therefore not atomic in the page store. Its writes
commit one by one. Open does not admit readers until it returns, and an
interrupted replay is replayed again whole (section 7), so no reader observes
the partial state.

## 6. Superseded operations

### 6.1 Duplicate versions on re-put

Replaying a write whose page commit is already durable puts a new version
with the same value on top of it (`db.rs:19049-19057`). The row's visible
value is unchanged. Durable index maintenance runs through the same
`apply_paged_index_upsert` / `apply_paged_index_delete` as the commit path
(`db.rs:23459`), given the row visible before the replayed write. For an
already-durable row that is the replayed row itself, so the index answers do
not change (I8). The reconciler checks this: index lookups equal a scan and
each unique key resolves to exactly its row. The superseded duplicate is dead
weight for vacuum. This is correct only because of I4: no later
commit of the same row can be page-durable while an earlier one is unmarked,
since row locks are held until the marker is written. Replay therefore never
re-puts an older image over a newer one.

### 6.2 Deletes

A replayed delete of a row that is already gone is a no-op in the page store
(`PagedStore::delete` returns `false`). Its index delete runs only if a
previous row was found. A replayed upsert after a replayed delete of the same
row recreates it, in `commit_seq` order, which is the committed order.

### 6.3 Dropped and recreated collections

DDL is not in either log. `drop_collection` takes `&mut self`
(`bicdb-core/src/db/flashback.rs:576`), so no commit is in flight. In the
default mode every earlier commit already has its marker, so none of the
dropped generation's writes is a replay candidate. In transaction-log mode
the sidecar barrier runs the materializer first (commit-durability-invariants
section 5). A replayed write whose collection is gone is skipped. A recreated
collection under the same name therefore never receives the old
generation's rows (I9). The crash matrix's reconciler checks this with three
reused names.

### 6.4 Truncated logs

A `Truncate{len}` frame (savepoint rollback) shortens the transaction's write
list (section 4.2). A cut log (section 4.5) is superseded by the page store up
to the cut point.

## 7. Repeated recovery: why it is idempotent

Each recovery step is restartable from any crash inside it:

- **Page recovery.** Pass 1 truncates only a torn tail, which a second pass 1
  finds already gone. Pass 2 writes full images, so re-applying any prefix
  gives the same bytes. Detaching the free list is idempotent. Nothing in the
  log changes during pass 2. Crash points: `page_recovery.after_scan`,
  `page_recovery.after_page_replay`, `page_recovery.after_flush`.
- **xid allocation.** `next_xid` is re-derived from the meta page and the log
  each time, and always passes every logged xid (`paged.rs:1053`). An
  interrupted recovery's replay transactions, committed or not, are in the log
  and are accounted for.
- **Logical replay.** The transaction log is not modified until the open-time
  cut, so a second open sees the same candidates, in the same order. A
  replay transaction committed before the crash is either still in
  `store.wal` (replayed, as a committed xid) or behind a completed recovery
  checkpoint. Re-applying the logical write over it is the duplicate-version
  case (6.1). A replay transaction that was cut off before its page commit
  leaves only skipped images and stolen pages (GAP-1).
- **Recovery checkpoint and log cut.** These are ordinary checkpoint and cut
  (4.4, 4.5). The cut runs only after the checkpoint returned.

`commit_durability_crash.rs::recovery_convergence` tests this against the
ledger. It crashes a workload, recovers one copy of the crashed state
without interruption (the baseline), then recovers other copies through
chains of crashes inside recovery. Every chain must reconcile against the
acknowledged-commit ledger and end in the baseline state, row for row, and
a further reopen must change nothing.

## 8. The two open questions

### Q1. Mid-transaction eviction and structural changes

**Answer: not correct.** There is a real window on the production path.

Recovery skips the page images of an uncommitted xid but cannot undo what that
xid already wrote to the page file (3.3). B-tree splits are done in place
(`btree.rs:1020-1077`). The left leaf is rebuilt in place with its lower half,
the right half moves to a newly allocated page, and the parent gains a
separator. The comment "a crash mid-split leaves the original leaf complete"
(`btree.rs:1054`) holds only while nothing is written back. Three
deterministic cases were run (`bicdb-page/tests/recovery_semantics.rs`). The
first two fail and are `#[ignore]`d as GAP-1. The third passes:

1. **Partial drain of a split leaf** (`q1_crash_after_partial_drain_of_an_uncommitted_leaf_split_keeps_committed_keys`).
   300 committed keys are checkpointed with an empty log. An uncommitted
   transaction inserts 40 keys into one leaf, and a bounded writeback (the
   Drain phase of `checkpoint_step`) writes `k` of the 49 dirty pages before
   the crash, for every `k`. For `k = 19..26`, committed keys that moved to
   the right sibling read as absent. For `k >= 27`, the stolen pages
   reference pages beyond the persisted page count. Reads of committed and
   uncommitted keys fail with "page 97 is beyond the end of the file (89
   pages)". Even with every dirty page written, the reopened store fails this
   way: the page count is published only by a checkpoint, and recovery does
   not replay the uncommitted images whose `reserve` would extend it.
2. **Root split** (`q1_crash_after_an_uncommitted_root_split_keeps_committed_keys`).
   `insert_separator` flushes the whole pool when the root splits
   (`btree.rs:1129`). That writes the old root, now holding only its left
   half, in place, while the meta page still names it as the root. With 2, 4,
   6 or 8 committed keys and an uncommitted transaction whose keys sort first,
   every committed key reads as absent after the crash.
3. **Full drain, then publish by checkpoint** (the safe direction). This
   passes (`a_fully_drained_uncommitted_transaction_keeps_committed_keys_and_stays_invisible`):
   a full checkpoint writes every page and publishes the page count, so the
   stolen tree is complete and the uncommitted versions stay invisible.

The windows on the production path:

- **Root split inside any paged transaction.** This covers a core row commit's
  page apply (`in_paged_transaction_inner`, `spatial_paged.rs:1491`, under
  `write_guard`) and every page-only transaction. A crash between the
  root-split flush and that transaction's durable `Commit` record loses
  committed keys of other transactions. For a row commit, the logical replay
  re-puts only that transaction's own rows. Reproduced end to end through
  `BicDb` (`commit_durability_crash.rs::gap1_a_crash_after_an_uncommitted_root_split_keeps_acknowledged_rows`).
  150 rows are committed and acknowledged one by one, then the database
  closes cleanly. One transaction then inserts 2,000 rows whose keys sort
  first, and the crash lands at `btree.after_root_split_flush:1`. After
  recovery, **124 of the 150 acknowledged rows are absent**.
- **Bounded checkpoint Drain concurrent with a commit.** The Drain phase does
  not take the writer gate (`paged.rs:2477-2483`), so the server's checkpoint
  supervisor can write back a page dirtied by an in-flight commit or build.
- **Eviction.** Any thread whose page miss evicts a dirty frame writes it
  through the barrier, whoever dirtied it (`pool.rs:1392`, `pool.rs:1661`).
- **Full checkpoints between operations** of a page transaction that does not
  hold `write_guard` for its whole length. The page store's own tests do this
  on purpose (`paged.rs` `crash_with_orphaned_head`).

What the store is missing is either no-steal for pages an uncommitted xid has
modified structurally, or undo, that is before-images or a structural redo
that runs regardless of the xid's outcome. Proposed options are in section 10.

### Q2. First-record check

**Answer: sound in a precise form, and implemented as a separate change.**

The literal proposal ("the first record of the active log must carry the
checkpoint's LSN") is **not** sound. A checkpoint that did not reset (backup
pin, unfrozen commits, or a crash between `publish_root` and `reset`) leaves
an active file that starts below `H` and contains the checkpoint record
`H - 1`. Archive retention keeps sealed segments that start below `H`. Both
are legitimate.

The sound rule follows from 3.4. **If the chain is non-empty, its first LSN
must be `<= H`**, where `H` is the page file's horizon and `H > 0`. Records
with LSN `>= H` are only ever appended after `H` was published, and are
deleted only after a larger horizon is published, so a chain that starts above
`H` has lost records the page file does not reflect. Every legitimate state
passes:

| State | First LSN of the chain | Passes |
|---|---|---|
| Empty log (after a reset, or fresh) | none | yes |
| Log after a completed reset | `H` | yes |
| Checkpoint without reset, crash before reset | `<= H - 1` | yes |
| Archive retention: retained sealed segments | `< H` | yes |
| Online-backup base (superblock copied first, sealed chain since the last reset) | `<= H` | yes |
| Roll-forward restore (base horizon older than the archived stream) | `<= H` | yes |
| Fresh or legacy store (`H = 0`) | n/a, check skipped | yes |
| Monolithic → segmented migration | unchanged chain | yes |
| A deleted or missing oldest sealed segment above `H`, or a `store.pages` older than its log | `> H` | **rejected** |

What it does not catch: an empty or wholly torn active file that should have
held records `>= H` (indistinguishable from a reset, GAP-3); the stale chain
of GAP-2, whose first LSN is `<= H`; gaps after the first record (GAP-3).

The companion rule a stale chain would need ("the chain must reach `H - 1`")
is also sound to *detect*, but rejecting that state would turn GAP-2's
legitimate crash state into an unopenable database. GAP-2 needs a repair
instead (section 10).

## 9. GAP list

| ID | What the code does not guarantee | Evidence | Severity |
|---|---|---|---|
| GAP-1 | Steal without undo: an uncommitted transaction's structural page changes (split leaves and roots, references to pages beyond the persisted page count) survive a crash in the page file. Committed keys of other transactions become unreachable, or reads fail. Pages taken from the free list by the crashed transaction may also still be listed free in the superblock when nothing is replayed (reasoned, not tested). | `recovery_semantics.rs` Q1 tests and `commit_durability_crash.rs::gap1_*` (fail, ignored: 124/150 acknowledged rows lost end to end); `btree.rs:1129`; `paged.rs:2477`; `paged.rs:5319` | Committed-data loss. Needs a crash during a paged transaction after a structural writeback. |
| GAP-2 | An interrupted `Wal::reset` with sealed segments replays stale images over newer pages, then gaps the LSN chain, so later commits are lost at the next recovery. | `recovery_semantics.rs::an_interrupted_log_reset_neither_regresses_pages_nor_loses_later_commits` and `commit_durability_crash.rs::gap2_*` (fail, ignored: acknowledged commits lost in 3 of 4 crash cases); `wal.rs:1025`, `wal.rs:1047` | Committed-data regression. Sealed segments exist with the default 64 MiB segments. |
| GAP-3 | Mid-log corruption in either log is treated as a torn tail and silently truncates later, possibly acknowledged, records. | `storage.rs:700-790`; `wal.rs:1130`, `wal.rs:984` | Silent loss on media corruption. Detection only. |
| GAP-4 | A page log that starts above the page file's horizon (lost prefix) is accepted, and its suffix is replayed onto pages that lack the missing changes. | `wal.rs:1246` (no check) | Closed by the Q2 change. |

## 10. Proposed behaviour changes (not implemented here; need approval)

1. **GAP-4 / Q2.** Reject a non-empty page-log chain whose first LSN exceeds
   the published horizon. This rejects only provably inconsistent states
   (8, Q2), so it is in a separate change. The error is `WalCorruption` with
   both LSNs.
2. **GAP-2.** Make the reset crash-ordered and the stale state harmless:
   - (a) in `Wal::reset`, delete the sealed segments **before** truncating
     the active file, and sync the directory before the truncation. A crash
     then leaves a chain that still reaches the checkpoint record
     (`a_chain_that_reaches_the_checkpoint_record_recovers_from_its_redo_point`
     shows that state recovers);
   - (b) in recovery, when the chain is non-empty and ends below `H - 1`,
     treat every record as covered by the horizon. Replay nothing, then
     finish the interrupted reset: delete those segments and sync before the
     first append. Records `< H` are reflected in the page file by the
     definition of `H` (3.4), so skipping them is sound. Deleting them would
     discard archive input only under archive retention, which (a) never
     leaves in that shape because it seals before truncating.
3. **GAP-1.** Options, in increasing cost:
   - (a) **no-steal for in-flight transactions**: `write_back_frame` skips a
     frame whose `dirtied_by` xid is in progress. Eviction picks another
     victim. Drain skips it and reports it as remaining. The root-split
     `flush_all` writes only frames not dirtied by the current xid, or is
     replaced by deferring the root publication to commit. A transaction
     larger than the pool would then fail with pool exhaustion instead of
     stealing, which needs a spill path for bulk loads and builds;
   - (b) **log structural changes as their own committed unit**: splits run
     as a nested structural transaction whose images are replayed regardless
     of the user xid's outcome. This works with steal because a split is
     logically a no-op for committed keys, but the split pages must then
     never carry uncommitted entries, so the split must be logged before the
     triggering insert;
   - (c) **before-images (undo)** for pages an uncommitted xid wrote back.
     This is the general fix and the largest.

   Option (a) is the smallest change that closes the window for row commits,
   whose page transactions are small and bounded by the batch. Bulk paths
   need (b) or (c).

None of these is in the docs-and-tests change. Change 1 is ready as its own
PR. Changes 2 and 3 await a decision.

## 11. Crash points added

All are one relaxed load when `BICDB_TEST_CRASH_AT` is unset
(`bicdb-page/src/test_faults.rs`). Points reached while the page log's file
lock is held use `crash_point_with_locked_wal`, which skips the power-loss
emulation for that log. Taking that lock again would deadlock.

| Area | Points |
|---|---|
| Full checkpoint | `checkpoint.after_status_spill`, `checkpoint.after_tail_reclaim`, `checkpoint.after_structural_frees` (plus the existing `after_wal_sync`, `after_page_flush`, `after_publish`, `after_wal_reset`) |
| Bounded checkpoint | `checkpoint_step.after_drain`, `after_freeze`, `after_finalize_drain`, `after_status_spill`, `after_meta_writeback`, `after_finish`, `after_tail_reclaim` |
| Page log | `wal_seal.after_rename`, `wal_reset.after_seal`, `wal_reset.after_active_truncate`, `wal_reset.after_segment_delete` |
| Page recovery | `page_recovery.after_scan`, `page_recovery.after_page_replay`, `page_recovery.after_flush` |
| Open | `open.after_recovery_checkpoint`, `open.after_txlog_floor` (plus the existing `replay.after_write`) |
| B-tree | `btree.after_root_split_flush` |

The power-loss emulation now records the offset a seal's sync covered. Before,
a crash right after a seal could cut synced bytes out of the sealed segment.

## 12. Results

Host s3ny, at the commit that adds this document. Every crash case checks the
acknowledged-commit ledger (every acknowledged commit present, per-commit
atomicity, index lookups equal a scan, unique keys exact, recreated
collections clean) and that a second reopen changes nothing.

| Test | Cases | Result |
|---|---|---|
| Full checkpoint phases + seals, production, with sealed segments (`production_path_survives_interrupted_full_checkpoints`) | 11 points + no-crash | pass |
| Same, small pool (eviction) | 11 + 1 | pass |
| Bounded checkpoint phases, supervisor-driven and commit-driven, production (`..._interrupted_bounded_checkpoints`) | 13 points + no-crash | pass |
| Same, small pool | 13 + 1 | pass |
| Transaction-log mode, full and bounded | 24 + 2 | pass |
| Existing matrices (both modes, with and without eviction) | 15-19 points each | pass |
| Interrupted recovery converges to uninterrupted recovery, 4 workload crash states × 13 recovery-crash chains (up to 5 crashes in a row) | 52 | pass |
| Repeated: convergence, 5 rounds each, for `commit.after_txlog_durable:150` (small pool), `checkpoint_step.after_finish:3` (small pool, bounded), `checkpoint.after_publish:8` (segments) | 3 × 5 × 13 | pass |
| Repeated: 25 rounds each of `checkpoint_step.after_drain:12` (small pool, bounded), `checkpoint_step.after_meta_writeback:3`, `checkpoint.after_status_spill:5` (small pool, segments), `wal_seal.after_rename:5` | 4 × 25 | pass |
| Page level: repeated recovery of one crash state, partial reset that still reaches the checkpoint record, fully drained uncommitted transaction | 3 | pass |
| **GAP-1** page level: partial drain of an uncommitted split (49 prefixes); uncommitted root split (4 sizes) | 53 | **fail** (ignored): committed keys absent or "beyond the end of the file" |
| **GAP-1** end to end: root-split flush in an uncommitted row commit | 1 | **fail** (ignored): 124 of 150 acknowledged rows lost |
| **GAP-2** page level: interrupted reset | 1 | **fail** (ignored): stale value, then a later commit lost |
| **GAP-2** end to end: `wal_reset.after_active_truncate:2/6`, `wal_reset.after_segment_delete:1/4` | 4 | **fail** (ignored): 3 of 4 lose acknowledged commits |

## 13. How to run the tests

- Page level: `cargo test -p bicdb-page --test recovery_semantics`
  (`-- --include-ignored` to see the GAP reproductions fail).
- End to end: `cargo build -p bicdb-core --example durability_child`, then
  `cargo test -p bicdb-core --test commit_durability_crash`
  (`-- --include-ignored` for the GAP cases and the repeat diagnostics).
  `repeat_recovery_convergence` takes `DURABILITY_CASE=<mode>/<point>` and
  `DURABILITY_REPEAT=<n>`.
