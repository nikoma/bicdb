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
- **The redo point** is the `redo_lsn` of the last checkpoint record below
  the page file's horizon `H` (`checkpoint_lsn` in the superblock),
  normally the record at `H - 1` that published `H`. It is capped at `H`.
  Without such a record, the redo point is `H` itself. `H` is published only
  after every page dirtied before it is durable (3.4), so nothing below it
  needs replay. Records between the record's redo point and `H` are
  replayed, so an unfenced append before publication is still recovered.
  Both checkpoint paths hold the writer gate across the publication. That
  does not obviously stop a thread outside the gate (an eviction, or a
  page-only transaction between its operations, W4 in 14.1) from appending
  a page image, which is why the redo point comes from the record. Whether
  such an append ever happens is open (OPEN-1 in section 9).

  This is exact for a roll-forward restore over an older base (later
  records describe the source's page files) and for a log that starts at
  `H`. The code used to replay both from LSN 0. `H = 0` (a new or
  pre-horizon store) replays the whole chain. *(Changed by the GAP-2 fix.
  Before it, the last record's redo point was used when `H >= redo_lsn`,
  and LSN 0 otherwise.)*
- **A chain that ends before `H - 1`** is classified before any file
  changes (`ResetShape`):
  - sealed segments with an **empty** active file is an interrupted reset.
    Recovery replays nothing and finishes the reset;
  - stale sealed segments followed by an active file whose first record is
    `H` is an interrupted reset after which an older engine kept committing.
    Recovery drops the stale segments and replays the active file. Before,
    those acknowledged commits were discarded as a torn tail;
  - anything else is refused with `WalCorruption`, and every file is left
    untouched.

  The first two shapes are counted in `stale_segments_discarded`.
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
**structure** it is not; see section 8 (Q1). *(The no-steal interim, 14.5,
keeps an uncommitted transaction's pages out of the page file on every path
except a last-resort steal and W4/W6; see 14.5 for what remains.)*

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
| After the checkpoint record, before `publish_root` | Record durable, horizon old | Replay starts at the old horizon `H`. Correct. |
| After `publish_root`, before or during `reset` with no sealed segments | Horizon `H`, log still full or truncated | Full log: honours the redo point. Truncated: an empty log, nothing to replay. Both are correct. |
| During `reset` **with sealed segments** | Oldest segments deleted, active file intact (it is truncated last) | Replay from `H`; the chain still reaches the checkpoint record. A stale chain left by an older engine is finished at open (GAP-2, fixed). |
| After `reset`, during tail reclaim or structural frees | Log empty | Nothing to replay. A trailing-page truncation is idempotent. Frees not yet finalized are at worst leaked space. |
| `wal_seal.after_rename` | Active renamed into a sealed segment, no new active file | `Wal::open` creates an empty active file. The chain is the sealed segments. |

**GAP-2 (an interrupted reset replays stale segments). Fixed**, see the
end of this subsection. Before the fix, `Wal::reset`
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

**The fix.** `Wal::reset` now deletes the sealed segments oldest first,
syncs the directory, then truncates and syncs the active file, which holds
the checkpoint record. Every step fails closed:
`page_log.reset_unlink`, `page_log.reset_dir_sync`, `page_log.reset`,
`page_log.reset_sync`. A crash or failure at any step leaves a chain that
still reaches the checkpoint record, or an empty log. Recovery replays from
the redo point of the last checkpoint record below `H`. A stale chain left by an older engine
is recognised: either its reset is finished, or the stale segments are
dropped and the active file replayed. Any other short chain is refused
untouched (3.2).

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

A second, independent reproducer came from the page-delta work
(`page_deltas.rs::preexisting_mid_transaction_eviction_window`, PR #48). It
is ported here as
`recovery_semantics.rs::an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash`
(ignored as GAP-1). One page transaction stays open while 400 others commit,
with a 32-page pool, then the process crashes.
- Every committed row reads back correctly.
- 77 of the open transaction's 400 keys fail with `ChecksumMismatch` on page
  340, a zero page, where they should read as absent.

The likely mechanism is the same steal: the open transaction's heap page was
written above the persisted page count under its own xid, then a committed
image with a higher page id made recovery extend the page count over it
unreplayed. The failure mode differs from the first reproducer: **reads
error instead of rows silently disappearing**.

A third observation comes from the fail-closed qualification runs (P0
evidence, s3ny `/root/bicdb-durability-s3/evidence-p0/`). One saved crash
state from the production matrix (`all-logs`, `pool.after_writeback:40`)
refuses to open with "page 6 is beyond the end of the file". The same state
fails the same way on the code before the fail-closed change. A page
written back mid-transaction lies above the persisted page count: the
unreadable-page mode below, reached through the ordinary crash matrix.

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

**Answer: sound in a precise form, and implemented as a separate change (#57).**

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
| GAP-1 | **Partly closed by the no-steal interim (14.5)**: W1 for refusable transactions, W2, W3, W5 and W7 are closed; open are W1 for a decided commit larger than the pool, W4 and W6. Steal without undo: an uncommitted transaction's structural page changes (split leaves and roots, references to pages beyond the persisted page count) survive a crash in the page file. Committed keys of other transactions become unreachable, or reads fail. Pages taken from the free list by the crashed transaction may also still be listed free in the superblock when nothing is replayed (reasoned, not tested). | `recovery_semantics.rs` Q1 tests and `commit_durability_crash.rs::gap1_*` (fail, ignored: 124/150 acknowledged rows lost end to end); `btree.rs:1129`; `paged.rs:2477`; `paged.rs:5319` | Committed-data loss. Needs a crash during a paged transaction after a structural writeback. |
| GAP-2 | **Fixed.** An interrupted `Wal::reset` with sealed segments replayed stale images over newer pages, then gapped the LSN chain, so later commits were lost at the next recovery. | Before the fix, 15-16 of 20 rounds per crash case lost acknowledged commits; after it, 0 of 20. Tests: `recovery_semantics.rs::an_interrupted_log_reset_*`, `wal_reset_ordering.rs`, `commit_durability_crash.rs::production_path_survives_an_interrupted_log_reset*` and `..._fails_closed_at_every_log_reset_step*` | — |
| GAP-3 | Mid-log corruption in either log is treated as a torn tail and silently truncates later, possibly acknowledged, records. | `storage.rs:700-790`; `wal.rs:1130`, `wal.rs:984` | Silent loss on media corruption. Detection only. |
| GAP-4 | A page log that starts above the page file's horizon (lost prefix) is accepted, and its suffix is replayed onto pages that lack the missing changes. | `wal.rs:1246` (no check) | Closed by the Q2 change. |

Open items (not GAPs yet: unexplained, no failing test in this suite):

- **OPEN-1, appends between a checkpoint record and its publication.**
  `finish_checkpoint` publishes the horizon as `next_lsn` read after the
  checkpoint record. If another thread appends in between, the horizon lies
  above `record + 1`. Recovery stays correct, because the redo point comes
  from the record (3.2). But it means the gate does not fence the log, and
  the "every image below `H` is in the page file" argument then depends on
  that thread's page also having been flushed.
  - Not observed so far. A `debug_assert_eq!(next_lsn, checkpoint_lsn + 1)`
    was tried during review; it was removed without having been seen to
    fire.
  - To look for it, add the assertion right after the `wal.checkpoint(...)`
    call in `Checkpointer::finish_checkpoint` (`wal.rs`), plus a backtrace
    capture in `Wal::append` while a "checkpoint publishing" flag is set.
    Then run debug builds of: the crash matrices
    (`cargo test -p bicdb-core --test commit_durability_crash`, which
    includes the bounded-supervisor runs), the pgwire suite, and the Rails
    acceptance (`.github/workflows/rails.yml`: `cargo build -p bicdb-cli
    --bin bicdb`, then `BICDB_BIN=target/debug/bicdb bin/verify` in
    `examples/rails/team_board`).
- **OPEN-2, "duplicate keys" after a kill and restart: explained, not a
  recovery bug.** The Rails acceptance intermittently failed its "Pending
  jobs and binary Solid Cache value survive BicDB SIGKILL/restart" check
  with `unique index index_solid_cache_entries_on_key_hash has duplicate
  keys`. The root cause is in the pgwire layer and reproduces without any
  crash. A concurrent autocommit `INSERT ... ON CONFLICT DO UPDATE` that
  lost the unique-key race at COMMIT returned `UniqueIndexConflict`, mapped
  to SQLSTATE `XX000`, instead of re-running and taking the update path.
  - The constraint held: the losing commit is rejected before the log
    append, and heap and index agree.
  - It appeared after the restart step only because that is when the two
    Solid Queue workers race on the same cache key.
  - Fixed by #88 ("Re-run upserts that lose a unique-key commit race"). No
    recovery change is needed.

## 10. Proposed behaviour changes (not implemented here; need approval)

1. **GAP-4 / Q2.** Reject a non-empty page-log chain whose first LSN exceeds
   the published horizon. This rejects only provably inconsistent states
   (8, Q2), so it is in a separate change. The error is `WalCorruption` with
   both LSNs.
2. **GAP-2. Implemented** in the follow-up PR, as proposed, plus the page horizon as the redo point (3.2):
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
3. **GAP-1.** Section 14 analyses the options and section 15 the exposure.
   The no-steal interim (14.5) is implemented in its own PR. It changes
   user-visible behaviour: a transaction without other redo that outgrows
   the buffer pool is refused (14.5).

None of these is in the docs-and-tests change. Change 1 is ready as its own
PR (#57). Changes 2 and 3 await a decision.

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
| Page commit | `paged.after_page_images` (a commit's page images are appended, its commit record is not) |

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
| **GAP-1** page level: partial drain of an uncommitted split (49 prefixes); uncommitted root split (4 sizes) | 53 | **fail** before the interim: committed keys absent or "beyond the end of the file". Pass with it (14.5) |
| **GAP-1** end to end: root-split flush in an uncommitted row commit | 1 | **fail** before the interim: 124 of 150 acknowledged rows lost. Pass with it |
| **GAP-1** interim: see 14.6 | | |
| GAP-2 page level: interrupted reset (stale chain from an older engine) | 1 | pass after the fix (failed before: stale value, then a later commit lost) |
| GAP-2 end to end: crash at every reset step (`wal_reset.after_segment_delete`, `after_segments_removed`, `after_active_truncate`), with and without eviction; EIO at every reset step (`page_log.reset_unlink`, `reset_dir_sync`, `reset`, `reset_sync`) | 9 + 12 | pass after the fix. Repeats: 20 rounds each of 4 reset crash cases, 0 failures (before the fix: 15-16 of 20 failed per case) |

## 13. How to run the tests

- Page level: `cargo test -p bicdb-page --test recovery_semantics`
  (`-- --include-ignored` to see the GAP reproductions fail).
- End to end: `cargo build -p bicdb-core --example durability_child`, then
  `cargo test -p bicdb-core --test commit_durability_crash`
  (`-- --include-ignored` for the GAP cases and the repeat diagnostics).
  `repeat_recovery_convergence` takes `DURABILITY_CASE=<mode>/<point>` and
  `DURABILITY_REPEAT=<n>`.

## 14. GAP-1: options analysis

GAP-1 needs a separate owner decision. This section compares the options.
Nothing here is implemented on `main`.

### 14.1 The windows

A window is any path by which a page holding an in-flight transaction's
unlogged change reaches `store.pages` before that transaction's `Commit`
record is durable.

| Window | Path | Writer gate held? | Who triggers it in production |
|---|---|---|---|
| W1 eviction | `BufferPool::evict_one` → `try_reclaim` → `write_back_frame` (`pool.rs:1392`, `pool.rs:1591`) | no | any thread whose page miss finds no clean victim: pool pressure |
| W2 bounded drain | `checkpoint_step` Drain → `writeback_step` (`paged.rs:2477`) | no | the pgwire server's automatic checkpoint supervisor (`automatic_paged_checkpoint`, default on, `bicdb-pgwire/src/lib.rs:422`), concurrently with commits |
| W3 root-split flush | `BTree::insert_separator` → `flush_all` (`btree.rs:1129`) | yes (inside the transaction) | every transaction that grows a tree's height |
| W4 checkpoint between operations | `PagedStore::checkpoint` / Finalize between two operations of a page transaction that does not hold `write_guard` throughout | yes, per operation | page-only transactions that begin and commit around per-operation locking: index builds (`db/index_build.rs:330`), FTS builds (`db/fts_build.rs:205`), the page-store tests' orphan fixtures |
| W5 aborted changes | after `PagedStore::abort`, in-memory pages keep the aborted changes (`paged.rs`, `abort` doc) and stay stealable under the aborted xid until the next commit logs them | — | any of W1-W4 after a rolled-back transaction |
| W7 commit logging window (found while implementing the interim) | `log_dirty_pages` cleared every page's unlogged flag before logging the images and before the commit record, so Drain and eviction could write a committing transaction's pages through the "already logged" path (`before_logged_writeback`) before its commit record existed | no (Drain, eviction) | any commit next to the checkpoint supervisor or read pressure |
| W8 allocation writes (found while testing the interim) | `PageStore::allocate` writes each page it hands out to the page file at once, zeroed with its type header (so a reused id never exposes a predecessor's bytes), and pops the free list in memory; any page-store flush (the root-split `flush_logged` among them) then writes the superblock with that state. A page taken from the free list is thereby retyped on disk before its transaction commits | n/a (direct write) | any page transaction that allocates, before its commit |
| W6 mislabelled steal | `dirtied_by` is set to the pool's current transaction on every access (`pool.rs:1272`), so with two interleaved page transactions a stolen image can carry the other transaction's xid; if that one commits, recovery replays uncommitted bytes as committed | — | W4 interleavings only (reasoned, not tested) |

For an ordinary row commit (`in_paged_transaction_inner`, which holds
`write_guard` for begin..commit), W1, W2, W3 and W5 apply. W4 and W6 need a
page-only transaction that interleaves.

**Two failure modes**, both reproduced:

| Mode | How it arises | Seen by | Reproducer |
|---|---|---|---|
| Silent loss of other transactions' committed rows | An uncommitted split is stolen: the left page is rewritten in place and the right sibling, which holds the committed keys that moved, is missing or unreachable. Typical with W3. | Nobody until the rows are missed. Reads return "absent", and `verify_integrity` reports `valid` for the root-split case | `q1_*` page tests; `gap1_a_crash_after_an_uncommitted_root_split_keeps_acknowledged_rows` (124 of 150 acknowledged rows lost) |
| Unreadable pages | A stolen page lies above the persisted page count, or committed structure (logged by other commits) references a page of the crashed transaction that recovery does not replay. Typical with W1 and W6. | Any reader of those keys, including full and range scans, gets `ChecksumMismatch` or "beyond the end of the file". Committed rows stay intact in the reproducers. | partial-drain `q1_*` states (`verify_integrity` errors); `an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash` (77 keys `ChecksumMismatch`, `verify_integrity` still reports `valid`) |

The second mode is less severe, because no acknowledged data is lost, but
it is not benign. A scan that meets an affected key fails, so a whole
collection can become unreadable until it is repaired. A fix must close
both modes.

### 14.2 Option (a): no-steal of unlogged pages, plus a spill path

A dirty frame whose `needs_log` flag is set holds changes no log record
describes yet. Those are exactly an in-flight (or aborted, not yet
re-logged) transaction's pages: commit clears the flag for every page it logs
(`log_dirty_pages`, `paged.rs:3234`). (a) never writes such a frame to
`store.pages` before commit.

- **Closes:** W1, W2, W3, W5. W4 only if the full checkpoint and Finalize
  also skip unlogged frames. They cannot simply skip them, because the
  checkpoint's meta page must then name only committed roots. W6 disappears
  with W4.
- **Spill path:** a transaction larger than the pool needs somewhere to put
  its unlogged pages. That means a per-transaction spill file outside the
  page file: read misses check it, commit logs from it, and open discards
  it. Bulk loads and index builds need this; row commits do not.
- **Cost:** eviction skips some candidates. Drain leaves in-flight pages to
  Finalize, which runs under the gate, so Finalize holds the gate slightly
  longer. The root-split flush writes less than before. The spill path costs
  one extra write and read per spilled page, and only for transactions
  larger than the pool.
- **Format and recovery:** none for the interim. The spill is a transient
  file deleted at open, so the format version is unchanged. Recovery is
  unchanged.
- **Risk:** low for the interim, which keeps steal as a last resort (14.4).
  Without that fallback, transactions larger than the pool fail with
  `PoolExhausted`. The spill path is a new subsystem of medium size.
- **Experimental modes:** deferred commits log their pages at
  `commit_deferred` (flag cleared), and the writeback barrier already syncs
  through the latest deferred commit before writing them (I5). No change.
  Page deltas get simpler: the page file only ever receives committed
  states, so no full image has to repair a stolen page.

### 14.3 Option (b): structural changes as their own committed unit

When a split runs, log the pages it rewrites (left, right, parent, and the
meta page on a root split) as structural images, xid 0, before any of them
can be written back. Then mark them logged. Recovery already replays xid-0
images unconditionally (`wal.rs:1349`). A stolen split is then redone whole,
and a page allocated beyond the published page count is reserved by the
replay (`wal.rs:1374`).

- **Closes:** W1-W4 and W6 for splits (and for every other structural change
  logged this way), whatever writes the pages. Entries the in-flight
  transaction put into those pages are replayed too, but they point to heap
  versions of an xid that never committed, so MVCC hides them (`write_base`
  skips aborted heads). The root-split flush becomes unnecessary.
- **Not closed:** non-structural steals stay without undo. That is harmless
  only where the bytes are MVCC-tagged. Every other structure that splits or
  allocates through the pool needs the same treatment: the catalog tree, FTS
  and spatial pages, overflow chains, the free list. Missing one leaves a
  hole.
- **Cost:** about four extra full-page images per split. Splits come roughly
  once per leaf's worth of inserts. The split pages are logged again at
  commit. No extra sync.
- **Format and recovery:** none. xid-0 images and their replay rule already
  exist, so older engines read the log.
- **Risk:** medium. The correctness argument depends on MVCC hiding every
  uncommitted entry the replayed structure contains, for every structure,
  including in-place head-locator updates. It needs an audit of each
  structure plus GAP-1 style tests per structure.
- **Experimental modes:** in transaction-log mode the barrier syncs only
  through deferred commit LSNs (`sync_deferred_commits`). It would also have
  to sync through the latest structural image before writing a page marked
  logged by one. Page deltas cannot replace these images: a split rewrites
  most of each page, so it stays a full-image boundary.

### 14.4 Option (c): before-images (undo)

Before stealing an unlogged frame, log its before-image. Recovery gains an
undo pass that, after redo, applies the before-images of xids without a
commit record, in reverse LSN order.

- **Closes:** W1-W6, for every structure, with no pool pressure.
- **Cost:** a read of the on-disk version, or a retained copy, plus a log
  record per first steal of a page per transaction. Stolen pages roughly
  double their log volume. Recovery gets one more pass.
- **Format and recovery:** a new record kind. An older engine stops at the
  unknown kind and treats the rest of the log as a torn tail, so downgrade
  becomes data loss. The change needs a format gate and a three-phase
  recovery whose undo is itself idempotent.
- **Risk:** high. A page-level before-image also rolls back changes another,
  committed transaction made to the same page since. That is safe only while
  page transactions never interleave, and W4 shows they do. A safe version
  needs page ownership or logical undo, which is a redesign.
- **Experimental modes:** deferred commits lost from the log tail would be
  undone, then replayed logically from `transactions.log`, which is
  consistent. Page deltas would need before-values.

### 14.5 The no-steal interim (implemented)

Option (a) without the spill path. No format change and no recovery change:
the page file only ever receives pages whose current content is described by
a committed (or structural, xid 0) record, except for the cases listed under
"still open".

**Mechanism.**

- *Unlogged pages.* A frame's `needs_log` flag means "changed since its last
  log record". The pool keeps an exact per-shard count of such frames
  (`BufferPoolSnapshot::unlogged_pages`). All of the following apply only
  when a log barrier is installed; a pool without a log has no unlogged
  state and behaves as before.
- *W1, eviction.* `evict_one` first looks for a victim without unlogged
  changes. Only if the shard has none does it take one with unlogged changes
  (counted in `unlogged_steals`).
- *Admission, instead of the last-resort steal.* Before each `put` and
  `delete`, under the writer gate and before anything is modified,
  `PagedStore::admit_write` checks that every shard still has at least an
  eighth of its frames (at least one) free of unlogged changes. If not, the
  operation fails with `TransactionExceedsBufferPool` and the caller aborts.
  The reserve is what the admitted operation and concurrent readers use, so
  a steal is left only to an operation already under way that dirties more
  than the reserve in one shard (in practice a single value larger than the
  shard, through its overflow chain), and to decided commits (below).
  What the reserve holds back is a share of each shard's frames that is
  free of unlogged changes when an operation starts; it is not a promise
  that the operation, or a read next to it, never steals (a single value
  larger than the reserve, or reads during a decided commit at the limit,
  still can, and `unlogged_steals` counts exactly the unlogged frames
  written). Admission is per shard, not per transaction: while one transaction holds a
  shard at the limit, every other refusable transaction's next write is
  refused too, until the first commits or aborts. Admitting it could need a
  steal in that shard. The error names the transaction holding the most
  unlogged frames (`holder`), so the one refused is not blamed for it.
- *Decided commits are never refused.* A row commit's page apply runs after
  its decision is durable in the transaction log. Refusing it would fail an
  already decided commit, and its replay at the next open would be refused
  the same way, so the database could not open. The core begins it with
  `PagedStore::begin_redo_backed`, which is exempt from admission. Such a
  transaction larger than the pool still steals, and GAP-1 stays open for it.
- *W2, the bounded checkpoint's Drain* and `PagedStore::writeback_step`
  skip unlogged frames (checked under the frame's data latch;
  `unlogged_writeback_skips`). Drain then escalates to Freeze and Finalize,
  which hold the writer gate as before.
- *W3, the root-split flush* writes only logged pages
  (`BufferPool::flush_logged`). The split pages and the meta page naming the
  new root are logged at commit, so the root change is atomic with its
  transaction.
- *W5, aborted pages.* `PagedStore::abort` logs every unlogged page under a
  fresh internal transaction and commits it durably, after the abort record.
  The next commit would have logged exactly these pages; logging them at once
  means no checkpoint can write part of an aborted split under the aborted
  xid, and a refused transaction's pages stop occupying the pool. The unit is
  all or nothing: its images are applied only if its commit record is in the
  log. (A first version logged them as xid-0 images, which recovery applies
  unconditionally, so a crash or a failed write after some of them replayed
  a partial aborted split; review reproduced it, 297 failures.) If the
  logging fails, the pages stay unlogged and out of the page file, and the
  abort returns the error (the core fences, as for any abort failure).
- *Failed operations poison their transaction.* An operation that fails
  after it changed a page can leave a half-done structure in memory: a
  B-tree split rewrites the left leaf before the parent records the split,
  and a failure in between (an eviction I/O error, `PoolExhausted`, no space
  to allocate) leaves the keys that moved reachable from nowhere. Before,
  the abort's unit (and, before the interim, the next commit) logged those
  pages, so the restart replayed the torn tree. Now the store fails closed
  at once (`DurabilityHealth`, as for a failed write), the transaction is
  marked poisoned, and its abort logs nothing and reports the fence. The
  restart recovers from the logs: a decided commit is replayed from the
  transaction log, a page-only transaction's work is lost as for a crash.
  "Changed a page" is exact: the pool counts released write guards, and an
  operation that fails before changing anything (a refusal, a write
  conflict, an oversized key, now checked before the heap version is
  written) is not affected.
- *Read-only aborts* (a transaction that wrote nothing, such as a full-text
  read session) append only their abort record: no unit, no sync.
- *The xid floor.* Every commit logs every unlogged page, whoever changed
  it, so its images can carry versions of another live transaction B that
  no record names. Recovery sets the next xid from the highest xid any record
  names, so B's xid could be handed out again after a crash, and the new
  holder's commit would make B's uncommitted versions visible. This is older
  than the interim (any commit next to an open page transaction), and the
  interim's abort unit made it more reachable. When a live xid is above the
  one being logged, `log_dirty_pages` now also appends the commit of a fresh
  empty transaction, inside the caller's durability. No format change. Not
  covered: an image stolen by eviction under a different transaction's xid
  (W6, still open).
- *Eviction cost.* A shard whose every frame is unlogged skips the no-steal
  pass, and the no-steal pass may take a clean metadata frame from an
  over-budget metadata tier before any steal.
- *Metadata tier.* Eviction normally never takes a metadata-class frame
  for another class, because the tier is held to its budget. Under no-steal
  that budget cannot be enforced against unlogged metadata frames (free-space
  map pages a large transaction dirties), so the tier could fill a whole
  shard and fail the next admission mid-operation with `PoolExhausted`. The
  last-resort pass may now reclaim from metadata when it is over budget.
- *Heap insert probe.* The free-space candidate ring was probed with write
  guards, which mark every probed page changed, so one insert could dirty
  the whole ring. Under no-steal that made one operation's footprint
  unbounded. Candidates are now probed with a read guard first.
- *W7, the commit logging window.* A commit now reads the list of unlogged
  pages without clearing it, logs their images, appends its commit record,
  and only then clears the flags (`BufferPool::mark_logged`). A page a writer
  touched after its image was read keeps its flag (a per-frame modification
  count, compared under the data latch).

**Behaviour changes (for the owner).**

1. A transaction that is not a decided commit and outgrows a shard of the
   buffer pool now fails with `TransactionExceedsBufferPool` ("refused ...
   Commit in smaller transactions or raise buffer_pool_bytes") instead of
   succeeding with a crash window. This affects `bulk_load_insert` with a
   batch larger than roughly 7/8 of the pool (in-tree callers already pass
   chunks), page-only transactions of the core (`in_paged_transaction`
   callers: DDL-scale metadata, rekey batches, build batches), and direct
   `PagedStore` users. With the core's default 256 MiB pool the limit is
   about 224 MiB of changed pages per transaction. Index and full-text
   builds commit in bounded batches (4,096 rows and 64 MiB by default,
   `PagedBTreeBuildLimits`; the rekey runs 8,192 entries): fine at the
   default pool, refusable below roughly 16 times their size, where they fail
   cleanly. The page-only batches whose size depends on the data are now
   bounded by the pool itself, to a sixteenth of it and to one page change
   per item (`PagedRecords::page_only_batches`): the external full-text
   build's doc-term, document-id and stored-text batches, the dropped-index
   entry cleanup, and the stale spatial-generation sweep at open (which,
   refused, would stop the open). The per-term impact-block and direct-block
   code in `backfill_full_text_direct_blocks` is unreachable (the function
   returns into the external build first) and is unchanged.
   **CREATE INDEX on an existing collection** (`create_index`, through
   `backfill_paged_index_entries`, for B-tree indexes and for full-text ones
   whenever the external full-text build does not complete the job) writes
   every entry of the backfill in one page transaction, which makes the new entries appear atomically. On a
   collection whose entries outgrow the pool it now fails cleanly ("refused
   ... raise buffer_pool_bytes") and leaves no index; before, it succeeded by
   stealing, with a crash window that could lose other rows. At the default
   256 MiB pool that is roughly a few million rows, depending on the key
   size. Splitting it needs a completion marker, as the full-text path has;
   the structural cuts of option (b) remove the limit. Pinned by
   `gap1_page_only_batches.rs::creating_an_index_whose_backfill_outgrows_the_pool_fails_cleanly_and_leaves_no_trace`
   (both index kinds). Row commits, and their replay at open
   (`apply_unmaterialized_transaction_log`), are not affected.
2. An abort that left changed pages appends their images and a commit of a
   fresh internal transaction, and syncs the page log once. Recovery reports
   one more transaction outcome for it.
3. A commit next to a live transaction with a higher xid appends one more
   empty commit record (the xid floor), inside its own sync.
4. New observability: `unlogged_pages`, `unlogged_steals`,
   `unlogged_writeback_skips` in the pool snapshot, and
   `PagedStore::refused_for_unlogged_pressure`.

**Still open.**

- W1 for a decided commit (row commit page apply) larger than the pool, and
  for a single operation that dirties more than the reserve of one shard
  (`unlogged_steals > 0` shows it). Reproduced by
  `commit_durability_crash.rs::gap1_a_decided_commit_larger_than_the_pool_keeps_acknowledged_rows`
  (ignored).
- **W8, allocation writes.** An uncommitted (or refused, then crashed)
  transaction's allocations are on disk at once: empty typed pages, and on a
  flush the superblock's page count and free-list head. No tuple or entry
  byte of the transaction reaches the page file (tested), but when the
  transaction took pages from the free list and the crash leaves nothing
  for recovery to replay, the superblock's free-list head names a page now
  typed as heap or B-tree, and **the next allocation fails** ("page 18 has
  type 2, expected 6"): the store cannot write until repaired. Reproduced:
  `a_crash_after_an_uncommitted_transaction_reused_free_pages_leaves_a_writable_store`
  (ignored). Present before the interim too; it is the "listed free in the
  superblock" note of the GAP list. When recovery replays any image it
  detaches the free list first (3.3), so the state needs a crash with
  nothing to replay. Closing it needs allocation to go through the pool
  (initialise the frame in memory, stage free-list pops until commit, as
  frees already are) or a free-list validation at open; either changes 80
  allocation call sites or recovery, so it is left to its own change.
- **W1-L, measured.** A redo-backed transaction that stole once and then
  crashed before its commit recovered correctly; after 5 and after 20 steals
  every acknowledged key was unreadable (`UnexpectedPageType` on the stolen
  page; 624 failing reads over the two states):
  `the_last_resort_steal_path_before_its_commit_keeps_acknowledged_keys`
  (ignored). On such a store `PagedStore::verify_integrity` does not return
  (it ran for more than 20 minutes at full CPU): the existing verifier has no
  bound on a damaged structure.
- W4 and W6: page transactions that do not hold the writer gate across their
  operations (`db/index_build.rs`, `db/fts_build.rs` direct `begin` users,
  and the page store's own tests). Finalize and the full checkpoint, under
  the gate, still write their unlogged pages, and a commit logs every
  unlogged page under its own xid. Reproduced by
  `recovery_semantics.rs::an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash`
  (ignored). Every core path through `in_paged_transaction` holds the gate
  from begin to commit, so it is not exposed.

Closing those needs option (b). Its design is proposed separately, for approval before any change to what recovery reconstructs.

**Recommendation (unchanged).**
1. The interim, as a reviewed PR with the GAP-1 tests un-ignored.
2. Then (b), as the complete fix.
3. (c) is not recommended: it needs a format gate, and page-level undo is
   unsafe with interleaving transactions.

### 14.6 Evidence for the interim

Host s3ny, at the interim's code. "Base" is the GAP-2 fix (#67 head,
`92a49f50`), with only the new core tests added; "branch" is the interim.

| Window | Test (fault and reconciliation) | Base | Branch |
|---|---|---|---|
| W1 | `gap1_no_steal.rs::w1_eviction_under_read_pressure_never_writes_an_uncommitted_split`: 64-frame pool, an uncommitted split, 0-300 reads evicting, crash, reopen and reconcile every acknowledged key, `verify_integrity` | caught by mutation M4 | pass, 0 steals |
| W2 | `recovery_semantics.rs::q1_crash_after_partial_drain...` (49 prefixes); `w2_bounded_drain_next_to_an_in_flight_split_keeps_committed_keys` (crash after 1-64 supervisor Drain steps) | q1: fail; w2: caught by M1 | pass |
| W3 | `q1_crash_after_an_uncommitted_root_split...`; `w3_an_uncommitted_root_split_never_reaches_the_page_file`; `w3_a_committed_root_split_survives_a_crash`; end to end `gap1_a_crash_after_an_uncommitted_root_split_keeps_acknowledged_rows` | fail (124/150 lost) | pass |
| W3, the saved P0 crash state (`all-logs`, `pool.after_writeback:40`: reopen refused, "page 6 is beyond the end of the file") | Its page log ends with page transaction 36's images of a split (new leaf 6, new root 7, old leaf 2 pointing at 6) and no commit record; the page file holds those pages: the root-split flush wrote them. Deterministic form: `gap1_a_crash_inside_the_root_split_flush_reopens_and_keeps_acknowledged_rows` (crash at each of the first 8 writebacks of the splitting process). Timing form: 1,200 repeats of the original case | deterministic: fail at writebacks 4-6 (124/150 lost); repeats: 0/1200 (the window needs timing that #55's head had, 2/240) | deterministic: pass; repeats: 0/300 |
| W2 + W3 + W7 end to end | `gap1_a_crash_at_every_boundary_of_a_splitting_page_apply_keeps_acknowledged_rows`: transaction-log decision, root-split flush, page images logged without commit record, after the page apply; bounded checkpoint worker running next to the transaction | fail at the root-split flush (124/150 lost); `paged.after_page_images` does not exist there | pass |
| W5 | `w5_an_aborted_split_checkpointed_page_by_page_keeps_committed_keys` (crash after every bounded-checkpoint step after an abort); `w5_an_abort_failing_at_every_log_write_leaves_committed_keys_intact` (EIO at every write of the abort's unit: abort record, each image, the unit's commit record, then a crash) | caught by M3; the sweep catches M8 (images as xid 0, the version review reproduced) | pass |
| Xid reuse | `an_open_transactions_xid_is_not_reissued_after_a_crash`: B stays open while A commits or aborts, crash, reopen; the next xid must exceed B's and a new commit must not expose B's version | caught by M9 | pass |
| Refusal reporting | `a_refusal_names_the_transaction_that_holds_the_pool` | new behaviour | pass |
| W7 | `paged.rs::pages_a_commit_is_logging_stay_out_of_the_page_file_until_its_commit_record`; `a_page_modified_after_its_image_was_logged_stays_unlogged`; crash point `paged.after_page_images` in the core matrices | caught by M5 | pass |
| Refusal | `a_transaction_that_outgrows_the_pool_is_refused_before_it_steals`; `a_refused_transaction_that_crashes_before_its_abort_leaves_no_trace`; `a_redo_backed_transaction_is_never_refused_and_its_steals_are_counted`; core `bulk_load_insert.rs::a_bulk_load_larger_than_the_buffer_pool_is_refused_whole_and_batches_succeed` | n/a (new behaviour) | pass |
| Still open (ignored) | `an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash` (W4/W6); `gap1_a_decided_commit_larger_than_the_pool_keeps_acknowledged_rows` (W1-L) | fail | fail, as documented ("page 7 has type 4, expected 5" at reopen) |

Mutations (one mechanism disabled at a time, at the interim's code; each
must fail at least one test, and each did):

| Mutation | Tests that fail |
|---|---|
| M1 Drain writes unlogged pages | `w2_bounded_drain...` |
| M2 root-split `flush_all` | `q1_crash_after_an_uncommitted_root_split...`, `w3_an_uncommitted_root_split...`, `a_refused_transaction_that_crashes...` |
| M3 abort leaves its pages unlogged | both W5 tests, the xid-reuse test, two refusal tests, `abort_exceptions_are_rederived...` |
| M4 eviction steals freely | `w1_eviction...` |
| M5 flags cleared before the commit record | `pages_a_commit_is_logging...`, the W5 write-failure sweep |
| M6 no admission | three refusal tests |
| M7 no last-resort metadata reclaim | `open_does_not_scan_the_file` (`PoolExhausted` mid-split) |
| M8 abort unit as xid-0 images (the first version) | the W5 write-failure sweep, `abort_exceptions_are_rederived...` |
| M9 no xid floor | `an_open_transactions_xid_is_not_reissued_after_a_crash` |

Suites at the interim's code: `bicdb-page` 422 passed, 0 failed, 1 ignored
(W4/W6); the core crash matrices, paged recovery, and backup/PITR set
(21 test files) 115 passed, 0 failed, 4 ignored (the three repeat
diagnostics, which are always ignored, and W1-L).

Benchmark (information only; s3ny, niced, 3 alternating rounds, release,
fsync on): small row commits with 4 writers ran about 12% slower with a
higher p99 (630 vs 728 commits/s median; p99 32-42 ms vs 9.6-9.8 ms), not yet
profiled; bulk and batched loads ran faster (95.7k vs 64.9k and 59.8k vs
11.5k rows/s), mostly from the heap-probe change, with 26% less page log
per commit. Memory unchanged.

## 15. Exposure and detection

### 15.1 GAP-2 (interrupted page-log reset)

**Exposed:** every `server_paged` store with page-log segmentation, which is
the default (64 MiB segments on stores with page extents, the default for
new stores), whose checkpoints truncate. Truncation needs no unfrozen
commits and no backup pin, which is the normal case.

The state needs at least one sealed segment at the reset, that is 64 MiB of
page log since the last reset. At the roughly 70 KB of page images per
small commit measured in commit-durability-invariants section 7, that is
about 900 commits between truncating checkpoints.

**Triggers and likelihood:**
- A process crash between the truncation and the last unlink. That window is
  microseconds per reset: low.
- A power loss before the next journal commit. Any later `fdatasync` forces
  one, so the window is milliseconds under load and up to the filesystem's
  commit interval (5 s on ext4) when idle: low to moderate.
- A failed unlink under fail-closed handling (the #55 reviewer's route). On a
  failing disk this is deterministic: the next reopen replays the stale
  chain. This is the likeliest trigger.

The crash matrix lost acknowledged commits in 15-16 of 20 rounds per case
before the fix.

**Detection today:** poor. Before the reopen, the state is recognisable:
sealed segments all below `H - 1` and no checkpoint record. After the
reopen has replayed them, the regressed rows are valid older versions, so
`verify_integrity` passes unless the mixture of page ages breaks a tree.
Only external evidence shows the loss: the commit-log archive, a standby,
or the application. The fix (follow-up PR) recognises the state at open,
replays nothing, and reports it as `stale_segments_discarded`.

### 15.2 GAP-1 (steal without undo)

**Exposed:** a crash (process kill, OOM, power loss) while a paged
transaction is in flight, after one of its structural pages reached the page
file through W1-W4.

| Deployment trait | Effect |
|---|---|
| Store or transaction working set large relative to the buffer pool (default 256 MiB) | W1 frequent: evictions of in-flight pages during large transactions |
| pgwire server with the automatic checkpoint supervisor (default) | W2 present for every commit: Drain runs concurrently |
| Young trees: initial loads, new indexes, FTS builds | W3: root splits happen when a tree grows a level. With 8 KiB pages that is at roughly 180, 36 000 and 7 million keys, so they are frequent early and rare later |
| Long transactions: bulk loads, batch imports, index and FTS builds | the window is the whole transaction, and these are where OOM kills happen |
| Small OLTP commits on a pool-resident store | windows of microseconds to milliseconds per commit: low |

**Likelihood:** low for a steady OLTP store. **High** for a crash during a
bulk load or an index build on a store larger than its pool, or during the
first loads into a new database.

**Detection today:** partial. Measured on the GAP-1 page-level states
(diagnostic run, s3ny):
- partial drain of a split leaf: `PagedStore::verify_integrity` fails with
  "page N is beyond the end of the file" in every state tried. Reads of the
  affected keys fail the same way. **Detected.**
- uncommitted root split: `verify_integrity` reports `valid = true`, because
  the tree is consistent and simply lacks the committed keys that moved
  right. **Not detected.**
- interleaved open transaction (second reproducer): `verify_integrity`
  reports `valid = true`, but reads of 77 keys fail with `ChecksumMismatch`.
  **Not detected by the verifier.** Only readers see the error.

The undetected case needs a reachability check: enumerate committed heap
versions through the heap catalog (not through the index), and look up
each record's key in the index. A committed version that is reachable from
the heap but not from the index is lost. No such pass exists today.
Proposed as a bounded maintenance step, `verify_heap_reachability`. In the
core, the record id is inside the stored record, so the core can also run
it per collection.
