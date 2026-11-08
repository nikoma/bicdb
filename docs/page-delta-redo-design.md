# Page-delta redo records for the page WAL: design

Status: design plus experimental prototype, 2026-11-08. **Approved for
experimental development on disposable test stores only.** Nothing is
enabled by default, and nothing here is approved for production stores or for
merging to `main`.

> **Compatibility boundary.** A store created with the disposable page-delta
> opt-in carries the superblock magic `BICDBPG5` from its creation. Engines
> that predate this format refuse to open it. It is **not upgradeable and not
> downgradeable**: there is no path into the format for an existing store and
> no path out of it. Use it only for data you can throw away.

The prototype needs **two independent locks**, both open (section 2.1):
1. the process gate `BICDB_PAGE_WAL_DELTAS=1` (only that exact value); and
2. a store CREATED as a disposable experimental store
   (`PagedStoreOptions::with_experimental_page_deltas_disposable(true)`, or
   `bicdb_core::page_deltas_experimental::create_disposable_page_delta_store`).

Every item marked CONTRACT CHANGE stays behind both locks.

Inputs:
- `docs/page-wal-write-amplification.md` (draft PR #40): a one-row commit logs
  59.6 KB of full page images for 836 changed bytes. Applying the full-page-write
  rule should cut page-WAL bytes by 90-96%.
- `docs/commit-durability-invariants.md`: invariants I1-I11.
- `docs/paged-wal-recovery.md`: the two-pass recovery algorithm and its memory
  contract.
- `docs/recovery-semantics.md` (branch `recovery-semantics`, in progress):
  whether recovery should keep skipping page records of uncommitted
  transactions or replay all of them ("repeat history"). Section 5 shows this
  design is correct under either answer. The only thing that changes between
  the two is how often a delta is possible.

Code citations are to `main` at `decb9d0`.

## 1. Summary

- **New record kind `PageDelta` (kind 5).** It carries byte runs that turn one
  earlier record's page into the current page. It names that earlier record
  (its `base_lsn`) and carries checksums of both the base and the result.
  `PageImage` records are unchanged.
- **Full-page-write rule.** A page's first record after an *epoch start* is
  always a full image. An epoch starts when any of these happens:
  - a checkpoint chooses its redo LSN;
  - the log is reset (truncated);
  - an active segment is sealed;
  - a backup pin is taken.

  Each epoch start clears the WAL's per-page "last record" table, so no delta
  can name a base from before it.
- **Delta-base rule.** A delta may only be based on a record that recovery is
  guaranteed to replay whenever it replays the delta. The base must be:
  - structural (xid 0), or
  - from the same transaction, or
  - from a transaction whose commit record is already in the log ahead of the
    delta.

  Otherwise the record is a full image. This holds both under today's
  skip-uncommitted recovery and under repeat-history recovery.
- **Self-verifying replay.** Recovery applies a delta only when all of these
  hold:
  - the page's last applied record in this replay is exactly `base_lsn`;
  - the page's content matches `base_crc`;
  - the result matches `result_crc`.

  Anything else fails the open as WAL corruption. A wrong base can therefore
  never produce a wrong page silently.
- **Torn pages** stay covered by the same argument as today. Replay always
  starts at an epoch start, so it overwrites a page with a full image before
  applying any delta to it, whatever is on disk. Section 6 shows this holds for
  crash recovery, for a restore over a fuzzy online copy and for archive
  roll-forward.
- **Old binaries are locked out** by a new superblock magic `BICDBPG5`,
  written and synced when a disposable store is created, and never on any
  other store. This follows the precedent of the segmented-WAL format gate.
  The marker is permanent.
- **Checkpoint trigger:**
  - *Today:* checkpoints trigger on WAL bytes alone.
  - *With deltas:* recovery cost no longer tracks bytes, so the trigger also
    counts page records.
  - *Why:* this bounds recovery work by the same number of page writes as today.
    Without the count, a 256 MiB log of deltas would hold about 25 times more
    records to replay.
- **With the gate off,** the WAL bytes, the superblock and the recovery path are
  identical to today. A test pins this.

## 2. Contract changes needing approval

Each item is a change to what `store.wal`, the superblock or recovery promise
today. None is enabled by default.

| # | CONTRACT CHANGE | Today | With deltas | Why it is needed |
|---|---|---|---|---|
| C1 | **WAL format** | Every page record is a self-contained full image (`wal.rs:15-26`: "replay is trivially idempotent") | A `PageDelta` record is replayable only after its named base record. Idempotence holds per *sequence from an epoch start*, not per record | It is the whole point: 90-96% fewer bytes |
| C2 | **On-disk compatibility** | Any binary since segmentation can open the store | A store created as a disposable page-delta store carries the superblock magic `BICDBPG5` from creation, and older binaries refuse it. An existing store can never acquire it, and a marked store can never shed it: not upgradeable, not downgradeable | An old binary treats an unknown record kind in the *active* file as a torn tail (`wal.rs` `scan_wal_file`: `from_u8` → `break`) and **truncates committed records silently**. The marker turns that into a refusal to open. Sealed segments already fail closed (a short decode is corruption) |
| C3 | **New recovery refusal class** | Recovery refuses only malformed or contradictory logs | Recovery also refuses a delta when its named base was not the page's last applied record, when its base or result checksum mismatches, or when its base precedes the redo point. The database then does not open | Applying a delta to the wrong base is silent page corruption. The refusal is the only safe answer, but it is a new way for an open to fail |
| C4 | **New invariant I12, full image first** | (none; every record is full) | Replay starts only at an epoch start, and the first record of each page after an epoch start is a full image | Torn-page protection and replay from any legal start point depend on it |
| C5 | **New invariant I13, replayable base** | (none) | A delta's base is a record recovery replays whenever it replays the delta (section 5) | Keeps deltas correct with skip-uncommitted recovery |
| C6 | **Epoch starts at seal, reset, redo choice and backup pin** | Sealing and backups do not affect the log's content | Each of these makes the next record of every page a full image. Frequent archiving (`archive_wal_segments` seals on every call) costs one full image per hot page per call | Archive roll-forward and the "redo not vouched → replay from the start of the chain" fallback (`wal.rs` `recover_with_outcomes`) must start at an epoch start |
| C7 | **Recovery memory and time contract** | One reusable page-sized buffer. Replay writes pages only | Plus one page-sized read buffer and a table of the last applied record per page, 16 bytes per distinct page with a full image in the replay range. That table is bounded by `(replay range bytes) / page_size`. A delta costs a page read plus a page write | `docs/paged-wal-recovery.md` "Memory contract" must be amended |
| C8 | **Checkpoint trigger** | `wal_bytes > wal_max_bytes` (`paged.rs` `commit_with_sync`) | With the gate on, the trigger also fires when page records since the last redo exceed `wal_max_bytes / page_size`. That is the same worst-case replay page count as today | Otherwise recovery after a crash replays up to about 25 times more page operations than today |
| C9 | **Online backup / PITR format** | Backup chains and archives hold only full images | Backup chains and archives taken from a gate-on store hold deltas. Restoring one needs a binary that supports deltas. Old binaries fail closed: the base's superblock carries the C2 magic and a sealed segment with an unknown kind is corruption | Follows from C1 |

What does **not** change:
- **The commit decision and the logical logs (I1, I2, I10):** unchanged.
  Standbys still receive only `transactions.log` frames; the page WAL is never
  shipped.
- **WAL before data (I5):** the writeback barrier still syncs through the
  page's last record, whether that record is a full image or a delta.
- **I3, I4 and I6-I11:** unchanged, because the commit and marker ordering and
  checkpoint truncation are untouched.
- **Under repeat-history recovery** (section 5.3), C5 stays correct as it is;
  only its delta rate improves.

### 2.1 The two locks

| Lock | Where | Refusals (tested, section 10.2) |
|---|---|---|
| Process gate | `wal_delta::gate_from_env`, read at every `PagedStore::open`: only `BICDB_PAGE_WAL_DELTAS=1` | Unset, `0`, `true`, ` 1`, `1 ` all leave deltas off |
| Disposable store | `PageStore::mark_disposable_page_delta_store`, reachable only from `PagedStore::open` with `experimental_page_deltas_disposable` on a page file this handle just created | Env set on a normal new store: no marker, no deltas. Env set on a pre-existing store: no marker, no deltas. The option on a pre-existing store: open fails, store untouched |

Deltas engage (`Wal::engage_deltas`, `BufferPool::engage_deltas`) only when
both are open. Otherwise no delta code path runs except one relaxed `bool`
load per write-guard acquisition, per guard drop and per page log.

**Test harness hook.** To run existing suites against delta stores, the
crates have a non-default cargo feature `page-delta-test-hook`. With it, and
`BICDB_TEST_PAGE_DELTAS_CREATE_DISPOSABLE=1`, every NEW store is created
disposable, and `BICDB_TEST_PAGE_DELTAS_TALLY=<file>` appends each engaged
log's counters on drop. A build without the feature has neither hook.

## 3. Record format and versioning

### 3.1 Record layout

The record header and the 16-byte fixed body are unchanged: magic, LSN, kind,
reserved bytes, length and CRC32, then `page_id` and `transaction`.
`PageDelta = 5`. Its payload:

| Offset | Bytes | Field |
|---|---|---|
| 0 | 8 | `base_lsn`: the LSN of the record (image or delta) this delta transforms |
| 8 | 4 | `base_crc`: CRC32 of the base page with the stamped fields masked |
| 12 | 4 | `result_crc`: CRC32 of the resulting page with the stamped fields masked |
| 16 | 4 | `run_count` |
| 20 | 6 per run, plus run bytes | `offset u32`, `len u16`, then `len` bytes. Runs are ascending and non-overlapping, and lie inside the page outside the masked fields |

- **Stamped fields are masked, never carried.** These are the bytes that
  `page::finalize` writes on every physical write:
  - the generation at `[16..24]`;
  - the checksum at `[32..36]`;
  - the trailer at `[len-8..len]`.

  Writeback stamps them into the *frame* in place (`pool.rs`
  `write_back_frame` → `store.write_page(&mut bytes)`), so a frame can differ
  from its last logged record in exactly these bytes. Replay re-stamps them
  through `write_page`, so they carry no information, exactly as with full
  images today.
- **Run encoding:** runs closer than 8 bytes merge, and a run is split at 65,535
  bytes.
- **Size cap:** a delta whose payload would exceed half a page is written as a
  full image instead.
- **Per-record overhead:** 60 bytes (24 header + 16 fixed + 20 delta header),
  plus 6 per run. The estimate in PR #40 assumed 40 + 4 per run.

### 3.2 Versioning and compatibility

- **New binary, old log:** full images are unchanged and remain valid. A log
  with no kind-5 record replays exactly as today.
- **Old binary, new log** (C2):
  - *When a disposable store is created* (and only then: the page file was
    empty when this handle opened it, `page_count == 1`, no root published),
    the store writes and syncs a superblock with magic `BICDBPG5`, whose body
    carries a feature word at `[76..84]`:
    - bit 0: segmented;
    - bit 1: tracked (backup format);
    - bit 2: disposable page-delta store.

    A reader refuses unknown feature bits, or `BICDBPG5` without bit 2.
    Asking an existing store for the marker is an error
    (`PagedStore::open` with the option set on an existing store), and the
    store is left untouched.
  - *Old readers* accept only `BICDBPG\0`, `PG2`, `PG3` and `PG4` (`manager.rs`
    `read_superblock` and `probe_page_size`). They refuse with `NotAPageFile`.
  - *New readers* decode the feature word.
  - *A monolithic (unsegmented) store* gets the marker too. It has no sealed
    segments, so it has no other protection.
- **Gate off:** the gate never writes the marker; only the creation opt-in
  does. A marked store opened with the gate closed writes full images only,
  and its WAL bytes are identical to an unmarked store's (section 10.2). The gate
  controls *writing* deltas. *Reading* them is always supported.
- **Tools:** every reader of WAL records must understand kind 5. In-tree today,
  the only readers are:
  - `read_all`;
  - `verify_integrity`;
  - `terminal_outcomes_in_range`;
  - `transaction_outcomes`;
  - the two recovery passes.

  All of them decode generically. Only pass 2 interprets the payload.

## 4. The full-page-write rule

### 4.1 State

| Where | State | Bound |
|---|---|---|
| `Wal`, under the file mutex | `last_record: FxHashMap<PageId, (Lsn, Xid)>`, the last page record appended for each page in the current epoch | One entry per page with a full image in the epoch, so at most `epoch bytes / page_size` entries. Cleared at every epoch start |
| `Wal`, under the file mutex | `appended_commits`: commit records appended in this epoch (the same range set as `committed_this_generation`, but filled *after* the append, under the same lock) | 4,096 ranges. If it overflows, the store stops taking deltas until the next epoch, which is safe |
| `Frame` (pool) | `delta: Mutex<FrameDelta { logged_lsn: Option<Lsn>, clean: bool, base: Option<Box<[u8]>> }>` | `base` is held only between a frame's first modification after a log and its next log. Total bytes are capped (default `min(pool/4, 256 MiB)`); past the cap the record is a full image |

### 4.2 Logging a page (commit-time `log_dirty_pages` or the writeback barrier)

1. **Read the image** under the frame's data latch (the read latch for
   `log_dirty_pages`, the write latch in the barrier). The latch is held until
   step 4, so the image cannot change underneath.
2. **Pick the candidate base:**
   - `frame.delta.base` if one was captured;
   - otherwise the image itself, if `clean` (unchanged since its last log);
   - otherwise none.

   With a base and `logged_lsn = Some(b)`, compute the masked runs, `base_crc`
   and `result_crc` outside the WAL lock.
3. **`Wal::append_page(page_id, xid, image, candidate)`,** under the file mutex:
   1. Seal first if needed. Sealing is an epoch start.
   2. A delta is **eligible** iff all of these hold:
      - `last_record[page_id] == (b, base_xid)`;
      - the replayable-base rule holds (section 5);
      - `xid != 0`.
   3. Append the delta if eligible, otherwise the full image.
   4. Set `last_record[page_id] = (lsn, xid)`.
4. **Update the frame** before releasing the latch: `logged_lsn = Some(lsn)`,
   `base = None`, `clean = true`.

The other paths:
- **Base capture (`get_mut`, under the write latch):** if `clean && logged_lsn
  is Some && base is None`, copy the page into `base`.
- **Guard drop:** sets `clean = false`.
- **Freshly loaded or allocated frames** start with `logged_lsn = None`, so
  their first record is always full.
- **Free, evict and reload** drop the frame's state.
- **Direct WAL writes that bypass the pool still update `last_record`.** These
  are:
  - the structural spill-page images (`paged.rs` `PagedOutcomeSpill::persist`);
  - `Checkpointer::log_and_write`.

  Any stale frame base therefore mismatches and degrades to a full image.

Why the frame needs `clean` rather than reusing `needs_log`: `log_dirty_pages`
clears `needs_log` by `swap` *without* the frame latch
(`take_pages_needing_log`). Between that swap and the image read, `needs_log ==
false` does not imply "unchanged since the last log". Capturing a base on
`needs_log` would therefore record a base the log never held. `clean` changes
only under the data latch.

### 4.3 When the cycle resets (epoch starts)

At each of these points, under the WAL file mutex, `last_record` and
`appended_commits` are cleared:

| Event | Where | Why |
|---|---|---|
| Checkpoint **chooses** its redo LSN | `Checkpointer::checkpoint` / `checkpoint_and_truncate` and the bounded `Finalize` (`paged.rs` `checkpoint_step`) replace `self.wal.next_lsn()` with `self.wal.begin_redo_epoch()`, which reads `next_lsn` and clears the state under the file lock | A delta appended between choice and publication must not have a base below the redo point. With the gate off, it returns `next_lsn()` and touches nothing |
| Bounded checkpoint | Same function, at `Finalize`. `Drain` and `Freeze` do not choose a redo point and do not reset the epoch | The redo is chosen only at `Finalize`, under the writer gate with zero dirty pages |
| Log reset (truncation) | `Wal::reset` | After reset the chain starts at `next_lsn`. With archive retention it starts at the oldest retained sealed segment |
| Segment seal | `Wal::seal_active_locked`, from `append`, `seal_active`, `reset` with retention, `seal_wal_for_backup` and `archive_wal_segments` | Every sealed segment is self-contained. Replay may start at any segment's first record (the unvouched fallback, archive roll-forward) |
| Backup pin | `PagedStore::begin_backup_pin`, `capture_backup` | Defence in depth (section 6.3): records after the backup start never reach back across it |

The epoch is also implicitly fresh after a process restart, because frames
start with no `logged_lsn`.

## 5. Delta bases and uncommitted pages

### 5.1 The rule (I13)

A delta logged by transaction `x`, based on the record `(b, y)`, is eligible
only if replaying the delta implies replaying the base:

- `y == 0`: structural records replay unconditionally (`wal.rs` pass 2);
- `y == x`, with `x != 0`: both replay iff `x` committed;
- `y`'s commit record is already in `appended_commits`: its LSN is below the
  delta's. If the delta survives a crash, everything before it survives (prefix
  durability), so `y`'s commit survives and `y`'s record is replayed.

A transaction that is in progress or aborted is never a base for another
transaction. Structural (`x == 0`) records are always full images. The rule is
checked under the file lock against records already appended. A commit record
inserted into `committed_this_generation` before its append does not count:
that set is updated before `append` (`wal.rs` `append_commit`), which is the
wrong order for this purpose.

### 5.2 Mid-transaction eviction and aborts

| Case | What is logged | What recovery does |
|---|---|---|
| T modifies P and P is evicted (barrier, record under T) | A delta on the last committed or structural record, or a full image | If T commits: the delta replays on its base. If not, it is skipped, which is fine because no later record may use it as a base |
| T modifies P again and commits | A delta based on T's own barrier record (`y == x`) | Both replay, or both are skipped |
| T aborts (or never finishes); Y then modifies P | A full image under Y. T is not committed, so the rule forbids T's record as a base | Y's image replays; T's records are skipped |
| T's bytes sit in P when Y commits (Y's commit scoops every `needs_log` page, `pool.rs` `take_pages_needing_log`) | Y's record (delta or full) contains T's bytes, exactly as Y's full image does today | Visibility of T's tuples is MVCC's job, as today |

The mid-transaction eviction window noted in
`commit-durability-invariants.md` §8.5 (a page evicted right after a leaf
split, then a crash before commit) is **not changed** by deltas. Records under
an uncommitted T are skipped either way. That window belongs to the
recovery-semantics track.

### 5.3 If recovery moves to repeat-history

Under repeat-history every record replays, so any earlier record in the epoch
is a valid base. Rule 5.1 stays correct, because it is strictly more
conservative. Only one change would follow: allowing an in-progress or aborted
base would raise the delta rate after aborts. That change belongs to whoever
approves repeat-history, and nothing here depends on it.

## 6. Torn pages, backup, archive and PITR

### 6.1 Crash recovery

- **Why every page write is covered:** a page write that a crash can tear is a
  writeback or checkpoint flush. Either one happened after the page was dirtied.
  Every page dirtied before the redo point R is flushed by the checkpoint that
  chose R before that checkpoint's record is written. The bounded checkpoint
  chooses R with zero dirty pages. The one-shot checkpoint chooses R first and
  then flushes, under the writer gate, logging any still-unlogged page at or
  after R through the barrier. So a page written after the checkpoint was
  dirtied after R, it has a record at or after R, and its first record at or
  after R is full (I12).
- **What replay does:** it overwrites the page with that full image before it
  applies any delta. From then on, every base is a page that replay itself just
  wrote. The disk's earlier content, torn or not, is never read as a base.

### 6.2 Start points other than the vouched redo

`recover_with_outcomes` falls back to replaying the whole retained chain when
the page files' horizon does not vouch for the latest redo. This happens:
- on a restore over an older base;
- locally, after a crash between the checkpoint record and `publish_root`.

The chain starts at a reset point or at a sealed segment's first record. Both
are epoch starts (section 4.3), so I12 holds from there too.

Without the seal reset this fallback could fail. With archive retention,
`reset` keeps unarchived sealed segments, so the local chain can start in the
middle of an older epoch. A delta whose base sits in an already-archived and
deleted segment would then have no base, and the open would refuse (C3). Making
seals epoch starts removes the case.

### 6.3 Online backup (fuzzy copy) and restore

`create_online_paged_base_backup` streams page segments while writes continue,
so the copy can contain torn pages. The restore replays the sealed chain taken
after the copy:
- from the vouched redo R, or
- from the start of the chain.

Any page torn in the copy was being written during the copy, so it was dirty
after R (6.1), or after the chain start (6.2). Its first record in the replay
range is full.

The backup pin's epoch start is therefore not required by this argument, but
it is kept for two reasons:
- **Defence in depth** against a future restore that starts at the backup's
  start LSN rather than at a checkpoint.
- **Robustness:** the argument in 6.1 depends on every checkpoint flushing
  all pages dirtied before its redo point, so it should not be the only
  protection.

The cost is one full image per page touched after each backup start.

### 6.4 Archive roll-forward and PITR

`apply_archived_wal` copies archived sealed segments next to a restored base.
Every sealed segment is self-contained (I12 at each seal), so roll-forward from
any segment boundary or vouched redo applies deltas only to bases inside the
replayed range. A roll-forward that started mid-epoch would be refused (C3),
never silently mis-applied.

Logical PITR (`log_archive_pitr`, `transactions.log`) does not read the page
WAL and is unaffected.

## 7. Recovery algorithm changes

**Pass 1** is unchanged except for one check: a `PageDelta` whose payload is
malformed (runs out of order, outside the page, or overlapping the masked
fields) fails the open as `WalCorruption`, the same class as a forged length.
Delta records count towards `max_record_transaction`, like images.

**Pass 2:**

1. **`PageImage`:** unchanged. On apply, set `applied[page] = lsn`.
2. **`PageDelta`** below `redo_lsn`: skip it, like images.
3. **`PageDelta` of a transaction that is not committed** (and not 0): skip it
   and count it in `uncommitted_skipped`. It needs no base.
4. **Any other `PageDelta`:**
   1. Require `applied[page] == base_lsn`. Otherwise refuse with `WalCorruption`
      "delta base not replayed". This covers a missing full image, a skipped
      base, a base before the redo point, and records out of order.
   2. Read the page from the store. It was written earlier in this pass.
   3. Check `base_crc` (masked), apply the runs, check `result_crc` (masked).
      Refuse on any mismatch.
   4. `write_page`, then set `applied[page] = lsn`.
5. **Free-list detach, `reserve` and `flush`:** unchanged.

**Interrupted recovery** restarts from the same redo point and repeats the same
sequence. Every delta's base is rewritten by the replay before the delta is
applied, so a second run produces the same pages. This is the I8 analogue at
the page layer.

**The replay table** `applied` is an `FxHashMap<PageId, Lsn>`. It is bounded by
the full images in the replay range, and it is populated only when pass 1 saw
at least one delta, so a log without deltas recovers with exactly today's
memory. Its size and the deltas applied are reported through
`Wal::delta_stats()` / `PagedStore::page_deltas()`
(`recovery_delta_base_entries`, `recovery_deltas_replayed`).
`RecoveryReport` is deliberately unchanged, so its serialized form, snapshots
and metrics are identical with the gate off.

**The first-record check** proposed in
`commit-durability-invariants.md` §8.5 is a separate production change and is
not part of this track. Deltas make one form of it automatic: a delta at the
start of the replay range can never apply.

## 8. Checkpoint, backup and archive changes (code)

- **`begin_redo_epoch()`** replaces the three `wal.next_lsn()` redo choices.
  With the gate off it is exactly `next_lsn()`.
- **`reset`, `seal_active_locked` and `begin_backup_pin` / `capture_backup`**
  call `start_epoch()`. With the gate off it does nothing.
- **Checkpoint trigger** (C8): `Wal` counts page records since the last epoch
  start that coincided with a redo choice or reset. `commit_with_sync` adds
  `|| (deltas && page_records > wal_max_bytes / page_size)` to the byte test,
  and the backpressure factor applies to both. With the gate off the expression
  is the current one.
- **Archive retention, sealed-segment naming and `mark_archived_through`:**
  unchanged.

## 9. Failure modes

| Failure | Effect | Detection or containment |
|---|---|---|
| Wrong base from a capture bug (frame state out of sync with the log) | The delta would produce a wrong page | `result_crc` check at recovery → refusal (C3). In tests: a shadow-replay oracle (section 10, Q1) |
| Base record skipped as uncommitted | Same | `applied[page] != base_lsn` → refusal. Prevented by I13 |
| A write that bypasses the pool and the WAL changes a page | Memory and disk diverge (an existing hazard; full images hide it by overwriting) | The delta reproduces the memory frame exactly as a full image would. `last_record` mismatch forces a full image after any direct WAL write of the page |
| Torn delta record | CRC failure → torn tail (active file) or corruption (sealed), as today | Unchanged scanner |
| Old binary opens a delta store | Would truncate committed work as a torn tail | C2 superblock magic: refusal |
| A restore or roll-forward starts mid-epoch | Delta without its base | Refusal (C3). Prevented by I12 at seal and reset |
| Capture memory exhausted | None. Records fall back to full images | Cap of `min(pool/4, 256 MiB)`; counter `BufferPool::delta_base_captures_skipped` |
| `appended_commits` overflow (4,096 ranges) | Bases from unrecorded commits are refused until the next epoch | Full images, counted in `DeltaStats::delta_refused` |
| Recovery time regression (many small records) | A long replay | C8 count trigger; measured in section 10.3 |
| Refusal in production | The database does not open: a fail-closed availability event | No automatic fallback exists. The refusal message names the page, the record LSN and the base LSN. A restore from backup is the remedy |
| Eviction then reload of a page | Its frame loses its delta state, so its next record is a full image | Costs bytes, never correctness. Visible as a lower delta rate with small pools (Q1: 470 deltas against 9,491 full images with a 16-page pool) |

## 10. Qualification program

The owner's decision: experimental development on disposable test stores
only, with its own qualification program, because this is a persisted-format
change, introduces recovery invariants I12/I13, and locks older binaries out
of upgraded stores. Every check below runs with both locks open unless it says
otherwise. All results are from s3ny (24 cores, consumer NVMe, ext4, shared
with other jobs at load 8-20), branch `page-delta-redo`.

### 10.1 How the suites run with deltas

- **New tests** (`crates/bicdb-page/tests/page_deltas.rs`, unit tests in
  `wal.rs` and `wal_delta.rs`) create disposable stores explicitly.
- **Existing suites** run unchanged, built with `--features
  page-delta-test-hook` and run with these variables:
  - `BICDB_PAGE_WAL_DELTAS=1`;
  - `BICDB_TEST_PAGE_DELTAS_CREATE_DISPOSABLE=1`, so every new store is
    disposable;
  - `BICDB_TEST_PAGE_DELTAS_TALLY=<file>`, which records each engaged log's
    counters. This is the evidence that deltas were really written and
    replayed.
- **Child processes inherit the variables.** This covers the core crash
  matrix's `durability_child`, which must be built with the same feature.

### 10.2 Results

| Area | Check | Result |
|---|---|---|
| Format and compatibility | **Two locks.** `deltas_need_both_locks_and_existing_stores_never_get_the_marker`: <br>• gate `1` on a normal new store → no marker, no kind-5 record; <br>• gate `1` on a pre-existing store → same; <br>• the disposable option on an existing store → open refused, store untouched; <br>• disposable store with gate unset, `0`, `true`, ` 1` or `1 ` → marker present, no deltas; <br>• both locks → deltas | pass |
| | **Gate-closed bytes, in-process.** `with_the_gate_closed_the_wal_bytes_are_identical`: the same workload with gate unset/normal store vs gate `1` normal, `0` normal, `true` disposable, unset disposable | `store.wal` byte-identical in all five |
| | **Gate-closed bytes, across binaries.** `page_wal_fingerprint` (400 commits, deletes, aborts) built from `main` `decb9d0` and from this branch, pools of 4,096 and 24 pages (eviction) | `store.wal` and `store.pages` SHA-256 identical for main, branch with gate unset, and branch with gate `1` |
| | **Old binaries refuse.** By construction: readers before this branch accept only `BICDBPG\0`, `PG2`, `PG3`, `PG4`. A reader also refuses `BICDBPG5` with unknown feature bits | not executed against an old binary |
| | **Delta encoding unit tests:** <br>• only the exact gate value `1` parses; <br>• round trip outside the stamped fields; <br>• an empty delta for an identical page; <br>• a full image when the delta would exceed half a page; <br>• malformed, masked-overlap and trailing-garbage runs rejected before any byte is written | 5/5 pass |
| I12 / I13 | **WAL unit tests:** <br>• a delta on a committed base, chained on its own transaction; <br>• an uncommitted or aborted base of another transaction forces a full image; <br>• structural records are always full; <br>• redo choice, backup pin and reset each force a full image; <br>• a segment seal forces a full image | 4/4 pass |
| | **Recovery refusal unit tests:** <br>• base skipped as uncommitted; <br>• no full image after the redo point; <br>• base checksum, result checksum, malformed payload, wrong `base_lsn`; <br>• an uncommitted delta is skipped without its base; <br>• a log without deltas recovers exactly as before and tracks nothing | 4/4 pass, every refusal a `WalCorruption` |
| | **I13 end to end.** `committed_deltas_never_build_on_an_uncommitted_transactions_eviction`: a never-committing transaction shares pages with 400 commits under a 32- or 64-page pool, then a crash; the committed state is checked after two reopens | pass |
| | **Mutation check.** Removing the epoch start at segment seal fails `replay_from_a_retained_sealed_segment_over_an_older_base`. Dropping the committed-base rule fails the unit test and the I13 end-to-end test, which refuses with "base LSN 222 is not the page's last replayed record 208" (fail-closed) | both mutants caught |
| Q1 oracle | **Delta oracle against full images.** `replaying_deltas_reproduces_every_page_the_pool_holds`: after every one of 160 commits, an independent decoder replays the log (images and deltas, from the last truncation) and compares every page, masked, with the buffer pool's copy, which is what a full image would have carried. A delta with no earlier record is an I12 failure. Pools of 16, 64 and 4,096 pages, with checkpoints every 37 commits | 470 + 2,495 + 8,511 deltas checked, 0 mismatches |
| Torn pages | `torn_pages_written_since_the_checkpoint_are_rebuilt`: after a crash, every page with a record at or after the redo point (121 pages) has its second half overwritten with garbage; recover twice; pools of 16 and 4,096 pages | pass |
| | The torn tail of a record in the active log (`crash_after_any_commit…` appends a half record) | truncated, pass |
| Interrupted checkpoints | **Retained checkpoints.** `crash_after_any_commit_with_retained_checkpoints_recovers_the_committed_state`: an open transaction keeps checkpoint records and redo points inside the log; crashes after 15 different commits; recover twice | pass |
| | **Checkpoint crash points.** The core crash matrix covers `checkpoint.after_wal_sync`, `after_page_flush`, `after_publish` and `after_wal_reset`, plus the bounded checkpoint (`bounded_checkpoint.rs`). The `after_publish` and `after_wal_reset` points were also repeated 100 times each | pass |
| | **Unvouched fallback.** `replay_from_a_retained_sealed_segment_over_an_older_base` (design 6.2): archive retention, the first sealed segment archived and deleted, older page files, newer checkpoints. Recovery takes the replay-from-chain-start path (`redo_lsn == 0`) and replays deltas; run twice | pass |
| Repeated recovery | Every crash test above reopens twice and checks the same state. `delta_after_committed_image…` recovers twice and checks the masked page | pass |
| Page-layer suites | Full `bicdb-page` test suite (lib, `crash_matrix`, `kill_recovery`, `bounded_checkpoint`, `wal_segments`, `page_backup`, `concurrent_commits`, …), both locks open via the hook | **402 passed, 0 failed, 1 ignored** (the reproducer in 10.4). 292 engaged stores; 56,577 deltas written; 45,397 deltas replayed by recovery |
| | Same suite with the gate off | 402 passed, 0 failed, 1 ignored |
| Crash matrix | `commit_durability_crash` (both durability modes, with and without `+small-pool` eviction, 15+ boundaries, interrupted replay) and `commit_durability_isolation`, both locks open | 4/4 + 1/1 pass |
| | **Repeats, 100 each, both locks open:** <br>• `all-logs+small-pool/commit.after_txlog_durable:150`: 7,594 recovery deltas; <br>• `transaction-log+small-pool/pool.after_writeback:120`: 31,022 recovery deltas; <br>• `all-logs+small-pool/checkpoint.after_publish:13`; <br>• `all-logs+small-pool/checkpoint.after_wal_reset:15`. <br>The two checkpoint points leave an empty log, so they replay no deltas. An earlier revision also passed `all-logs+small-pool/pool.after_writeback:60` (9,500 recovery deltas) and `checkpoint.after_page_flush:11` (8,051) | 400/400 pass |
| | The same core suites with the gate off | all pass |
| Backup / archive / PITR | `backup` 10, `backup_online` 3, `backup_online_wal_rotation` 1, `backup_paged` 4, `log_archive_pitr` 8, `commit_recovery_atomicity` 3, `mvcc_chain_recovery` 1, both locks open: 215 engaged stores, 22,061 deltas, 9,304 recovery deltas | all pass; same with the gate off |

### 10.3 Measurements

**Q7: bytes per commit.** The workloads are the PR #40 probe's, in
`examples/page_delta_bytes_probe.rs`. Bytes are the WAL's own
`bytes_appended`. The setup is 2,000 commits (500 for twenty-row and erp),
fsync on, and no checkpoint in the window. "Off" is a normal store; "on" is a
disposable store with the gate open.

| Workload | Off B/commit | On B/commit | Reduction | PR #40 estimate | Deltas / full images per commit |
|---|---|---|---|---|---|
| one-row | 59,631 | 3,202 | −94.6% | 2,581 | 7.08 / 0.16 |
| one-row, 8 writers | 62,463 | 3,306 | −94.7% | 2,682 | 7.44 / 0.15 |
| twenty-row | 325,089 | 33,202 | −89.8% | 28,815 | 36.69 / 2.80 |
| erp | 364,998 | 27,208 | −92.5% | 23,792 | 42.51 / 1.83 |
| update | 39,101 | 4,006 | −89.8% | 3,858 | 4.32 / 0.43 |
| update-indexed | 53,877 | 4,941 | −90.8% | 4,612 | 6.06 / 0.48 |
| delete | 8,535 | 2,799 | −67.2% | 2,740 | 0.70 / 0.33 |

The measured bytes run 4-24% above the estimate, for two reasons:
- each record carries the 20-byte delta header and a 6-byte header per run;
- the probe-dirtied pages that PR #40 counted as unchanged become 60-byte empty
  deltas, not nothing.

**Page-layer CPU, fsync off** (`examples/page_delta_cpu_bench.rs`, 20,000
commits of three puts each, 3 runs per arm, alternating):

| | Off | On |
|---|---|---|
| µs per commit | 42.1 / 42.3 / 41.7 | 34.1 / 32.8 / 30.8 |
| WAL B per commit | 60,034 | 1,957 |
| Checkpoints | 4 | 4 |

- **CPU:** with an equal number of checkpoints, the delta path costs less CPU
  than the full-image path: about −23%.
- **Checkpoints:** the C8 count trigger keeps the checkpoint rate the same as
  today.

**Q8: commit latency and throughput** (`commit_durability_bench`, all-logs,
fsync on, 20 s per trial, 5 trials per arm alternating off/on, median and
range):

| Writers | Arm | Commits/s median | Range | p50 µs | p95 µs | p99 µs |
|---|---|---|---|---|---|---|
| 1 | off | 275 | 154-605 | 3,272 | 4,185 | 8,374 |
| 1 | on | 215 | 175-628 | 3,004 | 12,249 | 21,882 |
| 4 | off | 433 | 142-707 | 8,034 | 23,246 | 73,687 |
| 4 | on | 428 | 356-552 | 8,296 | 12,587 | 15,650 |
| 16 | off | 595 | 355-712 | 26,070 | 38,561 | 73,529 |
| 16 | on | 391 | 184-690 | 29,724 | 104,605 | 186,926 |
| 64 | off | 342 | 314-676 | 116,654 | 580,638 | 685,905 |
| 64 | on | 397 | 305-686 | 98,877 | 533,903 | 718,180 |

**Q8 is inconclusive on this host.**
- **The ranges are too wide to separate the arms.** Within one arm, throughput
  varies 2-4x between trials, and every range overlaps the other arm's.
- **The bad periods hit both arms equally.** A page-layer A/B with fsync on
  (6,000 commits, 3 trials per arm) swung between 1.0 and 3.3 ms per commit in
  *both* arms, with p95 alternating between about 2 ms and about 9 ms. That
  tracks the disk's state, not the arm.
- **The one consistent signal is the single-writer tail.** The p95 at one
  writer was higher with deltas in the three core runs taken (12-20 ms against
  4-7 ms). It is not explained by CPU, which is lower, or by checkpoint rate,
  which is equal. It needs measurement on a quiet machine before any claim
  either way.
- **This matches PR #40's finding.** The fsync wait, not bytes, bounds
  small-commit throughput, so a 95% byte cut was not expected to raise commits/s
  much.

**Recovery time after a crash at 16 writers** (30 s run, then `recover`, 3
trials):

| Arm | Recovery open (ms) | Page WAL before recovery |
|---|---|---|
| Off | 172 / 427 / 659 | 107 / 397 / 706 MB |
| On | 249 / 124 / 151 | 16.7 / 4.2 / 3.4 MB |

- **Same order of magnitude.** Recovery opens take about the same time in both
  arms.
- **Why the logs differ in size:** the delta log is 25-200x smaller, because
  the count trigger keeps checkpoints as frequent as before while each record
  is small.
- **Not measured:** like the PR #36 bench, this is a process kill, not a power
  loss.

### 10.4 Defect found in the current recovery path (not caused by deltas)

`preexisting_mid_transaction_eviction_window` (ignored; run with `--ignored`)
reproduces on `main` at `decb9d0` with no delta code involved:
1. A page-level transaction stays open while 400 other transactions commit,
   under a 32-page pool.
2. Pages that only the open transaction logged are evicted. Their barrier
   records are under its xid, so recovery skips them, but committed structure
   already references those pages.
3. The process crashes.
4. Looking up the open transaction's keys then fails with `ChecksumMismatch {
   page_id: 340, stored: 0 }`, a page that was never written, instead of
   reading "absent".

This is the mid-transaction eviction window of
`commit-durability-invariants.md` §8.5, now with a deterministic reproducer.
It belongs to the recovery-semantics track. Committed data is intact in this
reproducer.

### 10.5 Remaining risks and gaps

- **The old-binary refusal** is argued from code, not executed against an
  older build.
- **Q8 on a quiet host.** Q8 needs to be repeated on a quiet or dedicated host,
  especially the single-writer p95.
- **Power loss** is emulated: a volatile WAL tail in the crash matrix, and
  process kills in the benchmark. A VM hard reset has not been run.
- **Small pools lose delta eligibility on eviction.** A reloaded frame starts
  without delta state. Keeping it across eviction is possible but adds a
  base-validity argument; it is not done.
- **Deltas are byte-diffs.** Physiological B-tree records would shrink leaf
  deltas further.
- **No fuzzing of delta payloads** beyond the structural validation tests.
- **Coverage depends on the existing suites.** The core crash matrix masks
  page-layer defects through `transactions.log` replay (PR #36 §7). The
  page-layer tests and the mutation checks are the primary evidence.

## 11. Rollout

Out of scope. The owner approved experimental development on disposable test
stores only. Merging even disabled delta code to `main` is a separate
decision, and so is any path for production stores. Prerequisites before
either could be considered:
1. the Q8 rerun on a quiet host;
2. a power-cut test;
3. an executed old-binary refusal test;
4. a decision on the recovery-semantics track, which affects I13's delta rate
   and owns the defect in 10.4.
