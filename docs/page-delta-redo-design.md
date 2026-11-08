# Page-delta redo records for the page WAL: design

Status: design, 2026-11-07. Nothing in this document is enabled. The prototype
it describes sits behind `BICDB_PAGE_WAL_DELTAS=1` (exact value), off by
default, and **every item marked CONTRACT CHANGE needs the owner's approval
before it can be enabled anywhere but a test.**

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
- **Old binaries are locked out** by a new superblock magic, written and synced
  before the first delta is appended. This follows the precedent of the
  segmented-WAL format gate. The marker is sticky.
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
| C2 | **On-disk compatibility** | Any binary since segmentation can open the store | A store that has logged a delta gets a new superblock magic `BICDBPG5`, and older binaries refuse it. The marker is sticky: no automatic downgrade. Downgrade is a restore, or a future explicit `store wal-downgrade` that checkpoints to an empty log and rewrites the magic | An old binary treats an unknown record kind in the *active* file as a torn tail (`wal.rs` `scan_wal_file`: `from_u8` → `break`) and **truncates committed records silently**. The marker turns that into a refusal to open. Sealed segments already fail closed (a short decode is corruption) |
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
  - *Before the first delta is appended,* the store writes and syncs a
    superblock with magic `BICDBPG5`, whose body carries a feature word at
    `[76..84]`:
    - bit 0: segmented;
    - bit 1: tracked (backup format);
    - bit 2: delta WAL.
  - *Old readers* accept only `BICDBPG\0`, `PG2`, `PG3` and `PG4` (`manager.rs`
    `read_superblock` and `probe_page_size`). They refuse with `NotAPageFile`.
  - *New readers* decode the feature word.
  - *A monolithic (unsegmented) store* gets the marker too. It has no sealed
    segments, so it has no other protection.
- **Gate off:** the marker is never written and the magic stays as it is today.
  A store that already carries the marker keeps it, and the new binary reads it
  with the gate off. The gate controls *writing* deltas. *Reading* them is
  always supported.
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
the full images in the replay range. Its peak size is reported as a new field,
`delta_base_entries`, next to the new counters `deltas_replayed` and
`delta_bytes_replayed`. `RecoveryReport` is `deny_unknown_fields` but every
field is `#[serde(default)]`, so older serialized reports still parse.

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
| Capture memory exhausted | None. Records fall back to full images | Cap counter, metric `delta_base_capture_skipped` |
| `appended_commits` overflow | Deltas are refused until the next epoch | Full images, metric |
| Recovery time regression (many small records) | A long replay | C8 count trigger; measured in Q6 |
| Refusal in production | The database does not open: a fail-closed availability event | No automatic fallback exists. The refusal message names the page, the record LSN and the base LSN. A restore from backup is the remedy |

## 10. Qualification plan

| # | Check | Pass criterion |
|---|---|---|
| Q0 | Gate off: same bytes. A test commits an identical workload twice in fresh stores, with the gate unset and with garbage values (`0`, `true`, ` 1`), and compares `store.wal` byte-for-byte plus the superblock | Identical. The gate parses only `1` |
| Q1 | Shadow oracle, gate on: in tests, every delta appended is also decoded and applied to a copy of its base kept by the test, then compared (masked) with the full image the gate-off path would log | Zero mismatches over the page unit suite and a randomized B-tree/heap workload |
| Q2 | Recovery unit tests: delta after image, delta chains, a delta after a skipped (uncommitted) base is refused, a delta before the redo point is refused, a checksum mismatch is refused, a torn delta tail is truncated, idempotent double replay, an old-format log replays unchanged | All pass |
| Q3 | `bicdb-page` suites with the gate on: `crash_matrix`, `kill_recovery`, `bounded_checkpoint`, `wal_segments`, `page_backup`, `concurrent_commits` and the full lib suite | Same results as gate off |
| Q4 | Core crash matrix with the gate on: `commit_durability_crash.rs` + `examples/durability_child.rs`, both durability modes, including `+small-pool` (eviction and writeback mid-transaction), plus repeats of the hardest boundaries | All pass; no acknowledged commit lost |
| Q5 | Backup, archive and PITR with the gate on: `backup_online.rs`, `backup_online_wal_rotation.rs`, `backup_paged.rs`, `log_archive_pitr.rs` | Same results as gate off |
| Q6 | Repeated recovery: reopen N times after a crash (each open re-replays when the checkpoint is suppressed) | Identical state; recovery time recorded |
| Q7 | Bytes per commit with the PR #40 probe (`wal_amplification_probe`), gate off vs on, for the one-row, twenty-row, erp, update, update-indexed and delete workloads | Report the measured reduction against the 90-96% estimate |
| Q8 | Latency and throughput with `commit_durability_bench` at 1, 4, 16 and 64 writers, gate off vs on, median of 3 | Report. PR #40 predicts about 10-15% for small commits (the fsync dominates) and more for large ones |

The core crash matrix alone is not enough. `commit-durability-invariants.md`
§7 found that replay from `transactions.log` masks page-layer defects, so Q1-Q3
at the page layer are the primary guards and Q4 is the end-to-end check.

## 11. Rollout

1. **Merge the prototype with the gate off.** This needs approval of C1-C9 as
   *code that exists but is disabled*. The gate-off cost: one relaxed `bool`
   check per `get_mut`, per guard drop and per page log.
2. **Qualify on target hardware,** including a real power cut (VM reset), for
   the recovery-time measurement.
3. **Opt in per store.** The first delta writes the C2 marker, which is
   irreversible without a restore until a downgrade command exists.
4. **Follow-ups, not part of this track:**
   - physiological B-tree records ("insert entry at slot i"), which would cut
     leaf deltas from about 240 to about 60 bytes;
   - relaxing I13 under repeat-history.
