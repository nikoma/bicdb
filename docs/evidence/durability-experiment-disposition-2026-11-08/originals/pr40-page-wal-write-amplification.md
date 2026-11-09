# Page-WAL write amplification in server_paged

Status: investigation, 2026-11-07. Measurement and proposals only. The
storage format and the commit path are unchanged. The only code this adds is
an observation trace that is off by default
(`crates/bicdb-page/src/wal_amplification.rs`), plus the probe that drives it
(`crates/bicdb-core/examples/wal_amplification_probe.rs`).

Background: `docs/commit-durability-invariants.md` (branch
`durability/txlog-only-commit`) found that a one-row commit writes about 70 KB
of full page images under the store-wide `write_lock`, and that throughput
stays flat at 400-700 commits/s from 4 to 64 writers. This document measures
where those bytes come from and what could remove them safely. It also checks
whether the bytes are really what bounds throughput.

## 1. Summary

- **The ~90x amplification is real and reproducible.** The bench's one-row
  commit appends **59.6 KB** of page WAL (7.2 page images) for a 755-byte
  logical record: **79x**. Over 20,000 commits it grows to 69.6 KB (91x).
  Across those images only **836 bytes actually change**.
- **Where the bytes go (one-row commit):**

  | Source | Share | Notes |
  |---|---|---|
  | Key-tree leaves | 52% | 3.8 images: the new row's key, the hot row's key (a 14-byte locator overwrite), and two index entries |
  | Heap pages | 33% | 2.4 images; 1.3 of them are byte-for-byte unchanged |
  | Catalog (free-space map) leaf | 14% | One image on every commit, for about 11 changed bytes |
  | Meta page | 0% | Never dirtied per commit |

- **A cheap bug-class fix, with no format change:**
  - **What happens:** heap inserts take write guards on candidate pages they
    then do not use. The buffer pool marks every write guard dirty, so those
    unchanged pages are imaged.
  - **What it costs:** 18% of the one-row commit's bytes, 60% of a 20-row
    commit's and 41% of the ERP mix's.
- **Biggest volume lever: full image on first touch after the redo point, then
  byte-range deltas** (the classic full-page-write rule).
  - **Estimated saving:** 90-96% of page-WAL bytes for insert-heavy commits;
    one-row 59.6 KB → about 2.6 KB.
  - **What it needs:** a new record kind and changes to recovery, backup start
    and archive replay (section 5.2).
- **The bytes are NOT what bounds small-commit throughput.**
  - **Where the time goes:** under `write_lock`, a one-row commit spends 51 µs
    applying, 85 µs logging images, and **847 µs appending the commit record
    and waiting for the fsync**. That is 86% of the hold.
  - **Evidence that bytes don't set the sync cost:** the sync takes about the
    same time for 8.5 KB (delete) as for 63 KB (one-row).
  - **What this means:**
    - Removing every image from small commits buys roughly 10-15% throughput.
    - Releasing the writer gate before the fsync wait is the throughput lever
      (section 6).
    - Bytes start to dominate the sync only at hundreds of KB per commit (ERP:
      388 KB, 3.6 ms).

## 2. Instrumentation

`bicdb_page::wal_amplification` (measurement only).

- **Off by default.** It is enabled by `BICDB_PAGE_WAL_AMPLIFICATION=1` (exactly
  `1`) or by `wal_amplification::enable()` from a probe.
  - **Cost when off:** each hook is one relaxed atomic load and a branch. No
    allocation, no lock.
  - **Pinned by a test:** `crates/bicdb-page/tests/wal_amplification_off.rs`
    asserts that:
    - it is off by default;
    - only the exact value `1` parses as on;
    - an off commit records nothing;
    - an on commit appends exactly the same WAL bytes as an off one.
- **Hooks:**

  | Hook | Where | What it records |
  |---|---|---|
  | Image | `Wal::log_page_image` | The image, its LSN and the redo LSN in force |
  | Commit and abort records | `Wal::commit` / `Wal::abort` | One commit or abort |
  | Dirtying | the buffer pool's `PageGuardMut::drop` | Which page was dirtied, in which context |
  | Context scopes | `PagedStore::put_returning_locator` / `delete` (op + key class from the core namespace), `mark_deleted` (`xmax_stamp`), `write_meta` (`meta`), `prepare_free_adoption`, status spill, vacuum, index sweep, `BTree::split_and_insert` (`split`), `Catalog` mutators (`catalog`), commit vs writeback-barrier log site | The labels attached to each dirtying |
  | Lock hold | `PagedStore::commit` and core's `in_paged_transaction_inner` | Writer-gate time split into apply, log images, and commit record + sync |

- **Per image it records:**
  - **Page class**, from the image header plus context: heap, key-tree
    leaf/interior, catalog leaf/interior, overflow, free, meta, status spill.
  - **The set of operations** that dirtied it since its previous image.
  - **Record bytes.**
  - **Bytes changed** versus the previous image of the same page.
  - **Estimated byte-range delta record:** 40-byte record overhead + 4 bytes per
    run + run bytes, merging runs that are at most 4 bytes apart.
  - **Repeat since redo:** whether the page already had an image at or after the
    redo LSN in force.
  - **Consecutive:** whether the immediately preceding commit also imaged it.

## 3. Method

- **Host:** s3ny (24 cores, consumer NVMe, ext4), shared at load 22-27.
- **Build:** release, `server_paged`, fsync on, outbox off, 8 KiB pages,
  default 256 MiB WAL checkpoint trigger.
- **Procedure:** each run creates a fresh store, runs the setup and any
  preload, takes a checkpoint, resets the counters, then runs the measured
  commits.
- **Reliability:** byte and image counts are deterministic to within page
  placement. Timings come from single runs on a busy host and carry the same
  ±30% spread the durability study saw.
- **Workloads:**

  | Run | Per commit |
  |---|---|
  | one-row | 1 insert into `rows` (B-tree index `owner`, unique index `uk`) + update of the writer's hot row (`commit_durability_bench` shape) |
  | twenty-row | 20 such inserts + the hot-row update |
  | erp | Document header with varchar ids and 5 secondary indexes (one unique composite), 5 lines with 3 indexes (one unique composite), update of a hot `ad_sequence` row, then completion of the header (status change moves its `docstatus` key). Shaped after document tables such as orders and order lines |
  | update | 1 random row of a 20,000-row preload, non-indexed field |
  | update-indexed | Same, but the update moves the `owner` index key |
  | delete | 1 random preloaded row |

  Variants: t8 = 8 writer threads; long = 20,000 commits; smallwal = 4 MiB WAL
  trigger (19 checkpoints in 5,000 commits).

## 4. Measurements

### 4.1 Per-commit totals

"Changed" is bytes that differ from the page's previous image. "Delta" is the
estimated delta-record bytes if every image were a delta. "FPW" applies the
full-page-write rule: a full image for the first touch after the redo point,
a delta otherwise. The last three columns are µs per commit under the writer
gate.

| Run | Commits/s | Checkpoints | Transaction log B | Page WAL B | Amplification | Images | Changed B | Unchanged images | Consecutive images | Delta B | FPW B | Apply µs | Log images µs | Commit record + sync µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| one-row | 375 | 0 | 755 | 59,631 | 79x | 7.24 | 836 | 1.32 | 5.72 | 2,569 | 2,581 | 51 | 85 | 847 |
| one-row t8 | 702 | 0 | 755 | 62,928 | 83x | 7.64 | 852 | 1.32 | 3.44 | 2,670 | 2,682 | 47 | 87 | 1,059 |
| one-row long | 221 | 1 | 768 | 69,608 | 91x | 8.45 | 902 | 0.84 | 5.95 | 2,758 | 2,800 | 72 | 224 | 1,317 |
| one-row smallwal | 249 | 19 | 759 | 66,158 | 87x | 8.03 | 893 | 0.29 | 5.88 | 2,741 | 4,752 | 73 | 140 | 1,124 |
| twenty-row | 153 | 0 | 6,218 | 344,689 | 55x | 41.87 | 9,437 | 25.20 | 12.15 | 28,798 | 28,815 | 533 | 544 | 1,701 |
| erp | 78 | 0 | 4,274 | 387,841 | 91x | 47.11 | 8,952 | 19.42 | 12.22 | 23,774 | 23,792 | 483 | 676 | 3,581 |
| erp t8 | 145 | 0 | 4,274 | 394,666 | 92x | 47.94 | 8,271 | 18.81 | 6.95 | 23,098 | 23,116 | 1,186 | 720 | 4,028 |
| update | 402 | 0 | 529 | 39,101 | 74x | 4.75 | 127 | 1.12 | 1.17 | 535 | 3,858 | 27 | 133 | 770 |
| update-indexed | 456 | 0 | 530 | 53,877 | 102x | 6.54 | 347 | 1.63 | 1.72 | 1,242 | 4,612 | 31 | 114 | 702 |
| delete | 347 | 0 | 370 | 8,535 | 23x | 1.03 | 8 | 0.00 | 0.00 | 66 | 2,740 | 32 | 170 | 884 |

How to read it:

- **Image volume:** a commit records about 1-10% of its image bytes as actual
  changes.
- **Same pages, commit after commit:** in one-row, 98% of key-tree leaf images
  and 100% of catalog images repeat the previous commit's page (section 4.2).
- **Why FPW is high for update and delete:** these runs start right after a
  checkpoint over a 20,000-row table. Random rows touch a page not yet imaged
  since the redo point, so most images are first-touch. That is the honest
  steady-state cost of random updates to a table larger than one checkpoint
  interval's working set. A database using the classic full-page-write rule
  has the same property.

### 4.2 By page class

Values are per commit; "unchanged" and "consecutive" are percentages of that
class's images.

| Run | Page class | Images | KB | Share | Changed B | Unchanged | Consecutive |
|---|---|---|---|---|---|---|---|
| one-row | key-tree leaf | 3.79 | 30.4 | 52% | 602 | 0% | 98% |
| | heap | 2.39 | 19.2 | 33% | 133 | 55% | 42% |
| | catalog leaf | 1.00 | 8.0 | 14% | 11 | 0% | 100% |
| | key-tree interior | 0.06 | 0.5 | 1% | 82 | 0% | 5% |
| | meta | 0.00 | 0.0 | 0% | | | |
| twenty-row | heap | 34.25 | 275.3 | 82% | 1,236 | 74% | 21% |
| | key-tree leaf | 5.12 | 41.2 | 12% | 6,926 | 0% | 74% |
| | catalog leaf | 1.33 | 10.7 | 3% | 65 | 0% | 80% |
| | key-tree interior | 1.16 | 9.3 | 3% | 1,184 | 0% | 8% |
| erp | heap | 30.49 | 245.1 | 65% | 1,876 | 64% | 12% |
| | key-tree leaf | 15.19 | 122.1 | 32% | 5,778 | 0% | 55% |
| | key-tree interior | 0.72 | 5.8 | 2% | 1,198 | 0% | 12% |
| | catalog leaf | 0.70 | 5.6 | 1% | 78 | 0% | 0% |
| update | heap | 3.15 | 25.3 | 66% | 116 | 36% | 32% |
| | key-tree leaf | 1.00 | 8.0 | 21% | 4 | 0% | 1% |
| | catalog leaf | 0.60 | 4.8 | 13% | 7 | 0% | 26% |
| update-indexed | heap | 3.68 | 29.6 | 56% | 129 | 44% | 27% |
| | key-tree leaf | 2.02 | 16.2 | 31% | 199 | 0% | 6% |
| | catalog leaf | 0.82 | 6.6 | 13% | 10 | 0% | 72% |
| delete | heap | 1.03 | 8.3 | 100% | 8 | 0% | 0% |

- **Writeback-barrier images** are below 0.1 per commit in every run with the
  default pool, and so are interior-node splits.
- **What is absent:** no overflow, free-list, status-spill or meta images
  appear per commit.

### 4.3 By operation (largest groups)

"Changed B" is per image here, not per commit.

one-row:

| Page class | Dirtied by | Images | KB | Changed B | Unchanged |
|---|---|---|---|---|---|
| key-tree leaf | put index entry (unique + B-tree) | 1.90 | 15.3 | 237 | 0% |
| key-tree leaf | put row (new key + hot-row locator overwrite) | 1.75 | 14.1 | 69 | 0% |
| heap | tail page: new row + entries + hot-row old version `xmax` | 0.99 | 7.9 | 134 | 0% |
| heap | put row (probe only) | 0.72 | 5.8 | 0 | 95% |
| heap | put index entry (probe only) | 0.64 | 5.2 | 0 | 100% |
| catalog leaf | free-space bucket move | 0.98 | 7.9 | 9-10 | 0% |

twenty-row:

| Page class | Dirtied by | Images | KB | Changed B | Unchanged |
|---|---|---|---|---|---|
| heap | put row (probe only) | 31.1 | 250.1 | 2 | 77% |
| key-tree leaf | put index entry | 2.94 | 23.6 | 1,949 | 0% |
| key-tree leaf | put row | 2.12 | 17.0 | 535 | 0% |
| heap | put index entry (probe only) | 1.20 | 9.7 | 0 | 100% |
| catalog leaf | free-space bucket move | 1.33 | 10.7 | 49 | 0% |

erp:

| Page class | Dirtied by | Images | KB | Changed B | Unchanged |
|---|---|---|---|---|---|
| heap | put row (probe only) | 19.3 | 155.4 | 2 | 69% |
| key-tree leaf | put index entry (5 + 3×5 entries, varchar keys) | 13.1 | 105.5 | 394 | 0% |
| heap | put index entry (probe only) | 8.0 | 64.5 | 2 | 66% |
| key-tree leaf | put row | 2.05 | 16.4 | 286 | 0% |
| heap | tail page: rows, entries, `xmax` stamps, deleted docstatus entry | 0.99 | 8.0 | 1,810 | 0% |

update / update-indexed / delete:

| Run | Page class | Dirtied by | Images | Changed B |
|---|---|---|---|---|
| update | heap | new version (often a probe) | 2.15 | 52 |
| update | heap | old version `xmax` stamp | 1.00 | 4 |
| update | key-tree leaf | locator overwrite | 1.00 | 4 |
| update-indexed | key-tree leaf | moved index entry | 1.02 | 192 |
| update-indexed | heap | old row + old entry `xmax` stamps | 0.98 | 6 |
| delete | heap | row + both entries `xmax` (co-located) | 0.98 | 8 |

Observations the tables establish:

1. **Probe-dirtied heap pages.**
   - **The code path:** `HeapFile::insert_tuple` (`heap.rs:203-252`) takes
     `pool.get_mut` on the catalog's candidate and then on each page of the
     free-space ring until one fits. `PageGuardMut::drop` (`pool.rs:536`) marks
     every write guard dirty and `needs_log` unconditionally. So every probed
     page that does not receive the tuple is imaged at commit.
   - **What triggers it:** ring-path inserts do not update the catalog. The
     catalog's free-space record therefore goes stale, its candidate stops
     fitting, and the next insert falls through to a ring scan of full pages.
   - **Measured cost:** these pages are 55% of one-row heap images, 74% in
     twenty-row and 64% in erp. They show 0 changed bytes, or a few bytes from
     a compaction attempt. Unchanged images never appear on B-tree pages.
2. **The free-space map leaf is imaged on every commit.**
   - **Why:** `Catalog::update_free_space` (`catalog.rs:167`) moves a page
     between 32 buckets of 256 bytes each. A commit that adds a few hundred
     bytes to the tail page almost always crosses a bucket boundary, which
     means a remove plus an insert in one catalog leaf: 8 KB logged for about
     10 bytes.
3. **Key-tree leaves are the irreducible part under full-page logging.**
   - **What each one costs:** every distinct key insert or locator change
     images its leaf.
   - **Why the diffs exceed the key size:** inserting into the middle of a leaf
     shifts the sorted 8-byte directory (`btree.rs:520`). So a 40-70 byte entry
     changes about 240 bytes in one-row and about 400 in erp.
   - **Hot rows:** a hot row costs a whole leaf image per commit for a 14-byte
     same-size locator overwrite (`btree.rs:491-498`), plus its old version's
     `xmax` stamp (usually on the shared tail heap page).
4. **The meta page is not a per-commit cost.**
   - **Measurement:** `write_meta` runs from `sync_roots` only when a root
     moves (`paged.rs:1187`), and that shows as 0.00 images per commit.
   - **Conclusion:** the "avoid dirtying the meta page per commit" candidate is
     refuted.
5. **Index entries share the tail heap page with their row.** Each entry is a
   heap tuple (version header + empty value) plus a leaf entry. A delete stamps
   `xmax` on the row's tuple and both of its entry tuples, all on one page, so
   delete is already cheap: 1 image, 8 changed bytes.
6. **Concurrency does not change the per-commit pattern.**
   - **What stays the same:** images per commit are within 6% from t1 to t8.
     Writers are serialized under the gate.
   - **What changes:** fewer images repeat the immediately preceding commit,
     because successive commits come from different writers with different hot
     rows.

## 5. Opportunities, ranked

Each opportunity is checked against these invariants:

- **I5:** WAL before data.
- **Torn-page protection.**
- **I8:** idempotent replay.
- **Checkpoint/redo point**, including I11.
- **Archive/PITR roll-forward.**
- **Standby.**

Standbys receive logical frames from `transactions.log` only
(`replication_live.rs`). The page WAL is never shipped, so no option below
changes standby behaviour. That holds as long as the transaction log keeps its
current role (I10).

Savings are per commit, taken from section 4. The "Save" row of each table
lists the workloads in the order one-row / twenty-row / erp / update /
update-indexed / delete.

### 5.1 Stop imaging pages that were probed but not modified (rank 1: cheap, no format change)

| | |
|---|---|
| What | Probe candidates with a read guard (`pool.get`) and take `get_mut` only on the page that receives the tuple, re-checking the fit under the write guard. Keep the catalog in step on ring-path inserts, so the ring scan stops being the common path. Optionally, make `PageGuardMut` dirty only when the bytes were actually written: for example, an explicit `mark_unmodified()` used by probe-style callers. |
| Save | Unchanged images only: 10.9 KB (18%) / 207 KB (60%) / 160 KB (41%) / 9.2 KB (24%) / 13.4 KB (25%) / 0. Counting the near-unchanged probe images too, twenty-row reaches about 74%. Writebacks and checkpoint flushes of these pages disappear as well. |
| Safety | An image identical to the page's previous image adds nothing a replay needs: the earlier image, or the on-disk page when the previous image was before the redo point and flushed, already holds those bytes. I5 is unaffected (a page that is not dirty is never written). Torn pages are unaffected (no write). I8 holds (fewer identical images). The redo point and archives are unaffected. Only pages whose bytes really did not change may skip the log. The compaction path does change bytes and must still mark the page dirty. |
| Recovery change | None. |
| Risk | Low. The risk is a caller that mutates through a guard it then reports as unmodified. Use read guards for probes rather than a "trust me" flag where possible. A page-level test can assert the image count for an insert that probes N full pages. |

### 5.2 Full image on first touch after the redo point, deltas after (rank 1 for volume: format and recovery change)

| | |
|---|---|
| What | New record kind `PageDelta {page_id, xid, runs[(offset u16, len u16, bytes)]}`, computed against the page's last logged content. A page gets a full image when its last full image is older than the current redo point; every later record for the page until the next redo point is a delta. To compute the delta, capture the page at its first write guard after its last log (copy-on-first-write into a per-transaction side buffer, bounded by the transaction's dirty set), or emit physiological records from the heap and B-tree operations. |
| Save | Default 256 MiB trigger: one-row 59.6 → 2.6 KB (−96%), twenty-row 345 → 29 KB (−92%), erp 388 → 24 KB (−94%), update-indexed 54 → 4.6 KB (−91%), delete 8.5 → 2.7 KB (−68%). With a 4 MiB trigger (a checkpoint about every 260 commits), one-row is 4.75 KB (−93%). Applied after 5.1, the remaining saving is the same in absolute terms. Physiological B-tree records ("insert entry at slot i") would cut leaf deltas further: one-row leaves 1.7 KB → about 0.4 KB. |
| I5 | Unchanged: the writeback barrier syncs through the page's last record, full image or delta. |
| Torn pages | Each page's first record after the redo point is a full image, so replay overwrites whatever is on disk (torn or not) before applying any delta. |
| Redo point | **The full-image epoch must flip when a checkpoint *chooses* its redo LSN, under the same lock as the append** (`write_lock` today, `paged.rs:2657`), not when it is published. Otherwise a delta appended between choice and publication would have its base before the new redo point. The trace in this branch approximates this with the published redo LSN, which overestimates repeats very slightly. |
| I8 | Replay is per page: the full image, then deltas in LSN order. An interrupted replay restarts from the same redo point and repeats the same sequence. No page LSN is needed (the header's `lsn` field is currently always 0). A delta whose page has no full image earlier in the replay range is detectable and must fail recovery, never be skipped. |
| Uncommitted bases | Recovery skips images of uncommitted and aborted transactions (`wal.rs:1260`). A delta must never use such a record as its base. Rule: take a delta only when the page's last record belongs to a committed transaction or to this one; otherwise log a full image. Track each page's last-record xid (aborts are rare). Alternatively, replay every page record in LSN order regardless of outcome ("repeat history") and leave visibility to MVCC. That would also close the mid-transaction eviction window noted in `commit-durability-invariants.md` §8.5, but it is a separate decision. |
| Free-list detach, structural xid-0 images | Unchanged: they stay full images (free-adoption pages are rewritten wholesale anyway). |
| Archive / PITR / online backup | `recover_with_outcomes` falls back to replaying from LSN 0 when the base copy's checkpoint horizon predates the stream's redo point (`wal.rs:1239`). With deltas, roll-forward must start at a redo point the archive covers, never mid-epoch. Backup start must also force a new full-image epoch: an online page copy can tear any page, and deltas after the backup's start would then apply to a torn base. This is the same reason another database forces full-page writes during a base backup. `begin_backup_pin` needs to move the epoch, and the restore needs to record the backup's start redo LSN. |
| Tools | Every reader of `PageImage` records (`terminal_outcomes_in_range`, backup verification, inspection) must understand `PageDelta`. Old binaries must refuse a WAL that contains deltas: bump the WAL format, and old stores keep working because full images remain valid. |
| Risk | Medium-high: this is the recovery code path. Mitigations: <br>• The crash matrix from the durability branch (`commit_durability_crash.rs`, `durability_child.rs`) with eviction, run over both formats. <br>• A replay oracle check: replay the deltas and compare with the full image the old path would have logged. This is cheap to build from this trace, because it already holds both. <br>• A first-record check (a full image is required). <br>• Fuzzing of torn deltas. |

### 5.3 Free-space map hysteresis (rank 3: cheap, no format change)

| | |
|---|---|
| What | Move a page between catalog buckets only when its free space crosses a coarser threshold. For example: buckets of 1/8 page, or update only when free space falls below the largest common tuple size. Alternatively, defer the catalog update for the current tail page until the page leaves the "current insert target" role. |
| Save | About 1 catalog image per commit: 8.0 KB (14%) for one-row, 8.0 KB (20%) for update-indexed, 4.8 KB (12%) for update. Under 5.2 this would shrink to about 80-100 B of delta. So it is worth doing standalone only if 5.2 is not pursued. |
| Safety | The bucket is a floor that `find_page_with_room` trusts (`catalog.rs:183`). Hysteresis that moves pages down eagerly and up lazily keeps the floor guarantee: a page may look *fuller* than it is, never emptier. The catalog stays fully logged, so recovery is unchanged. |
| Risk | Low. Space reuse gets slightly worse. Combine with 5.1, so a stale catalog cannot trigger ring scans. |

### 5.4 Coalesce page images across commits; logical log as redo (rank 4: architectural)

| | |
|---|---|
| What | The page images of consecutive commits mostly re-image the same pages: one-row has 98% consecutive key-tree leaves and 100% consecutive catalog. For row commits, whose logical redo is already durable in `transactions.log` (I1), the commit could append no images at all. Pages would be imaged only at writeback (the barrier) and at checkpoint, and recovery would replay the logical tail from `transactions.log`. This generalises the experimental `BICDB_COMMIT_DURABILITY=transaction-log` mode from "do not fsync the page WAL" to "do not write it per commit". It is also the general form of "defer hot-row image logging". |
| Save | Images become distinct pages per checkpoint interval. One-row t1: 7.14 of 7.24 images per commit repeat since redo, so about 0.1 image per commit (−98%), plus the writeback images. |
| Safety | Requires: <br>• I3/I4 markers whose meaning becomes "covered by a checkpoint". <br>• I7 (cut only after a checkpoint). <br>• I8 for every row-write kind (unique indexes, lazy tables, TRUNCATE, FTS, spatial). Not yet demonstrated beyond the durability matrix. <br>Page-only transactions (bulk load, index builds, FTS folds, spatial packs) have no logical redo and must keep their page images and fsync (I6). The writeback barrier must still log an image before writing a page (I5). |
| Recovery | Replay time grows with the logical tail since the last checkpoint. The durability study could not measure that after a real power loss. |
| Risk | High; this is an architectural decision (section 7). |

### 5.5 Index entries without heap tuples (rank 5: format change, small image saving)

| | |
|---|---|
| What | Keep MVCC visibility for B-tree entries in the leaf, or treat entries as non-versioned with deletion by a vacuum-time recheck, instead of storing each entry as a heap tuple with a version header. |
| Save | Small in images: entry tuples share the tail heap page with their row, so the image count barely moves (update-indexed: the 0.98 `xmax`-stamp image stays for the row). It saves heap bytes, about 40 B per entry, and a descent per entry write. |
| Risk | High; it changes the TID-hint and visibility design. Not recommended for WAL volume alone. |

### 5.6 Smaller pages for index leaves (rank 6: subsumed)

| | |
|---|---|
| What | 4 KiB pages for the key tree would halve leaf image bytes: one-row 30.4 → about 15 KB. |
| Why not | Page size is one per store and pages are shared between the heap and both trees, so this means a mixed-page-size store. 5.2 removes the same bytes and more. Not recommended. |

### 5.7 Smaller items found in the measurements

- **One `write_all` syscall per record.** `Wal::append` (`wal.rs:549-614`) seeks
  and writes each record separately: 9 syscalls for a one-row commit.
  - **Fix:** encode all of a commit's records into one buffer and write once.
  - **Safety:** this does not change the format, and the LSN order still comes
    from the same file lock.
  - **Saving:** part of the 85 µs image-logging time.
- **Page bytes are copied twice.** `log_dirty_pages` copies each page with
  `to_vec()` and `append` copies it again into the record buffer. Writing the
  page into the record buffer directly halves the copying.

## 6. The store-wide writer gate (`write_lock`)

### What it protects (code citations in `paged.rs` unless noted)

- **B+ tree structural changes.** The trees are single-writer: there is no
  latch coupling and no B-link tree (`paged.rs:740-759`). Two concurrent splits
  of one node lose rows. This was measured before the lock existed: 378 of
  1,600 committed rows lost.
- **Root publication (`sync_roots`, `write_meta`).** A lost root orphans a
  subtree.
- **Commit attribution of dirty pages.**
  - `log_dirty_pages` scoops *every* page whose `needs_log` is set, from the
    whole pool, under the committing xid (`pool.rs:2193`).
  - Frames carry one `dirtied_by` stamp.
  - So only one transaction may have unlogged changes at a time. Core holds the
    gate from `begin` through `commit` (`spatial_paged.rs:1506`) for
    exactly this reason.
- **Heap placement and the free-space map** (the ring and catalog).
- **Checkpoint safety (I11).** Nothing may be appended between the
  pre-truncation sync and `Wal::reset`.
- **Visibility.** `transactions.commit` runs after the commit record is durable
  and inside the gate.

### Measured hold per commit

| Run | Apply | Log images | Commit record + fsync | fsync share |
|---|---|---|---|---|
| one-row | 51 µs | 85 µs | 847 µs | 86% |
| one-row t8 | 47 µs | 87 µs | 1,059 µs | 89% |
| erp | 483 µs | 676 µs | 3,581 µs | 76% |

- **The bound this sets:** at 8 writers, 1.19 ms of serialized hold per commit
  caps throughput at about 840 commits/s. Observed: 702.
- **Bytes barely move the fsync:**
  - It takes about the same time for 8.5 KB (delete: 884 µs) as for 63 KB
    (one-row: 847-1,059 µs).
  - It grows only at hundreds of KB (twenty-row: 1.7 ms; erp: 3.6 ms).
- **What fixes what:**
  - Image reduction (5.1, 5.2) fixes WAL volume, checkpoint frequency, archive
    size and the large-commit sync.
  - Small-commit throughput needs the fsync wait moved out of the gate.

### What could move outside the gate

1. **The fsync wait (early lock release; the throughput lever).**
   - **The change:**
     1. Append the commit record under the gate.
     2. Release the gate.
     3. Call `sync_through(commit_lsn)` outside it.
     4. Only then call `transactions.commit` (visibility).
   - **Durability:**
     - The WAL is one sequence, so a later transaction's commit record is
       durable only if every earlier one is (prefix durability).
     - The next writer may read and modify pages that contain an
       undurable transaction's bytes. Its own images then contain those bytes.
       Recovery replays them only if its commit record survived, and then the
       earlier one survived too.
   - **Required changes:**
     - (a) **Write conflicts:** page-level `check_write_conflict` treats an
       in-progress xmin/xmax as a conflict (`paged.rs:1792`). Today core
       row locks keep the same row from reaching the page layer, but
       engine-namespace keys shared across transactions would raise spurious
       conflicts. Examples: the intern-dictionary counter, FTS dictionary and
       statistics keys, spatial meta. There needs to be a "committing" status
       that waits rather than fails.
     - (b) **fsync failure:** fail every commit whose LSN is above the failure
       and set `recovery_required`. The same rule applies today, but now
       several commits can be in flight.
     - (c) **Checkpoints and `abort`** keep taking the gate and syncing before
       reset, so I11 holds.
     - (d) **Commit order:** the materialization marker (I3/I4) must still
       follow the sync of the commit it names.
   - **Combination:** with the earlier change that syncs without holding the
     WAL file mutex (durability branch, 1f78e4f), concurrent committers share
     one fsync (group commit), which the current gate prevents.
2. **Image copy and record encoding** (85 µs). These need the gate only for the
   `needs_log` scoop and LSN order. Moving the copy out needs per-transaction
   dirty tracking: a set of dirtiers per frame instead of one stamp. That is
   the same prerequisite as concurrent writers, so it is not worth doing alone.
   5.1, 5.2 and 5.7 shrink this time instead.
3. **Concurrent apply** (51-480 µs). This needs latch coupling or a B-link
   tree and per-transaction dirty sets. It is the large project
   `paged.rs:752-758` already describes, and it is not justified while the
   fsync is 76-89% of the hold.

## 7. Decisions needed

1. **5.1 and 5.3:** fix probe-dirtying and add free-space hysteresis in the
   production write path. They change no format and no recovery, but they do
   change the production page path, so they need a separate PR with the crash
   matrix run.
2. **5.2:** adopt a delta record kind and the full-page-write rule. This is a
   WAL format change with a version bump. It changes recovery, backup start
   (forced full-image epoch) and archive roll-forward (start at a redo point).
3. **Early lock release (section 6.1):** it changes commit concurrency
   semantics and fsync-failure handling. It is the only option measured to
   address the small-commit throughput ceiling.
4. **5.4:** decide whether `transactions.log` may become the redo for row-commit
   page state. It overlaps with the rollout decision still open for
   `BICDB_COMMIT_DURABILITY=transaction-log`.
5. **Separately from all of the above:** whether recovery should repeat history
   for page records, instead of skipping uncommitted images. It matters for 5.2's
   delta-base rule and for the mid-transaction eviction window.

## 8. Reproducing

- **Build:**

  ```sh
  cargo build --release -p bicdb-core --example wal_amplification_probe
  ```

- **Run one workload:**

  ```sh
  target/release/examples/wal_amplification_probe <fresh-dir> one-row 2000 1
  ```

  - Workloads: `one-row`, `twenty-row`, `erp`, `update`, `update-indexed`,
    `delete`.
  - The optional fourth argument is the number of writer threads.
  - `BICDB_PAGED_WAL_MAX_BYTES` changes the checkpoint trigger.
  - Output is one JSON object: per-commit totals, lock hold, and a row per
    (site, page class, operations) with raw counters.
- **Measure any other process** by setting `BICDB_PAGE_WAL_AMPLIFICATION=1` and
  reading `bicdb_page::wal_amplification::report()`.
