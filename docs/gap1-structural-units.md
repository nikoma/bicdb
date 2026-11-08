# GAP-1, the full fix: structural units (design, for approval)

Status: **proposal**. Nothing here is implemented. It changes what recovery
reconstructs (section 6), so it needs the owner's approval before any code.
It builds on the no-steal interim (`docs/recovery-semantics.md` 14.5) and
uses the window names of 14.1 (W1-W7).

## 1. The goal

After the interim, three ways remain for a page to reach `store.pages` while
its content is described by no record that recovery will apply:

- **W1-L**: a decided commit (a row commit's page apply, never refused)
  larger than the buffer pool, and a single operation that dirties more than
  a shard's reserve, steal as a last resort;
- **W4**: Finalize and the full checkpoint, under the writer gate, write the
  unlogged pages of a page transaction that does not hold the gate across
  its operations (index and full-text builds, direct `PagedStore` users);
- **W6**: a stolen image is tagged with whichever transaction touched the
  page last.

The full fix makes one invariant hold on every path:

> **I-S.** A page is written to `store.pages` only when its current content
> is described by a durable record that recovery applies: a committed
> transaction's image, or a structural image (xid 0).

With I-S, nothing recovery skips is ever on disk, so "skipped" and "undone"
coincide again. The interim's refusal (`TransactionExceedsBufferPool`) also
becomes unnecessary.

## 2. Why logging each split on its own is not enough

Option (b) as sketched in 14.3 logs the pages a split rewrites (left, right,
parent, meta) as xid-0 images. That is unsafe in this store, for a reason
14.3 did not list:

- `put` on an existing key rewrites that key's index entry **in place** to
  point at the new, uncommitted head version (`put_returning_locator`).
- A leaf's xid-0 image therefore carries in-flight locators. If the heap page
  holding the new head is not durable (it is unlogged and, under no-steal,
  never written), the replayed leaf points to a slot that does not exist, or
  to a page beyond the file. The key reads as absent: **a committed row is
  lost**, the very failure GAP-1 describes.
- New keys have the same problem with newly allocated heap pages (reads fail
  with `ChecksumMismatch` or "beyond the end of the file").

A structural image is only safe if everything it references is recoverable
too. Per-structure closure rules (leaf → heap pages → catalog → overflow
chains → free-space map → meta page) would have to be derived and audited
for every structure, and a missed edge is a hole.

## 3. The proposal: structural cuts at operation boundaries

A **structural cut** logs every page with unlogged changes, pool-wide, as
xid-0 images, then syncs the log, then marks those pages logged. It is taken
only at an **operation boundary**: under the writer gate and the structure
lock, with no `put`/`delete`/vacuum step in progress.

At an operation boundary the in-memory pages form a consistent structure
(every tree, heap, catalog and overflow chain is complete), and every
uncommitted change in them is MVCC-tagged: a version whose xid has no commit
record, and index entries whose head is such a version with a chain to the
last committed version below it. This is exactly the state a full checkpoint
already writes when a transaction is in flight, which recovery already
handles (`recovery_semantics.rs::a_fully_drained_uncommitted_transaction_keeps_committed_keys_and_stays_invisible`).
A cut writes it to the **log** instead of the page file, so a crash at any
point recovers either the previous cut or this one, never a mixture.

Because the cut contains every unlogged page, the closure is automatic:
whatever a cut page references was either logged by this cut, logged
earlier (committed or by an earlier cut), or unchanged since the page file
last received it. No structure needs its own rule. Splits and merges of
B-tree leaves and interior nodes, heap page allocation, the free-space map
and catalog, overflow chains, the meta page with its roots, and the status
spill are all covered by being pages.

### 3.1 Where cuts are taken

| Trigger | Replaces | Closes |
|---|---|---|
| Admission of a write operation finds a shard under pressure (today's `admit_write`) | the refusal, for every transaction including decided commits | W1-L |
| Start of Finalize's writeback and of the full checkpoint's flush (both hold the gate) | writing unlogged pages under the in-flight xid | W4 |
| `abort` (the interim already logs the aborted pages, as a committed unit) | could become a cut | W5 |
| Before the root-split flush, or drop that flush entirely | `flush_logged` | (W3, already closed) |

After a cut, the cut pages are logged and so may be evicted and written like
any committed page. Eviction then never needs an unlogged victim except
inside one operation (3.2), so `unlogged_steals` stays zero and W6 cannot
arise: no image is ever written under an in-flight xid.

### 3.2 Inside one operation

A cut cannot be taken mid-operation. The admission reserve (an eighth of each
shard) covers almost every operation. For the rest (a value whose overflow
chain is larger than the reserve, a deep cascade of splits), the proposal is
that the pool **admits a frame above its budget** instead of stealing, for
the rest of that operation only, counted (`overcommit_frames`) and returned
at the next cut or commit. Memory overshoot is bounded by one operation's
footprint. The alternative, failing the operation mid-way, would leave a
torn in-memory structure that the next commit logs: worse than either.

A value larger than a fixed bound (proposal: a quarter of the pool) is
refused before the operation starts, as today's `ValueTooLarge` already does
for the page-level limit.

## 4. Log format

**No new record kind.** Cuts are `PageImage` records with xid 0, the kind the
status spill, vacuum and free-page reclaim already write
(`log_dirty_pages(0)`), and recovery already applies them unconditionally
(`wal.rs` pass 2, rule 3). Older engines read the log.

One addition is needed, and it is the point that needs a decision:

- **The xid floor.** Cut images name xid 0, so a cut can carry versions of
  live transactions that no record names. Recovery sets the next xid from
  the highest xid any record names, so without more, a crashed
  transaction's xid could be handed out again, and the new holder's commit
  would make its versions visible. The interim already has the mechanism
  (`PagedStore::log_xid_floor_above`, added after review found the same
  problem in commit logging): when a live writer is above the logged xid,
  the log also gets the commit of a fresh, empty transaction. A cut uses the
  same call. Existing record kind, no format change. (An earlier draft of
  this document proposed an aborted placeholder instead; each such abort
  would occupy one of the store's bounded abort-exception slots until the
  watermark passed it, which a cut per checkpoint could exhaust.)

  Separately, the meta page's own next-xid watermark has to be checked to
  cover in-flight xids at a full checkpoint; that question exists today for
  W4 and is part of the audit below.

## 5. Recovery

**No change to the recovery code.** What changes is what it reconstructs:

- after a crash, the page state may contain uncommitted transactions'
  versions and index entries (MVCC-invisible), where today it contains them
  only after a full checkpoint that ran during the transaction;
- a decided commit whose page apply was cut and then crashed is re-applied by
  the core's logical replay on top of those versions. `write_base` already
  chains past an aborted or crash-orphaned head (semantics 6.1), so this is
  the existing re-put path;
- crash-orphaned xids are already converted into durable abort exceptions by
  the open-time freeze. A cut can leave more of them in pages; their count is
  bounded by the transactions in flight at a crash, as now.

## 6. Interactions

- **#67's horizon rule** (the redo point is the page file's horizon `H`):
  cut images are appended after the last published `H`, so they are replayed
  unless a later checkpoint publishes a larger `H`, which it does only after
  writing those pages. A cut taken at the start of a full checkpoint is below
  the `H` that checkpoint publishes and is covered by it. Truncation rules
  are unchanged: cuts add no commits to freeze.
- **Deferred commits (experimental, transaction-log mode)**: the barrier
  syncs deferred commit LSNs before writing a logged page. A cut syncs
  itself before marking its pages logged, so no extra rule is needed; a
  deferred commit's own pages are still covered as today.
- **Page deltas (experimental)**: a cut image is a full image and therefore
  a delta base boundary. The delta branch's assumption "the page file only
  receives committed states" (14.2) no longer holds; it receives committed
  states or cut states. A committed delta whose base is a cut image is safe
  because the base is replayed unconditionally; an uncommitted delta after a
  cut is skipped as now. The delta branch needs one audit for this.
- **Online backup and archive retention**: cuts are ordinary log records in
  the chain a backup copies and an archive keeps.
- **Interleaved page transactions (W4/W6 sources)**: a cut covers every
  transaction's pages at once. The commit path, which today logs every
  unlogged page under the committing xid, keeps doing so; that is a cut that
  is replayed iff that transaction committed, which is consistent at that
  boundary because the commit holds the gate.

## 7. Cost

- Log volume: a page is logged once per cut it is dirty in, plus once at
  commit if changed again. A transaction smaller than the pool takes no cut,
  so ordinary row commits are unchanged. A transaction larger than the pool
  logs roughly its footprint once more than today's commit-only logging,
  which matches what today's steals already log (one image and one fsync
  per stolen page); cuts batch that into one fsync per cut.
- A checkpoint during a page-only build logs the build's in-flight pages
  once.
- Memory: bounded overshoot inside one operation (3.2).

## 8. Plan, after approval

1. Cut primitive, using the interim's xid floor (section 4). Tests: crash at every record of a
   cut; reissued-xid regression; cut during each checkpoint phase.
2. Admission takes a cut instead of refusing; remove the refusal and
   `begin_redo_backed`'s exemption. Un-ignore
   `gap1_a_decided_commit_larger_than_the_pool_keeps_acknowledged_rows`.
3. Finalize and the full checkpoint cut first. Un-ignore
   `an_open_transaction_interleaved_with_commits_reads_absent_after_a_crash`
   and add W4 crash cases for index and full-text builds.
4. In-operation overcommit (3.2) with a steal count that must stay zero.
5. Audits: meta-page next-xid watermark, page-delta branch, every
   `log_dirty_pages(0)` caller.

Each step is its own PR with fault injection, ledger reconciliation and the
crash matrices, as for the interim.

## 9. Decisions requested

1. Structural cuts (this document) instead of per-split structural images
   (14.3), because of section 2.
2. The xid floor via the interim's empty committed transaction (section 4)
   instead of a new record kind.
3. Recovery may then reconstruct invisible in-flight versions after any
   crash (5), not only after a checkpoint that ran during the transaction.
4. In-operation overcommit (3.2) instead of a mid-operation steal or error.
