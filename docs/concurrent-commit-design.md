# Concurrent Commit Engine — Design & Phased Plan

**Status:** Draft / proposal. No code changes yet.
**Goal:** Remove the single exclusive write lock as the commit-throughput ceiling
by letting independent (non-conflicting) transactions commit concurrently.
**Author context:** Written after the serial commit path was heavily optimized
(WAL group-commit, index-maintenance skips, `Arc<Record>` sharing, memcomparable
index keys, WAL-encode-out). Single-VU work is fast; the remaining ceiling is
`semaphore_wait_trap` on the database write lock at higher concurrency.

---

## 1. Goal and non-goals

### Goal
Concurrent transactions that touch **disjoint records** must be able to validate
and apply their commits **in parallel**, instead of serializing on one
`RwLock<BicDb>` write guard. The target workload is TPC-C, where concurrent
transactions hit the *same tables* (warehouse/district/customer/stock/order…)
but mostly *different rows* (different warehouses/districts). Parallelism is
therefore **per record**, not per collection — per-collection locking gives zero
TPC-C benefit and is explicitly out of scope as a solution.

### Non-goals
- Changing the isolation level. We keep **snapshot isolation** with
  first-committer-wins write-write conflict detection (current behaviour).
- Distributed / multi-node commit. Single-process only.
- Reworking the read (execution) path — it already runs concurrently under the
  shared read lock; this project only changes the commit path.
- A general lock-free everything. We use the minimum lock-freedom needed (the
  shared index), and fine-grained locking elsewhere.

### Success criteria
- `randomized_concurrent_stress_preserves_invariants` and the whole concurrency
  + recovery suite stay green, plus new fuzz coverage, under ThreadSanitizer.
- HammerDB TPC-C NOPM scales with VU past the current plateau (measure vu8 →
  vu16 → vu32 → vu64); target: vu32/vu64 materially above vu8 instead of flat.
- Zero new failures in the full test suite; a shadow-index audit reports zero
  divergence during rollout.

---

## 2. Current architecture (what actually serializes)

Key types (in `crates/bicdb-core/src/db.rs`):

- `BicDb` is shared as `RwLock<BicDb>` by the pgwire server. **Execution** takes
  the read guard (concurrent); **commit** takes the write guard (exclusive).
- `BicDb.collections: FxHashMap<String, CollectionState>` and
  `BicDb.indexes: FxHashMap<String, IndexState>` — mutated through `&mut self`.
- `CollectionState` holds `records: FxHashMap<String, RecordEntry>` (current
  rows) and `versions: FxHashMap<String, Vec<VersionedRecord>>` (the MVCC chain),
  plus a vector store and metadata.
- `RecordEntry.record` and `VersionedRecord.record` are `Arc<Record>` (shared,
  immutable once stored).
- `IndexState.map: BTreeMap<Vec<u8>, BTreeSet<String>>` — memcomparable byte keys
  → sets of record ids. **One map per index, shared across all rows of a table.**

The commit path, `BicDb::commit_transaction(&mut self, tx)`:

1. Take the `commit_lock` (a `Mutex<()>`; redundant with the write guard today
   but it serializes commit-sequence assignment).
2. `tx_index_mutations_by_collection` — reads current rows to build
   `IndexRecordMutation { old_record, new_record }` (reads shared state).
3. `detect_commit_conflicts` — for each write, scans the record's version chain;
   if any version was `created_tx` / `deleted_tx` **after** `tx.snapshot_tx`,
   abort with `TransactionConflict` (first-committer-wins).
4. `validate_prepared_tx_index_mutations` — unique-constraint check.
5. Assign `commit_seq` (monotonic).
6. Encode + enqueue WAL frames (durable write is already deferred/group-committed
   via `TxLogHandle`, ordered by `commit_seq`).
7. `apply_committed_tx_writes` — mutate `records`, append to `versions`, update
   the index `BTreeMap`s.
8. Bookkeeping: `tx_states`, `last_committed_tx`, release record write locks.

**The serialization point is the write guard held across steps 2–8.** Steps that
*mutate shared state* are 7 (records/versions/index). Steps 2–6 read shared state
or are CPU-only. The WAL durable write (post-commit) is already concurrent.

### What the conflict model lets us do
Snapshot isolation + version-chain validation means: **two commits conflict iff
they write a record whose version chain the other changed since snapshot.** Two
commits writing *disjoint records* never conflict and can apply independently —
*if* the data structures permit concurrent mutation of disjoint entries. They do
not today (`FxHashMap`, `Vec`, `BTreeMap` are not concurrent), which is the whole
problem.

---

## 3. Target architecture

Replace "one exclusive guard for the whole commit" with:

1. **A short, global, ordered section** (the *commit point*): assign
   `commit_seq`, run write-write conflict validation against the latest
   committed state, and "claim" the write set (per-record commit intents). This
   section is small and serial but does no heavy work.
2. **A fine-grained, concurrent apply**: mutate records/versions per record under
   per-record (or per-shard) locks, and update the indexes through a
   **concurrency-safe index** (the hard part). Disjoint-record commits run in
   parallel here.

Concretely, `BicDb` commit becomes `&self` with interior mutability:

- `collections`/`indexes` maps become **read-mostly** (only DDL mutates the map
  shape; data lives inside). Wrap the map in `RwLock` for DDL, or an
  append-mostly concurrent map; per-`CollectionState` data is interior-mutable.
- `CollectionState.records` / `.versions`: **sharded concurrent maps**
  (`DashMap`-style: an array of `RwLock<FxHashMap<…>>` keyed by `hash(record_id)
  % N_SHARDS`). Disjoint record ids land in different shards → concurrent.
- `IndexState.map`: a **concurrent ordered index** (Section 4) with its own
  internal synchronization (ROWEX) + `crossbeam-epoch` reclamation.
- Commit-sequence + conflict claim: a global `Mutex` (or a sharded set of
  per-record "commit intent" cells) held only for the ordered validate-and-claim,
  not the apply.

The WAL `TxLogHandle` already orders by `commit_seq` and group-commits — it needs
no change; the apply just must enqueue under the same `commit_seq` it validated
with.

### Correctness invariant to preserve
For any record R, the sequence **(validate R against latest) → (append R's new
version)** must be atomic with respect to other commits touching R. With
per-record locking this is: hold R's shard lock (or R's commit-intent lock)
across validate+append for R. Commits touching different records hold different
locks → parallel. Commits touching the same record serialize on R's lock (the
genuine conflict — exactly what we want).

---

## 4. Phase 0 — the index decision (must benchmark before building)

The index is the one structure that is *shared across all rows*, so it is the
true concurrency bottleneck and the riskiest component. The choice is not
obvious for bicdb and **must be decided by benchmarking on our workload**, not by
the general literature.

### Candidates
| Structure | Point lookup | Range / ordered scan | Write contention | Complexity |
|---|---|---|---|---|
| `BTreeMap` + global lock (today) | good | **best** (contiguous) | **serial** | trivial |
| **OLC B+Tree** | good | best | **collapses** (reader restarts) | medium |
| **ROWEX B+Tree** | good | best | good (readers never restart) | **high** |
| **ART (ROWEX)** | **best** | **3–10× slower** | good | high (Rust node layout) |
| **Bw-Tree** | good | good | best | **very high** (delta chains, SMOs, help-along) |

### bicdb-specific constraints
- We have **real range/ordered/prefix scans**: `range_index`,
  `range_index_with_prefix_filters`, `ordered_index_records`, and the prefix path
  in `lookup_index`. ART's iteration penalty is a direct hit to these. This
  argues **against ART** unless point lookups dominate decisively.
- Our keys are already **memcomparable byte strings** (`encode_index_key`) — they
  feed ART *or* a byte-keyed B+Tree equally well. That work is done.
- TPC-C is **write-heavy on hot nodes** (district/warehouse). OLC's reader-restart
  storm under write contention is the failure mode the report calls out → prefer
  **ROWEX** semantics (readers never block/restart; writers exclude writers).

### Recommendation (to be confirmed by benchmark)
Default to a **ROWEX-synchronized B+Tree over `Vec<u8>` keys** (preserves the
range/iteration performance bicdb depends on, with non-blocking reads). Keep ART
as the fallback if a benchmark on our actual read mix shows point lookups
dominate enough to outweigh the iteration regression.

### Phase 0 deliverable
A criterion microbenchmark (`crates/bicdb-core/benches/`) replaying a TPC-C-like
index op mix (point lookups, prefix scans, ordered scans, inserts, updates,
deletes) across thread counts, comparing: today's `BTreeMap`+lock, a ROWEX
B+Tree, and an ART (e.g. evaluate `congee`/`art`-style crates or a vendored
impl). **Do not pick a structure until this exists.** Also evaluate mature crates
before writing our own (a vendored lock-free tree is a large maintenance
liability).

---

## 5. Phase 1 — concurrent storage (records & versions), no index change yet

Make the commit apply concurrent for the **records/versions** while keeping the
index under a per-index lock (so this phase is shippable and measurable before
the hard index work).

1. Shard `CollectionState.records` and `.versions`:
   `records: [RwLock<FxHashMap<String, RecordEntry>>; N]` indexed by
   `fxhash(record_id) % N`. Start `N = 64` (tune later).
2. Change `commit_transaction(&mut self)` → `commit_transaction(&self)`, taking
   the **read** guard of `RwLock<BicDb>` in pgwire (so commits no longer
   exclude each other at the top level). The map *shape* (`collections`) is
   guarded separately for DDL.
3. Commit point (short global `Mutex`): assign `commit_seq`, run
   `detect_commit_conflicts` against the latest versions, and atomically record
   the write set as committed (so a concurrent committer sees it). Release.
4. Apply per record under that record's **shard write lock**, appending the new
   version and updating `records`. The **index** update for this phase stays
   under a single `Mutex` per `IndexState` (still serial, but smaller than the
   whole-commit lock).
5. WAL enqueue under the same `commit_seq` (the `TxLogHandle` queue is already
   gap-free / ordered).

**Why this is correct:** the commit-point `Mutex` makes conflict-validation +
write-set-claim atomic, so first-committer-wins still holds; per-record shard
locks make the version append for a record exclusive vs. other committers of the
same record. Index updates remain serialized (a known, temporary bottleneck
removed in Phase 3).

**Expected result:** partial scaling — records/versions apply in parallel, but
the per-index `Mutex` still serializes index maintenance. Measure to quantify how
much of the commit is index vs. record work (we already know index maintenance is
significant). This de-risks the protocol before the hardest component.

---

## 6. Phase 2 — the per-record commit protocol, hardened

Refine Phase 1's commit point into a proper protocol and remove the global
`Mutex` from the hot path where possible:

- Replace the single commit-point `Mutex` with **per-record commit-intent locks**
  (a sharded lock map keyed by `(collection, record_id)` — bicdb already has a
  `RecordLockMap` for in-flight write locks; reuse/extend it). A committer:
  1. Acquires intent locks for all its records (in a canonical order →
     deadlock-free).
  2. Validates each record's version chain vs. `snapshot_tx`.
  3. Allocates `commit_seq` (a single atomic increment — the only truly global
     step; cheap).
  4. Appends versions + updates records (per shard) + enqueues WAL.
  5. Releases intent locks.
- `commit_seq` ordering vs. WAL: the `TxLogHandle` requires gap-free contiguous
  sequences. Two options: (a) keep `commit_seq` allocation + WAL enqueue inside a
  tiny global section, or (b) let the WAL writer tolerate out-of-order arrival
  with a reorder buffer (it already drains contiguous prefixes). Prefer (a) for
  simplicity first; it is a single atomic + a `Vec` push, not heavy work.

This is the genuine parallelism: two `neword`s for different warehouses lock
different records, validate and apply concurrently, and only contend on truly
shared rows (e.g. the same district — which *should* serialize).

---

## 7. Phase 3 — the concurrent index (the hard part)

Swap each `IndexState`'s per-index `Mutex<BTreeMap>` for the structure chosen in
Phase 0, with:

- **ROWEX synchronization**: readers (lookups, scans) traverse without locks and
  never restart; writers take pessimistic locks only against other writers. This
  matches our write-heavy hot-node pattern without OLC's restart storms.
- **`crossbeam-epoch` for SMR**: never hand-roll reclamation. Nodes unlinked
  during splits/merges are `defer_destroy`-ed; readers pin an epoch guard for the
  duration of a traversal. This is the single highest-corruption-risk area; using
  the vetted crate is non-negotiable.
- **Atomic, ordered node mutations**: per the ROWEX rules — append-then-publish
  (write child pointer, then atomically bump count/index), lazy delete (null the
  slot, reclaim on consolidation), copy-on-grow with an atomic parent-pointer
  swap.

The index must still support everything `IndexState` exposes today: exact lookup,
prefix range, ordered scan (asc/desc), extreme/min-max with filters, and the
verification/`flatten_btree_index_entries` paths. Keep the memcomparable byte-key
encoding (no change) and the `decode_index_field` helpers for scans.

---

## 8. Phase 4 — validation & incremental rollout (do this *throughout*, not last)

The report is unambiguous: silent data corruption is the failure mode, and a
heavy validation pipeline is mandatory. For bicdb:

1. **Stress/fuzz harness** — *done* (`randomized_concurrent_stress_preserves_
   invariants`). Expand it each phase: more threads, skewed key distributions,
   delete/range-scan mixes, randomized aborts, and an interleaved
   reopen-and-recheck.
2. **ThreadSanitizer** — add a nightly CI job running the concurrency suite under
   `-Zsanitizer=thread`. This is the primary detector for the data races a
   concurrent index/storage introduces. (Local runs are on stable today; this
   needs a nightly toolchain.)
3. **Shadow index during rollout** — run the new concurrent index *alongside* the
   existing `BTreeMap` for one or more indexes behind a flag; on every read,
   compare results and assert equality; on a mismatch, log + fail loudly. Ship to
   shadow-only first, promote per index.
4. **Background consistency checker** — extend the existing `verify_index` /
   `diff_index_state` machinery into a periodic auditor that traverses the live
   concurrent index and asserts: key order, that every record id in the index
   resolves to a live record, and that every indexed field of every record is
   present in the index. Run it in the stress test between rounds.
5. **Sanitizers in CI**: ASAN/UBSAN on the unit + integration suites (the
   `unsafe` in any vendored node layout must be ASAN/Miri-clean; prefer
   `cargo miri` on the index unit tests).

---

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Silent data corruption from a concurrency bug | Shadow index + background checker + TSan + Miri on index unit tests; promote per-index, never big-bang. |
| Use-after-free in index reclamation | `crossbeam-epoch`; no hand-rolled SMR. |
| Range/ordered scans regress (ART) | Phase 0 benchmark gates the structure choice; default to ROWEX B+Tree which preserves iteration. |
| Deadlock in per-record locking | Canonical lock ordering (sort write set by `(collection, id)`); reuse the existing `RecordLockMap` discipline. |
| WAL ordering breaks under concurrent commit | Keep `commit_seq` allocation + enqueue in a tiny global section first; only later teach the writer to reorder. |
| Effort vs. payoff | Phase 1 (concurrent records/versions, index still locked) is shippable + measurable on its own — bail or pause there if the index work proves too costly for the remaining gain. |

---

## 10. Milestones (each independently shippable + measured)

- **M0 — Index benchmark** (Phase 0): criterion bench comparing BTreeMap+lock vs
  ROWEX B+Tree vs ART on a TPC-C-like op mix; pick the structure. *Gate.*
- **M1 — Sharded records/versions** (Phase 1): `commit_transaction(&self)` with
  sharded record/version maps + commit-point `Mutex`; index still per-index
  `Mutex`. A/B HammerDB vu8/16/32; expand stress harness; TSan CI.
- **M2 — Per-record commit protocol** (Phase 2): replace the commit-point Mutex
  with per-record intent locks; re-measure scaling; shadow nothing yet.
- **M3 — Concurrent index, shadow only** (Phase 3+4): new index behind a flag,
  run alongside BTreeMap with equality assertions in stress + a sample of prod
  reads; no promotion.
- **M4 — Promote index + background checker** (Phase 3+4): per-index promotion to
  the concurrent index as the source of truth; background consistency auditor;
  final HammerDB scaling numbers across VU.

**Stop conditions:** if M1 shows index maintenance is the overwhelming residual
cost (likely), M3/M4 are justified; if records/versions dominate (unlikely given
profiles), we may stop at M2. Every milestone keeps the full suite + stress
harness + recovery green, measured back-to-back to cancel host drift.
