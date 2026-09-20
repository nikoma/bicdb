# Design: Concurrent Write Transactions

Status: **Draft / proposal** — written 2026-06-24 after the HammerDB optimization pass.
Author: performance investigation (M1–M4 + profiling).

## 1. Problem and evidence

BicDB serializes **all** write transactions behind one process-wide lock:

```rust
// crates/bicdb-pgwire/src/lib.rs:348
db: RwLock<BicDb>,
```

Every write statement takes the exclusive write side for its whole duration
(parse + execute + commit + WAL write) via `write_db_with_admission()`
(`lib.rs:852`). Reads share the read side. TPC-C is ~92% writes, so writes
fully serialize.

Measured on HammerDB TPC-C (1 warehouse, stored procs, 1-min, fresh DB each run),
BicDB on current `main`:

| VUs | BicDB TPM | Postgres 18.4 TPM |
|-----|-----------|-------------------|
| 1   | ~28–33k (noisy, latency-bound) | ~42–57k |
| 2   | **~54k** | ~240k |
| 4   | **~54.5k** | — |

**vu2 ≈ vu4** is the key result: throughput does not increase past 2 connections.
The global write lock is a hard serial ceiling at **~54.5k TPM** (≈900 commits/s,
≈1.1 ms per serial write transaction). Postgres scales vu1→vu2 by 5.8× because it
commits many writers concurrently; BicDB cannot.

No amount of per-transaction micro-optimization breaks this ceiling — it only
raises it. The M1–M4 squeeze pass (below) lifted the single-VU path and trimmed
per-commit work, but the wall is structural.

### Squeeze pass already landed (raises the serial ceiling, does not remove it)

- **M1** `3edadab` — stop walking the whole data dir on every write
  (`ensure_server_writable` no longer calls `total_dir_size`). vu1 ~28k→~33k.
- **M2** `22929e7` — `RoutineFrame` syncs only changed variables. vu2 ~52→~54k.
- **M4** `2fd0a23` — move (not clone) updated records into the write path.

Remaining per-commit costs (from `samply`/`sample` under load): SQL parse of the
inline-literal `CALL` (~18% of the locked section), the commit `write()` +
per-commit `open()`/`fstat` of `transactions.log`, and pervasive
allocation/clone/SipHash churn in the value representation. Each is ≤ ~10% and now
below vu2 benchmark noise.

## 2. What already exists (the good news)

BicDB is **not** a from-scratch MVCC project. The core already has a
snapshot-isolation foundation; the global lock is bolted on top of it.

- **Per-row version chains** — `crates/bicdb-core/src/db.rs`:
  `state.versions: HashMap<String, Vec<VersionedRecord>>`, each version carrying
  `created_tx` / `deleted_tx`. `apply_versioned_upsert` / `apply_versioned_delete`
  append new versions instead of mutating in place.
- **Snapshot visibility** — `visible_version(versions, tx_id)` (`db.rs:~7327`)
  already filters by `created_tx <= snapshot < deleted_tx`.
- **Snapshot-based conflict detection** — `detect_commit_conflicts` (`db.rs:5095`)
  already rejects a commit if any touched record gained a version after the
  transaction's `snapshot_tx`. This is the heart of optimistic concurrency.
- **Per-record lock map, currently unused** — `write_locks: HashMap<(String,
  String), TransactionId>` with `lock_tx_record` / `release_write_locks`
  (`db.rs:5227`). Scaffolding for row-level locking already present.
- **Write admission queue** — `try_admit_write` (`lib.rs:823`) is independent of
  the RwLock and already bounds in-flight writers.

So the data model is ~60–70% of the way to concurrent writers. The blockers are
the **single lock** and a handful of **globally-mutated structures** touched on
every commit.

## 3. What forces serialization today

Mutated on every commit under the global write lock (`commit_transaction`,
`db.rs:5040`):

| State | Scope | Concurrency need |
|-------|-------|------------------|
| `next_tx_id`, `last_committed_tx` | global counters | atomics (CAS) — easy |
| `tx_states` | global map | small dedicated lock |
| `collections[c].records` / `.versions` | per **row** within per **collection** | per-row visibility already exists; needs per-collection guard |
| `indexes` (esp. UNIQUE) | per collection | concurrent uniqueness check — hard |
| `graphs` (projections) | cross-collection | `refresh_graph_projections` on every commit — must defer |
| WAL `transactions.log` | single append stream | needs a commit sequence number |

## 4. Why per-collection locks are NOT enough for TPC-C

The obvious first step — replace one DB lock with one lock per collection — does
**not** help this workload. Every `neword` writes ORDERS, NEW_ORDER, ORDER_LINE,
DISTRICT, and STOCK; every `payment` writes WAREHOUSE, DISTRICT, CUSTOMER,
HISTORY. Concurrent transactions touch the **same collections**, so
collection-grained locks still serialize them.

The concurrency in TPC-C is at the **row** level: different transactions touch
different warehouses / districts / customers / items. Postgres scales because it
locks rows, not tables. To get TPC-C scaling, BicDB must detect conflicts and
commit at **row granularity** — which is exactly what the existing version chains
+ `detect_commit_conflicts` already do, *once the global lock is removed*.

(Per-collection locks remain worthwhile for multi-tenant / multi-table workloads
where transactions naturally partition by collection — just not for TPC-C.)

## 5. Proposed phasing

### Phase A — shrink the critical section (continuation of M1–M4, low risk)
Move work out of the locked window so the serial ceiling rises while we build the
real fix. Highest-value items, in order:
1. **Parse outside the lock.** Parse the SQL / `CALL` to an AST before acquiring
   `write_db`; execute the pre-parsed statement under the lock. ~18% of the locked
   section is parsing today. Self-contained to `execute_server_db_sql_for_state`.
2. **Keep the `transactions.log` handle open.** Avoid `open()`+`fstat()`+`close()`
   per commit; cache the append handle and invalidate it on
   `truncate_log_file`/checkpoint. Removes the per-commit `__open` (~6% of locked
   section). *Risk: must invalidate on truncation/compaction — see §6.*
3. Faster hashing on the hot routine/catalog maps; trim residual value clones.

Expected: pushes the serial ceiling from ~54k toward ~70–90k. Does **not** reach
Postgres. Strictly incremental and safe; ship as individual milestones.

### Phase B — row-level concurrent commits (the real unlock)
Remove the global write lock and serialize only the parts that must be.

1. **Replace `RwLock<BicDb>`** with:
   - atomic `next_tx_id` / `last_committed_tx`;
   - a short **commit critical section** (a dedicated `Mutex`) that only: assigns a
     commit sequence number, runs `detect_commit_conflicts` against current
     versions, validates unique-index deltas, and appends the WAL frame;
   - everything else (executing the transaction body, building writes) runs
     without the global lock against a consistent snapshot.
2. **WAL ordering** — add a monotonic `commit_seq` to `TxFrame::Commit`; recovery
   orders by it. The commit `Mutex` makes seq assignment + append atomic, so the
   log stays totally ordered even with many concurrent executors.
3. **Unique indexes** — the hard correctness point. Two concurrent commits writing
   the same unique key must not both succeed. Options: (a) validate unique-key
   deltas inside the commit `Mutex` against current index state (simple, correct,
   serializes only unique-key commits); (b) per-key locks for finer granularity
   later.
4. **Graph projections** — stop calling `refresh_graph_projections()` inside every
   commit; recompute lazily at read against a snapshot, or on a background worker.
5. **Use the existing `write_locks`** for optional pessimistic row locking
   (`SELECT … FOR UPDATE`).

This keeps the durable-commit step serialized (microseconds: conflict check +
unique check + WAL append) while the expensive transaction *execution* runs
concurrently. That is the model that lets row-disjoint TPC-C transactions commit
in parallel.

### Phase C — optimize the commit critical section
Group-commit the WAL (batch fsync across concurrent committers), MVCC garbage
collection of old versions (chains currently grow unbounded), per-key unique
locks. Only needed if Phase B's commit mutex becomes the new ceiling.

## 6. Top correctness risks (must be designed before coding)

1. **WAL ordering & recovery** — concurrent appends must replay deterministically.
   Commit-seq + atomic (assign-seq, append) under the commit mutex. Recovery
   (`recover_transaction_log`, `db.rs:7112`) already tolerates per-tx framing;
   add seq sorting.
2. **Unique-index races** — the canonical concurrency bug. Must be validated
   inside the serialized commit step until per-key locking exists.
3. **Checkpoint/compaction vs cached fd / live writers** —
   `ensure_no_pending_transactions` (`db.rs:5656`) already requires quiescence;
   a cached WAL handle (Phase A.2) must be dropped/reopened across
   `truncate_log_file`.
4. **Version chain growth** — no GC today; concurrent writers accelerate growth.
   Needs a min-active-snapshot watermark and pruning (Phase C).
5. **Snapshot consistency for reads** — reads must continue to see a stable
   snapshot while writers commit; `visible_version` already provides this if the
   snapshot `tx_id` is captured atomically.

## 7. Recommendation

- **Now (safe, incremental):** Phase A — land parse-outside-lock and the WAL
  handle cache as individual verified milestones. Realistic target ~70–90k TPM.
- **Next (the actual parity work):** Phase B — row-level concurrent commits built
  on the existing version chains and `detect_commit_conflicts`. This is the only
  path to Postgres-class vu2+ throughput. It is a multi-week effort whose risk is
  concentrated in WAL ordering and unique-index correctness; both are designable
  with the serialized commit-step approach above. Requires explicit go-ahead and
  a dedicated test plan (crash/recovery, concurrent unique inserts, conflict
  retries) before implementation.

Full parity (~240k) is achievable in principle because the MVCC substrate already
exists — but it is an architectural change to the commit path, not a micro-opt.
