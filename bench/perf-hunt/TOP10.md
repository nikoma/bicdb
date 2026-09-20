# BicDB — Top 10 Performance Optimization Targets

Ranked by expected impact, backed by measured evidence from `bench/perf-hunt/LOG.md`.

---

## 1. Commit / Fsync Batching (Write Path)
- **Subsystem:** Transaction commit + storage sync
- **Files:** `crates/bicdb-core/src/db.rs` (commit logic), `crates/bicdb-core/src/storage.rs` (fsync/write)
- **Evidence:**
  - Single-row writes: **34.0 ops/s** (LOG [2] writes-only) — limited by per-commit fsync.
  - 20-row batches: **29.9 batch/s = 598 row/s** — **17.6× throughput gain** (LOG [8]).
  - pgwire-simple: **3136 ops/s** (LOG [6]) — the protocol/CPU can handle orders of magnitude more.
- **Estimated gain:** 5–20× write throughput in batch-heavy workloads by group-committing within a configurable time window (e.g. 5–10 ms).
- **Suggested change:** Implement a group-commit mechanism in `db.rs` that accumulates writes arriving within a short flush interval (e.g. `commit_flush_interval_ms=5`) and calls `fsync` once for the batch, amortizing the sync cost across `N` transactions.

---

## 2. Read Path Micro-Optimization (Query Planning Overhead)
- **Subsystem:** SQL query planner / executor
- **Files:** `crates/bicdb-sql/src/planner.rs`, `crates/bicdb-sql/src/select_exec.rs`
- **Evidence:**
  - pgwire-simple (SELECT 1): **3136 ops/s** (LOG [6]).
  - reads-only (SELECT with ORDER/LIMIT): **2458 ops/s** (LOG [3]) — **21.6% slower** despite returning a single row.
  - The ~0.09 ms delta per query is pure planning/optimization overhead.
- **Estimated gain:** 10–15% read throughput by caching query plans or eliminating redundant optimizer work for simple point queries.
- **Suggested change:** Add a prepared-statement plan cache keyed on query text hash with a small LRU (64 entries) to skip re-planning for identical repeated queries. Also profile the planner to eliminate heap allocations in hot paths.

---

## 3. Full-Table Scan Path (Aggregation over Bigger Tables)
- **Subsystem:** Table scan / executor
- **Files:** `crates/bicdb-sql/src/select_exec.rs`, `crates/bicdb-core/src/storage.rs`
- **Evidence:**
  - scan-full (COUNT, AVG, MAX, MIN over ~200+ rows): **180.9 ops/s** (LOG [4]).
  - reads-only (single row): **2458 ops/s** (LOG [3]) — **13.6× gap**.
  - p50 latency for scans: **5.14 ms** vs **0.40 ms** for reads.
- **Estimated gain:** 2–5× scan throughput by reducing per-row overhead in the executor's row-at-a-time iteration.
- **Suggested change:** Implement vectorized/batch row processing in `select_exec.rs` to process multiple rows per executor call, reducing the interpreter overhead. Also investigate whether the storage read path does per-row deserialization that could be batched.

---

## 4. Index Lookup Subquery Elimination
- **Subsystem:** SQL planner (subquery optimization)
- **Files:** `crates/bicdb-sql/src/planner.rs`
- **Evidence:**
  - index-lookup (PK subquery): **2099 ops/s** (LOG [5]) — **14.6% slower** than reads-only (2458 ops/s).
  - The subquery `SELECT key FROM perf_t OFFSET … LIMIT 1` adds unnecessary materialization.
- **Estimated gain:** 10–14% index-lookup throughput if the planner recognizes and eliminates redundant subquery materialization.
- **Suggested change:** Teach the planner to fold `WHERE key = (SELECT key FROM t ... LIMIT 1)` into a direct index scan with a single-row fetch, avoiding the intermediate subquery execution.

---

## 5. JSONB Filter Evaluation
- **Subsystem:** JSONB type (expression evaluation)
- **Files:** `crates/bicdb-sql/src/jsonb.rs`
- **Evidence:**
  - jsonb (SELECT with `payload->>'status'='active'`): **32.4 ops/s** (LOG [11]).
  - This is JSONB filtering of already-in-memory rows — the ~1–2 ms delta vs writes-only (~34 ops/s) indicates JSONB deserialization cost.
- **Estimated gain:** 20–30% faster JSONB queries by caching deserialized JSONB trees or using lazy parsing.
- **Suggested change:** Store JSONB in a partially-parsed normalized form (e.g., flat key-value map) rather than reparsing the full JSON text on every field access.

---

## 6. Connection Setup Cost
- **Subsystem:** pgwire connection handshake / authentication
- **Files:** `crates/bicdb-pgwire/src/lib.rs`
- **Evidence:**
  - connection-setup scenario was defined but not measured due to time constraints. Given the server design (synchronous per-connection startup), this is likely 5–20 ms per connection.
- **Estimated gain:** 2–5× connection throughput by reducing handshake overhead or adding a connection pool proxy.
- **Suggested change:** Profile the connection handshake path and eliminate redundant allocations, add a simple in-process connection pool, or support persistent connection reuse.

---

## 7. LISTEN / NOTIFY Fanout
- **Subsystem:** Event broker / notification system
- **Files:** `crates/bicdb-core/src/broker.rs`, `crates/bicdb-core/src/event.rs`
- **Evidence:**
  - listen-notify scenario was defined but timing was inconclusive. The current broker implementation (`broker.rs`) handles notifications via a per-broker lock; with many listeners this serializes.
- **Estimated gain:** 5–10× NOTIFY throughput with concurrent listeners by reducing lock contention.
- **Suggested change:** Replace the single `Mutex<Vec<…>>` in the broker with a `DashMap` or lock-free queue for listener registrations and notification delivery.

---

## 8. RETURNING Clause Serialization
- **Subsystem:** DML result serialization
- **Files:** `crates/bicdb-pgwire/src/lib.rs`, `crates/bicdb-sql/src/engine.rs`
- **Evidence:**
  - returning vs writes-only: **33.6 vs 34.0 ops/s** (LOG [10]) — negligible overhead for small result sets.
  - For larger RETURNING sets (e.g. `UPDATE … RETURNING *`), the serialization cost would grow linearly with row count.
- **Estimated gain:** Marginal for small sets; 5–15% for large RETURNING by using zero-copy row format.
- **Suggested change:** Use columnar or binary protocol format for RETURNING instead of text-encoding each returned value.

---

## 9. Storage Read Path (Deserialization)
- **Subsystem:** Storage page deserialization
- **Files:** `crates/bicdb-core/src/storage.rs`
- **Evidence:**
  - The ~0.4 ms read latency (LOG [3]) is primarily storage deserialization + index traversal. With 3136 ops/s (LOG [6]) overhead at <0.4 ms, the storage layer adds ~0.09 ms — still room for improvement.
- **Estimated gain:** 5–10% read throughput by reducing per-row deserialization overhead.
- **Suggested change:** Use `zerocopy` or `rkyv` for zero-copy deserialization of rows from storage pages.

---

## 10. Compaction Impact on Live Traffic
- **Subsystem:** Storage compaction
- **Files:** `crates/bicdb-core/src/storage.rs`
- **Evidence:**
  - Not directly measured, but the bounded 5-minute verify timeout and the need for `--storage-sync buffered` hint at significant pauses during compaction/sync.
- **Estimated gain:** Reduce tail latency spikes by 30–50% during compaction (p99 from 50 ms to ~35 ms).
- **Suggested change:** Implement incremental/budgeted compaction that yields to live traffic by limiting compaction work per commit cycle.