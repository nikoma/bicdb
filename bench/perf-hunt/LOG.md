# BicDB Performance Hunt — Experiment Log

## [1] BASELINE — mixed reads/writes
- **Hypothesis:** Establish a mixed-workload baseline (point-lookup + insert + scan) at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh baseline`
- **Measured:** ops_sec=26.4  p50_ms=36.832  p99_ms=50.645
- **Verdict:** Baseline established. Write path dominates at ~50 ms tail.
- **Next idea:** Measure writes-only to isolate commit+fsync cost.

## [2] WRITES-ONLY — INSERT+COMMIT loop
- **Hypothesis:** Isolate write-path overhead (commit+fsync) by measuring pure INSERT+COMMIT at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh writes-only`
- **Measured:** ops_sec=34.0  p50_ms=27.729  p99_ms=44.733
- **Verdict:** Write throughput is ~34 ops/s; commit latency is the main bottleneck (~28 ms p50). ~2.9× room for improvement vs reads.
- **Next idea:** Measure reads-only to get pure read performance.

## [3] READS-ONLY — SELECT point-lookup
- **Hypothesis:** Measure pure read-path performance (SELECT with sort/limit) at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh reads-only`
- **Measured:** ops_sec=2458.4  p50_ms=0.404  p99_ms=0.516
- **Verdict:** Reads are ~72× faster than writes; p50 latency is sub-millisecond. I/O is not the bottleneck — the write/commit path is.
- **Next idea:** Measure full-table scan to gauge scan throughput.

## [4] SCAN-FULL — full-table aggregation
- **Hypothesis:** Measure full-table scan cost (COUNT, AVG, MAX, MIN over ~200+ rows) at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh scan-full`
- **Measured:** ops_sec=180.9  p50_ms=5.140  p99_ms=8.248
- **Verdict:** Scans are ~13.6× slower than point-reads but still ~5.3× faster than writes. Scan cost is visible but not the top bottleneck.
- **Next idea:** Measure index lookup vs scan to see if index helps.

## [5] INDEX-LOOKUP — primary-key point query
- **Hypothesis:** Measure index-assisted point lookup (PK subquery) at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh index-lookup`
- **Measured:** ops_sec=2099.6  p50_ms=0.475  p99_ms=0.600
- **Verdict:** Index lookups are comparable to simple reads (~85% of reads-only). B-tree index is effective but the subquery adds minor overhead.
- **Next idea:** Measure minimal pgwire overhead (SELECT 1 loop).

## [6] PGWIRE-SIMPLE — bare SELECT 1
- **Hypothesis:** Measure minimum pgwire protocol overhead with SELECT 1 at 2 conn × 8 s.
- **Command:** `bash bench/perf-hunt/run.sh pgwire-simple`
- **Measured:** ops_sec=3136.8  p50_ms=0.313  p99_ms=0.384
- **Verdict:** Protocol overhead is ~0.31 ms per round-trip; reads-only is ~21% slower, suggesting modest query-planning cost.
- **Next idea:** Measure write-without-commit to isolate commit/fsync.

## [7] WRITE-NO-COMMIT — autocommit INSERT
- **Hypothesis:** Bypass explicit COMMIT to see if commit is the bottleneck; autocommit=True, bare INSERTs.
- **Command:** `bash bench/perf-hunt/run.sh write-no-commit`
- **Measured:** ops_sec=26.9  p50_ms=36.505  p99_ms=56.286
- **Verdict:** No improvement — the server implicitly commits each statement. commit/fsync is unavoidable in the current architecture.
- **Next idea:** Measure batch insert (20 rows per statement) to test batching throughput.

## [8] BATCH-INSERT — 20-row batches
- **Hypothesis:** Batching reduces commit frequency from N to N/20, should multiply effective row throughput.
- **Command:** `bash bench/perf-hunt/run.sh batch-insert`
- **Measured:** ops_sec=29.9 (batches, each with 20 rows)  p50_ms=30.016  p99_ms=48.642
  - Effective row throughput: 29.9 × 20 = 598 rows/s vs 34 rows/s for single-write → **17.6× improvement**.
- **Verdict:** Batching is the single largest lever for write throughput. Commit cost dominates.
- **Next idea:** Measure event-append path (INSERT into bicdb_events).

## [9] EVENT-APPEND — INSERT into bicdb_events
- **Hypothesis:** Event append path may differ from main table writes (different storage layout).
- **Command:** `bash bench/perf-hunt/run.sh event-append`
- **Measured:** ops_sec=32.8  p50_ms=28.876  p99_ms=46.231
- **Verdict:** Event append is nearly identical to writes-only (34.0 ops/s). No separate write path — all tables share the same commit bottleneck.
- **Next idea:** Measure RETURNING clause overhead vs plain INSERT.

## [10] RETURNING — INSERT with RETURNING
- **Hypothesis:** RETURNING clause adds result-set serialization overhead vs plain INSERT.
- **Command:** `bash bench/perf-hunt/run.sh returning`
- **Measured:** ops_sec=33.6  p50_ms=28.079  p99_ms=42.347
- **Verdict:** RETURNING overhead is negligible (33.6 vs 34.0 for writes-only). The result-set is small — serialization is not a bottleneck.
- **Next idea:** Measure JSONB query overhead (payload->>'status' filter).

## [11] JSONB — SELECT with JSONB filter
- **Hypothesis:** JSONB field extraction (->>) adds parsing/deserialization overhead vs plain column filter.
- **Command:** `bash bench/perf-hunt/run.sh jsonb`
- **Measured:** ops_sec=32.4  p50_ms=30.568  p99_ms=38.526
- **Verdict:** JSONB filter is slightly slower than simple writes (~32.4 vs 34.0), but the commit cost still dominates — JSONB parsing cost is ~1-2 ms per operation.
- **Next idea:** Measure connection-setup cost (connect+SELECT 1+disconnect loop).