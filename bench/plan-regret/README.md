# Plan-regret harness (Step 0 of the tiny-optimizer study)

This measures how good BicDB's single-table access-path choices are before
any learned model is considered. It measures; it does not train anything.

## What it measures

For every generated query, `plan-regret`:

1. records the candidates and choice of **the chooser that actually executed**,
   and the cost-based planner's candidates, estimates and choice;
2. forces every executed candidate, plus a full scan. It reshuffles the
   candidate order on every repetition, under two cache conditions: *warm*,
   and *flushed* (a 256 MiB buffer is written before each run to evict CPU
   caches; the data stays in RAM);
3. records latency, the rows each access path fetched, and the result size;
4. verifies each forced plan returns exactly the full-scan result, using an
   order-insensitive multiset hash;
5. runs `ANALYZE`, then records the planner's estimates and choice again,
   measuring any newly chosen candidate.

`analyze.py` scores these policies against the same measured latencies:

| policy | meaning |
|---|---|
| `rule` | the executed chooser's own pick (BicDB today) |
| `heuristic` | cost-based planner without statistics, mapped onto executed candidates |
| `fitted` | per-index selectivity constants fit on a train split, in the planner's no-stats cost formulas |
| `calibrated` | fitted constants plus per-plan-kind latency coefficients fit on the train split |
| `analyze` | cost-based planner after a fresh `ANALYZE` |
| `oracle` | fastest measured legal candidate |

Reported: p50/p95 latency, total workload time, workload regret against the
oracle, per-query regret, choices more than 2× and more than 10× slower than
the oracle, cardinality q-error (the planner's per-index and output estimates
against observed rows), planning overhead, and measurement noise.

## Two choosers

Ordinary SELECTs execute through the row-set path, which picks its index with
a rule-based locator: the most matched index columns wins, ties are broken by
running the tied probes, and it falls back to a full scan only when no index
matches. The cost-based `plan_query` drives EXPLAIN and a few streaming
paths. The harness traces and forces whichever chooser ran, so EXPLAIN and
execution can be compared directly.

## Running

```sh
CARGO_PROFILE_RELEASE_LTO=false cargo build --release -p bicdb-bench --bin plan-regret
target/release/plan-regret --out out/ --data-root /dev/shm \
  --datasets narrow:100000:16:64,wide:100000:1200:2400 --queries 300 --reps 5
python3 bench/plan-regret/analyze.py out/measurements.jsonl   # writes report.md, summary.json
```

Run it on a dedicated benchmark host, one job at a time.

## Safety and privacy

Tracing and forcing live in `bicdb_sql::plan_diagnostics`. They are inert
unless an embedding Rust caller installs a scope on its own thread. They
cannot be reached from SQL, GUCs, pgwire, or environment variables. The trace
types hold only plan kinds, index identifiers, prefix lengths and row counts;
they cannot hold literal values, parameters, keys or row contents. Forcing
only selects among candidates the engine already generated, and every path
re-applies the full WHERE clause. The harness checks this on every forced run.

## Storage modes

`--storage memory` (default) measures memory-resident storage; the second condition flushes CPU
caches. `--storage paged` measures server-paged storage: the dataset is reopened after loading, so
rows come from pages, and the second condition is **cold**. Each cold run closes the database,
drops its files (only) from the OS page cache with `dd iflag=nocache count=0`, and reopens it.
`--data-root` must be on a real disk for this, not tmpfs. Each statement runs in a fresh session
that carries the catalog cache, as the pgwire server does.
