# BicDB Performance Campaign

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

This document defines the repeatable gate for performance work. A change is
measured in isolation, against the same parent build and immutable seed. Winning
changes are kept. Regressions, invalid trials, and statistically inconclusive
ideas are recorded in `docs/performance-campaign-ledger.md`; rejected decisions
and their curated evidence are never deleted. Bulky runtime logs, HammerDB state,
and profiling scratch data are archived outside Git when needed.

## Three lanes

| Lane | Purpose | Effective configuration |
| --- | --- | --- |
| `default-durable` | Out-of-box product behavior | Durable storage sync, default BTree index store, default admission limits, update-lock attempts 64, sync outbox on, 4 GB auto-checkpoint. |
| `tuned-durable` | Best proven production configuration | Durable storage sync; sharded indexes 64; orchestration shards 16; update-lock attempts 2; sync outbox off; active query/read/write limits 32/32/24; 4 GB auto-checkpoint. |
| `cpu-ceiling` | Diagnostic upper bound | Tuned settings, buffered storage sync, sync outbox off, auto-checkpoint off. It is never eligible as a production keeper. |

`--storage-sync durable` is the default. `--storage-sync buffered` disables
configured database fsyncs; it exists only for the explicitly non-durable
CPU-ceiling lane.

On tmpfs, the durable lane executes the durable code path but cannot measure
power-loss persistence or real-device sync latency. Use a dedicated NVMe
filesystem for durability claims. Use tmpfs only to compare with the historical
`benchmark-primary` CPU/lock campaign, and label the storage medium in the report.

## Trial order

Run one discarded warmup per lane, followed by all six permutations:

```text
D T C    T C D    C D T    D C T    C T D    T D C
```

`D`, `T`, and `C` mean default durable, tuned durable, and CPU ceiling. This
balances position and predecessor effects. The reportable workload is 32 VUs,
one minute ramp-up, two measured minutes. `PERMUTATIONS=3` is permitted only as
a screening run and must be labeled as such. Throughput trials run with
HammerDB time profiling disabled. The matrix then runs one separately marked
latency profile and one allocation profile per lane; neither contributes to
throughput statistics.

## Metrics and artifacts

Every `reports/performance/<campaign>/trials/<trial>/result.json` contains:

- source commit, source-state hash including untracked source inputs, binary
  hash, Rust toolchain, host, storage,
  container image, seed manifest, effective environment, and server arguments;
- district-sum useful commits, HammerDB counters, procedure mix, failed VUs,
  server query/write counts, handled serialization/deadlock errors;
- separately collected HammerDB transaction latency profiles and server queue
  percentiles;
- sampled CPU time, mean cores, RSS/HWM, threads, faults, process I/O, host memory,
  and filesystem headroom;
- cumulative WAL bytes, write calls, sync-equivalent calls, grouping, current WAL
  size, checkpoint phase times, and checkpoint failures;
- initial open time and graceful post-run reopen time.

Allocation uprobes materially slow an allocation-heavy server. The matrix
therefore runs one separately marked allocation profile per lane. Allocation
counts/bytes are evidence for allocation deltas but their throughput is excluded
from campaign statistics. These profiles default to four VUs for one minute;
override `ALLOCATION_VU` and `ALLOCATION_DURATION` when needed. `perf record`
and syscall tracing are also diagnostic trials, never throughput trials.

The matrix verifies the declared seed content hash before creating a campaign.
Graceful reopen is not crash recovery; record a separate SIGKILL/reopen trial
when process-crash recovery time is required.

Use `null` for unavailable metrics. Do not emit a numerical zero for a metric
that was not collected.

## Validity and decisions

A trial is invalid when any of the following occurs:

- district sums are missing/non-numeric, HammerDB exits nonzero, any VU fails,
  the exact worker-plus-monitor count does not complete, or the run does not
  complete;
- the server reports failed queries, the harness reports an error, requested
  perf counters are absent, or graceful-reopen state differs from post-run state;
- panic, corruption, ENOSPC, memory-guard, or checkpoint errors appear;
- the committed visibility watermark ends more than one sequence behind;
- the seed, binary, toolchain, lane configuration, or storage medium differs
  within a comparison block.

A performance change is a **keeper** when correctness gates pass and its target
durable lane improves by at least 2% with a positive paired Student-t 95%
confidence interval, without more than 5% peak-RSS/WAL growth or more than 10%
p99 latency regression. Every arm must also be internally homogeneous for
binary, source, seed, host, workload, and server configuration. A correctness
fix is mandatory even when performance-neutral, but its performance cost must
still be reported.

A change is a **reject** when it fails correctness, produces a reproducible
durable-lane regression, or exceeds a resource budget. A result whose confidence
interval crosses zero is **inconclusive** and receives more repetitions; it is not
silently promoted. The CPU-ceiling lane helps attribute cost but cannot rescue a
durable-lane loser.

Generate a paired decision report with:

```bash
python3 bench/tpcc/summarize.py compare \
  reports/performance/<baseline> reports/performance/<candidate> tuned-durable
```

## Commands

```bash
cargo build --release --bin bicdb

SEED=/dev/shm/bicdb-tpcc-seed WAREHOUSES=16 BUILD_VU=8 \
  bench/tpcc/build_seed.sh

CAMPAIGN_ID=$(date -u +%Y%m%dT%H%M%SZ)-head-baseline \
SEED=/dev/shm/bicdb-tpcc-seed \
DATA_ROOT=/srv/bicdb-bench \
  bench/tpcc/run_matrix.sh
```

For a screening run:

```bash
PERMUTATIONS=3 RUN_LATENCY_PROFILES=0 RUN_ALLOCATION_PROFILES=0 \
PERF_STAT=0 DURATION=1 \
CAMPAIGN_ID=$(date -u +%Y%m%dT%H%M%SZ)-screen \
  bench/tpcc/run_matrix.sh
```

Never run two matrices on the same port or data root. The harness only terminates
its own server PID and named HammerDB container; it does not use global `pkill`.

## Improvement loop

1. Start from the recorded baseline parent and make one isolated change.
2. Run focused correctness tests and the deterministic regression test.
3. Build one release binary and record its hash.
4. Run the same matrix with the same seed, host, filesystem, and toolchain.
5. Compare paired blocks, latency, CPU, RSS, WAL, checkpoints, and graceful
   reopen behavior with `summarize.py compare`.
6. Append a ledger row and link its report before keeping or reverting the idea.
7. The next candidate starts from the last keeper, never from an undocumented
   stack of experiments.

The canonical historical host is `benchmark-primary` (`192.0.2.10`). If it is unavailable,
local results are useful for harness validation and screening only and must not be
compared numerically with the historical `benchmark-primary` ledger.
