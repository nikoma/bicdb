# BicDB Server Concurrency

This report captures pgwire concurrency measurements before and after replacing
the unbounded thread-per-connection listener path. BicDB now uses a hybrid
runtime: plain TCP connections are accepted and kept idle on a bounded Tokio
runtime, while complete frontend messages are handed to a bounded blocking pool
for existing protocol and SQL execution code. TLS connections keep the existing
blocking stream path after startup. Read-only SQL executes through a shared
`RwLock<BicDb>` read guard. Eligible buffered DML and ordinary buffered commits
also use shared guards and can overlap. DDL, non-transactional mutations, and
fallback paths can require exclusive access. Bounded write admission does not
itself serialize committers. See [current write concurrency](../TRANSACTIONS.md#write-concurrency).

The measurements below were captured on June 20, 2026. They describe those
builds and workloads, not a capacity guarantee for the current engine.

## Benchmark Surface

`bicdb bench server` now supports:

- `--clients <N>`: total pgwire client connections to open.
- `--active-query-concurrency <N>`: number of pooled clients that actively
  issue queries. Defaults to `--clients`.
- `--queries <N>`: total workload queries across active clients.
- `--scenario <name>`: `idle-pooled`, `read-only`, `mixed`, `long-scan`,
  `cancel-contention`, or `churn`.
- `--json-out`, `--csv-out`, and `--markdown-out` for exported results.

`bicdb bench server-cert` runs the certification gate across the required
scenario set. `--profile ci` is the reduced CI-safe profile, while
`--profile full` defaults to 1000 pooled clients, 32 active query workers, and
1000 workload queries per non-idle scenario, and a 30-second idle pooled soak.
It fails with a non-zero exit code if any budget is missed.

Scenarios:

- `idle-pooled`: opens pooled connections and holds them briefly without
  workload queries.
- `read-only`: active clients issue `SELECT COUNT(*) FROM patients`.
- `mixed`: active clients issue 80% `SELECT COUNT(*)` and 20% `INSERT`.
- `long-scan`: active clients issue a larger ordered read against the seeded
  table.
- `cancel-contention`: one active client runs a cancellable long query while
  the remaining active clients issue short reads; the benchmark sends a real
  PostgreSQL `CancelRequest` and verifies recovery on the long-query
  connection.
- `churn`: active workers repeatedly connect, issue one query, and disconnect.

The benchmark starts a real local pgwire listener, seeds a small `patients`
table, then runs the selected workload. Server counters include setup DDL/seed
queries; workload query counts are reported separately as `select_queries` and
`insert_queries`.

## Instrumentation

The pgwire server now tracks:

- peak active connections and rejected connections;
- configured max connections and configured max active queries;
- configured max queued queries, queued query peak, and queue wait p50/p95/p99;
- active, peak active, queued, and rejected query executions;
- active/peak active reads and writes plus queued/peak queued reads and writes;
- query failures, cancellation counts, timeout counts, and last-cancel
  metadata;
- shared DB-lock acquisitions;
- total and max DB-lock wait time;
- total and max DB-lock hold time;
- read-lock acquisitions plus total/max read-lock wait and hold time;
- write-lock acquisitions plus total/max write-lock wait and hold time.
- configured max queued writes;
- current and peak write queue depth;
- total/max write wait time and total/max write execution time;
- rejected and timed-out write counts.
- streamed row and byte counts for cursor fetches and `COPY TO STDOUT`;
- active cursor count, cursor memory estimate, and spill-byte counter.

Large large transactional reports and exports should use the cursor and `COPY TO STDOUT`
patterns in [Large Result Streaming](large-results.md). Simple query protocol
results remain capped by `max_result_rows`; batched extended-query fetches and
`COPY TO STDOUT` can stream past that cap while exposing the counters above.

The benchmark report includes connection setup p50/p95/p99, query p50/p95/p99,
throughput, peak active connections, rejected connections, query queue
depth/wait/read/write metrics, write queue metrics, RSS from
`/proc/self/status` when available, and process thread count from
`/proc/self/status` when available. JSON and CSV outputs include the same
query queue and overload counters.

RSS and thread count are sampled while the pool is active. In benchmark runs the
server and benchmark driver live in the same process, so thread count includes
server runtime, blocking-pool, benchmark driver, and client threads. Query
execution is separately admitted by `max_active_queries`, `max_active_reads`,
and `max_active_writes`, so idle pooled clients remain admitted connections but
do not consume active query slots. When all active slots are busy, work waits in
bounded read/write queues up to `overload_timeout`; full queues return SQLSTATE
`53300`, and overload timeouts return SQLSTATE `57014`.

Write execution is separately admitted by `max_queued_writes`, defaulting to
four times available parallelism with a floor of 16. Once that queue is full,
new write, DDL, COPY, sequence, and transaction commit requests return
`too many queued writes; max_queued_writes is N`. An admitted operation that
needs exclusive database access waits for that lock until `write_timeout` elapses, then returns
`write timed out waiting for database writer after Nms`. Ordinary shared-path
commits do not acquire that exclusive lock. Admission bounds outstanding work;
it is not evidence that only one writer can execute or commit.

Recommended settings:

| Profile | Starting point |
| --- | --- |
| Local dev | 50 connections, 4 active queries/reads, 2 active writes, 16 queued queries/reads/writes, 30s overload timeout. |
| LAN deployment | 200 connections, active queries/reads near CPU parallelism, active writes at half to full CPU parallelism, queued limits around 4x active limits, 15s overload timeout. |
| 1000 pooled connections | 1000 connections, active queries/reads near CPU parallelism, active writes near half CPU parallelism, queued limits around 4x active limits, 5s overload timeout. |

## Local Environment

Captured on June 20, 2026:

- OS: Linux `6.8.0-110-generic` x86_64.
- File descriptor limit: `ulimit -n` = `1048576`.
- Build profile: Cargo dev profile (`cargo run -p bicdb-cli ...`).
- Server bind address: local ephemeral `127.0.0.1:<port>`.

The host limit was sufficient for 1000 local sockets. No separate `ulimit`
raise or sysctl change was required.

## Certification Budgets

The full certification profile uses explicit pass/fail budgets for the local
certification environment:

| Budget | Full profile threshold |
| --- | ---: |
| Clients | 1000 pooled connections |
| Active query concurrency | 32 workers |
| Max RSS | 256 MiB |
| Max process threads | 160 |
| Min connection success rate for pooled scenarios | 100% |
| Max rejected connections | 0 |
| Max unexpected failed queries | 0 |
| Max timed-out queries | 0 |
| Read-only throughput | at least 100 q/s |
| Read-only p95 / p99 | <= 250 ms / <= 500 ms |
| Mixed read/write throughput | at least 25 q/s |
| Mixed read/write p95 / p99 | <= 2500 ms / <= 5000 ms |
| Cancellation short-read p99 | <= 500 ms |
| Churn p99 | <= 1500 ms |

The reduced CI profile runs the same five scenarios at small connection and
query counts with looser latency/throughput thresholds. It is intended to catch
regressions in scenario wiring, counters, cancellation recovery, and final data
correctness without requiring 1000 local sockets on every CI worker.

## Final Certification Run

Captured on June 20, 2026 with:

```bash
cargo run -p bicdb-cli --features bench -- bench server-cert \
  --profile full \
  --queries 1000 \
  --json-out target/server-cert-full.json \
  --csv-out target/server-cert-full.csv \
  --markdown-out target/server-cert-full.md
```

Result: passed. The run exercised a 30-second idle pooled soak, read-heavy
traffic, mixed read/write traffic with final row-count verification, long-query
cancellation with recovery, and repeated connection churn.

| Scenario | Clients | Active query concurrency | Queries | Throughput q/s | Query p50 / p95 / p99 ms | Setup p95 ms | Peak active | Rejected | Failed / canceled / timed out | RSS | Threads | Final patients |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| idle-pooled | 1000 | 32 | 0 | 0.00 | 0.000 / 0.000 / 0.000 | 0.214 | 1000 | 0 | 0 / 0 / 0 | 46.85 MiB | 31 | 200 |
| read-only | 1000 | 32 | 1000 | 624.25 | 42.028 / 44.003 / 45.553 | 0.202 | 1000 | 0 | 0 / 0 / 0 | 57.41 MiB | 82 | 200 |
| mixed | 1000 | 32 | 1000 | 200.32 | 47.004 / 542.992 / 1345.015 | 0.221 | 1000 | 0 | 0 / 0 / 0 | 66.95 MiB | 89 | 400 |
| cancel-contention | 1000 | 32 | 1000 | 554.02 | 42.012 / 43.989 / 44.031 | 0.219 | 1000 | 0 | 1 / 1 / 0 | 87.94 MiB | 41 | 200 |
| churn | 1000 | 32 | 1000 | 195.89 | 49.823 / 571.717 / 1370.741 | 0.392 | 36 | 0 | 0 / 0 / 0 | 95.69 MiB | 88 | 400 |

The single failed/canceled query in `cancel-contention` is the expected
cancelled long query. The canceled connection recovered with `SELECT 1`, and
the unrelated short reads completed with p99 44.031 ms. The mixed and churn
scenarios both ended with the expected `patients` row count, proving inserts
were applied exactly once while concurrent reads continued.

## Before Results

These measurements are the pre-hybrid-runtime baseline that motivated bounded
connection and active-query admission.

| Scenario | Clients | Active query concurrency | Queries | Throughput q/s | Query p50 / p95 / p99 ms | Setup p50 ms | Peak active | Rejected | Failed / canceled / timed out | DB lock wait avg / max ms | DB lock hold avg / max ms | Server memory estimate | RSS | Threads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mixed | 100 | 100 | 10000 | 80.81 | 1268.015 / 1860.984 / 1985.999 | 25.160 | 100 | 0 | 0 / 0 / 0 | 1126.299 / 2915.490 | 12.236 / 56.209 | 7.09 MiB | 50.09 MiB | 205 |
| read-only | 500 | 32 | 1000 | 69.84 | 52.996 / 59.009 / 99.613 | 25.139 | 500 | 0 | 0 / 0 / 0 | 10.601 / 91.806 | 4.579 / 66.468 | 31.32 MiB | 63.68 MiB | 537 |
| idle-pooled | 1000 | 32 | 0 | 0.00 | 0.000 / 0.000 / 0.000 | 25.128 | 1000 | 0 | 0 / 0 / 0 | 0.000467 / 0.001383 | 18.698 / 59.906 | 62.57 MiB | 82.09 MiB | 1005 |

Commands used:

```bash
cargo run -p bicdb-cli --features bench -- bench server \
  --clients 100 \
  --queries 10000 \
  --json-out target/server-mixed-100.json \
  --csv-out target/server-mixed-100.csv \
  --markdown-out target/server-mixed-100.md

cargo run -p bicdb-cli --features bench -- bench server \
  --clients 500 \
  --active-query-concurrency 32 \
  --queries 1000 \
  --scenario read-only \
  --json-out target/server-readonly-500.json \
  --csv-out target/server-readonly-500.csv \
  --markdown-out target/server-readonly-500.md

cargo run -p bicdb-cli --features bench -- bench server \
  --clients 1000 \
  --active-query-concurrency 32 \
  --queries 0 \
  --scenario idle-pooled \
  --json-out target/server-idle-1000.json \
  --csv-out target/server-idle-1000.csv \
  --markdown-out target/server-idle-1000.md
```

## After Results

The idle pooled target was rerun after the hybrid runtime change:

| Scenario | Clients | Active query concurrency | Queries | Setup p50 ms | Peak active | Rejected | Failed / canceled / timed out | Server memory estimate | RSS | Threads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| idle-pooled | 1000 | 32 | 0 | 0.198 | 1000 | 0 | 0 / 0 / 0 | 62.57 MiB | 46.31 MiB | 31 |

Command used:

```bash
cargo run -p bicdb-cli --features bench -- bench server \
  --clients 1000 \
  --active-query-concurrency 32 \
  --queries 0 \
  --scenario idle-pooled \
  --json-out target/server-idle-1000-async.json
```

The matching pre-change idle pooled run used the same 1000 client shape and
reported 82.09 MiB RSS with 1005 process threads. The post-change run reported
46.31 MiB RSS with 31 process threads while still reaching 1000 peak active
connections and rejecting none.

The read-only workload was rerun on June 20, 2026 after replacing the server
SQL database mutex with read/write lock execution. These runs used 32 active
query workers against 100, 500, and 1000 pooled pgwire clients. The setup DDL
and seed inserts still use the write path, so server counters include 201 setup
writes; the workload itself is 1000 `SELECT COUNT(*) FROM patients` queries.

| Scenario | Clients | Active query concurrency | Queries | Throughput q/s | Query p50 / p95 / p99 ms | Setup p50 ms | Peak active | Rejected | Failed / canceled / timed out | DB lock wait avg / max ms | DB lock hold avg / max ms | Server memory estimate | RSS | Threads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| read-only | 100 | 32 | 1000 | 704.12 | 42.883 / 44.004 / 46.008 | 0.187 | 100 | 0 | 0 / 0 / 0 | 0.000652 / 0.003797 | 4.948952 / 53.860328 | 6.32 MiB | 52.16 MiB | 84 |
| read-only | 500 | 32 | 1000 | 650.04 | 42.025 / 43.999 / 46.473 | 0.205 | 500 | 0 | 0 / 0 / 0 | 0.000631 / 0.003627 | 4.574210 / 59.282200 | 31.32 MiB | 54.37 MiB | 80 |
| read-only | 1000 | 32 | 1000 | 611.18 | 42.986 / 44.016 / 45.617 | 0.195 | 1000 | 0 | 0 / 0 / 0 | 0.000605 / 0.004368 | 4.715377 / 59.368720 | 62.57 MiB | 57.07 MiB | 84 |

Commands used:

```bash
cargo run -p bicdb-cli --features bench -- bench server \
  --clients 100 \
  --active-query-concurrency 32 \
  --queries 1000 \
  --scenario read-only \
  --json-out target/server-readonly-100-rwlock.json \
  --csv-out target/server-readonly-100-rwlock.csv \
  --markdown-out target/server-readonly-100-rwlock.md

cargo run -p bicdb-cli --features bench -- bench server \
  --clients 500 \
  --active-query-concurrency 32 \
  --queries 1000 \
  --scenario read-only \
  --json-out target/server-readonly-500-rwlock-seq.json \
  --csv-out target/server-readonly-500-rwlock-seq.csv \
  --markdown-out target/server-readonly-500-rwlock-seq.md

cargo run -p bicdb-cli --features bench -- bench server \
  --clients 1000 \
  --active-query-concurrency 32 \
  --queries 1000 \
  --scenario read-only \
  --json-out target/server-readonly-1000-rwlock-seq.json \
  --csv-out target/server-readonly-1000-rwlock-seq.csv \
  --markdown-out target/server-readonly-1000-rwlock-seq.md
```

The directly comparable pre-change 500-client read-only run reported p95/p99
of 59.009/99.613 ms with average/max DB-lock wait of 10.601/91.806 ms. The
post-change 500-client run reported p95/p99 of 43.999/46.473 ms with
average/max DB-lock wait of 0.000631/0.003627 ms. The 100- and 1000-client
read-only runs above document the new target shape; the earlier report did not
include matching read-only 100- or 1000-client baselines.

The issue-46 cancellation-contention benchmark was added to exercise a long
cancellable query, concurrent short reads on other active clients, a real
PostgreSQL `CancelRequest`, and recovery on the canceled connection. The
pre-change server only checked query timeout after execution for many storage
paths, so a long execution path could keep its active slot or lock until
completion or timeout. The post-change run below uses cooperative cancellation
checks and reports the bounded short-read latency while the long query is
active.

| Scenario | Clients | Active query concurrency | Short read queries | Throughput q/s | Short read p50 / p95 / p99 ms | Peak active | Rejected | Failed / canceled / timed out | DB lock wait avg / max ms | DB lock hold avg / max ms | RSS | Threads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cancel-contention | 32 | 8 | 64 | 102.17 | 42.085 / 43.145 / 43.903 | 32 | 0 | 1 / 1 / 0 | 0.000971 / 0.018745 | 14.785389 / 79.177502 | 42.36 MiB | 31 |

Command:

```bash
cargo run -p bicdb-cli --features bench -- bench server \
  --clients 32 \
  --active-query-concurrency 8 \
  --queries 64 \
  --scenario cancel-contention \
  --json-out target/server-cancel-contention-32.json \
  --markdown-out target/server-cancel-contention-32.md
```

The single failed query in this scenario is the expected canceled long query;
the 64 unrelated short reads completed with p99 below 44 ms and the canceled
connection recovered with `SELECT 1`.

## Bottlenecks Proven

The old 100-client mixed run proved SQL work was serialized. Average DB-lock
wait was 1126.299 ms while average DB-lock hold was 12.236 ms. Tail latency
followed that queueing: p99 query latency reached 1985.999 ms with no query
failures, cancels, or timeouts. The read-only runs above show that read
execution no longer waits on a global mutex; average DB-lock wait stayed below
0.001 ms at 100, 500, and 1000 pooled clients.

The issue-45 write-admission run added explicit write queue metrics to the same
mixed read/write benchmark surface:

| Run | Clients | Active query concurrency | Queries | Query mix | Throughput q/s | Query p50 / p95 / p99 ms | DB lock wait avg / max ms | DB lock hold avg / max ms | Max queued writes | Peak write queue depth | Write wait avg / max ms | Write execution avg / max ms | Write rejected / timed out |
| --- | ---: | ---: | ---: | --- | ---: | --- | --- | --- | ---: | ---: | --- | --- | --- |
| Before historical mixed | 100 | 100 | 1000 | mixed | not recorded | not recorded / not recorded / 1985.999 | 1126.299 / not recorded | 12.236 / not recorded | not instrumented | not instrumented | not instrumented | not instrumented | not instrumented |
| After bounded write queue | 100 | 32 | 1000 | 776 SELECT / 224 INSERT | 217.94 | 45.010 / 502.982 / 1190.998 | 61.558160 / 1549.498281 | 8.316798 / 54.066816 | 96 | 24 | 163.798652 / 1549.499152 | 19.177729 / 54.066155 | 0 / 0 |

Command:

```bash
cargo run -p bicdb-cli --features bench -- bench server \
  --clients 100 \
  --active-query-concurrency 32 \
  --queries 1000 \
  --scenario mixed \
  --json-out target/server-mixed-100-write-queue.json \
  --csv-out target/server-mixed-100-write-queue.csv \
  --markdown-out target/server-mixed-100-write-queue.md
```

The after run used the Cargo dev profile on the local benchmark host. The mixed
workload reported 104 failed setup/workload statements from the existing
duplicate-key insert pattern; the new admission counters reported zero write
rejections and zero write timeouts under normal load.

The before 1000 idle pooled run proved the old server could accept 1000 local
pgwire connections on this host, but did so with thread-per-connection growth:
1005 process threads for 1000 active pooled connections. The after run proves
idle pooled connections no longer imply one OS thread each.

The memory estimate is currently a connection-count estimate
(`db_size + peak_connections * 64 KiB`) and does not include thread stack
reservation. RSS is therefore the practical process-level memory number in this
report.

## Known Limitations

- This is a local single-process benchmark, not a distributed or multi-host
  benchmark.
- The benchmark uses BicDB's current pgwire simple-query path; it does not
  claim PostgreSQL-equivalent scalability.
- Shared execution support is operation-dependent. DDL, non-transactional
  mutation, and fallback paths can require the exclusive database guard;
  ordinary buffered writes and commits are not universally exclusive.
- RSS and thread counts are Linux `/proc` measurements and are omitted on
  platforms without `/proc/self/status`.
- Query cancellation and deadline timeouts are cooperative. Pgwire passes a
  cancellation/deadline token through SQL execution, scan filtering, row joins,
  grouping/projection, vector ordering, ANN search, COPY finalization, and
  other long loops where practical. Sort calls are checked immediately before
  and after the sort because Rust comparators cannot safely abort mid-sort.
  Most benchmark scenarios do not issue cancellation requests.
- Setup table creation and seeding are included in server-level counters, but
  not in workload query totals.
