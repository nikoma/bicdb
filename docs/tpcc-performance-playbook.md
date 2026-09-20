# TPC-C Performance Playbook

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

This is the operating procedure for BicDB pgwire and embedded performance work.
It exists so benchmark history, harness discipline, and accept/reject rules do
not depend on one person's shell history or memory.

## Objective

- Primary target: catch up toward PostgreSQL's roughly 600k NOPM reference on the
  same HammerDB-style TPC-C workload.
- Current working milestone: reach at least 300k NOPM without benchmark-specific
  shortcuts.
- Current pgwire high-water: 250,762 DSUM NOPM on the `853d6b5` keeper binary
  with `BICDB_RC_UPDATE_LOCK_ATTEMPTS=4`, VU32, and
  `MAX_ACTIVE_WRITES=24`. Attempts=4 has a 248,931 supporting third run and one
  lower 233,924 repeat; attempts=8 reproduced 246,782 / 239,554; attempts=6
  reached 248,635 and attempts=2 reached 248,621. Attempts=3/5/7 were valid but
  lower; attempts=0 and attempts=1 were also valid but below the high-band
  peaks. MAW20/26/28 plus VU40 were all below the VU32/MAW24 keeper point.
  The prior attempts=32 high-water was 214,182 after repeat, and the prior
  code-only high-water was 195,877 DSUM NOPM with 190,225 companion on the same
  keeper stack and default `BICDB_RC_UPDATE_LOCK_ATTEMPTS=64`.
- Current embedded high-water: 280,593 DSUM NOPM with parsed `Statement::Call`
  execution (`CALL_AST=1`, `THREADS=32`, 180 sec). The durability-matched
  pgwire-path embedded diagnostic with `DURABLE=1` reached 241,278 DSUM NOPM,
  so WAL durability alone does not explain the pgwire gap.
- Latest clean-main baseline: 119,721 DSUM NOPM on `26cc25e`, 2 min test plus
  1 min ramp, VU16, `MAX_ACTIVE_WRITES=16`.

## Principles

- Optimize generic database behavior, not the benchmark.
- TPC-C/HammerDB is a stress harness. It is used to expose bottlenecks in MVCC,
  pgwire scheduling, row/index maintenance, commit, transaction retries, and
  stored-procedure execution.
- Do not keep changes that skip required writes, weaken recovery, special-case
  TPC-C table/procedure names, bypass correctness checks, or silently change SQL
  semantics only to raise NOPM.
- A rejected TPC-C idea can still be a useful design lead for other workloads.
  Record those notes instead of deleting the evidence.
- One experiment changes one main axis. If an idea changes both code and pressure
  settings, first run the code at the previous high-water settings.

## Branch Discipline

- The baseline is the latest intended `main`, not an old work branch.
- Create a branch/worktree from the clean baseline before testing an idea.
- Record the exact base commit and describe every local patch in the ledger row.
- Do not compare a dirty worktree result to historical numbers unless the dirty
  changes are explicitly listed in the row.
- Never overwrite or reuse an old benchmark row after changing harness settings.
  Add a new row.

## Test Host

- Host: `192.0.2.10`
- Remote source checkout: `/home/benchmark/bicdb-head`
- Benchmark harness directory: `/home/benchmark/bicdb-bench`
- Canonical seed: `/home/benchmark/seeds/seed-clean`
- Benchmark binary: `/home/benchmark/bicdb-head/target/release/bicdb`

Do not store the server password in repository files. For ad-hoc SSH use:

```bash
export SSHPASS='...'
sshpass -e ssh -o StrictHostKeyChecking=no benchmark@192.0.2.10 'hostname'
```

## Canonical Pgwire Harness

Use `/home/benchmark/bicdb-bench/dev9_trial.sh` unless a row explicitly says otherwise.
A repository copy is kept at `bench/tpcc/dev9_trial.sh` so harness behavior is
reviewable even if the benchmark host is rebuilt. The current keeper pressure
point is:

```bash
cd /home/benchmark/bicdb-bench
TAG=<descriptive-tag>-$(date +%H%M%S) \
COMMIT="<base commit + patch summary>" \
VU=32 DURATION=2 RAMPUP=1 SEED=/home/benchmark/seeds/seed-clean \
BIN=/home/benchmark/bicdb-head/target/release/bicdb \
MEM_GUARD_MB=8192 MEM_TRACE_SECS=10 \
MAX_ACTIVE_QUERIES=32 MAX_ACTIVE_READS=32 MAX_ACTIVE_WRITES=24 \
BICDB_SYNC_OUTBOX=0 BICDB_PROC_MIX_TRACE=1 \
BICDB_READ_COMMITTED_STATEMENT_SNAPSHOT=1 \
BICDB_RC_UPDATE_LOCK_ATTEMPTS=4 \
BICDB_PLPGSQL_INTO_NO_DATA=1 \
BICDB_INDEX_STORE=sharded BICDB_INDEX_SHARDS=64 BICDB_ORCH_SHARDS=16 \
BICDB_PGWIRE_CALL_FASTPATH=1 BICDB_QUERY_QUEUE_WAIT_TRACE=0 \
./dev9_trial.sh
```

The clean-main baseline sanity run that reproduced ~119k used VU16:

```bash
cd /home/benchmark/bicdb-bench
TAG=<descriptive-tag>-$(date +%H%M%S) \
COMMIT="<base commit + patch summary>" \
VU=16 DURATION=2 RAMPUP=1 SEED=/home/benchmark/seeds/seed-clean \
BIN=/home/benchmark/bicdb-head/target/release/bicdb \
MEM_GUARD_MB=8192 MEM_TRACE_SECS=10 MAX_ACTIVE_WRITES=16 \
BICDB_SYNC_OUTBOX=0 BICDB_PROC_MIX_TRACE=1 \
BICDB_READ_COMMITTED_STATEMENT_SNAPSHOT=1 \
BICDB_INDEX_STORE=sharded BICDB_INDEX_SHARDS=64 BICDB_ORCH_SHARDS=16 \
./dev9_trial.sh
```

When testing admission pressure, change only `VU` and/or `MAX_ACTIVE_WRITES` and
say so in the ledger. Common comparison points:

- Baseline pressure: `VU=16 MAX_ACTIVE_WRITES=16`
- Historical high-water pressure: `VU=24 MAX_ACTIVE_WRITES=24`
- Current high-water pressure: `VU=32 MAX_ACTIVE_WRITES=24`

## Canonical Embedded Harness

Use `crates/bicdb-sql/examples/tpcc_direct_probe.rs` for embedded TPC-C
stored-procedure comparisons. This harness opens the copied seed directly with
`BicDb::open`, shares it through `Arc<BicDb>`, and drives the real HammerDB
stored procedures with `SqlSession::new_shared`.

Do not use `crates/bicdb-sql/examples/tpcc_decay_probe.rs` for keeper/baseline
numbers. That file is a synthetic local index-decay probe; it creates its own
schema and does not use the canonical seed.

Canonical embedded run:

```bash
cd /home/benchmark/bicdb-bench
TAG=embedded-keeper-$(date +%H%M%S)
DD=/dev/shm/bicdb-$TAG
docker rm -f $(docker ps -aq --filter 'name=hdb-') >/dev/null 2>&1 || true
find /dev/shm -maxdepth 1 -type d -name 'bicdb-*' ! -path '/home/benchmark/seeds/seed-clean' -exec rm -rf {} +
df -h /dev/shm | awk 'NR==2{print "SHM_AFTER_CLEAN used="$3" free="$4}'
cp -a /home/benchmark/seeds/seed-clean "$DD"
source /home/benchmark/.cargo/env 2>/dev/null || true
cd /home/benchmark/bicdb-head
SEED="$DD" THREADS=32 SECS=180 PGWIRE_PATH=1 FLOOR=1 RETRIES=8 \
  cargo run --release -p bicdb-sql --example tpcc_direct_probe
rm -rf "$DD"
df -h /dev/shm | awk 'NR==2{print "END shm_used="$3" shm_free="$4}'
```

Avoid `pkill -f '/home/benchmark/bicdb-head/target/release/bicdb server'` inside a long
quoted SSH command: the pattern can match the remote shell command line and kill
the SSH session itself. Use explicit PIDs from `pgrep` if a server is actually
running.

Embedded result validity checks:

- The run starts from `cp -a /home/benchmark/seeds/seed-clean "$DD"` into a unique
  `/dev/shm/bicdb-*` datadir.
- `SHM_AFTER_CLEAN` and final `END` tmpfs lines are recorded.
- The output includes numeric `start_dsum`, `end_dsum`, and `dsum_nopm`.
- `errors=0` in the final `DONE` line.
- `THREADS`, `SECS`, `PGWIRE_PATH`, `FLOOR`, and `RETRIES` are recorded in the
  ledger row.

Embedded mode variants:

- SQL-direct: default `tpcc_direct_probe` mode. It constructs `CALL ...` SQL
  text per operation and executes it through `SqlSession::execute`, so it removes
  pgwire/TCP/Tokio but still pays SQL parse, literal evaluation, and normal
  stored-procedure dispatch.
- Pgwire-path embedded: add `PGWIRE_PATH=1 FLOOR=1 RETRIES=8`. This still has no
  network/protocol layer, but it uses deferred shared commit plus bounded retry
  to mirror most of the pgwire server's engine-side commit path. The older
  `853d6b5` harness row omitted `tx_log.write_durable(commit_seq)` after commit;
  the follow-up `DURABLE=1` diagnostic added that durable write and still reached
  241,278 DSUM NOPM with zero errors. Remaining pgwire gap is therefore more
  likely admission/session/connection-loop/retry-shape overhead than raw WAL
  durability.
- Procedure-direct / AST-direct diagnostic: add `AST_DIRECT=1`. This bypasses
  SQL text and `CALL` AST construction by invoking
  `SqlSession::call_procedure(name, &[SqlValue...])` directly. Treat this as a
  diagnostic unless the typed argument construction is proven byte-equivalent to
  SQL literal evaluation for all five procedures.
- Parsed CALL-AST diagnostic: add `CALL_AST=1`. This builds `Statement::Call`
  values directly and executes them through `SqlSession::execute_statement_ast`,
  preserving normal SQL literal expression evaluation while bypassing SQL text
  parsing. This is the cleaner AST/direct comparison for clients that could send
  structured calls. Keep the `tpcc_direct_probe` Cargo example target buildable;
  this mode is the embedded 280k measuring stick.

## Fresh Database Guarantee

The harness must start every run from a clean copy of the seeded database.
`dev9_trial.sh` does this by:

1. Stopping any existing BicDB server process.
2. Removing any HammerDB containers named `hdb-*`.
3. Removing every `/dev/shm/bicdb-*` entry except the configured seed.
4. Printing `SHM_AFTER_CLEAN`; valid clean runs should show `used=0` or no
   unexpected BicDB trial datadirs.
5. Copying the seed with `cp -a "$SEED" "$DD"` into a unique trial datadir.
6. Starting the server against the copied trial datadir, never against the seed.
7. Measuring `SUM_BEFORE` after the server is ready.
8. Running HammerDB.
9. Measuring `SUM_AFTER`.
10. Stopping the server.
11. Removing the trial datadir and generated Tcl file.
12. Printing final `END ... shm_used=...`.

Freshness checks for every result:

- `SHM_AFTER_CLEAN` is present.
- `START` shows `seed=... dd=...`; the datadir is a copy of the seed.
- `READY` has a numeric `SUM_BEFORE`.
- `SUM_AFTER` is numeric.
- Final `END` is present and tmpfs is cleaned.
- `FAILED_COUNT=0`.

If any of these are missing, the row is invalid or diagnostic only.

## Metric

Primary metric is DSUM NOPM:

```text
DSUM_NOPM = (SUM_AFTER - SUM_BEFORE) / (RAMPUP + DURATION)
```

`SUM_BEFORE` and `SUM_AFTER` come from:

```sql
select sum(d_next_o_id) from district;
```

HammerDB's reported NOPM is useful context, but DSUM NOPM is the acceptance
metric because it measures actual useful district-order progress inside BicDB.

## Accept / Reject Gates

Hard validity gate:

- `FAILED_COUNT=0`
- No `query.failed` server errors.
- No panics, no out-of-space errors, no missing-rowid/index corruption errors.
- Fresh seed copy and tmpfs cleanup checks pass.

Performance gate:

- A normal keeper must beat the latest clean-main baseline by a meaningful margin.
- A new high-water must beat the current valid high-water under comparable
  settings or explain the pressure difference.
- A run above high-water but with failures is not a keeper. It is a lead.
- A correctness fix below high-water can be kept as a correctness fix, but not as
  a TPC-C throughput keeper.

Historical interpretation:

- The old 125k line is a useful sanity line, not the current high-water.
- The current clean-main baseline is around 119k DSUM NOPM.
- The current pgwire high-water is 250,762 DSUM NOPM with
  `BICDB_RC_UPDATE_LOCK_ATTEMPTS=4`; a third run reached 248,931, while one
  exact repeat drew 233,924. Treat attempts=4 as the current fastest valid
  setting, but keep attempts=6 and attempts=8 in the retest set because all three
  are in the same high band and retry churn is high. The default-attempt keeper
  remains 195,877 peak / 190,225 companion.
- The active target remains 300k+.

## Knobs And Retest Inventory

Keep useful generic ideas behind explicit env knobs or small, rebased branches
whenever practical. A TPC-C rejection means "not useful under the current
bottleneck and pressure shape", not "bad forever". Faster parsing, less
telemetry, or lower allocation can increase contention and lose while the
storage/retry choke point is still dominant, then become valuable once that choke
point moves.

Retest promising rejected ideas after each durable high-water move, especially
when embedded mode improves first:

- JSON/native metadata paths: `walknorth-json-rs` native decode is much faster,
  but serde_json `Value` bridging erased the win. Retest after hot metadata can
  stay native or typed.
- Simple `CALL`/AST routes: valuable for clients we control, especially if CQL or
  structured calls remove SQL text before pgwire.
- RLS-off and collection/relation-handle fast paths: likely useful for normal
  application workloads even when TPC-C pacing regresses.
- Telemetry and allocator knobs: keep available for combinations, but do not
  assume lower overhead improves NOPM while conflict storms dominate.
- Lock-wait/retry knobs: `BICDB_RC_UPDATE_LOCK_ATTEMPTS`, notify waits, and
  backoff strategy should be swept again after row-operation retry semantics
  improve.

Fuzzy combination tests are allowed only after each ingredient has a valid
single-axis row. Run the best embedded harness first when possible; if embedded
does not improve, pgwire is unlikely to make the combination a keeper.

## Result Ledger

Record every experiment in `docs/tpcc-performance-ledger.md`.

Minimum row fields:

- Commit/build description.
- DSUM NOPM.
- Duration and ramp.
- Seed.
- VU and active-write settings when not default.
- Any environment flags.
- HammerDB NOPM.
- `FAILED_COUNT`.
- Serialization/deadlock/no-data counts from server metrics.
- `SERVER_STATS` when present. The harness emits `SELECT * FROM
  bicdb_server_stats`; do not project columns because the virtual query handler
  currently recognizes only the exact `SELECT *` shape.
- Decision: `keeper`, `reject`, `diagnostic`, `invalid`, `repeat`, or
  `design lead`.

Do not replace old rows. Add a new row for repeats, even if the repeat disproves
the first result.

## Standard Remote Build Flow

From the local worktree:

```bash
export SSHPASS='...'
sshpass -e rsync -av -e 'ssh -o StrictHostKeyChecking=no' \
  crates/bicdb-core/src/db.rs \
  benchmark@192.0.2.10:/home/benchmark/bicdb-head/crates/bicdb-core/src/db.rs

sshpass -e rsync -av -e 'ssh -o StrictHostKeyChecking=no' \
  crates/bicdb-sql/src/lib.rs \
  benchmark@192.0.2.10:/home/benchmark/bicdb-head/crates/bicdb-sql/src/lib.rs

sshpass -e ssh -o StrictHostKeyChecking=no benchmark@192.0.2.10 \
  'source /home/benchmark/.cargo/env 2>/dev/null || true; cd /home/benchmark/bicdb-head && cargo build --release -p bicdb-cli --bin bicdb'
```

Only sync files touched by the experiment. If the change spans many files, prefer
syncing the branch/worktree intentionally and record that in the ledger.

## Local Verification Before Remote Runs

At minimum:

```bash
cargo check -p bicdb-pgwire
```

Run focused tests that match the touched path. Examples:

```bash
cargo test -p bicdb-sql --test sql plpgsql_exception_handler_rolls_back_block_writes -- --nocapture
cargo test -p bicdb-sql --test sql plpgsql_delete_using_cte_returning_feeds_select_into_generically -- --nocapture
```

If tests are skipped, filtered out, or known-unrelated failures appear, say so in
the ledger or final notes.

## Reading A Result

After a run:

```bash
cd /home/benchmark/bicdb-bench
L=$(ls -t <tag-prefix>*.log | head -1)
sed -n '1,160p' "$L"
```

Important fields:

- `CONFIG`: confirms VU, ramp, duration, admission settings.
- `ENV`: confirms critical env knobs.
- `SHM_AFTER_CLEAN`: confirms tmpfs was empty before copying the seed.
- `START`: confirms seed and copied datadir sizes.
- `SUM_BEFORE`, `SUM_AFTER`, `DSUM_NOPM`.
- `RESULT_LINES`: HammerDB result and VU success/failure.
- `FAILED_COUNT`.
- `SERVER_MIX_TAIL`: query volume and handled routine exceptions.
- `bicdb pgwire messages` in the server log when `BICDB_PGWIRE_MSG_TRACE=1`:
  confirms whether the workload uses simple query (`Q`) or extended protocol
  (`Parse`/`Bind`/`Execute`). HammerDB stored-procedure TPC-C currently uses
  simple query only, so prepared-statement/portal optimizations do not move that
  benchmark unless the client workload changes.
- `SERVER_STATS`: queue and lock counters captured before shutdown. The
  `853d6b5` bounded-prefix keeper stats run showed `CALL` traffic was admitted
  as read/query work (`peak_active_reads=32`, `peak_active_writes=0`) with tiny
  queue wait p99 (~301 ns). A conservative `CALL`-as-write experiment correctly
  moved traffic to `peak_active_writes=24` but dropped throughput to 166,740
  DSUM NOPM, so the remaining gap is not solved by pgwire admission alone.
  Queue-wait percentile sampling itself is now off by default because the global
  sample mutex was measured as hot-path overhead; set
  `BICDB_QUERY_QUEUE_WAIT_TRACE=1` only for diagnostics that need those
  percentiles.
- `SERVER_ERRORS`: panics, query failures, missing rowids, out-of-space issues.
- `END`: confirms tmpfs cleanup after completion.

## Current Investigation Map

Confirmed keepers/leads:

- Persistent blocking pgwire connection loop: removed per-message async/sync
  scheduling churn and produced the biggest pgwire gain.
- READ COMMITTED update recheck: closer to PostgreSQL executor behavior, reduces
  full transaction retry waste.
- PL rollback fix: required correctness for handled routine exceptions.

Current active leads:

- READ COMMITTED delete recheck for `DELETE ... RETURNING` and CTE delivery
  shapes. PostgreSQL rechecks deletes too; BicDB's update-only recheck left a
  delivery gap.
- `UPDATE ... FROM` current-row semantics. A commit-conflict trace on the
  195,877 keeper showed sampled conflicts were mostly `STOCK` rows, not
  `DISTRICT`/`WAREHOUSE`, and every sampled failed write had
  `statement_snapshot=0`. TPC-C NewOrder's stock update is `UPDATE stock ...
  FROM UNNEST(...)`, so the earlier update recheck did not cover the real
  hotspot.
- Empty/no-op index mutation handling. A delete for an already-absent row can
  create an empty mutation; BTree apply must treat that as no-op before requiring
  a rowid.
- PL/pgSQL `INTO` no-data behavior. This can prevent NULL variable leakage in
  HammerDB procedures under high pressure, but it has to be classified as a
  correctness/compatibility decision, not a pure throughput keeper.

Rejected for TPC-C, but keep as design notes:

- Row wait/backoff changes that reduce serialization failures but underfill CPU.
- Procedure/warehouse queueing that removes retry storms but serializes too much
  useful work.
- JSON parser swaps that route back into `serde_json::Value`; native decode can
  be much faster, but the Value bridge erases most of the win.
- Protocol response encoding optimizations; TPC-C procedures return tiny
  responses, so pgwire output is not the main bottleneck.
- Prepared/portal AST execution for HammerDB TPC-C. A protocol-message
  diagnostic on `853d6b5` showed HammerDB sends simple query messages only
  (`simple_q=815709`, `parse=0`, `bind=0`, `execute=0` by 120s), so extended
  protocol optimization cannot affect this benchmark. It remains relevant for
  client workloads that we control and can structure.
- Naive `UPDATE ... FROM` READ COMMITTED recheck. A generic implementation that
  refreshed the target slots, kept the same `FROM` row slots, and re-evaluated
  the predicate reduced some serialization churn but fell to 178,571 DSUM NOPM
  from the 195,877 keeper. The semantics direction is still right; the hot path
  needs a cheaper row-operation retry/recheck than locking and rebuilding every
  candidate row inside the current SQL executor shape.
- Collection/relation handle reductions. Perf showed `RawRwLock::lock_shared_slow`
  and `BicDb::collection_state` as major sampled costs, but narrow lock-count
  reductions reached only 193,809 and 188,591. This remains a relation-cache
  design lead, not a proven TPC-C keeper yet.

## PostgreSQL Comparison Notes

PostgreSQL's relevant advantages for this path:

- One dedicated backend loop per connection: blocking read, execute, write, no
  per-message runtime scheduling bounce. In the local PostgreSQL source, this is
  visible in `src/backend/tcop/postgres.c`: `PostgresMain()` creates
  `MessageContext` once, loops forever, resets that context for each command,
  calls `ReadCommand()`, then processes the command on the same stack.
- PostgreSQL's wire read path in `src/backend/libpq/pqcomm.c` uses a process-owned
  receive buffer (`PqRecvBuffer`) and `pq_getmessage()`/`StringInfo` rather than
  allocating a fresh per-message transport object through a runtime. BicDB tested
  a reusable frontend payload buffer on the current keeper; it reached 189,954
  DSUM NOPM, below the 195,877 keeper, so message payload allocation is not the
  next dominant bottleneck.
- PostgreSQL keeps transient per-command allocations in `MessageContext` and
  drops them with `MemoryContextReset()` at the top of the next loop. BicDB still
  pays Rust allocation/drop costs in SQL/session/record paths, but the TPC-C
  evidence says protocol payload allocation alone is too small; current perf
  points more at repeated collection-state access, `String::clone`, BTree scans,
  and conflict replay.
- PostgreSQL-style activity publication is not on the normal per-statement
  execution path as a global contended map insert. BicDB tested disabling pgwire
  snapshot publication; it tied/trailed the keeper and did not unlock a new core
  curve, so telemetry map churn is not the remaining factor by itself.
- READ COMMITTED row-update/delete behavior waits for the concurrent updater,
  fetches the latest version, re-evaluates the predicate, and only then updates
  or skips.
- For `UPDATE ... FROM`, PostgreSQL's EvalPlanQual recheck happens around the
  affected row operation. BicDB's first generic attempt rebuilt the candidate row
  inside the SQL executor and paid too much per-candidate work; future work
  should avoid full stored-procedure replay without adding a heavy recheck to
  every candidate.
- TPC-C hot rows do not become full stored-procedure retry storms as easily,
  because the executor resolves many conflicts at the row operation boundary.
- BicDB's current `BICDB_RC_UPDATE_LOCK_NOTIFY=1` condition-variable wait test
  reduced handled serialization failures to 70,206 but dropped throughput to
  167,311 DSUM NOPM. The missing piece is not simply "wait longer"; it is
  resolving conflicts with less full procedure replay while keeping enough
  concurrent useful work in flight.

BicDB should copy the generic behavior, not PostgreSQL internals blindly.

## DuckDB Comparison Notes

DuckDB is not a pgwire/TPC-C OLTP target in the same way PostgreSQL is, but the
local `/home/benchmark/duckdb` source still explains why its execution core is much
leaner on object churn:

- Catalog binding resolves tables and columns before execution. Prepared
  statements carry catalog identity/version checks, so the hot executor mostly
  works with resolved table/index objects rather than repeatedly hashing table
  names and reacquiring catalog state by string.
- Physical operators execute vector-at-a-time. That amortizes expression,
  validity, and materialization overhead across chunks instead of rebuilding
  row-shaped state for every candidate row.
- ART index code uses fixed-size allocators and compact pointer wrappers around
  index nodes. That is the opposite direction from repeated string clones,
  `Arc<Record>` fan-out, and per-row map/lock lookups in BicDB's current SQL
  path.

The useful lesson is relation/index handle caching and lower-copy row movement,
not trying to turn BicDB into DuckDB. The failed collection-guard,
`ensure_collection`, and RLS-disabled fetch fast-path experiments show that
removing one superficial call at a time changes timing but does not fix the
larger execution-shape gap.

## Materialization Interpretation

The 2026-07-01 materialization diagnostics make the Postgres/DuckDB tuple gap
concrete:

- A canonical attempts=4 run performed about 75M-80M
  `StoredRecord::to_record()` / raw metadata parses by 180s, roughly 43 raw JSON
  parses per server query in the first counter run.
- The slot-row counter follow-up saw 22.1M `slot_row_from_record*()` calls,
  641M field visits, and 1.28B slot cells by 180s. The TPC-C path is the slot
  row fast path, not the older `row_from_record` map path.
- Full `serde_json::Value` caching (`BICDB_RECORD_METADATA_CACHE=1`) was valid
  but much slower and raised peak RSS to about 34.8 GB. It replaces parse cost
  with object/memory pressure.
- Alias-only slot rows were valid but slower. Duplicate alias/table cells are
  overhead, but not the 300k lever.
- Streaming raw-metadata projection from `StoredRecord`
  (`BICDB_SQL_STORED_SLOT_ROWS=1`) was also valid but slower. It avoids building
  a full `Record` for an indexed SELECT slice, but it still reparses raw JSON
  row-at-a-time and adds visitor/target matching overhead.

The next materialization attempt should store or maintain a resident
schema-known typed/native tuple for SQL tables, then project `SqlValue` cells
without any raw JSON parse in the hot read/update path. Parser swaps, lazy raw
projection, and `Value` caches have all failed because they keep the JSON bridge
in the execution loop.

## What Makes A Result Invalid

- `FAILED_COUNT > 0`.
- Any `query.failed` line.
- Missing `SHM_AFTER_CLEAN`, `START`, `READY`, `SUM_AFTER`, or `END`.
- A run pointed at the seed instead of a copied trial datadir.
- Different duration, VU count, seed, or DSUM calculation compared to the row it
  is being compared against.
- Leftover tmpfs datadirs from previous runs.
- Special-case benchmark shortcuts.

## If The Server Reboots Or History Is Lost

1. Do not trust memory.
2. Start from latest intended `main`.
3. Rebuild `/home/benchmark/bicdb-head/target/release/bicdb`.
4. Run the canonical clean-main baseline.
5. Confirm the harness prints the freshness checks.
6. Compare only against rows in `docs/tpcc-performance-ledger.md`.
7. Resume from the latest valid high-water and current active leads in this file.
