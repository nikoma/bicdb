# Performance Campaign Handoff: 2026-07-09

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

This is the authoritative handoff for the performance campaign work stopped on
2026-07-09 at approximately 15:08 PDT. Read this file before running another
benchmark. It records the pre-publication repository state, benchmark
provenance, completed and aborted evidence, confirmed WAL defect, harness
limitations, and the exact order for resuming on `benchmark-primary`. The campaign changes
and curated evidence are published with this document in its containing commit.

No performance improvement has been measured in this campaign. There are no new
keeper or reject decisions. The only completed three-lane result is a local
harness smoke test, not a baseline.

## Non-negotiable conclusions

1. The 41k-45k local measurements do not show a BicDB regression. They were
   VU2, time-profiled runs on `development.example.com`, not the canonical VU32 runs on
   `benchmark-primary`.
2. The historical valid pgwire high-water is 419,228 DSUM NOPM on `benchmark-primary` at
   commit `e0d0aab`, with three results of 419,228, 417,780, and 418,094
   (mean 418,367).
3. The canonical 4 GB checkpointer-on reference is 398,685, 388,924, 394,093,
   and 391,040 DSUM NOPM (mean 393,186) on the `7420ad3` keeper.
4. The current source HEAD descends from `e0d0aab`, but it must be rebaselined.
   Neither historical number can substitute for a current-HEAD baseline.
5. The WAL acknowledgement race described below is confirmed and is not fixed.
   The campaign changes captured here add instrumentation and a diagnostic
   non-durable mode only.
6. Do not begin improvement candidates until the current HEAD has a valid
   baseline on `benchmark-primary` and the remaining limitations in this document have been
   handled.

## Pre-publication repository state

- Workspace: `/home/benchmark/bicdb`
- Branch: `mobile-deps`, tracking `origin/mobile-deps`, zero ahead/behind at the
  time of handoff
- HEAD: `06aadd4f822145fca5481e3db609a2c1f27121f8`
- HEAD subject: `Mobile: make ort load-dynamic and drop tokenizers C++ deps`
- `origin/main`: `ee9eaef`
- No staged changes
- Seven tracked files modified, seven new harness/document files, plus new
  benchmark reports
- Tracked diff size before this handoff file: 220 insertions, 28 deletions
- `git diff --check`: passed

This section describes the working tree when the evidence was captured, before
publication. Checking out `06aadd4` alone does not reproduce the binary or
harness. Resume from the commit containing this handoff (or the branch into
which it was merged); trial metadata intentionally retains the original dirty
source provenance.

Release binary used for all local evidence:

```text
/home/benchmark/bicdb/target/release/bicdb
SHA256 154ad7dbd2593234bec3fdf37ad95785d138a299b941dc9788d8f778ca52a937
built  2026-07-09 13:21:31 PDT
rustc  1.96.0 (ac68faa20 2026-05-25), LLVM 22.1.2
```

The tracked dirty-diff hash recorded in trial metadata is:

```text
edfe79002e5fa776421257c0e9b3a6dbbcfdced86261e9b929c156db3370c2e1
```

That historical trial hash is incomplete because the capture-time harness used
`git diff`, which excluded untracked files. The published harness replaces it
with a source-state hash that includes untracked source inputs while excluding
generated campaign reports.

### Modified tracked files

- `bench/tpcc/README.md`: documents the new seed and matrix entry points.
- `bench/tpcc/run-vu.tcl.tmpl`: parameterizes port, VUs, ramp, duration, and
  HammerDB time profiling.
- `crates/bicdb-cli/src/main.rs`: adds `--storage-sync durable|buffered` to both
  pgwire serve aliases. The default is durable.
- `crates/bicdb-core/src/db.rs`: adds `TxLogStats`, WAL counters, configurable
  sync behavior, and two focused tests.
- `crates/bicdb-core/src/storage.rs`: selects `O_DSYNC` or ordinary append from
  `DbConfig.fsync`.
- `crates/bicdb-pgwire/src/lib.rs`: adds `PgWireConfig.fsync`, procedure/error/WAL
  fields to `bicdb_server_stats`, and checkpoint phase timing logs.
- `docs/benchmarking-tpcc.md`: directs new decisions to the three-lane campaign.

### New files before this handoff

- `bench/tpcc/build_seed.sh`
- `bench/tpcc/buildschema.tcl.tmpl`
- `bench/tpcc/run_matrix.sh`
- `bench/tpcc/run_trial.sh`
- `bench/tpcc/summarize.py`
- `bench/tpcc/tests/test_summarize.py`
- `docs/performance-campaign.md`
- `docs/performance-campaign-ledger.md`
- `reports/performance/`

This handoff adds `docs/performance-campaign-handoff-2026-07-09.md`.

## Machines and access

### Local machine used accidentally for screening

```text
host:       development.example.com
CPU:        AMD Ryzen 9 7900X, 12 cores / 24 threads
RAM:        62 GiB
/dev/shm:   32 GiB tmpfs
kernel:     Linux 6.8.0-111-generic x86_64
workspace:  /home/benchmark/bicdb
test port:  55433
```

At handoff, there is no matrix, trial, HammerDB, `perf`, or temporary pgwire
server process. Port 55433 is free and `/dev/shm` contains only the immutable
campaign seed.

An unrelated pre-existing BicDB server was intentionally left untouched. Do not
use a global `pkill bicdb` on `development-host`.

### Canonical benchmark machine

```text
name:       benchmark-primary
address:    192.0.2.10
CPU:        24 logical CPUs
RAM:        approximately 93 GiB historically
storage:    /dev/shm for historical CPU/lock comparisons
source:     /home/benchmark/bicdb-head
harness:    /home/benchmark/bicdb-bench
old seed:   /home/benchmark/seeds/seed-clean
```

The exact command reaches SSH but fails authentication from the current
environment:

```text
ssh -l root 192.0.2.10
benchmark@192.0.2.10: Permission denied (publickey,password).
```

No working SSH credential was available in the capture environment. The next
session must run in a shell that has an authorized key or password. Verify
access from the actual shell used by the resumed agent; access from the user's
desktop shell does not automatically make a credential available inside another
execution environment.

Do not store a password in this repository. The existing playbook permits an
ephemeral environment variable:

```bash
export SSHPASS='...'
sshpass -e ssh -o StrictHostKeyChecking=no benchmark@192.0.2.10 hostname
```

## Historical comparison points

The authoritative history is `docs/tpcc-performance-ledger.md`, especially the
rows around lines 429-445 and 474-483. The summary near the top of that file is
newer than parts of `docs/tpcc-performance-playbook.md`; the playbook still says
250,762 in places and is stale.

### No-checkpoint high-water

- Commit: `e0d0aabaec47d38a87bd8f8fbe92c710403db2a2`
- Change: thin LTO for release builds
- Workload: VU32, one-minute ramp plus two measured minutes
- Pressure: `MAX_ACTIVE_WRITES=24`
- Lock setting: `BICDB_RC_UPDATE_LOCK_ATTEMPTS=2`
- Seed: `/home/benchmark/seeds/seed-clean`
- Results: 419,228 / 417,780 / 418,094 DSUM NOPM
- Mean: 418,367 DSUM NOPM
- All three were valid

### Durable 4 GB checkpointer reference

- Keeper: `7420ad3`, raw-metadata passthrough in checkpoint serialization
- Auto-checkpoint threshold: 4096 MB
- Results: 398,685 / 388,924 / 394,093 / 391,040 DSUM NOPM
- Mean: 393,186 DSUM NOPM
- Four checkpoints per run were observed in the initial keeper pair
- Typical peak WAL was approximately 5.3-6.0 GB

The user correctly remembered that BicDB was already above 300,000 NOPM. In
fact, the current historical high-water is approximately 419,000. Any resumed
campaign that starts near 40,000 on `benchmark-primary` has a harness/configuration problem
and must stop before drawing conclusions.

## What the current code changes do

### Sync mode and WAL counters

`TxLogHandle` now carries an `fsync` boolean and counters for:

- highest written sequence;
- logical WAL flush calls;
- sync-equivalent calls;
- bytes passed to the WAL write path;
- commit sequences included in writes;
- largest grouped commit batch.

Durable mode opens the cached transaction-log append handle with `O_DSYNC` on
Unix. Buffered mode opens it without `O_DSYNC`. `--storage-sync durable` remains
the default. `--storage-sync buffered` disables configured sync behavior for the
database and is diagnostic, not crash durable.

`wal_sync_calls` is also a logical count of durable-mode WAL write operations,
not an observed count of `fsync(2)` syscalls. Durable writes use `O_DSYNC`, and
`write_all` can internally issue more than one `write(2)`. Do not label the
counter as an exact syscall count without tracing the process.

### Server statistics and checkpoint timing

`bicdb_server_stats` now includes TPC-C procedure mix, handled routine exception
counts, and the WAL counters. The auto-checkpoint log includes phase 0, phase 1,
lock acquisition, truncate, and total elapsed times. Checkpoint timings are
parsed from logs; there is not yet a structured checkpoint counter table.

### Three-lane harness

The full default matrix is:

1. One discarded warmup for each lane.
2. Six balanced lane orders: `DTC`, `TCD`, `CDT`, `DCT`, `CTD`, `TDC`.
3. Eighteen measured throughput trials total.
4. One separate latency profile for each lane.
5. One separate allocation profile for each lane.

Default measured workload: VU32, one-minute ramp, two measured minutes, with
HammerDB time profiling disabled. Latency profiles enable it and are excluded
from throughput statistics.

Lane definitions in `bench/tpcc/run_trial.sh`:

| Lane | Effective configuration |
| --- | --- |
| `default-durable` | Durable configured sync; product-default index/admission/outbox settings; product-default 4 GB checkpoint. |
| `tuned-durable` | Durable configured sync; sharded indexes 64; orchestration shards 16; update-lock attempts 2; sync outbox off; PL/pgSQL no-data flag; checkpoint 4096 MB; connections/queries/reads/writes 64/32/32/24. |
| `cpu-ceiling` | Tuned pressure and index settings; buffered/non-durable configured sync; sync outbox off; checkpoint disabled. Diagnostic only. |

Every trial attempts to record source/binary/toolchain/host/container/seed,
district-sum useful progress, HammerDB counters, procedure mix, handled errors,
transaction latency percentiles, queue latency percentiles, CPU/RSS/faults/I/O,
WAL counters, checkpoint timings, and graceful reopen time. Allocation profiles
use `bpftrace` uprobes and are excluded from throughput statistics.

The published harness also verifies the seed content hash once per matrix,
includes untracked source inputs in the source-state hash, binds BicDB only to
loopback, scopes runtime names by campaign, refuses stale datadirs/containers,
and validates HammerDB exit/completion, server failures, requested perf data,
and graceful-reopen state.

The intended keeper gate is documented in `docs/performance-campaign.md`: at
least 2 percent improvement in the target durable lane with a positive paired
Student-t 95 percent confidence interval, no correctness failure, no more than
5 percent peak-RSS/WAL growth, and no more than 10 percent p99 latency
regression. Every comparison arm must be internally homogeneous for binary,
source, seed, host, workload, and server configuration.

## Immutable local seed

The only benchmark object left in local `/dev/shm` is:

```text
/dev/shm/bicdb-tpcc-seed
warehouses:          16
build VUs:           8
district sum:        480160
materialized:        true
transactions.log:    0 bytes
manifest data bytes: 3452936438
content SHA256:       8af88e30be42a1833498614667dcf2851b3e3148c12b4f714867df699c4a252b
manifest SHA256:      aab37b9805f4bd6cc566d7879bc00b8c3f59e7496308cc03851996b19f69ec5d
```

The seed was built in buffered mode with outbox and checkpoints disabled, then
force-compacted, reopened, and checked for the same district sum. Its manifest
records the local binary hash above.

The historical `/home/benchmark/seeds/seed-clean` on `benchmark-primary` probably has no
`seed-manifest.json`, which the new harness requires. Do not silently change
seeds within a comparison block. Either transfer this exact materialized seed,
or build and record a fresh remote campaign seed for every arm.

## Pre-hardening local evidence (invalidated)

`reports/performance/20260709-harness-smoke` completed under the original,
weaker harness. It used VU2, zero-minute ramp, one measured minute, HammerDB
time profiling enabled, one sample per lane, and no allocation profile. The
retained raw evidence is insufficient to prove HammerDB exit/completion and the
requested profiling contract, so the current summarizer marks all three trials
invalid. The values below are historical screening observations only.

| Metric | Default durable | Tuned durable | CPU ceiling |
| --- | ---: | ---: | ---: |
| DSUM NOPM | 43,761 | 45,183 | 44,996 |
| HammerDB NOPM | 43,622 | 44,735 | 44,822 |
| Failed VUs | 0 | 0 | 0 |
| Server failed queries | 0 | 0 | 0 |
| Mean CPU cores | 1.5998 | 1.5886 | 1.6036 |
| Peak RSS bytes | 9,477,373,952 | 9,110,814,720 | 9,135,374,336 |
| WAL bytes | 1,013,183,683 | 1,050,552,013 | 1,038,656,290 |
| WAL write calls | 89,125 | 92,756 | 92,046 |
| Sync-equivalent calls | 89,125 | 92,756 | 0 |
| Commits written | 91,630 | 95,274 | 94,479 |
| Initial ready time | 9.169 s | 14.433 s | 14.322 s |
| Graceful reopen time | 19.302 s | 23.894 s | 23.674 s |
| NewOrder p50/p95/p99 | 1.209/1.480/1.776 ms | 1.146/1.409/1.715 ms | 1.145/1.403/1.669 ms |
| Payment p50/p95/p99 | 0.831/1.156/1.348 ms | 0.830/1.174/1.367 ms | 0.835/1.184/1.387 ms |

No checkpoint ran. No allocation profile ran. Hardware perf counters were not
available. One sample per lane provides no variance estimate and the order was
not a complete balanced matrix. Do not use these values as a baseline or a
keeper/reject comparison.

## Aborted and interrupted local evidence

- `reports/performance/20260709-local-head-baseline-screen`: VU32 default
  warmup hit the 8192 MB host-memory guard around 17.6 GB server RSS, 4.48 GB
  WAL, and 7821 MB host `MemAvailable`. No NOPM result.
- `reports/performance/20260709-local-vu16-head-baseline-screen`: VU16 default
  warmup hit the guard around 17.4 GB RSS, 4.53 GB WAL, and 7730 MB available.
  No NOPM result.
- `reports/performance/20260709-local-vu4-head-baseline-screen`: VU4 default
  warmup hit the guard when the 4 GB checkpoint began. RSS rose from about
  13.8 GB to 16.6 GB, WAL reached about 4.24 GB, and available memory reached
  7936 MB. No NOPM result.
- `reports/performance/20260709-local-vu2-head-baseline-screen`: only discarded
  warmups completed. Default was 41,824 and tuned was 43,975.33 DSUM NOPM. The
  CPU-ceiling warmup was interrupted after the server mismatch was identified.
  There are zero measured trials; `STOPPED.md` records why no aggregate summary
  is published.

These are infrastructure/screening records, not performance comparisons. Their
`ABORTED.md` or `STOPPED.md` files preserve the reason. No candidate improvement
was run.

During this handoff audit, invoking the pre-hardening `run_matrix.sh --help`
unintentionally started a VU32 campaign because that version ignored positional
arguments. It was
stopped, its exact child processes/container were removed, and its datadir and
report directory were deleted. No artifact from that accidental run remains.
The published script now implements `--help` and rejects other positional
arguments.

## Confirmed WAL acknowledgement race

This is the mandatory correctness issue to address after a trustworthy HEAD
baseline has been captured.

Current sequence:

1. `commit_transaction` reserves and publishes sequence `N` with
   `commit_seq.fetch_add(...)` in `crates/bicdb-core/src/db.rs` near line 9179.
2. It performs fallible and preemptible WAL frame encoding.
3. It enqueues the bytes in `TxLogHandle.pending` near line 9200.
4. After the database guard is released, the caller invokes
   `write_durable(N)`.

Two committers can interleave as follows:

1. Committer N reserves N and pauses before enqueue.
2. Committer N+1 reserves and enqueues N+1.
3. `write_durable(N+1)` can only drain a contiguous prefix. N is missing.
4. The current implementation returns `Ok(())` when it drains nothing, and it
   does not verify that `written_seq >= N+1` after a partial drain.
5. N+1 can therefore be acknowledged before its WAL is durable. A crash in that
   window loses an acknowledged commit.

A fallible encoding error after reservation can also leave a permanent sequence
gap while later durability calls return success. Existing comments claiming the
queue is assigned/enqueued gap-free are not true under this interleaving.

No deterministic regression test exists yet. Existing concurrent recovery tests
wait for all committers and miss the acknowledgement window.

Recommended fix order:

1. Add a deterministic hook immediately after sequence reservation and before
   enqueue. Pause N and prove that old code lets N+1 return while
   `stats.written_seq < N+1`.
2. Introduce a dedicated sequencing critical section. Compute the candidate
   next sequence, finish fallible encoding, enqueue it, and only then publish
   the sequence. Do not consume a sequence before work that can fail.
3. Coordinate imported replication commits with the same invariant.
4. Harden `write_durable(seq)`: success must imply `written_seq >= seq`. A gap
   must wait/retry or return an internal error, never `Ok(())`.
5. Test forced interleaving, injected encoding failure, crash/reopen recovery of
   every acknowledged commit, and existing concurrent commit/recovery suites.
6. Re-run the identical three-lane campaign as a mandatory correctness keeper,
   reporting any performance cost.

## Verification completed

The following focused tests passed in the dirty tree:

```text
cargo test -p bicdb-core buffered_transaction_log_drains_without_sync_calls
cargo test -p bicdb-core transaction_commit_is_durable_across_reopen_with_fsync
cargo test -p bicdb-pgwire server_runtime_handles_multiple_clients_and_status_tables
```

The core tests assert buffered writes make progress with zero logical sync calls,
durable writes record three write/sync/commit calls, and data survives a clean
reopen. The pgwire status test now exercises the added WAL counters through
`bicdb_server_stats`; buffered configuration is covered in the core test.

Shell syntax checks, Python parsing, and `git diff --check` also passed. Existing
compiler warnings remain. There is no test for the confirmed WAL race.

Broader release-gate tests were also run before publication. The SQL package
completed with 396 passing tests and one failure in
`secure_sql_rejects_plaintext_phi_filters_and_allows_blind_index_lookup`. The
core suite had six encryption integration failures, and the pgwire suite had
one failure in `broker_publish_wakes_queue_listeners`. Every failing test was
re-run against an untouched worktree at `06aadd4` and failed there with the same
error, so these are pre-existing HEAD failures rather than regressions from the
campaign work. The focused tests above, 117 SQL library tests, and 12 broker SQL
tests passed.

A final local VU1 durable publication smoke exercised the release binary and
hardened harness end to end. It was valid with zero failed VUs, 22,358
district-sum NOPM (22,188 reported by HammerDB), 46,799 WAL writes and syncs,
and a matching district sum after graceful reopen. The server listened only on
`127.0.0.1:55433`, and the harness removed its container, listener, and copied
datadir afterward. This is low-concurrency harness evidence, not a replacement
for the balanced remote baseline or a comparison with prior 300k NOPM runs.

## Remaining harness limitations

Publication hardening resolved time-profile separation and metadata,
source-state hashing, seed verification, `perf` finalization, paired decision
gates, graceful-reopen validation, campaign-scoped runtime names, stale-datadir
refusal, loopback-only serving, tool preflight, and argument handling.

The remaining limitations are:

1. Graceful reopen is not crash recovery. Add a separately marked SIGKILL/reopen
   trial before making process-crash recovery claims.
2. Handled serialization/deadlock counts are available, but there is no literal
   internal retry counter. Do not present handled errors as exact retry counts.
3. Campaign Markdown summaries remain concise; detailed CPU, failure, latency,
   sync, allocation, and checkpoint evidence is in each `result.json` and the
   paired comparison output.
4. The seed content hash is verified once before a matrix, not after every trial
   copy.
5. The hardened harness still requires a short remote smoke to prove `perf`,
   `bpftrace`, Docker host networking, and the release binary work together on
   `benchmark-primary` before the expensive full matrix.

Do not hide these limitations or compare graceful reopen with crash recovery.

## Exact resume order on benchmark-primary

### 1. Verify access and remote idleness

Run from the actual resumed execution environment:

```bash
ssh -l root 192.0.2.10 \
  'hostname; nproc; free -h; df -h /dev/shm; \
   pgrep -ax bicdb || true; \
   docker ps --format "{{.Names}} {{.Status}}" | grep "^hdb-" || true'
```

Do not clean anything until the output is reviewed. The historical
`dev9_trial.sh` uses global process/tmpfs cleanup and is safe only when `benchmark-primary` is
a dedicated idle benchmark host.

### 2. Check out the published campaign state

Check out the commit containing this handoff, or the branch into which it was
merged. Confirm that the source includes the new harness files,
core/pgwire/CLI modifications, and curated report evidence. Do not overwrite
unknown remote work in `/home/benchmark/bicdb-head`.

After checkout, record on `benchmark-primary`:

```bash
cd /home/benchmark/bicdb-head
git status --short --branch
git rev-parse HEAD
git diff --check
rustc -Vv
cargo -V
```

### 3. Smoke-test the hardened harness

Run a short VU2 smoke on `benchmark-primary` and inspect every resulting JSON field before a
full matrix. Confirm that the server listens only on loopback, source and seed
hashes are present, and requested `perf`/allocation profiles contain data. Do
not compare the smoke NOPM with historical throughput.

### 4. Build once and record the binary

```bash
cd /home/benchmark/bicdb-head
cargo build --release --bin bicdb
sha256sum target/release/bicdb
```

All arms in a baseline block must use that exact binary.

### 5. Select one explicit seed

Option A is to transfer the exact local materialized seed and verify both hashes.
Option B is to build a new remote seed and use it for every arm:

```bash
cd /home/benchmark/bicdb-head
SEED=/dev/shm/bicdb-tpcc-seed WAREHOUSES=16 BUILD_VU=8 \
  bench/tpcc/build_seed.sh
jq . /dev/shm/bicdb-tpcc-seed/seed-manifest.json
```

Do not mutate `/home/benchmark/seeds/seed-clean` merely to add the new manifest. Preserve
it as the historical control seed.

### 6. Reproduce a historical control before rebaselining

Use a separate detached worktree/binary at `e0d0aab` and the historical
`/home/benchmark/bicdb-bench/dev9_trial.sh` plus `/home/benchmark/seeds/seed-clean`. Reproduce the
VU32, MAW24, attempts=2, no-checkpoint high-water configuration with time
profiling off. This is a host/harness sanity check, not the current-HEAD baseline.

If the control is nowhere near the historical 418k band, stop and diagnose the
machine, seed, binary, and harness. Do not tune BicDB against a broken control.

### 7. Run the current-HEAD three-lane baseline

After the harness corrections and short smoke pass, run detached with tmux or a
transient systemd unit so an SSH disconnect does not terminate it. The semantic
full-matrix command is:

```bash
cd /home/benchmark/bicdb-head
CAMPAIGN_ID=$(date -u +%Y%m%dT%H%M%SZ)-head-baseline \
BIN=/home/benchmark/bicdb-head/target/release/bicdb \
SEED=/dev/shm/bicdb-tpcc-seed \
DATA_ROOT=/dev/shm \
VU=32 RAMPUP=1 DURATION=2 \
PERMUTATIONS=6 RUN_WARMUPS=1 RUN_LATENCY_PROFILES=1 \
RUN_ALLOCATION_PROFILES=1 \
MEM_GUARD_MB=8192 \
  /home/benchmark/bicdb-head/bench/tpcc/run_matrix.sh
```

Use `/dev/shm` only for historical CPU/lock comparability. Durable-mode code
paths on tmpfs do not prove power-loss durability or real-device sync latency.
Run a separate dedicated-NVMe durability campaign for those claims.

### 8. Review before any candidate

Require all of the following:

- all measured VUs succeeded;
- no panic, corruption, query failure, ENOSPC, memory-guard, or checkpoint error;
- DSUM and graceful-reopen DSUM match;
- exact binary, seed, toolchain, filesystem, and lane settings are constant;
- latency, CPU, RSS, WAL, sync-equivalent, checkpoint, and reopen data are present;
- per-lane repetitions and paired order are complete;
- the result is numerically plausible relative to the historical control.

Append the valid baseline to `docs/performance-campaign-ledger.md`. Commit the
curated manifest, summary, metadata, and result files under
`reports/performance/<campaign>`; retain bulky raw runtime artifacts outside Git
when they are needed for diagnosis.

### 9. Fix the WAL race, then test it as a mandatory keeper

Implement the deterministic regression first, make publication/enqueue gap-free,
harden the durability postcondition, run correctness/crash tests, and then run
the same three-lane matrix against the exact HEAD baseline.

### 10. Continue one candidate at a time

Each candidate starts from the last keeper, changes one principal axis, uses the
same host/seed/toolchain/matrix, and is recorded even when rejected. Winners are
kept. Regressions, resource losers, invalid trials, and inconclusive results stay
in the append-only ledger with links to their curated report evidence. Bulky raw
runtime artifacts may be retained separately when they are needed for diagnosis.

## Stop state

At handoff:

- no valid current-HEAD `benchmark-primary` baseline exists;
- no post-baseline improvement has been implemented or measured;
- the WAL acknowledgement defect remains open;
- the local immutable seed and curated local report evidence remain present;
- local benchmark processes and port 55433 are clean;
- an unrelated local BicDB server was intentionally left untouched;
- resuming on `benchmark-primary` still requires working access, the published campaign
  commit, and a successful hardened-harness smoke.
