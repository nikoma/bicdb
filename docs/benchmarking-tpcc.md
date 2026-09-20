# TPC-C Benchmarking Guide (bicdb)

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

How to run HammerDB TPC-C against bicdb **correctly and reproducibly**, and the
hard-won lessons that make results trustworthy. Read this before running or
interpreting any TPC-C number.

---

## TL;DR

- **Run on `192.0.2.10` (host `benchmark-primary`, 24 cores, 93 GB RAM). NOT the local dev
  sandbox** — it is much slower (software RAID, fewer cores) and its numbers are
  **not comparable** to our historical results, which all come from `benchmark-primary`.
- The harness lives at **`/home/benchmark/bicdb-bench/`** on `benchmark-primary`. Data dirs go on
  **tmpfs (`/dev/shm`)** to expose lock/CPU behavior (not disk fsync).
- **Trust the district-sum NOPM, not HammerDB's reported NOPM** (the latter is
  unreliable for some bicdb builds — it once reported 19k when the real figure
  was ~98k). NOPM = `Δ sum(d_next_o_id)` over the run ÷ run-minutes.
- **Always restore the seed snapshot** before each trial, **clean the datadir
  after each trial**, run a **discarded warmup trial**, and use **balanced trial
  order + repeats** — otherwise warmup drift (±40%) and tmpfs ENOSPC silently
  corrupt the comparison.
- New performance decisions use the three-lane campaign in
  [`performance-campaign.md`](performance-campaign.md). The older scripts below
  remain useful historical context but do not collect the complete gate.

---

## Where to run, and how code gets there

- All benchmarks run on **`benchmark-primary` (192.0.2.10)**. SSH as root. It has the
  HammerDB docker image (`tpcorg/hammerdb:postgres`), `psql`, and the harness.
- **`benchmark-primary` cannot pull from GitHub** (no creds). Ship a candidate **from the
  local sandbox** as an incremental git bundle:

  ```bash
  # on the local sandbox (which CAN reach GitHub)
  git tag -f benchcand <commit>
  git bundle create /tmp/cand.bundle <base-already-on-benchmark-primary>..benchcand   # e.g. 8291b13..benchcand
  scp /tmp/cand.bundle benchmark@192.0.2.10:/home/benchmark/

  # on benchmark-primary, in a checkout that already has <base>
  cd /home/benchmark/bicdb-concurrent
  git fetch /home/benchmark/cand.bundle 'refs/tags/benchcand:refs/tags/benchcand'
  git checkout <commit>
  cargo build --release --bin bicdb      # ~2.5 min on benchmark-primary
  ```
- Build **both arms with the same toolchain** (rebuild the baseline too; same
  `rustc`). Binary path: `<checkout>/target/release/bicdb`.

---

## The metric: district-sum NOPM (ground truth)

HammerDB's reported "NOPM"/"TPM" are derived from bicdb's emulated
`pg_stat_database` counters and have proven **unreliable across builds** (TPM
inflated, NOPM deflated → bogus ~3% NOPM/TPM ratios). **Compute NOPM ourselves**
from the actual committed new-orders:

In TPC-C every new-order increments its district's `d_next_o_id`, so:

```
NOPM = ( SUM_AFTER − SUM_BEFORE ) / run_minutes
where SUM = `SELECT sum(d_next_o_id) FROM district`   (queried via psql)
```

Query it right after the server is up (before load) and right after the run ends.
Validation that this is correct: for the lock baseline, `Δ/2` (rampup 1m +
duration 1m) matched HammerDB's own base NOPM (~119k) and our historical lock
baseline (~115k). For builds where HammerDB's counter is broken, only the
district-sum is right.

`psql` one-liner used in the scripts:
```bash
PGPASSWORD=x psql -h 127.0.0.1 -p 55433 -U bicdb -d bicdb -t -A \
  -c 'select sum(d_next_o_id) from district'
```

---

## Seed-restore methodology (fast + byte-identical)

Build the 16-warehouse schema **once**, snapshot the tmpfs datadir, then **restore
that exact seed for every trial** (no per-trial schema rebuild; every arm starts
from byte-identical data):

```bash
# build seed once (base binary, default flags)
<bin> serve-pg /dev/shm/bicdb-seed ... &      # then run buildschema-16wh.tcl via hammerdb
# snapshot
cp -a /dev/shm/bicdb-seed /dev/shm/bicdb-seed-snap
```
The seed snapshot (`/dev/shm/bicdb-seed-snap`, ~5.3 GB) is reusable across builds
(format is forward-compatible). Each trial: `cp -a $SNAP $datadir`, serve, run,
measure, **then `rm -rf $datadir`**.

---

## Running an A/B (the rigorous recipe)

Driver TCL template: `/home/benchmark/bicdb-bench/work/run-vu.tcl.tmpl` (`@VU@` substituted;
`pg_storedprocs true`, `pg_allwarehouse false` so each VU drives its home
warehouse, `pg_driver timed`, `pg_rampup`/`pg_duration` in minutes).

A clean lock-free-vs-serialized A/B on the **same binary** is in
`/home/benchmark/bicdb-bench/lockab3.sh` (the canonical pattern). It does, per trial:
1. `pkill -9 -x bicdb` (see footgun below) + remove stale hdb containers
2. tmpfs free-space guard (abort if < 15 GB free), then `cp -a $SNAP $dd`
3. set config env (below), `serve-pg` on `$dd`, wait for listener
4. `SUM_BEFORE` (district-sum) → run `hammerdbcli auto run-*.tcl` (docker) → `SUM_AFTER`
5. record `Δ`, NOPM, FAILED count, tx_states; `stop`; **`rm -rf $dd`**

Run it detached so it survives the SSH session:
```bash
systemd-run --no-block --unit=bicdb-ab /home/benchmark/bicdb-bench/lockab3.sh
# poll: grep 'DONE' /home/benchmark/bicdb-bench/lockab3-summary.txt
```

**Controls that make it trustworthy:**
- **Discarded warmup trial** first (absorbs CPU-turbo / cache settling — early
  trials run ~40% slow otherwise).
- **Balanced order** (e.g. `free,lock,lock,free,free,lock`) so monotonic drift
  cancels instead of biasing one arm. A naive `free,lock,free,lock` interleave
  puts both `lock` trials later and biases the result.
- **≥3 repeats per arm**, average them (single trials vary ~36%).
- **2–3 min duration** (the default 1 min is too warmup-sensitive).

---

## Config flags

| env | effect |
|---|---|
| `BICDB_GLOBAL_COMMIT_LOCK=1` | re-acquire a global commit lock (serialized commit, old-baseline behavior). **Default off = concurrent/lock-free.** One binary, two arms — use this to isolate the commit lock with everything else held constant. |
| `BICDB_INDEX_STORE=sharded BICDB_INDEX_SHARDS=64 BICDB_ORCH_SHARDS=16` | value-based sharded index + 16-way orchestration sharding (the concurrent-commit config). |
| `BICDB_MEM_GUARD_MB=2048` | in-process memory guard (aborts if MemAvailable drops below). **Guards process RAM, NOT tmpfs fullness** — see gotchas. |
| `BICDB_MEM_TRACE=1 BICDB_MEM_TRACE_SECS=10` | periodic `MEMTRACE` lines (rss, wal, tx_states, commit_seq vs last_seq, records, versions) — use for health. |

serve-pg flags used: `--host 0.0.0.0 --port 55433 --allow-remote-no-auth
--max-connections 64 --max-active-queries 32 --max-active-reads 32
--max-active-writes 16 --max-result-rows 1000000`.

HammerDB container reaches the host via `--add-host=host.docker.internal:host-gateway`,
`pg_host=host.docker.internal`, `pg_sslmode=disable`, user/db `tpcc`,
superuser/defaultdbase `bicdb`.

---

## Health signals to check every run

- **FAILED count** = `grep -c 'FINISHED FAILED'` in the hdb log — must be 0.
- **tx_states** (MEMTRACE) must stay **bounded** (single digits). If it grows
  linearly to hundreds of thousands, the tx_states leak is present (pre-fix
  builds) and will OOM longer runs.
- **Watermark contiguous**: `commit_seq == last_seq` (or off by ≤1) every sample;
  `min_snap` tracking close behind — else watermark lag / snapshot pileup.
- **No `No space`/`panic`** in the server log (tmpfs ENOSPC, see gotchas).
- New-order fraction sanity: `Δsum(d_next_o_id)` vs total commits (commit_seq
  delta) should be ~40–56%. A much lower ratio means heavy new-order
  conflict/retry (hot-index contention).

---

## Gotchas (hard-won — ignore at your peril)

1. **tmpfs ENOSPC silently corrupts results.** Each trial's datadir balloons from
   the 5.3 GB seed to ~11 GB under vu16 writes. If datadirs accumulate (scripts
   that don't `rm -rf $dd` after each trial) or stale dirs from earlier runs
   linger, `/dev/shm` fills, writes fail, and throughput "collapses" — this looks
   like an architectural wall but is a disk artifact. **Always clean per-trial,
   and guard free space (≥15 GB) before each trial.**
2. **The memory watchdog does NOT catch tmpfs fullness.** It reads `MemAvailable`,
   which can show 30 GB free while `/dev/shm` is 98% full. tmpfs is a separate
   axis — check `df -h /dev/shm` yourself.
3. **`pkill -f "serve-pg"` (or `-f cargo`) kills your own SSH session** — your
   remote shell's argv contains that string, so `-f` matches it. Symptom: command
   hangs / "no output". **Use `pkill -9 -x bicdb`** (exact comm match), which hits
   the server binary but not your shell. Same trap with `pgrep -f` self-matching
   (gives false "cargo is running" counts).
4. **Warmup drift.** The first 1–2 trials run ~40% slow (CPU turbo / cache /
   build-settling). Always discard a warmup trial and balance order.
5. **Don't trust HammerDB's NOPM/TPM** for bicdb — use the district-sum.
6. **Long jobs**: launch with `systemd-run --no-block --unit=...` so they survive
   the SSH session; poll the summary file. The Bash-tool timeout is short; never
   hold one SSH connection for the whole multi-trial run.
7. **A/B must isolate one variable.** Comparing an old baseline commit against a
   much newer candidate conflates the change-under-test with every other commit
   between them. Prefer **same binary + a runtime flag** (e.g.
   `BICDB_GLOBAL_COMMIT_LOCK`), or two builds differing by exactly one change.

---

## Reference numbers (benchmark-primary, 16wh, tmpfs)

- **Lock baseline** (commit `8291b13`), vu16: **~125k NOPM** (district-sum;
  matches HammerDB's 119k and historical ~115k).
- **Postgres 18** (same host/harness), for scale: vu8 ~422k / vu16 ~642k NOPM —
  the gap we are chasing (~5x at the time of the original measurement; ~2.3–4.3x
  after the pgwire TCP_NODELAY fix). Numbers predate later changes; re-measure.
- A naive concurrent-vs-lock comparison across 16 commits showed the lock-removed
  candidate ~22% *slower* at vu16, but that A/B was confounded (old base vs new
  candidate) and warmup-contaminated. The clean isolation (same binary, lock flag)
  supersedes it — see the latest `lockab3-summary.txt`.

---

## File map on benchmark-primary

```
/home/benchmark/bicdb-bench/
  work/run-vu.tcl.tmpl          # timed driver template (@VU@, pg_duration/rampup)
  work/buildschema-16wh.tcl     # one-time 16wh schema build
  lockab3.sh                    # canonical same-binary lock-free vs serialized A/B
  snapbench.sh                  # interleaved cand/base via seed snapshot (older)
  candvu16.sh / candvu8.sh      # single-arm district-sum runs (note: leave datadir)
  bench.sh                      # full VU-ladder with mem watchdog (builds schema each arm)
/dev/shm/bicdb-seed-snap        # the reusable 16wh seed snapshot (~5.3 GB)
/home/benchmark/bicdb-concurrent          # candidate checkout (rebuild to the commit under test)
/home/benchmark/bicdb-lockbase            # lock-baseline checkout (8291b13)
```
