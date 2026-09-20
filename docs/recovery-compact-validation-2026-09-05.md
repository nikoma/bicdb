# Compact WAL recovery validation — 2026-09-05

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

The resumed goal is correctness with at least 600,000 database-verified NOPM.
It remains open. The most recent 569,136 NOPM result is a buffered-tmpfs
diagnostic whose BicDB financial audit has not completed. It cannot qualify
the throughput goal or establish durability.

## Recovery change

The candidate streams transaction frames using the existing torn-tail-aware
reader, decodes full rows into `StoredRecord` without eagerly expanding JSON
metadata, and releases each transaction's recovered writes after applying it.
It preserves commit-sequence replay order, savepoint truncation, patch
materialization, abort filtering, legacy segment handling and outbox recovery.
It still accumulates decoded WAL writes before replay and defers MVCC version
garbage collection. This is a reduction in recovery memory, not a claim of
constant-memory recovery independent of log length.

Four focused compact-WAL tests passed before the interruption, including
value/hash preservation and commit/abort/savepoint recovery. The earlier broad
run stopped without a final result. Its partial successes are not a suite pass.

## Resumed operation handles

Session: `01a070ef-5823-7161-aa47-53c5e19de0f0`.
Worktree: `/home/benchmark/bicdb-recovery-compact`, branch `fix/recovery-compact-writes`.

- Local user service `bicdb-recovery-build-20260905`: optimized diagnostic
  build, one compiler job, 24 GiB memory limit, no swap. Log:
  `/tmp/bicdb-recovery-compact-pgo-build-resume.log`. Uses the retained
  `/tmp/bicdb-integrated-workload.profdata`; stale-profile warnings mean this
  is not a freshly trained performance candidate.
- Local user service `bicdb-recovery-tests-20260905`: complete core/SQL tests,
  one compiler job, two test threads, 10 GiB memory limit, no swap. Log:
  `/tmp/bicdb-recovery-compact-core-sql-resume.log`.
- VM2 (`benchmark@192.0.2.12`) service `bicdb-recovery-clone-20260905`:
  copies the closed corrected-Payment dataset into
  `/mnt/codex-recovery-compact-20260905-r1/data`, then checks every file
  against the retained relocation manifest. `clone-verified` is created only
  after verification succeeds. Recovery must use the clone, preserving the
  original under `/mnt/codex-retained-audits/`.

Inspect these authoritative service states before resuming or restarting any
operation. The VM has no swap enabled. Its `/mnt` is a temporary resource disk;
recovery there does not establish host-loss durability.

The local kernel recorded a global OOM at 02:32 PDT, killing a Rust compiler
with 29,881,280 KiB anonymous RSS. The resumed local disk initially had no free
space. Only rebuildable debug executables and obsolete core libraries were
removed; cleanup manifests are `/dev/shm/bicdb-rebuildable-artifact-cleanup-20260905*.json`.

## Remaining acceptance evidence

1. Complete core/SQL validation and test the optimized binary against the
   preserved full-scale WAL; record recovery time, memory peak and exact
   financial reconciliation, not just successful procedure-call counts.
2. Resolve every discovered financial or atomicity discrepancy. The stock
   Payment defect occurs on both engines; use identical, explicitly labelled
   corrected procedures for paired diagnostics. Delivery and New-Order field
   omissions documented in the Payment audit remain separate scope.
3. Validate acknowledged transactions after crash/reopen on persistent storage,
   including fault paths affected by WAL changes. Prior crash evidence covers
   a finite process-crash experiment, not arbitrary failures or power loss.
4. Run repeated, comparable durable-lane trials at >=600,000 verified NOPM,
   with business-completion and financial gates, checkpoints, workload mix,
   seed, binary and storage provenance retained. CPU-ceiling runs do not qualify.
5. Consolidate validated fixes onto main and inspect required CI results.

## Resumed optimized build and clone

The optimized diagnostic build completed in 11m 33s. The systemd completion
record reports 18.5 GiB memory peak and zero swap use under its 24 GiB limit.

- Compiled parent: `6a1e08ca2706a8038ad1417a2825a2a525d1d250`.
- Compiled two-file code diff SHA-256:
  `4bc48b47865a289cad38b844a10183a34768efffc0a9596dbe935433b29991e9`.
- Full binary SHA-256:
  `6ce8cf6166f36412d785211ccf79c4d4fedb995d6750cfce1524194a61212868`.
- Debug-stripped binary SHA-256:
  `753a0dd753230c411545070a546f5fec54378c776b7a464b3cfc4bce931cf977`.
- Local and VM2 artifact: `/tmp/bicdb-recovery-compact-pgo-20260905`.

The VM2 clone passed every retained file hash. Service
`bicdb-compact-audit-20260905-r1` now opens that clone, limited to 95 GiB RAM
with no swap. Its log is `recovery.log` under the audit root; the separate
`bicdb-compact-audit-monitor-20260905-r1` service records RSS/HWM and service
state in `recovery-samples.jsonl` until readiness or a terminal state.
The active monitor is revision `bicdb-compact-audit-monitor-20260905-r3`:
readiness requires a successful `SELECT 1`, because pgwire binds its listener
before database recovery completes. Earlier socket-only observations are
preserved as `socket-samples.jsonl` / `socket-observation.json` and must not be
called recovered readiness. `pre-sql-monitor-samples.jsonl` retains intervening
memory observations. Inspect `recovery-observation.json` when present;
existence alone is not a pass.

Local service `bicdb-recovery-pgo-train-build-20260905` builds fresh profile
instrumentation from the same frozen functional source, one compiler job,
32 GiB RAM limit, no swap. Log:
`/tmp/bicdb-recovery-pgo-train-build-resume.log`. Neither an instrumented run
nor its speed can qualify throughput. Do not modify the functional source
while this build is running.

## Complete local validation

The resumed combined core/SQL run completed successfully: **2,157 tests passed,
zero failed, nine ignored**, across 190 test binaries and both doctest targets.
The WAL kernel-fault child reports one additional nested success, excluded
from that total. The suite used a 9.0 GiB cgroup memory peak and zero swap.
Its log SHA-256 is
`9e19a4d196cc5ee8f8e6c492b86858a2e65f7e1b3731ab2ff47b8d1787837fb4`.
Formatting, whitespace, licensing and dependency-boundary checks also passed.

## Full-scale recovery and financial result

The exact cloned corrected-Payment dataset reached SQL readiness. Database
open traces report 116.346 seconds for WAL decoding, 126.083 seconds through
segment loading, 1,655.019 seconds through replay, and 1,714.169 seconds
through index/graph initialization. Peak process RSS was **81,188,476 KiB
(77.4 GiB)**, with zero swap. The cgroup reached its 95 GiB limit, which
includes reclaimable file cache as well as process memory. The independent
monitor required a successful `SELECT 1`; its own elapsed clock starts later
than database open and must not replace the open-trace duration.

This closes the prior inability to recover the dataset. Replay remains slow
and retains obsolete MVCC versions. A separately tested follow-up candidate,
`fix/recovery-version-pruning`, addresses that issue; it is not part of this
binary. The 20-second CPU sample during replay makes these recovery timings
diagnostic, not a throughput acceptance result.

The retained financial audit then checked all **480,000 customers** against
the exact seed and **2,188,002 history rows**, in **175.199 seconds**. It found
zero balance, customer YTD-payment, customer payment-count, district-YTD or
warehouse-YTD discrepancies. This is the full financial pass previously
blocked by recovery, for the explicitly corrected Payment workload. It does
not retroactively turn a buffered-tmpfs run into a durable throughput keeper
or establish all TPC-C fields/specification requirements.

- Financial result: `/tmp/bicdb-financial-reconcile-bicdb-compact-v1-20260905.jsonl`,
  SHA-256 `1ae289335b5d1f503b33bfdc477a46d48a92884bd6e5d92ab994478c23abad85`.
- Seed delivery totals SHA-256:
  `5328884e834cf0f2e179012b771335af275739a3a2e343f7df8b6a231d18de5f`.
- VM2 SQL-readiness observation SHA-256:
  `d349c5d2edccebc847ec31d5fd9b05381b5b1489f175e6961156d4a61dec9ad8`.
- VM2 final monitor samples SHA-256:
  `d177ef0c80468107c789910a02a4e9e67114b22934736a7604256fe3b06609ac`.

PR #833's complete CI run `33959796337` passed all eight jobs, including
workspace compilation, core, SQL, pgwire and the rest of the workspace.
The per-order/Delivery audit covers **2,187,408 orders, 21,877,069 order lines
and 750,028 pending orders**, with exact global-count coverage. No orphan IDs,
duplicates, missing lines, line-number disagreements, backlog/carrier
disagreements or partial delivery states were found. The raw audit reports
24 timestamp exceptions and therefore exits nonzero; a separate check proves
their exact IDs, distinct-timestamp counts, minima and maxima match the
independently captured seed. The raw failed result is retained unchanged.

- Raw per-order result: `/tmp/bicdb-delivery-order-compact-v1-20260905.jsonl`,
  SHA-256 `f7c8d2d8b5869b792a5deeec243cc0945d4eea43aebde11f28d3a6defeb7c3c5`.
- Exact seed exception reconciliation:
  `/tmp/bicdb-order-seed-exceptions-compact-20260905.json`, SHA-256
  `1ff5fdabde370c1d82505ba8aef73f0b5597de3884802d80b99b7c13d02eace7`.

A fresh 16-client process-crash audit used the same compact-only binary on
VM2's **managed OS filesystem**, in its own
`/home/benchmark/bench-out/codex-persistent-compact-20260905-r1/bicdb` database. The
external acknowledgement ledger is fsynced only after COMMIT returns.
After SIGKILL during active transfers and restart, **10,067 of 10,067
acknowledged operations recovered**, with zero missing acknowledgements,
duplicates, unexpected IDs, invalid amounts, or unexplained worker errors.
All balances reconcile: 989,933 and 1,010,067. Seven attempted operations were
not acknowledged; none was recovered committed. This is a process-crash test,
not a host/power-loss test or a performance measurement.

- Ledger: `/tmp/bicdb-persistent-ack-compact-20260905-r1/client-events.jsonl`,
  SHA-256 `6ceb974d0b37a6fa71bae0a49f4660e9986743a924bee2c68d6496efe394bbbf`.
- Result: `/tmp/bicdb-persistent-ack-compact-20260905-r1/result.json`, SHA-256
  `ee14c3439ec5cf2758af371aa7d5328ce2c061a60306bf48314fe255e0c0b164`.

No 600k acceptance result is claimed. Shared workload field omissions and
the separate obsolete-version recovery cost remain explicit open work.

## Profile-build containment and relocation

The local fresh-profile compiler exceeded its 32 GiB cap and its service was
OOM-killed after 26m 57s CPU, with zero swap. This was a contained cgroup OOM;
no instrumented binary or successful training result came from that attempt.
The queued pruning build started only after that compiler became terminal.

VM1's own quiescent BicDB/PG audit instances were stopped. Its closed BicDB
dataset was relocated, with every file hash checked, from
`/dev/shm/bicdb-codex-correctness-20260905-outcomes-d1` to
`/mnt/codex-retained-audits-vm1-20260905/data`. The manifest and verification
log remain alongside it. Only the verified former tmpfs copy was removed;
PostgreSQL containers and datasets are retained. The host then had about
104 GiB available and no swap.

Fresh instrumentation now builds on VM1 under a 64 GiB cap in service
`bicdb-profile-build-vm1-20260905`, with the matching Rust 1.96.0 toolchain,
source worktree `/mnt/bicdb-correctness-profile-src-20260905` at `77347085`,
target `/mnt/bicdb-profile-target-20260905`, and log
`/mnt/bicdb-profile-build-20260905.log`. No training or speed pass yet.
