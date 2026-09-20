# Reclaim obsolete MVCC versions during recovery — 2026-09-05

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

Continuation of the active correctness and >=600,000 NOPM goal. This candidate
builds on compact-WAL commit `77347085` (PR #833, merged as `7c77e22a`).
Full-scale recovery, financial reconciliation, order/Delivery structure,
and the managed-filesystem acknowledged-commit crash audit pass. This
is not a new throughput result or a claim of universal correctness.

## Reproduction and change

The compact decoder finishes the retained VM2 WAL in 116.346 seconds, but
replay remains slow. A 20-second CPU-clock sample of the new binary contains
1,984 samples and zero lost samples; 65.78% of self samples fall in
`apply_unmaterialized_transaction_log` (including inlined work). The sample
is at `/mnt/codex-recovery-compact-20260905-r1/replay-cpu.perf.data` on VM2.

Recovery passes GC watermark zero while applying every historical write.
`apply_versioned_upsert` scans the existing chain to mark its live version,
then GC scans it again. Hot warehouse/district rows retain every historical
version, so these scans grow quadratically with updates to each row.

The regression confirms that 256 updates leave 257 versions of one row after
reopen, despite there being no surviving pre-crash snapshot. Before-fix log:
`/tmp/bicdb-recovery-prune-before-r2.log`. The earlier `before.log` only
records a fixture compilation error, not the behavioral reproduction.

The candidate uses the replayed commit sequence as the GC watermark.
Recovery is already ordered by that sequence; no snapshot can outlive the
database process or open before recovery finishes. Later patch replay needs
the current row, not superseded versions. The test checks final values,
deleted rows, with and without an initial checkpoint, a post-recovery pinned
snapshot across a later commit, and another checkpoint/reopen.
The six focused recovery tests pass in
`/tmp/bicdb-recovery-prune-focused.log`.

## Validation and preserved inputs

The combined core/SQL suite completed with 2,158 tests passed, zero failed,
and nine ignored across 190 test binaries and two doctest groups. Exclude
one nested WAL child-process success from a naive sum of log summaries.
The suite used a 10 GiB cgroup cap, no swap, one compiler job and two test
threads. Log `/tmp/bicdb-recovery-prune-core-sql.log`, SHA-256
`c96a10b80f98ccc829ebdf504bd000e1e5e92f8bf6f1e349a45b9fefa85cfae4`.
Formatting and diff checks passed.

The optimized candidate build completed in 18m38s with 21.3 GiB peak cgroup
memory and no swap under a 24 GiB limit. Preserved stripped binary locally
and on VM2: `/tmp/bicdb-recovery-prune-pgo-20260905`, SHA-256
`9246e24c2a9b86eb46c02a5560d5141f6a5a8e85e7e000a54f66fbc8eb013114`.
It uses the retained old profile, not a newly trained performance profile.
Its frozen source is commit `098e1e82` on base `77347085`; the later main-based
integration branch adds the already merged recovery documentation and tests.

A second clone of the exact retained 32 GiB VM2 dataset was verified against
the original file manifest before opening. The original remains untouched.
Clone: `/mnt/codex-recovery-prune-20260905-r1/data`.
Service: `bicdb-prune-audit-20260905-r1`, PID 147920, port 55436,
95 GiB cgroup cap and zero swap. The SQL-readiness monitor is
`bicdb-prune-audit-monitor-20260905-r1`; its samples and observation JSON
are beside the clone. A bound socket alone does not prove readiness.

Full-scale recovery succeeded with these cumulative open-trace durations:

| Milestone | Compact only | Compact plus pruning |
| --- | ---: | ---: |
| WAL decode | 116.346 s | 118.962 s |
| Segment load | 126.083 s | 128.604 s |
| Transaction replay complete | 1,655.019 s | 784.128 s |
| Database open | 1,714.169 s | 852.455 s |

Peak observed process RSS fell from 77.4 GiB to 48.7 GiB. The candidate's
cgroup also contains filesystem cache and peaked near its 95 GiB cap;
process RSS is not total cgroup memory. Neither run used swap. These are
single sequential observations of the same input, not repeated performance
trials. Both clones reside on the temporary resource disk, so this is not a
host-loss durability test.

The prior compact-only clone passed all 480,000 customer financial checks
and the order/Delivery structural audit (with 24 exact independently captured
seed timestamp exceptions). It was stopped only after those audits completed
and read/write admission was quiescent. Its files and logs are retained at
`/mnt/codex-recovery-compact-20260905-r1`. Its separate managed-filesystem
process-crash audit recovered all 10,067 externally recorded acknowledged
commits, with no reconciliation errors; see
`docs/recovery-compact-validation-2026-09-05.md` for scope and hashes.

## Completed candidate audits

The unchanged financial audit passed all 480,000 customers and 2,188,002
history rows in 176.421 seconds, with zero balance, YTD, payment-count,
district, or warehouse discrepancies. Result:
`/tmp/bicdb-financial-reconcile-bicdb-prune-v1-20260905.jsonl`, SHA-256
`393228d3b3f2461a03154b3174c25e182cba4a77ac4e14d32824dbeef38cfa97`.

The unchanged order/Delivery structural audit covered all 2,187,408 orders,
21,877,069 lines, and 750,028 pending orders in 141.955 seconds. The raw
result correctly retains 24 timestamp failures; the independent checker
proves their exact IDs and distinct-count/min/max values match the seed,
with no other discrepancies. Raw result
`/tmp/bicdb-delivery-order-prune-v1-20260905.jsonl`, SHA-256
`592911f298e3cc50eb4559cf2b12d86e031c4a5b2f797976cabfb8e9b13c9e95`.
Qualified result `/tmp/bicdb-order-seed-exceptions-prune-20260905.json`,
SHA-256 `c44cfb95d6aca54f4bf3c21e72a9e6ce6365bce8f576f5de0e1b83d25b24478e`.

The fresh managed-filesystem process-crash audit passed: 10,046
externally recorded acknowledged commits, 10,047 recovered, zero lost
acknowledgments, unexpected operations, duplicates, invalid amounts, or
balance errors. The ledger is fsynced outside the server after each COMMIT;
the process is killed during concurrent transfers and restarted. This tests
process SIGKILL, not host or power loss. Remote dedicated data:
`/home/benchmark/bench-out/codex-persistent-prune-20260905-r1/bicdb`; service
`bicdb-persistent-prune-20260905-r1`, port 55439, 4 GiB cap and no swap.
Local tunnel port 55448. Both data and evidence remain retained.

`/tmp/bicdb-persistent-ack-prune-20260905-r1/result.json` SHA-256:
`32ea44f4fe581fbafbc799c87e76789e7769ac7b497160d88fd056afcad57a51`.

`/tmp/bicdb-persistent-ack-prune-20260905-r1/client-events.jsonl` SHA-256:
`6fb77a3402dee05d30a8fcae7201a9705d1c59e0c3c2f8b6cd15d05d8901dca4`.

## Active follow-up

- A fresh instrumented compiler is running on VM1 as
  `bicdb-profile-build-vm1-20260905`, with a 64 GiB cap, no swap, Rust 1.96.0,
  and frozen compact-only source `77347085`. Its source is
  `/mnt/bicdb-correctness-profile-src-20260905`; log
  `/mnt/bicdb-profile-build-20260905.log`. No training run has happened yet.
  `bicdb-profile-train-vm1-20260905` is queued behind successful compilation,
  with a separate 96 GiB cap and no swap. Its explicitly excluded warmup
  uses corrected Payment v1 and captures runtime counters after startup;
  scripts are `/tmp/bicdb-profile-{train,capture}-vm1-20260905.sh` on VM1.
  Training log: `/mnt/bicdb-profile-train-20260905.log`.
  The earlier local 32 GiB instrumented build hit its cgroup limit and failed
  without taking down the session; its failed target was removed using Cargo.

The active goal remains correctness-qualified >=600,000 NOPM. No new speed
claim is made here. Shared HammerDB Delivery/NewOrder counter omissions still
need an explicitly corrected workload and parity audit before qualification.
