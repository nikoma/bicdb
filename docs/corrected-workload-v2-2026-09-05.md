# Corrected HammerDB workload v2 candidate

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

This is an unqualified candidate for the active correctness/performance goal,
not stock HammerDB or a TPC-C certification. It includes the tested Payment v1
repair, Delivery counter maintenance, and a NewOrder rewrite that avoids
zipping unrelated row arrays. The retained tax/discount-adjusted line-amount
formula and handled-conflict policy remain explicit. CALL success alone is
not evidence of a committed business operation.

NewOrder validates all requested item IDs before mutations. A missing item
raises P0002 and rolls back. Stock requests are grouped by item and warehouse;
quantity, YTD quantity, line count and remote count include every requested
line. The quantity wrap formula is equivalent to sequential requests for the
verified 10..100 seed range. Updated stock rows are joined back to input lines
by item and supplier keys, so duplicate requests keep their own line numbers
and amounts. Missing stock coverage raises an uncaught exception.

The deterministic fixture scripts under `bench/tpcc/fixtures/neworder-v2/`
are for a newly created disposable database only. They override DBMS_RANDOM
with constants to reproduce the invalid-item and duplicate-key cases. The
original procedure commits the invalid item and changes stock 50 -> 48 for
three quantity-2 lines, leaving two amounts NULL, on both BicDB and PostgreSQL
18.6. BicDB additionally stored 87.55 where PostgreSQL stored 87.56; PR #839
addresses the missing routine assignment coercion behind that discrepancy.

The v2 candidate must pass both engines' fixtures, full financial/counter/
amount reconciliation, operation-outcome gates, and recovery checks before
its speed is qualified. No throughput result is claimed here. Validation is
in progress; these scripts are committed as a WIP checkpoint.

## Resume: explicit operation outcomes

`CORRECTED_WORKLOAD_V2=1` installs the same versioned SQL and client accounting
in the BicDB and PostgreSQL runners. NewOrder returns `no_d_next_o_id=-1` only
for the deliberate invalid-item path, before any mutations. Each client records
calls, positive order IDs, expected rollbacks, unexpected zero IDs and Payment
calls. The checks require every VU exactly once and reconcile those counts
with district/history deltas. Expected invalid requests are reported separately;
they are never counted as committed orders or included in NOPM. Missing valid
commits, missing client reports and duplicate reports fail qualification.

PostgreSQL 18.6 fixture verification during the resumed session confirmed the
-1 reply with unchanged district/order state; three duplicate quantity-2 lines
each at 87.56; stock quantity/YTD/order count 44/6/3; and Delivery balance/count
262.68/1. The client hook was also executed against the actual captured
HammerDB NewOrder body with positive, invalid and unexpected outcomes. All 13
Python harness tests pass, including false-acceptance regressions. The new
BicDB OUT-marker assertion still requires execution with the new binary.

Both existing Azure benchmark machines remain running Spot Standard_D32ads_v5
VMs: VM1 192.0.2.11 and VM2 192.0.2.12. VM1 is building engine source
8e29e143 with the preserved Payment-v1 workload profile, fat LTO, one codegen
unit and x86-64-v3. This profile is explicitly older than corrected NewOrder v2;
it is a starting candidate, not an asserted optimized keeper.

Build service: `bicdb-workload-v2-pgo-build-20260905`, 64 GiB memory cap, no
swap, two Cargo jobs. Source `/mnt/bicdb-workload-v2-src-20260905`, target
`/mnt/bicdb-workload-v2-pgo-target-20260905`, log
`/mnt/bicdb-workload-v2-pgo-build-20260905.log` on VM1. Check the live service
before starting any replacement. Completed audit services on VM2 were stopped
after confirming no active clients; all their datasets remain in place.

No corrected-v2 speed result, full-state audit, or recovery pass is claimed yet.

## First corrected-v2 PostgreSQL reference (VM2)

`codex-pg-corrected-v2-20260905-r2` ran 32 VUs, fixed layout A, one minute ramp
and one minute measurement. Database deltas give **363,825 NOPM** across both
minutes; HammerDB reports **504,049 NOPM** for the timed minute. Every active
client reports exactly once: 734,976 NewOrder calls, 727,650 positive/committed
orders, 7,326 intentional invalid-item rollbacks, zero unexpected outcomes;
735,610 Payment calls and history inserts. All 33 VUs completed.

This is a short corrected-v2 reference, not a repeated keeper or a result
comparable directly with older Payment-v1 scores. PostgreSQL 18.6 image
`sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`
was copied from VM1. It uses fsync and synchronous_commit on, on tmpfs: no
host/power-loss durability claim. R1 failed during setup because VM2 lacked
that image; its outputs/data remain preserved and it has no speed result.

The exact imported PostgreSQL seed was copied from VM1's
`/dev/shm/codex-pg186.zZrxCM` to VM2's `/mnt/codex-pg-seed-v2-20260905`.
All 1,728 file hashes matched; the manifest SHA-256 is
`061088f4ffb290fdfbd777d102bcdcaf3577425f997cc1b24e2f2803b13e2c3b`.
The completed trial's data is `/dev/shm/codex-pg186.8CqHnz`, and artifacts are
`/mnt/bench-out/codex-pg-corrected-v2-20260905-r2`, both on VM2. Its PostgreSQL
container `codex-pg186-trial-153308` was stopped after read-only audits.

The full financial audit passed all 480,000 customers against 1,215,610 history
rows, with no balance or Payment-counter/YTD discrepancies. The new independent
Python Decimal/counter audit checked all 7,277,116 post-seed lines, all 1,600,000
stock rows and all 480,000 customer Delivery counters: zero discrepancies,
41.139 seconds. Amounts use the retained tax/discount-adjusted procedure formula.
Stock expectations accumulate every post-seed line independently and apply the
sequential quantity-wrap equivalent to the hash-checked original seed. Delivery
expectations subtract the exact original per-customer delivered-order counts.

Raw outcomes and both audits are checked in under
`docs/evidence/corrected-workload-v2-2026-09-05/`. Executed expanded audit script
SHA-256: `62f1674237a5e1a52a08550e0ed86dd15f89f8f3f45b2c4c89a52dbe1ef05a28`.
The checked-in script subsequently adds a pinned Delivery-seed hash check; the
captured input already matched that hash. Full order-structure/timestamp and
recovery qualification still remain separate requirements. BicDB v2 has no
speed result yet; the optimized compiler remains active on VM1.

## Next execution checkpoint

Draft PR #840 stacks on #839. Current runnable harness on VM2:
`/mnt/bicdb-v2-harness-a3635cee` (a3635cee9e3a5d62e8b59a9819169399fb95276e).
All 16 Python harness/audit tests pass locally. New BicDB marker fixture and
Rust CI remain pending. The full state audit script now pins both seed hashes.

VM2 script `/mnt/bicdb-v2-fixture-and-trial-20260905.sh` is prepared and syntax
checked, **not started**. It expects the successfully built VM1 artifact copied
and hash-verified at `/mnt/bicdb-v2-pgo-8e29e143-20260905` on VM2. It runs isolated
invalid/duplicate/Delivery fixtures first and only starts the 32-VU fixed-layout
tuned-durable tmpfs screen if every assertion passes. Use a bounded service
(90 GiB MemoryMax, MemorySwapMax=0), preserve its stdout/stderr under `/mnt`,
and inspect any failure before retrying with new trial IDs. Binary source is
8e29e143; harness source is a3635cee, recorded separately.

The VM1 compiler was last observed live as PID 436037, in the existing build
service, with 23.3 GiB cgroup peak and ample target-filesystem space. Continue
polling that service; do not start a duplicate compiler. Its build script emits
the binary SHA-256 on successful completion. VM2 has no active audit/benchmark
after its PostgreSQL reference and financial/expanded audits completed.

## Build completion and first BicDB execution

The optimized build completed successfully in 36m09s, source 8e29e143.
Binary SHA-256: `5814377a990c50330a51d4af4d53db822aa81b7e7deee0d97c752c743e477684`.
The VM2 copy at `/mnt/bicdb-v2-pgo-8e29e143-20260905` matches that hash.
The isolated CLI fixtures passed the -1 invalid-item marker with rollback,
duplicate line amounts and stock counters, and Delivery balance/count.

`pgo-r1` stopped before workload execution because the system service did not
set HOME. No speed result came from it. R2 runs with systemd User=root and
SetLoginEnvironment=yes, retaining the same 90 GiB/no-swap memory limits.
Live service `bicdb-v2-trial-r2-20260905` executes
`/mnt/bicdb-v2-trial-r2-20260905.sh`; log
`/mnt/bicdb-v2-trial-r2-20260905.log`. Artifacts are under
`/mnt/bench-out/codex-bicdb-corrected-v2-20260905/trials/pgo-r2` on VM2.
This trial is running; inspect its result before any repeat.

PostgreSQL's order-structure audit covers all 1,207,650 orders, 12,074,672 lines
and 266,360 pending orders. All structural and Delivery-state checks pass;
the raw nonzero result retains the 24 timestamp exceptions whose exact IDs,
counts, minima and maxima match the independently captured seed. Raw and
qualified results are checked in alongside the financial and expanded audits.
Its audit container was stopped again before starting the BicDB benchmark.

Full Rust CI was explicitly dispatched for the stacked draft branch (its base
does not trigger the main-only PR workflow): run `33969575673`, source f000d4a9.
Check that live run before dispatching another one.

## Verified BicDB v2 baseline and profiling continuation

R2 completed: **432,362.5 database whole-run NOPM**, HammerDB timed NOPM
435,905, corrected workload v2, 32 fixed-layout VUs, 1-minute ramp + 1-minute
measurement, tuned-durable storage-sync on tmpfs with 4-GiB checkpoints. All
864,725 valid New-Order calls and 871,407 Payment calls committed; 8,871
intentional invalid-item requests rolled back, with no unexplained outcomes,
failed queries, deadlocks or serialization failures. Three checkpoints completed;
watermark gap was zero. This is not a persistent-disk/power-loss benchmark.

The retained post-SIGKILL dataset was copied with every file hash verified to
VM2 `/mnt/bicdb-v2-audit-20260905/data`; manifest SHA-256
`a22b1dcb03ad0023bfec1a686d722b28acd9fee05f611d3e19a4ff17f584a679`.
Recovery preserved district and history counts. Financial reconciliation passed
all 480,000 customers and 1,351,407 history rows. Expanded reconciliation passed
all 8,642,094 post-seed lines, 1,600,000 stock rows and 480,000 Delivery counters
in 348.66 seconds. Structure checks cover 1,344,725 orders, 13,439,650 lines and
390,399 pending orders. The only raw failures are the exact 24 original seed
timestamp exceptions, independently matched by identity/count/minimum/maximum.
Raw and qualified evidence is retained in the adjacent evidence directory.

The first expanded audit hit BicDB's host-memory guard during a full-table
filtered COUNT: RSS reached about 83 GB with retained tmpfs data also resident.
This is an unresolved unbounded materialization problem, not a passing audit.
The retry uses the unfiltered total count minus the independently verified
canonical seed count of 4,797,556 lines; bounded district scans still reconcile
every post-seed line. Both exported PostgreSQL and BicDB seed row files have
that same count. Closed datasets were hash-verified and relocated to `/mnt`
to free tmpfs; benchmark R2's original remains untouched. The retry and exact
seed-exception qualification passed; its audit server was then killed before
the next benchmark. All 16 harness/audit tests pass with the bounded count.

VM1 CPU diagnostic R3 recorded 69K samples with zero lost samples, 558.710 MB,
under `/mnt/bench-out/codex-bicdb-v2-cpu-20260905-r3`. Self CPU: allocation
`_mi_page_malloc_zero` 8.41%, `StoredRecord::cells_into` 4.72%, `mi_free` 4.31%,
stored UPDATE 2.19%. The inclusive report is still processing. R2's earlier
recording hung in symbol/build-ID finalization and was killed; its unfinalized
2.2-GB file is failed evidence only. R3 disables build-ID processing and inline
symbolization. All profiled trials are diagnostic and excluded from speed claims.

VM2 service `bicdb-v2-buffered-r1-20260905` now runs the same binary, workload,
seed, VUs, layout and duration with the cpu-ceiling lane (buffered writes and
checkpoints disabled). Script/log: `/mnt/bicdb-v2-buffered-r1-20260905.{sh,log}`;
result: corrected-v2 campaign `trials/buffered-r1/result.json`. This comparison
helps separate setting costs from the old 569,136 figure, whose workload also
differed. It cannot qualify as a durable result. Do not start a duplicate.

CI run 33969575673 for f000d4a9 completed all eight jobs successfully.

## Settings comparison, completed

Same VM2/binary/workload/layout/duration: buffered/no-checkpoint R1 reached
462,075.5 database NOPM (HammerDB timed 479,612), with 924,151 valid New-Order
commits, 9,307 expected invalid requests and all 933,248 Payments committed.
That is 6.87% above tuned-durable R2 in one comparison, about 29.7k NOPM; it
does not explain the entire difference from the older 569k workload result.
The buffered dataset was hash-verified and moved to
`/mnt/codex-retained-tmpfs-20260905-v2/` with an original-path symlink.

Increasing MAX_ACTIVE_WRITES from 24 to 32 with tuned-durable/checkpoints gave
431,373.5 database NOPM (HammerDB timed 437,101), no meaningful gain. All
862,747 valid New-Orders and 872,741 Payments committed; 8,710 intentional
invalids rolled back. Three checkpoints completed and watermark gap was zero.
Both screens retain results and data but have not repeated the full state audit.
Keep the fully audited 432,362.5 result as the qualified baseline.

Full DWARF call-stack processing ran over ten minutes; a depth-16 retry hit its
180-second cap. A one-second/depth-24 sample slice completed successfully but
has incomplete user-space call chains (unknown frames and allocator children
nearly equal to self time). Do not use it to attribute all allocation costs to
specific callers. The full 30-second self-time report remains usable.

PR839 merged as 49b7d3ae after eight CI checks passed. PR840 has been rebased
onto main with an identical resulting tree and retargeted to main. Experimental
inline cell-scratch work is isolated in `/home/benchmark/bicdb-inline-cells`, branch
`perf/inline-cell-scratch`; it is not yet a measured optimization.

## PostgreSQL durability lanes

`postgres_reference.sh` defaults to `REFERENCE_LANE=durable`: fsync and
synchronous commit enabled, 4 GiB maximum WAL size, five-minute checkpoint
timeout. `REFERENCE_LANE=cpu-ceiling` explicitly disables fsync and synchronous
commit and uses 64 GiB/one-day checkpoint limits to keep checkpoints outside
the short comparison run. It is a buffered lane with no durable-commit claim.
Full-page writes, the workload, seed and outcome checks remain the same.
The selected lane is retained in `reference_lane.txt`; `settings.csv` records
the effective PostgreSQL settings. Compare matching client assignments,
concurrency and lanes, and report both whole-run counters and timed HammerDB
numbers separately. A buffered BicDB result must not be presented as a
durability-matched win over the existing durable PostgreSQL reference.
