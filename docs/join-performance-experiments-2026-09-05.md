# Join performance experiments

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

**Latest qualification (September 6, 04:04 UTC):** the fully audited buffered
record remains 499,343.5 whole-run NOPM, with a repeat of 493,478.5. The durable
record remains 456,514.5. The latest CASE column-pruning screen reached
489,948.0 against a matched fresh-profile control of 493,228.5; it did not
improve throughput. The >=600,000 goal remains unmet. See the final section
for the latest exact result files and qualification limits.

**Historical qualification update (17:30 UTC):** the 482,352.5 run failed its full stock
audit. Older outcome screens below used the same pre-existing batch-read
conflict bug. The new fixed candidate reached 489,050.5 NOPM, with full audit
pending. See the latest section for details; chronological pending statuses
in earlier sections are historical.

The fully audited baseline remains432,362.5 whole-run NOPM, tuned-durable on
tmpfs with4GiB checkpoints. Its identical corrected-workload buffered control
is462,075.5. Inline scratch PR841 was set aside without merging:432,991.5 in
one durable screen, only0.15% different. All valid NewOrder/Payment calls
committed in these screens; the nonwinning inline screen did not repeat full
state audits. Its branch retains evidence and binary hash279b9bd9….

## Integer conjunction hashing (PR842)

Source0db4cf1bf650a863d0604263dfa6afd876369c12, independently based on main.
All284SQLunits and17cell-row/3corrected-workload/5routine-type integration tests
pass; all8CIjobs passed in run33974018329. Extract a necessary equality within
AND, admit plain integer keys, and evaluate the complete ON condition for each
candidate. Noninteger keys retain the existing execution path; no key is
extracted from OR. The10-line conjunction test checks10pairs instead of100.

Local optimized build completed22m00s with Rust1.96, fatLTO/codegen1/x86-64-v3,
relocations and the same old Payment-v1 PGO profile as the baseline. Source of
profile SHA2930c42fad14ff53bd5d8a4248758aee02387504304b67cee7384c4e35eed9b1.
Profile predates v2, so hash-mismatch warnings are retained in the build log.
The baseline was built on VM1; this candidate was built locally with the same
Rust settings. Exact candidate binary SHA256:
`5253b88be525789c6dc04aabd6692e11390c811b7829e793bd0000a605c21859`.
Preserved local artifact:
`/home/benchmark/bicdb-build-artifacts/20260905/bicdb-hash-conjunction-0db4cf1b`;
VM2 artifact `/mnt/bicdb-hash-conjunction-pgo-0db4cf1b-20260905` matches.
Do not treat the reused local Cargo target as the immutable artifact.

The initial deploy watcher stopped at its guard because the dataset-copy job
was still running. After that job finished, deployment resumed from the staged,
hash-checked binary. CLI invalid/duplicate/Delivery fixtures passed.
Layout-A buffered/no-checkpoint trial `hash-conjunction-buffered-r1` reached
475,332.5 database NOPM (HammerDB timed492,003),2.87% above the462,075.5 control
in a single comparison.950,665valid NewOrders and961,638Payments committed;
9,768expected invalids rolled back, with no unexplained failures/watermark gap.
A control repeat and full state qualification remain pending.

## Diagnostics and subsequent work

The completed32VU time-profile run is excluded from speed claims. Its raw log
contains32individual VUs followed by an aggregate summary; avoid double-counting.
835,868NewOrder calls average2.213ms;835,283Payments average1.167ms. NewOrder is
52.66% of summed instrumented procedure time; Payment27.75%. The wrapper's
exclusive time is reported separately (~0.05ms each), not as another transaction.

The100sequential NewOrder exec/plan diagnostic is also excluded.99were valid.
Combined stock/line CTE signature04d1d6ec3de952f0 (length1848) totals65.994ms;
for a10-line request it records80rows,110candidate pairs and21index lookups.
100pairs belong to the item/warehouse conjunction. Loop timing includes nested
trace overhead and must not be added to its children's time. Raw trace is
compressed alongside the timing report; the remote diagnostic seed copy remains
in `/mnt/bench-out/codex-bicdb-v2-neword-trace-20260905` with its server stopped.

PR843 separately binds materialized-join predicates once per stable layout.
Sourceaf2de48a4e41a1a1b6367b8a9e887dd37ac5e277;283SQLunits and25integration
checks passed. VM1 service `bicdb-bound-join-predicate-pgo-build-20260905` builds
in `/mnt/bicdb-bound-join-predicate-pgo-target-20260905`,64GiB/no-swap. Its
source and logs have the same naming stem under/mnt. No speed result yet.

A combined branch `/home/benchmark/bicdb-join-combined` applies both changes. Its local
build service `bicdb-join-combined-pgo-build-20260905` reuses the hash Cargo
target after preserving the hash artifact.32GiB/no-swap, two jobs. Source/HEAD
must remain frozen during compilation.286 SQL unit tests and 25 focused integration checks passed. Draft PR844
contains the combined change at da6712334553efd6b4e6522729d31a783cbc3740;
performance results remain pending. All eight CI jobs for PR843 passed.

## Client assignment screen

Layout A concentrates most clients in a few warehouses. A separate balanced
assignment gives each of16warehouses two of32clients, on districts1and6.
VM2 input `/home/benchmark/bench-logs/terminal-layout-balanced-16w-32vu-20260905.tsv`, SHA
`e43383a55a2e03156f35376e52fc606dbb48d8b551bdff1d689695e5746b87d9`.
This changes client placement, not SQL semantics or warehouse count. Results
must be labelled separately; do not attribute its gain to the engine change.
VM2 service `bicdb-hash-conjunction-balanced-r1-20260905` first hash-verifies and
relocates the stopped layout-A hash dataset, then runs the balanced buffered
screen. Script/log `/mnt/bicdb-hash-conjunction-balanced-r1-20260905.{sh,log}`;
trial result `hash-conjunction-balanced-r1` in the corrected-v2 campaign.

The closed latency and inline datasets were hash-verified and moved from tmpfs
to `/mnt/codex-retained-tmpfs-20260905-v2`, preserving original-path symlinks.
The first copy stopped at a4GiB memory cap with an8.2GiB partial destination;
originals stayed intact. Retrying with32GiB finished both copies and verified
all hashes before removing originals. Tmpfs returned to22GiBused/42GiBfree.
Local disk had354GiBfree; targets were5.9GiB(release) and3GiB(tests).

A separate original-binary probe exposed a pre-existing CTE numeric join issue:
1.00(numeric6,2) and1.000(numeric6,3) match zero rows in an AND join, whereas
Int1 against numeric1.000 matches one. VM2 repro
`/mnt/bench-out/codex-bicdb-v2-numeric-join-control-20260905` preserves SQL,
results and original binary hash. The new integer-only guard avoids extending
untyped-label coercion problems; this does not claim general numeric CTE join
correctness. Corrected workload join keys are integers.

## Continuation at 16:02 UTC

The balanced hash screen completed at **482,352.5 whole-run NOPM**
(HammerDB timed 495,282), with 964,705 valid NewOrders, 9,838 expected
invalid rollbacks, and 976,675 Payments. All valid calls committed and the
watermark gap was zero. This is 1.48% above the hash layout-A screen in
one comparison, with different placement; full state audit is still pending.
Its original tmpfs data was hash-verified and relocated with a symlink.

The baseline buffered reverse control `buffered-r2` is running on layout A.
Service `bicdb-hash-conjunction-durable-r1-20260905` waits for that control,
requires a valid result, preserves its dataset, then runs the hash candidate
with durable sync and 4 GiB checkpoints on layout A. Benchmarks are serialized.
The combined local build and the bind-once VM1 build remain active;
source trees are frozen until compilation completes. No performance candidate
has been merged or accepted as satisfying the 600k goal.

## Matched follow-up results

All of these are corrected v2, layout A, 32 clients on VM2, with exact
client/DB NewOrder and Payment completion and zero watermark gap:

| Candidate | Lane | Whole-run NOPM | HammerDB timed NOPM | Qualification |
| --- | --- | ---: | ---: | --- |
| Original baseline, reverse control `buffered-r2` | Buffered, no checkpoints | 454,319.5 | 469,489 | Outcome screen |
| Hash conjunction `hash-conjunction-durable-r1` | Durable, 4 GiB checkpoints | 435,123 | 448,198 | Three checkpoints completed; outcome screen |
| Combined `join-combined-buffered-r1` | Buffered, no checkpoints | 468,612.5 | 486,884 | CLI fixture and outcome screen |
| Bind once `bound-join-predicate-buffered-r1` | Buffered, no checkpoints | 470,803.5 | 484,987 | CLI fixture and outcome screen |

The combined and bind-once changes do not demonstrate an additive gain over
the hash-only screen. Full state audits have not been repeated for these
candidates; no performance PR is accepted or merged. The fully qualified
baseline is still 432,362.5.

Combined build completed in 14m50s with source
`da6712334553efd6b4e6522729d31a783cbc3740`, binary SHA256
`b6b5c06376a47e0e37ca9ecb9cd55b6e05dc483c0d088c09ef5776be9784d945`.
Bind-once build completed in 36m12s with source
`af2de48a4e41a1a1b6367b8a9e887dd37ac5e277`, binary SHA256
`ed7108a44967a345bb471b8e3e632ca0c926a62e76715c4cbde5c02a9e461ef4`.
Both immutable artifacts are retained locally in
`/home/benchmark/bicdb-build-artifacts/20260905` and hash-verified on VM2.

The combined source remains frozen for a new instrumented build, service
`bicdb-v2-combined-profile-build-20260905`, local target
`/home/benchmark/bicdb-v2-profile-target-20260905`, 32 GiB cap, no swap, two jobs.
This will allow compiler training on corrected v2; the initial benchmark
builds all used the older Payment-v1 profile.

The 48-client balanced hash screen reached only 413,779 NOPM, despite exact
commit completion. CPU migrations rose to 1,210,245, versus 161,789 for the
32-client balanced screen. A 32-writer variant is running, followed by a
16-client screen. These change client placement/concurrency and cannot be
credited as engine-only improvements. Layout 48 SHA256
`c69c405877261b13e3ea0a6f3d4dfe2104b38476e0041a24244c0a77648e6d26`;
layout 16 SHA256
`fe92841ed1337008b2e580c66b1cd8b84c92283333d131dbc6acf5553ec2f05f`.

VM1 CPU diagnostic r4 uses the hash candidate and 49 Hz sampling for five
seconds with 65,528-byte DWARF stacks (343 MB capture, excluded from speed
claims). Its inclusive report has a 180-second limit. Earlier r2/r3 diagnostic
datasets were hash-verified and moved to `/mnt/codex-retained-tmpfs-20260905-v2-cpu`
with original-path symlinks, freeing tmpfs for training. Both existing VMs
remain Spot instances; no new machine was provisioned.

## Fixed candidate and evidence retention (17:30 UTC)

The expanded audit of `hash-conjunction-balanced-r1` found one stock mismatch
among 1.6 million stock rows. Warehouse 4/item 75176 recovered quantity/YTD/
order-count/remote-count 38/30/5/0 instead of 15/53/9/0. Financial reconciliation
passed, but that does not qualify the run. Structural auditing did not run
after the stock failure. All 39,559,115 WAL frames passed CRC verification.
The trace proves a live stale batch read overwrote an earlier committed stock
update; recovery then skipped inconsistent subsequent patches. The bug was
reproduced on main and fixed in PR846, now merged after all eight CI checks
passed. See `docs/batch-point-read-conflicts-2026-09-05.md` on main for the
regression and trace. The old 432,362.5 baseline dataset passed its own full
audit, but its engine also predates this rare bug fix.

The hash-plus-fix candidate is source
`a8b9482edba123b11e96cdbbec97df021cf255b1`, binary SHA256
`bbd128b79a51f28615aaddd84da0f32f79d73f81b09a0b150cc4a0221e732bde`.
Its balanced 32-client buffered/no-checkpoint trial
`hash-batch-conflicts-balanced-r1` reached **489,050.5 whole-run NOPM**
(HammerDB measured-minute 511,489). Exactly 978,101 valid NewOrders and
987,408 Payments committed; 9,849 expected invalid-item calls rolled back.
No unexpected failures or watermark gap occurred. This is a single screen,
1.39% above the old 482,352.5 screen, not a fully audited or durable result.
Recovery and full state auditing are running. The optimized binary still
uses the old Payment-v1 profile. A fixed-source instrumented build on VM1
will supply new corrected-v2 training; it has not yet produced training data.

The 48-client/32-writer variant reached 410,829.5 NOPM, and 16 clients reached
353,830.5. Neither improved the 32-client placement.

VM1 was deallocated and restarted as the same Spot instance. Its ephemeral
/mnt and /dev/shm contents were lost, including recent raw CPU/GDB profiles.
Locally saved binaries and extracted reports survived. New source, scripts,
logs and final binaries use its persistent OS disk; rebuildable Cargo targets
still use /mnt. VM2's baseline and failed balanced datasets were archived
locally and every source file hash verified. Other retained screens have
also transferred locally; full per-file verification is in progress. The new
489k dataset is being archived separately. No 600k qualification is claimed.

## Audited records and fresh-profile screens (19:30 UTC)

The fixed 489,050.5 buffered result above subsequently passed the full recovery,
financial, stock, amount, Delivery-counter and structural audits. The structural
check matched exactly the 24 independently identified seed timestamp exceptions.
The retained full audit archive SHA256 is
`81b241e00fd222ff7147a4f0242b90747fad75df30dac8f642c76fd92b288b65`.

The same fixed binary reached **456,514.5 whole-run NOPM** with durable writes
and 4 GiB checkpoints: 913,029 valid NewOrders, 922,297 Payments and 9,150
expected invalid-item rollbacks; all valid calls completed and all three
checkpoints completed. Full recovery and state audits passed. VM2 was then
Spot-evicted before the complete audit report transferred locally. The original
31-file dataset had already been archived and every file hash verified. The complete audit passed again on a verified copy of that dataset. The
new locally retained report archive SHA256 is
`13e8f791f5655be4eadb592843fdc3770972378c4eb428d592e1ce4667220fd6`;
the old zero-byte audit transfer is not usable evidence. These tmpfs runs do not test
host or power-loss durability.

Both existing VMs were restarted as Spot instances after their respective
interruptions. VM2 lost ephemeral `/mnt`, `/dev/shm` and `/tmp` contents; retained
local archives restored the canonical seeds and artifacts. New reports and
scripts live on its persistent OS disk, with stopped datasets archived locally.

The post-restart matched buffered comparison used the same corrected workload,
32 balanced clients, 16 warehouses and explicit non-durable settings:

| Candidate | Whole-run database NOPM | Valid NewOrders | Payments | Full state audit |
| --- | ---: | ---: | ---: | --- |
| PostgreSQL 18.6, buffered | 446,932.5 | 893,865 | 905,870 | Passed |
| Fixed BicDB A8, old compiler profile | 494,663 | 989,326 | 999,726 | Passed |
| Fixed BicDB A8, fresh corrected-v2 profile | 492,647.5 | 985,295 | 995,263 | Pending |
| Fresh A8, 32 writer slots | 492,434 | 984,868 | 995,092 | Pending |
| Borrowed type-resolution iterator | 473,768 | 947,536 | 959,782 | Pending |
| Shared CTE row backing | 492,130.5 | 984,261 | 991,553 | Pending |

Every listed run passed the database/client completion gate with no unexpected
NewOrder failures or Payment commit gap. These are individual screens, not a
statistical estimate. The old-profile A8 screen was about 10.7% above PostgreSQL;
the later compiler-profile and writer-count screens did not improve it.
PostgreSQL's live container inspection verified its 90 GiB memory limit and
no-swap configuration. PostgreSQL passed the full recovery and state audits, with exactly the 24
seed timestamp exceptions independently matched. Its locally retained audit
archive SHA256 is `311b0f47b11c2739a08308ba3745d8c2c7800b3b66c42ba2dff2cd32ca85da72`.
The BicDB control subsequently passed the full financial, stock, amount and
structural audits, including the exact 24 independently matched seed timestamp
exceptions. The fully audited buffered record is now 494,663 NOPM. Its locally
retained audit archive SHA256 is
`86f4edf9f820a1d74615a32b43094b6fc494aac55cf12c52ada86cba08187440`.

Fresh A8 binary SHA256:
`568fe05c21b6e6d61c7dd1374023cedbdd64e68e2c5fc6aaa187c8ea280960b8`.
Borrowed type iterator binary SHA256:
`20857f20810500758671df4a84315246c83701897830c2677392c071f20259c0`.
Both use Rust 1.96, fat LTO, x86-64-v3 and frame pointers with corrected-v2
profile SHA256 `a1fa1651cc3e51c92602fb11fdb70b83b057c573d639b6380d5db2b2f31a26ec`.
The harness metadata's `source.rustc` field records the runner-installed
Rust 1.98, not the compiler used to build these copied binaries. The old-profile
control lacks the new frame-pointer setting, so the control comparison does not
isolate compiler training alone. Type-iterator runtime source `c32d30f1` matches
the runtime crates on its rebased draft PR848 (`9b0fe2c5`). Its first two launch
attempts failed before server startup because the transient service lacked
`HOME`; the successful trial is `type-columns-balanced-r3`.

A 15-second, 49 Hz optimized CPU diagnostic collected 19,193 samples with zero
lost samples. Allocation, copying, string comparison and row decoding remain
substantial costs. The profile was excluded from throughput claims. New CTE
sharing and owned UPDATE-row experiments are being tested separately; no speed
improvement is claimed for them. The 600k target has not been reached.

The shared-CTE candidate (`fb3eeb9f`, PR849) completed 287 SQL unit and 25 focused
integration tests plus the live CLI fixture. Its optimized binary SHA256 is
`ca387d29a65401deed95b36521a004270e4fc09f3df7d88432d6674c8c76a99d`.
It reached 492,130.5 whole-run NOPM, essentially unchanged, with 9,934 expected
invalid-item rollbacks and all valid calls committed. Mean server CPU was
22.506 cores versus 23.760 for the fresh-profile control; a throughput gain
has not been demonstrated.

A separate fresh-A8 client CPU diagnostic (excluded from speed records) sampled
Docker CPU counters and host CPU counters once per second. Across 112 intervals
where the client used more than one core, HammerDB averaged 1.638 cores and the
host averaged 27.641 busy cores. This does not establish the client as the main
throughput limit. The diagnostic completed 959,521 valid NewOrders, 969,301
Payments and 9,785 expected invalid rollbacks with exact valid-call completion.

The owned UPDATE target-row candidate (`cfd053d2`, PR850) passed 285 SQL unit and
25 focused integration tests. Its optimized build completed in 23m24s, with
SHA256 `afc5c3dd8cff04fa9aa2d8b78eba70d333b518002b79b074f9b02ccc2696d9a2`.
It reached 468,977.5 whole-run NOPM, with 937,955 valid NewOrders, 948,751
Payments and 9,337 expected invalid-item rollbacks. All valid calls completed,
but throughput regressed and the candidate remains unmerged.

An LLVM BOLT post-link experiment is in progress. Exact-binary baseline
instrumentation training passed the completion gate; it is excluded from speed
records. The cumulative profile includes startup as well as workload, so it is
not a workload-only profile. No BOLT speed improvement has been measured yet.

BOLT 20.1.2 produced baseline and owned-UPDATE binaries using profiles collected
from their exact input binaries. An additional baseline candidate uses the
earlier 19,193-sample steady-workload CPU recording, with matching build ID.
The non-LBR converter required `--nl` and suppression of displayed call chains
(`perf script --hide-call-graph`); 20.1% of samples lay outside the executable,
and 774 functions received sample coverage. This sparse profile is an
experimental alternative to the startup-inclusive instrumentation profile.
The first baseline BOLT fixture passed; throughput tests are pending.

The startup-inclusive BOLT baseline completed at **497,031.5 whole-run NOPM**:
994,063 valid NewOrders, 1,003,204 Payments and 9,925 expected invalid rollbacks,
with no unexpected failures or valid-call commit gaps. This is 0.48% above the
494,663 audited control; a repeat and full state audit remain necessary before
claiming a confirmed improvement. Binary SHA256:
`31cfba94b24adaff3c8089026fc586c1ec4936f00738b4c19f9343c8bf931852`.

The sampled-profile BOLT baseline regressed to 436,615 whole-run NOPM:
873,230 valid NewOrders, 879,953 Payments, 8,724 expected invalid rollbacks.
The exact-profile owned-UPDATE BOLT candidate reached **499,343.5 NOPM**:
998,687 valid NewOrders, 1,006,340 Payments and 10,147 expected invalid rollbacks.
Both passed the valid-call completion gate. The 499k screen is 0.95% above the
audited 494k control and remains provisional pending a repeat and full audit.
Owned-UPDATE BOLT binary SHA256:
`a6130b9f6a843bf9f24bba062a4c06afa7abcf35c1dbe56dde297ecfee16252f`.

## Exact-profile UPDATE qualification (September 6, 01:23 UTC)

The 499,343.5 screen passed recovery and all financial, stock, amount, counter
and structural checks. The audit covered 480,000 customers, 1,600,000 stock
rows, 9,986,827 post-seed order lines, and 1,478,687 orders. Exactly the same
24 pre-existing timestamp exceptions matched the independent seed capture.
All 31 source dataset hashes were verified in both the retained local archive
and the audit copy. The locally retained full audit archive SHA256 is
`4146b36b1d969b0b2b5887f5644154d9a0662070d2e16ed41687423a7d4ebec6`.

The repeat reached 493,478.5 NOPM, with 986,957 valid NewOrders, 997,102 Payments
and 9,906 expected invalid-item rollbacks. All valid calls completed, but the
repeat did not exceed the audited 494,663 control. This does not establish a
consistent throughput improvement from the UPDATE change or from BOLT.
PR850 remains unmerged; all eight CI jobs passed. The 600k target is not met.


## Subsequent screens (September 6, 02:47 UTC)

All three screens below used the corrected v2 workload, the same 16-warehouse
seed, 32 replayed balanced clients, 24 concurrent writers and one minute each
of ramp-up and measurement. Storage was buffered on tmpfs, with checkpoints
disabled. NOPM here is the database district-counter delta over the configured
two-minute whole run, consistent with the earlier comparisons. It is distinct
from HammerDB's timed-minute figure.

| Candidate | Whole-run NOPM | Valid NewOrders committed | Payments committed | Expected invalid-item rollbacks |
| --- | ---: | ---: | ---: | ---: |
| Workload-only BOLT, owned UPDATE | 485,418.5 | 970,837 | 978,245 | 9,763 |
| Shared CTE rows + owned UPDATE, PR852 | 482,132 | 964,264 | 975,391 | 9,629 |
| Shared CTE column metadata, PR853 | 489,003.5 | 978,007 | 987,264 | 9,785 |

Each screen had 100% valid NewOrder and Payment completion, no unexplained
failures and no watermark gap. None exceeded the audited 494,663 control or
the 499,343.5 best. Full state audits were not repeated for these nonwinning
screens; their completion checks are not substitutes for full audits. PR852
and PR853 remain draft and unmerged. PR853 is stacked on PR852 and has no CI
jobs; its 314 passing local tests are not CI qualification.

Exact result files, including settings, binary hashes and completion counts:

- [Workload-only BOLT](evidence/corrected-workload-v2-2026-09-06/move-update-rows-bolt-workload-balanced-r1.json)
- [Combined CTE/UPDATE](evidence/corrected-workload-v2-2026-09-06/shared-cte-owned-balanced-r1.json)
- [CTE metadata](evidence/corrected-workload-v2-2026-09-06/cte-column-metadata-balanced-r1.json)

The result files' `run.source.rustc` describes the benchmark host's installed
compiler. These retained binaries were built locally with Rust 1.96; the build
logs, rather than the host compiler field, identify the artifact compiler.
The workload-only BOLT profile combines independently cleared periodic
instrumentation intervals after warm-up. Cumulative profile subtraction was
rejected because reconstructed edge counts were not monotonic.

All 31 files in each dataset archive matched the source manifest. Retained
local dataset archives and SHA256:

- `move-update-rows-bolt-workload-balanced-r1.tar.gz`:
  `345fe96c1b5a625dc0b7eff329179184e2b67165085dc26792dc4d6d0135d9ff`
- `shared-cte-owned-balanced-r1.tar.gz`:
  `b1158cdf2810cede7007d6f18ba6a1eecfb01023900d76afc9158ffd295cbb1e`
- `cte-column-metadata-balanced-r1.tar.gz`:
  `df8042f988e979c1f95deee6692cb537a58f5fdc344329eb43868c4d74a05ccb`

An additional optimized combined-binary CPU diagnostic captured 18,941
samples with no lost samples. Its build ID matched the retained executable.
Allocation/free routines accounted for about 12% of sampled self CPU time,
record decoding 4.59%, and generic CASE evaluation 2.72% including callees.
The diagnostic is excluded from throughput records. Its dataset also passed
all 31 source-file hash checks; archive SHA256:
`0a4927f82aaa5157cfa029b40a8d6f57c3397691dc48994031a9d1ab7d257069`.

The next independent candidate, PR854 at source
`124d48d4e221646237fa53d09db06f445d895a5c`, caches bound routine RETURNING
projections while checking catalog generation and row layout. It passed 287
SQL unit and 25 focused integration tests, with three existing ignored tests.
Its optimized build and subsequent live qualification are pending. No speed
gain is claimed, and the >=600k goal remains unmet.


## CASE column pruning and post-restart control (September 6)

PR856 avoids copying unselected district strings for eligible stored UPDATE
RETURNING CASE expressions. The release binary uses production source
`5cfabc85ce01684ef0263247ecb0621d4fb3df89`. A subsequent test-only commit,
`f6825aa46d043dd9f8db1c9305f8f4b85fcf4ba3`, adds an exact corrected-NewOrder
regression test; it is not part of the measured binary. All eight CI checks
passed on that test commit. The exact-binary rollback, duplicate-item stock
and amount, and Delivery fixture also passed.

VM2 was evicted before this screen. The same Spot VM was restarted, and the
canonical seed was restored from its preserved local archive. The restored
seed manifest and terminal assignments matched the prior inputs. A fresh
control was measured on the restarted VM before the candidate.

| Screen | Whole-run database NOPM | Valid NewOrders committed | Payments committed | Expected invalid-item rollbacks |
| --- | ---: | ---: | ---: | ---: |
| Fresh-profile A8 control | 493,228.5 | 986,457 | 996,918 | 9,904 |
| CASE column pruning | 489,948.0 | 979,896 | 991,104 | 9,748 |

Both used corrected-v2, buffered storage, 32 clients, 16 warehouses, identical
balanced terminal assignments, one minute of ramp-up and one timed minute.
The database NOPM shown here covers the whole two-minute workload interval.
All valid NewOrder and Payment calls committed, with zero watermark gaps.
These outcome checks do not substitute for a full financial/order audit;
no full audit was run for these nonwinning screens. PR856 remains unmerged.

Exact result files:

- [Fresh control](evidence/corrected-workload-v2-2026-09-06/a8-fresh-pgo-post-eviction-r1-result.json)
- [CASE pruning](evidence/corrected-workload-v2-2026-09-06/case-column-pruning-balanced-r1-result.json)

The candidate binary SHA-256 is
`f6547124bbd3b1e8f533156d172ee3eb1a4ee10dde3fd5b14abca83a336f2308`;
the control is
`568fe05c21b6e6d61c7dd1374023cedbdd64e68e2c5fc6aaa187c8ea280960b8`.
Both measured binaries used Rust 1.96.0; the result metadata's Rust 1.98.0
identifies the harness host toolchain, not the compiler used for these binaries.

The local fixture and screen reports archive is
`/home/benchmark/bicdb-retained-evidence/20260906/case-pruning-postrestart-screen-reports.tar.gz`,
SHA-256 `8675fa3fca39b54cca96ccbd8afdc796253e507b7bbf12b73bfb97fae95b6ee3`.
Remote retained dataset copies and both local archives passed all 31 source-file
hash checks. Local dataset archives under the same evidence directory:

- Control: a8-fresh-pgo-post-eviction-r1.tar.gz, SHA-256
  `546a6112f22aff968e6dc0e321c2e81e5d2af37028c8656c765baa421ff5dedc`.
- Candidate: case-column-pruning-balanced-r1.tar.gz, SHA-256
  `fa71f4f606054c11d3aec7861b993284f7adb5530a5513507a2157b896b3cad1`.

A separate instrumented build is in progress to collect an exact-source
compiler profile; it has no new throughput result.
