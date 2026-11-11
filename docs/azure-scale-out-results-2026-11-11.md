# Azure four-to-five-node experiment: actual results

**Automatic movement observed; capacity gain and fault-safe rebalance remain unqualified.**
The owner authorized an isolated $25/eight-hour Azure experiment. The experiment
ran on 2026-11-11 UTC with optimized **1.0.461-beta**, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`, binary SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
No database engine feature or production deployment was changed.

## Actual configuration and evidence

Five North Europe `Standard_D2s_v3` guests, 2 vCPU/8 GiB each, each used a
128 GiB Premium data disk with Azure host caching `None`, ext4/noatime,
`server_paged`, requested/observed 256 MiB pools, fsync, mTLS, RF3 and 64 ranges.
Quota prevented the proposed larger six-VM deployment. Distinct OS instances
were verified; five distinct physical hosts/failure domains were not. Native
traffic ran on data node n0, where its ACK ledger was fsynced at phase
completion. Only the separate external probes fsynced each ACK off-cluster. External per-attempt/per-ACK fsync probes invoked
member clients through SSH; they were not a sixth dedicated load generator.
The small, growing corpus fits caches. These limits preclude independent
physical-host, saturation or large-data qualification claims.

The [actual infrastructure](evidence/azure-infrastructure-2026-11-11/README.md),
[build identity](evidence/azure-release-artifact-2026-11-11/README.md),
[completed short smoke](evidence/azure-scale-smoke-2026-11-11/README.md),
[longer failed growth](evidence/azure-long-growth-failure-2026-11-11/README.md), and
[guest-poweroff probe](evidence/azure-vm-poweroff-2026-11-11/README.md) retain
source/binary identities, sanitized measurements, raw-artifact digests and
portable verification scripts. Private database/log archives were copied to the
external controller before restart or teardown; account identities, network
addresses and generated keys are not published.

## Maturity demonstrated by this experiment

| Requirement | Actual observation | Status and remaining blocker |
|---|---|---|
| Add a node without manual range assignment | n4 joined the existing four-node cluster during native append/read traffic. | Implemented and VM exercised; no ordinary SQL gateway qualification. |
| Automatically move existing data | At the first recorded converged checkpoint, 38 relocations to n4 reported 537,266 snapshot bytes. Five prejoin payloads matched through routes naming n4 as leader. | Real data movement, not membership alone. Snapshot counters are not total physical I/O. |
| Redistribute leadership | First recorded converged five-node checkpoint: 13/13/13/13/12 leaders; RF3/192 replicas, zero active/failed/unavailable ranges at that checkpoint. | Count balancing observed; continuous availability and request-load balancing not established. |
| Balance measured bytes/hotspots | All sampled range byte/QPS estimates and node used-byte inputs were zero. | Default live load collection and bytes-only redistribution trigger missing; see [source correction](distributed-placement-load-inputs-2026-11-11.md). |
| Preserve acknowledged writes | Short process-fault and guest-off ledgers reconciled under the scopes below. Longer growth initially failed two ACK reads; both values subsequently matched after restart. | Narrow positive reconciliation, with failed availability preserved. General acknowledged-loss safety remains unqualified. |
| Fault-safe online rebalance | Short timed process kill occurred after convergence; guest-off probe and longer failed growth are separate trials. | No complete snapshot/catch-up/promotion/cleanup physical fault matrix. Diagnostic convergence failed its 300-second bound. |
| Increase sustained aggregate capacity | Short-run five-node goodput was lower. Longer growth failed before a steady-five comparison. | No capacity gain demonstrated. |
| Keep ordinary PostgreSQL applications unchanged | This experiment used the existing immutable-event append/get binding. | General transparent distributed SQL, joins and recoverable multi-range SQL transactions remain missing. |
| Production qualification/deployment | Frozen binary was observed on disposable test guests only. | No production admission, release qualification or production deployment. |

## Successful-call latency and failures

All values are milliseconds for successful logical calls only. Failed calls are
counted separately; native failed-call latencies and individual successful-call
latency samples were not retained. Native percentiles are reported aggregates,
not independently recomputable from published per-call samples. Successes/s
counts reads plus writes. The short run included extra SSH append/observation traffic;
the longer native comparison removed extra append probes but retained observation
costs and an n0-hosted client. Traffic duration includes draining in-flight calls;
verification follows it. These are observed VM measurements, not physical-device
benchmarks or an equal-latency/error-budget capacity result.

| Trial/phase | Traffic seconds | Successful ops/s | Failed calls | ACK writes | Write p50 / p95 / p99 ms | Read p50 / p95 / p99 ms |
|---|---:|---:|---:|---:|---|---|
| Short steady 4 | 60.54 | 14.39 | 0 | 438 | 507.808 / 758.457 / 1203.079 | 14.294 / 50.508 / 100.368 |
| Short growth 4→5 | 60.71 | 8.65 | 0 | 265 | 710.155 / 2111.757 / 3942.743 | 12.653 / 87.528 / 239.896 |
| Short steady 5 | 60.65 | 7.40 | 0 | 227 | 758.353 / 2551.845 / 4327.311 | 11.804 / 64.887 / 147.790 |
| Short process failure | 90.90 | 6.33 | 6 | 287 | 791.064 / 3907.361 / 12873.140 | 12.203 / 81.310 / 223.491 |
| Short recovered 5 | 63.18 | 1.95 | 0 | 64 | 3505.353 / 8935.123 / 11856.031 | 10.352 / 96.335 / 146.509 |
| Longer steady 4, 16 workers | 907.00 | 8.37 | 0 | 3804 | 3097.119 / 8003.782 / 11135.865 | 96.034 / 595.940 / 904.493 |
| Longer growth 4→5, aborted | 927.50 | 2.96 | 404 | 1223 | 4862.509 / 15487.779 / 21462.305 | 120.009 / 946.799 / 1939.600 |

The longer run never reached steady-five, its later fault, or recovered-five
phases. Its native verifier collapsed read errors, absent values and mismatches
into two unverified ACK IDs. The original failure, all known ACK identities,
20 bounded error samples out of 404 failed calls, and stopped database images
were preserved. After one bounded same-root restart, both IDs returned the exact
expected values through all five member client contexts (10 matching routed reads,
not five independent replica copies). Policy convergence still failed its
300-second diagnostic bound; a sampled state had 61 active and three failed moves.
Those failed moves to n4 reported connection resets while learner-allocated.
No successful subsequent read erases that failed availability/verification trial.

Full post-restart baseline/growth ledger verification is reported in the final
long-trial evidence; it must be distinguished from the original failed pass.

## Observed durability and recovery intervals

All **1,281** short-run native ACK writes were verified after process fault and
subsequently after the guest-poweroff probe. All **107** external per-ACK fsynced
writes were verified after whole-cluster process reopen and the guest-off probe.
The guest-off probe added **38** ACK writes, all subsequently matched. Three
uncertain probe calls were retained separately: one later whole value present,
two absent. Timeouts were not treated as rollback acknowledgments.

Five fresh writes started after confirmed abrupt n4 guest stop and acknowledged
before its first start request. First such ACK was **97.495 seconds after confirmed
stop**, 105.608 seconds after the stop request. It is a measured probe interval,
not first possible server availability or a certified RTO. Selected range RF3
voters were n4/n0/n1; two survived. Guest boot-ID change and a cold database archive
support guest RAM loss/reopen, not physical storage-device power loss. Sampled survivor
status reported all members dead while fresh writes succeeded;
status/liveness and foreground availability need separate qualification.

Zero observed lost ACKs in bounded reconciliations is not universal RPO=0.
The complete production crash/recovery, relocation-phase, stale-writer fencing,
WAL/index/FTS/queue/audit and unchanged-business-SQL gates remain open. See the
[VM-fault diagnostic follow-up](distributed-vm-fault-followup-2026-11-11.md) for
source hypotheses and required regressions; no causal engine repair is claimed.

The longer baseline's sampled server process write counters grew about 159 GB
across four members. They include background/control/WAL and verification work.
They are not isolated range-log write amplification or proof of the bottleneck.
The [source cost review](distributed-range-log-costs-2026-11-11.md) identifies
whole-catalog durable replacement at ordinary range-write transitions as a
performance hypothesis requiring isolated measurement; it does not diagnose
the failed trial or authorize weakening durability.

## Release and qualification boundary

Main advanced to 1.0.462-beta during this experiment. Documentation/evidence and
test-runner delivery versions are separate from the frozen 1.0.461 runtime.
Neither a source version bump, this optimized build, portable evidence checks,
nor the observed guest deployment qualifies the newer integrated binary.
The original [stronger experiment plan](scale-out-experiment-2026-11-11.md)
and 1/5/20 TB certification gates remain unpassed.
