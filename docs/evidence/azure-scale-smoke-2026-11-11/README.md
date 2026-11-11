# Initial Azure four-to-five-node smoke result

**Completed exploratory VM run; not production qualification or a capacity win.**
On 2026-11-11 UTC, five isolated Azure Linux VMs exercised the default automatic
range controller with optimized **1.0.461-beta**, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`, binary SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
See the separate [build identity](../azure-release-artifact-2026-11-11/README.md).
This directory covers only the initial completed growth/process-failure smoke.
Later VM fault probes, longer trials, infrastructure deletion and bills are
separate evidence and must not be inferred from these files.

## Environment and workload

The subscription's feasible allocation was five **Standard_D2s_v3** guests in
North Europe, each 2 vCPU/8 GiB, rather than the proposed six-VM/22-vCPU setup.
Guest observations show Ubuntu 24.04.5 LTS, kernel `7.0.0-1017-azure`, Intel Xeon
E5-2673 v4, Python 3.12.3 and OpenSSL 3.0.13. Dedicated test data mounts were
ext4 with `rw,noatime`. Guest image metadata records the resolved image version;
`latest` in the request alone is not immutable image provenance.

The native workload used RF3, 64 ranges, mTLS, fsync enabled, four client workers,
1,024-byte payloads (1,079-byte encoded values), five-second RPC timeout and
one-minute phases (90 seconds for the process-failure phase). `server_paged` is
both requested and observed in persisted format files at four nodes, five nodes
and the later restart. A **256 MiB** paged pool is requested and observed in
process environment; this is not an independent buffer-pool telemetry measurement.
Declared capacity was 100,000,000,000 bytes per node; this does not measure capacity.
This tiny corpus fits caches and is far below the proposed 8 GiB working set.

Five different OS-machine hashes were observed, but **five independent physical
hosts/fault domains were not established**. These are cloud VM/process tests,
not physical-device or host power-loss evidence. The native workload client ran
on data node n0 and fsynced its acknowledgment ledger at phase completion. A
separate controller also ran a per-attempt/per-ACK fsynced ledger, invoking native
RPC clients through SSH on data members. It is not a dedicated sixth load VM or
a direct independent-network load generator. SSH/session and observation costs
remain part of these exploratory measurements.

## Observed phase results

Percentiles below are **milliseconds for successful logical calls only**, with
failures listed separately. Small sample sizes, short windows, no saturation
sweep and overlapping SSH/diagnostic load prevent a capacity inference. Verification
is outside the reported traffic duration.

| Phase | Seconds | Successful ops | Failed ops | ACK writes | Successful ops/s | Write p50/p95/p99 ms | Read p50/p95/p99 ms |
|---|---:|---:|---:|---:|---:|---|---|
| steady 4 | 60.54 | 871 | 0 | 438 | 14.39 | 507.808 / 758.457 / 1203.079 | 14.294 / 50.508 / 100.368 |
| growth 4→5 | 60.71 | 525 | 0 | 265 | 8.65 | 710.155 / 2111.757 / 3942.743 | 12.653 / 87.528 / 239.896 |
| steady 5 | 60.65 | 449 | 0 | 227 | 7.40 | 758.353 / 2551.845 / 4327.311 | 11.804 / 64.887 / 147.790 |
| failure 5 | 90.90 | 575 | 6 | 287 | 6.33 | 791.064 / 3907.361 / 12873.140 | 12.203 / 81.310 / 223.491 |
| recovered 5 | 63.18 | 123 | 0 | 64 | 1.95 | 3505.353 / 8935.123 / 11856.031 | 10.352 / 96.335 / 146.509 |

**Sustained capacity did not improve in this run.** Five-node and recovered-phase
throughput were lower, and write tails were higher. Additional SSH and diagnostic
load, placement/recovery work, tiny corpus and phase sequencing confound causality;
these observations do not establish that node count alone caused the difference.
No repeated 15-minute matched read/write/mixed throughput campaign is supplied.

## What completed and survived

- Node n4 joined during unchanged native traffic. At the first five-node convergence
  checkpoint, 38 completed relocations to n4 reported **537,266 snapshot bytes**.
  Those are controller snapshot counters, not total disk/network I/O amplification.
  All 64 ranges had RF3/192 replicas; unavailable, under-replicated and active/failed
  relocation counts were zero. Leader/replica skew was one. The four-node baseline
  also had zero pending/unavailable ranges. These are checkpoints, not continuous
  availability guarantees.
- The n4 process was killed after initial convergence, then restarted. Six calls
  failed: five connection refusals and one range-leader-recovery-not-complete
  error. No failure is removed from the report. The restart delay and convergence
  timeline are **not a certified RTO**. This timed kill did not target snapshot,
  catch-up, promotion or cleanup boundaries.
- All **1,281 native acknowledged writes** were payload-verified after that
  process crash/recovery by the native harness. Its phase-end ledgers are retained.
  This does not prove a per-ACK independently durable native ledger or universal RPO.
- The separate external run recorded **107 attempts and 107 ACKs**, zero uncertain
  calls, with fsync per attempt and ACK. Following whole-cluster process restart,
  its completed verification checked all **107** with no failures. The whole-cluster
  restart did **not** recheck all 1,281 native writes again in this artifact.
  The external traffic recorded 0.883 ACKs/s and SSH-mediated append p50/p95/p99
  of 2119.684/3028.933/3667.733 ms; these are transport-inclusive controller calls,
  not engine-only latency or throughput certification.
- Five sampled payloads acknowledged before n4 joined matched after restart,
  with n4 shown as leader in routes captured before and after each read. Fourteen
  prejoin ACKs were candidate records under n4 leadership. This is concrete routed
  user-data evidence, distinct from membership alone. Later relocation history
  contains more entries; it is not substituted for the initial 38-move checkpoint.

A post-restart progress snapshot temporarily showed active relocations. The
separately retained later convergence checkpoint returned to 192 replicas,
zero active/failed/unavailable/under-replicated counts and skew one. We do not
relabel the intermediate work as continuously quiescent.

## Evidence, provenance and remaining gates

`provenance.json` records each original raw file SHA-256 and its separately
normalized/projected export SHA-256. Raw files remain private; exports omit
network endpoints, cloud/account identities, credentials and certificate material.
Generated test IDs, payloads, counts, timestamps, latency and error measurements
are preserved. Some ledgers/resources use gzip; the external JSON-lines ledger
is exported as a JSON array. **Hashes of raw originals do not authenticate the
sanitized bytes**; the separate export hashes do that relative to this committed
publication. No raw database archive or private key is included.

Operational helper hashes were captured after execution. Retained mtime/ctime
and `launch_time_digest_recorded: false` cannot prove launch-time code identity.
Do not promote these post-run hashes to an execution attestation. Binary identity,
source-pinned build evidence, native report and external ledger remain distinct
provenance assertions.

Run `python3 docs/evidence/azure-scale-smoke-2026-11-11/verify.py` to verify export
hashes and internal counts/identities. It does not rerun live reads or manufacture
a production qualification result. Independent physical placement, large/cache-
controlled data, repeated matched windows, majority/minority network faults,
relocation-phase faults, FTS/queue/audit integrity and stock PostgreSQL business
transactions remain separate gates. No production qualification, universal
acknowledged-commit preservation, physical RPO/RTO or deployed business application
is claimed by this smoke result.
