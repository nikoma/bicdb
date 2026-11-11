# Instrumented event replay with a pinned current runtime

This is a separate diagnostic client for reproducing the November 11 Azure growth failure. It changes no database implementation. The next physical replay requires current-main source `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, version `1.0.463-beta` or newer, with its actual server binary hash recorded separately. The original failed `1.0.461-beta` artifact/evidence remains unchanged. The four distribution/event/transport policy files are byte-identical between original source `b05385d4b033edf0db03f3eccf4bdd86f7c8eddb` and this pinned main. `policy_source` identifies that unchanged algorithm; `linked_core_source`, `linked_core_version`, library hashes, and server artifact identity identify the actual runtime. A policy-source label alone is not evidence that a particular binary ran.

The controller implements the frozen event client's five route attempts, 30-second request budget, 100/200/400/800 ms backoffs, sorted-member metadata snapshot discovery and legacy fallback. Its shared topology rejects cluster changes and conflicting equal generations. Initial routing is loaded from the validated gateway member store; the stock workload command's separate up-front refresh is not copied. Retry-time discovery and validation follow the reference request loop. It is a Python policy replica, **not the unchanged Rust CLI, PostgreSQL application, or a new database feature**. Journal fsync, SSH framing, process scheduling and observer activity affect timings and may consume the request budget. This is diagnostic evidence, not a capacity certification.

A persistent member-side Rust worker performs exactly one explicitly requested RPC per message. It uses the frozen core's TLS builder and snapshot validation, checks caller membership, server SAN and membership certificate pin, and verifies cluster/protocol/destination/request identity. Private member keys stay on that member. Logical topology addresses remain unchanged; no loopback address rewrite or weaker bootstrap authority is used. The controller communicates through SSH with the authenticated member gateway; it is not an independent network-origin database client. Wire IDs are globally unique journal-sequence-derived numbers, rather than the stock client's private counter; they are not application idempotency identities.

The worker permits only append/get in one exact test namespace, metadata discovery, and read-only `RangeWriteProgress`. Administrative mutation RPCs are rejected. Range IDs, epochs, leadership and cached topology generations come from automatic metadata routing; the application fixture supplies keys, not shard assignments.

| Evidence | Coverage |
| --- | --- |
| Logical attempt, stable key, complete expected value and canonical payload hash | Every dispatched logical operation, durable on the external controller before dispatch |
| Internal event retry, backoff, discovery RPC and fallback | Every call made by this instrumented policy; each wire call has a correlated ID |
| Server remote error | Complete serialized response body/envelope, exact code/message, no 20-sample truncation |
| Transport/worker failure | Full available error chain, stage, timestamps and uncertain response classification; partial network-frame bytes are not retained and an incomplete response remains unknown |
| Routing | Key token, range ID/epoch, leader, cached generation, gateway, actual worker-selected address/generation and TLS peer pin |
| Latency | Individual logical outcomes, controller RPC duration, member wire duration, connect/TLS timings and ACK fsync receipt |
| Server-internal consensus/relocation retries | **Not observed** by this client; require separate server evidence |
| Read consistency | Event Get remains eventual, with no quorum read-index barrier |

Every trace record is hash-chained and fsynced on the external controller. Each counted client ACK requires a previously durable successful database response, a linked ACK record/durability receipt, and acknowledged terminal linkage. Merely seeing a trailing ACK or database-reply row after its fsync failed does not prove that row durable; the scanner reports these separately. A controller or SSH failure can leave attempts with unknown outcomes; the trace retains their stable identities for reconciliation. A durable database success reply without a later ACK receipt is reported separately. An error or timeout never establishes rollback. Unknown RPC history remains recorded even if a later retry resolves the logical operation. A timed-out, disconnected, invalid or misaligned persistent worker channel is poisoned and never reused or automatically restarted.

The application gateway is explicitly checked as `n0`. An observer command map supplies leader-local SSH workers: every lazy session is checked for the selected member ID, cluster, namespace, helper hash, linked core and mTLS. Current leader sessions are warmed before traffic; the not-yet-joined fifth member is intentionally absent until its published configuration exists. Member keys never leave those hosts. A separate observer caches timestamped range-log progress and probes the voters listed in the triggering cached route after each event RPC error, missing value, mismatch, and selected success control. These are separately timed additional read RPCs. They do not establish the current global quorum, a read barrier, or a common snapshot. Missing/stale pre-watermarks remain explicit. `RangeWriteProgress.current_epoch` describes retained command history and can legitimately lag the requested metadata/routing epoch; both are logged separately and their inequality is not a readiness gate. Fully captured probe rejections or network errors yield unknown watermarks rather than application failures. Periodic admission stops before already-recorded observer jobs drain. Observer backpressure, lost workers, unfinished trigger IDs, drain gaps, or failed stderr capture invalidate complete coverage and fence new logical dispatch. A later completion record cannot erase a prior fence. Normal duration/operation-ceiling termination is distinguished from resource, budget, and unplanned aborts; an empty run cannot pass.

The external journal defaults to a 2 GiB ceiling (failure-heavy runs may explicitly choose a larger bounded quota) and 4 GiB filesystem headroom; the controller uses a 2 GiB address-space ceiling, lower CPU/I/O priority and an 8 GiB shared-host RAM guard. Exhaustion fences dispatch and reports incomplete telemetry rather than truncating records or inventing success. Workers should additionally be launched through a task-owned absolute timeout. No fault experiment may start until the local instrumentation checks and the isolated cloud smoke both pass their telemetry checks.

Build only against the existing frozen optimized cache:

```sh
python3 scripts/platform/instrumented-events/build-wire-worker.py \
  --frozen-source /path/to/d49a391b-checkout \
  --expected-source-commit d49a391b3c6157f90f29e9f2f5214143c60f9f4b \
  --core-rlib /path/to/new463/libbicdb_core-CORE_HASH.rlib \
  --expected-core-sha256 EXACT_CURRENT_CORE_LIBRARY_SHA256 \
  --artifact-target /path/to/frozen-release-cache \
  --output /new/private/helper-build
```

The builder refuses missing exact libraries, records their hashes/compiler/argv and monitors memory. It never starts an engine build. The private build report and executable hash must be frozen before provisioning. Local protocol fixtures exercise trace boundaries, not HA guarantees:

```sh
BICDB_FROZEN_BINARY=/path/to/frozen461-bicdb \
BICDB_TRACE_WIRE_WORKER=/path/to/wire-worker \
python3 -m unittest discover -s scripts/platform/instrumented-events -p test_trace_client.py -v
```

Create a private JSON argv file for a pinned persistent SSH command running `wire-worker MEMBER_ROOT EXACT_NAMESPACE` on the gateway. Pass its recorded binary hash to the controller:

```sh
python3 scripts/platform/instrumented-events/trace_client.py traffic \
  --worker-command /private/worker-command.json \
  --expected-worker-sha256 EXACT_HELPER_SHA256 \
  --expected-linked-core-source d49a391b3c6157f90f29e9f2f5214143c60f9f4b \
  --expected-linked-core-version 1.0.463-beta \
  --namespace EXACT_NAMESPACE --phase baseline-4 --seconds 900 \
  --concurrency 16 --rpc-timeout-ms 5000 --output /new/private/baseline
python3 scripts/platform/instrumented-events/trace_client.py validate /private/baseline/trace.jsonl
python3 scripts/platform/instrumented-events/trace_client.py summarize /private/baseline/trace.jsonl
```

Exit 0 means complete planned execution/telemetry and successful workload outcomes; 1 means complete planned execution/telemetry with unsuccessful database outcomes; 2 means incomplete, fenced, empty or controller-failed execution. Trace integrity, coverage completeness, planned completion, and workload success are separate fields. All three retain their actual records. The logical elapsed duration is sampled before fsync of its terminal record. Controller and member Unix clocks are separate uncalibrated domains; member wire elapsed durations use monotonic clocks. Raw bodies, network addresses and diagnostic files stay private; public reports should use reviewed allowlists and hashes.

`--corpus-ack-ledger` can supply a retained baseline or growth ACK ledger. It preserves those known namespaces/keys/payloads first, then labels newly generated replay keys separately. The original failed attempts and complete interleaving were not retained, so this is a new replay, not an exact deterministic reproduction. Never invent the original unsampled failures.

During orchestration, record every member's topology, heartbeat/consensus state and complete relocation cursors/bytes/watermarks/errors. A steady-five window requires 30 seconds with zero active/failed relocations, zero unavailable/under-replicated ranges and RF3 policy restored; sample member views without demanding simultaneous identical heartbeat generations. If moves restart, segment or invalidate that steady window. A higher observed rate alone does not establish capacity gain when data/history/client paths differ.

`snapshot-controller.py` captures bounded read-only diagnostics for only the identity-checked controller/SSH children: thread wait channels/syscalls, descriptor/socket mappings, progress cursors and CPU/I/O/memory pressure. A `--reconcile-journal` input switches the guarded client to read-only reconciliation of every unique prior write, including uncertain outcomes, with source ACK evidence and expected hashes retained. No new application events are written. The trace records explicit required, durably started, terminal, verified and unattempted target-ID sets. The scanner and validator independently reconcile these sets; allocated worker counters are not completion evidence. All targets attempted is distinct from all acknowledged targets matching. Unattempted or unterminated targets at a configured limit make reconciliation incomplete; eventual-read absence never establishes rollback.

It reads no process memory, sends no signals, and performs no intrusive profiling. Shared-host counters are not isolated workload costs. Capture it only for stalls or explicitly labeled diagnostic windows.
