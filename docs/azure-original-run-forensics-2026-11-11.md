# Original Azure run: findings, unresolved causes and next tests

This report examines the **original, immutable failed run** and its already
completed diagnosis. It does not include a new reproduction, engine change,
additional restart or production qualification. Runtime: **1.0.461-beta**, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`, binary SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
The [original bundle](evidence/azure-long-growth-failure-2026-11-11/README.md),
[cold-catalog check](evidence/azure-cold-catalog-reconciliation-2026-11-11/README.md),
and [numeric forensic projections](evidence/azure-original-run-forensics-2026-11-11/README.md)
retain distinct evidence scopes. Newer runtime fixes cannot retroactively pass this run.

## Status matrix

**Passed** means the stated narrow observation/check passed. **Failed** means
the recorded check or operation failed. **Inconclusive** means retained evidence
cannot decide the question. **Not tested** means the relevant experiment was not
performed. A passed observation is not production qualification.

| Finding or requested guarantee | Status | Actual evidence and boundary |
|---|---|---|
| Pinned optimized runtime on five distinct guests | Passed | Source/binary and guest identity checks; physical-host independence unverified. |
| Automatic physical movement and leader-count redistribution | Passed | Earlier short run: 38 completed n4 moves/537,266 snapshot bytes and 13/13/13/13/12 leaders at its first recorded converged checkpoint. Not continuous availability or traffic balancing. |
| Native growth availability | Failed | 404 failed logical operations during the longer growth window. **404 is a count, not an HTTP status.** |
| Original growth ACK read pass | Failed | Two of 1,223 known ACK identities unverified; result predicate discarded error/absence/mismatch distinctions. |
| Later baseline full-ledger read pass | Failed | 3,804 checked: 3,693 matched/111 unverified; only 20 unverified IDs retained. |
| Later growth full-ledger read pass | Failed | 1,223 checked: 1,205 matched/18 unverified; all 18 retained. Both commands completed, without timeout. |
| Original two values subsequently visible | Passed | Ten exact matches through five routed member-client contexts after restart. Not five independent copies or a causal recovery explanation. |
| Retained Applied/resolved command evidence for all 5,027 ACKs | Passed | Exact expected payloads and internal catalog/command checksums; not live row reads, independently signed quorum evidence or RPO certification. |
| Any permanently lost ACKed record | Inconclusive | Neither failed reads nor retained command records establish final row loss or universal preservation. The live read pass remains failed. |
| Exact cause of the two original unverified reads | Inconclusive | Per-key responses, request routes, retry histories and read-time watermarks were not retained. |
| Policy convergence within the diagnostic 300-second bound | Failed | 68 sampled statuses; active moves never zero; boundary 61 active/3 failed. |
| Bounded relocation progress, total deadlock, specific move oscillation or actual all-node OS failure | Inconclusive | Metadata advanced and derived liveness cycled; per-move polling and continuous OS/service measurements are absent. |
| Demonstrated sustained four-to-five capacity gain | Inconclusive | Short goodput was lower; longer trial aborted before steady five. No completed matched capacity comparison. |
| Live byte/hotspot-weighted placement | Failed | Sampled byte/QPS inputs zero; default live-feedback integration absent in the examined source. Count-placement success does not satisfy this criterion. |
| Relocation-phase fault matrix, stale-writer attempts, WAL/index/FTS/queue/audit reconciliation | Not tested | Original short kill was after convergence; no complete operation-aware fault matrix or subsystem reconciliation. |
| Unchanged PostgreSQL business SQL and multi-range atomicity | Not tested | These experiments used immutable-event append/get; missing general SQL capabilities are covered by the source audit. |
| Independent physical-host/device power loss, general RPO/RTO, production qualification | Not tested | Guest-off observations do not establish these gates. The full qualification programme remains unpassed. |

## The two originally unverified ACKs

The public original bundle contains generated test keys. The projection uses
SHA-256 of the full `namespace/key` identity to join those keys to range histories:

| Label | Full record-ID SHA-256 | Range | Retained command: epoch / index / declared leader |
|---|---|---:|---|
| A | `04fbba34f4a4eac064060eefd057b8e4a964a79cc9874330811898e6f09711e7` | 41 | 11 / 74 / n2 |
| B | `de0055b2ef2e46c099356896ad745859a636b0594c4846d8bc8659af41ec75f0` | 11 | 11 / 55 / n0 |

These ranges follow the exact versioned hash in
[`distribution.rs::distribution_key_token`](../crates/bicdb-core/src/distribution.rs)
over collection `__bic_cluster_events` and record ID `namespace/key`, as defined
in [`distribution_events.rs`](../crates/bicdb-core/src/distribution_events.rs).
The command's declared leader/epoch is durable command evidence, not a retained
trace of the later failed Get's destination.

Original cold range-log watermarks (`epoch; last/resolved/compacted`):

| Node | A: r41, index 74 | B: r11, index 55 |
|---|---|---|
| n0 | 13; 107/107/0, Applied | 27; 69/69/0, Applied |
| n1 | No retained range log | 27; 69/69/0, Applied |
| n2 | 13; 107/107/0, Applied | No retained range log |
| n3 | 13; 107/107/0, Applied | 11; 57/57/0, Applied |
| n4 | 13; 107/107/0, Applied | 27; 69/69/0, Applied |

Every matching command contains the exact expected immutable payload. Four
retained histories do not mean RF4: snapshots/cleanup/compaction and former-owner
history differ from current replica placement. No retained log is not proof of
an absent row. These cold local states were captured following sequential owned
shutdown, not at the failed Get instant.

Sampled owner evolution also differs from command epoch:

| Snapshot | Generation | A: r41 epoch / leader | B: r11 epoch / leader |
|---|---:|---|---|
| Four-node checkpoint | 2124 | 10 / n0 | 9 / n3 |
| Mid-growth local status | 8314 | 13 / n3 | 27 / n4 |
| Original cold persisted views | 8969–9073 | 16 / n4 | 29 / n4 |
| Last 300-second diagnostic local status | 9616 | 18 / n0 | 31 / n1 |

These are separate sampled views. They do not reconstruct every intervening
lease/leader transition or identify the original Get route. Original per-key
Get errors, absent/mismatched returned values, request topology generation,
attempt timestamps, internal retries and per-call latency are **NOT RETAINED**.
All five original server logs contain zero occurrences of the two original key
strings. Relevant range-log fragments are untimestamped. They include r11 tail
recovery reaching only 1/2 voters and schema changes, and r41 applying-command
deferral, but cannot be assigned to either failed call. Startup-era schema
messages must not be promoted into a growth-time root cause.

The first retained successful A/B reads completed approximately **322.857** and
**326.805 seconds after the diagnostic helper recorded all five restarts complete**.
That helper waited through the entire policy observation before these reads.
All ten routed reads matched. These are delayed observation upper bounds, not
first visibility, a recovery SLA, or proof restart itself restored visibility.
No causally correlated replay, catch-up, route change or visibility-restoration
event was retained. The subsequent complete 18-ID growth failure list excludes
A and B; other read failures remained.

## What EventGet actually promises in the frozen source

The module describes events as eventually consistent, and explicitly gives scans
no global ordering or cross-range snapshot. `ClusterEventRequest` has an epoch,
range and operation; it has no consistency selector, session minimum commit
position or read-your-ACK token. There is no alternative consistency mode selected
by the experiment's default Get.

`ClusterEventClient::request` routes to the leader in its cached topology. It
allows up to five attempts, with membership refresh after errors and exponential
backoff. Its default request budget is 30 seconds, and an in-flight transport call
can outlive that budget by its configured connect/I/O timeout. Discovery can
obtain committed metadata from followers; this is **not**
follower event-data serving. The server's `ApplicationEvent` dispatch in
[`distribution_transport.rs`](../crates/bicdb-core/src/distribution_transport.rs)
authenticates an active voting-member caller, checks schema compatibility and
requires the request epoch to equal the locally
published range epoch and the receiving node to be that view's named leader.
Old routes and ordinary follower destinations are rejected.

The Get branch then invokes `BicDb::get`. Its paged path in
[`db/access_mutation.rs`](../crates/bicdb-core/src/db/access_mutation.rs) reads
`paged.latest_snapshot()`, a snapshot of **local** committed MVCC state, with
local collection/durability-health checks through
[`db/commit_snapshot.rs`](../crates/bicdb-core/src/db/commit_snapshot.rs).
[`paged_collection.rs`](../crates/bicdb-core/src/paged_collection.rs) and
[`mvcc.rs`](../crates/bicdb-page/src/mvcc.rs) implement that local visibility.
The dispatch contains no explicit per-Get quorum read-index, lease freshness,
minimum applied position or completed range-leadership-recovery barrier.
Append separately requires commit admission; that write check must not be
assumed to guard Get. The range/epoch check is against the receiving server's
published view, not a fresh independent quorum confirmation on every read.

This source path does not establish linearizable/read-your-ACK visibility across
ownership changes. It also does **not** justify classifying either original
failure as an allowed stale read: the retained evidence does not tell whether
the response was an error, absence or mismatch, or which server/view served it.
Permitted consistency semantics, an incorrectly stale owner, incomplete catch-up,
local row visibility and transport failure remain distinct questions. No precise
staleness bound or eventual completion deadline was qualified here.

## The 300-second convergence failure

All 68 polls returned statuses; their sampled span was 298.748 seconds inside
the helper's 300-second policy loop. Generation advanced **9127→9616** and metadata
commit index **1832→1884**, with term **13** and metadata leader **n2** in every
sample. Active moves ranged **48–63**, failed moves **0–15**, and final state was
**61 active/3 failed**. Derived live-node counts repeatedly returned from zero to
five and back; under-replicated/unavailable counts also temporarily reached zero.
Those derived labels do not establish continuous foreground availability or
actual OS restarts/failures.

The final generation was first sampled at 279.447 seconds and remained sampled
through 298.748 seconds: an approximately **19.301-second observed plateau**,
not a proved 300-second deadlock. The boundary topology retained 250 completed,
52 learner-allocated, eight snapshot-copying, one catch-up and three failed moves.
Metadata advancement is proven; **bounded relocation progress versus churn or
repeated work within the 300-second window is inconclusive**. Per-relocation
phase/cursor/byte/watermark history at each poll is **NOT RETAINED**. The earlier
original cold views retained 224–227 completed moves, versus 250 at the diagnostic
boundary, but this comparison spans sequential shutdown, restart and the policy
observation; it does not time those completions within individual polls or prove
a bounded-progress contract. Active-count changes alone cannot prove useful
movement, a particular move's oscillation or the longest-blocked range.

Boundary failed move IDs **257/258/259** concern ranges **58/59/60**, targeting n4
from n0/n1/n1, resuming at `learner_allocated`. Their retained error is
`io error: Connection reset by peer (os error 104)`. These are specific sampled
failures, not a demonstrated common cause for all unfinished moves or the two
Get failures. Later cold-export failed-move counts are cleanup-confounded and
must not be substituted for this pre-cleanup diagnostic boundary.

## The 404 failed logical operations

The growth window counted **404 unsuccessful logical calls**, with only **20**
bounded error samples retained. Their distribution is eight connection resets,
seven `Resource temporarily unavailable` errors, four range-leader-recovery
incomplete errors (r8/r14/r25/r29), and one no-reachable-member error. This is a
classification of the **20 samples only**. The other **384** operation bodies,
operation IDs/types, timestamps, destinations, topology generations and retry
paths are **NOT RETAINED**. Sample errors cannot classify all 404 calls by
gateway/range/cause or determine which had an uncertain committed outcome.
Individual successful latency samples and failed-call latencies are also absent;
successful-only percentile aggregates cannot be independently reconstructed.

## Open issue reports and the next smallest deterministic tests

These are reproducible engineering issue records, not claimed completed fixes
or newly executed tests. The original invocation/source and immutable input
identities are in the original bundle's launch record and executed harness.
The reported failure must remain recorded even if a new reproduction passes.

| Issue | Confirmed observation / unresolved cause | Smallest next test and required evidence |
|---|---|---|
| ORIG-1: ACK Get verification fails during owner changes | Original A/B failed verification; later payloads matched. Exact original response/route cause inconclusive. | Two immutable keys, RF3 and one controlled relocation/leader change. Record every ACK/Get attempt with monotonic time, complete value hash or exact error, cached/refreshed topology generation, range/epoch/leader, request target, and served applied/resolved position. Compare before/after refresh and each relocation phase. No collapsed predicates. |
| ORIG-2: policy convergence fails its 300-second bound | Metadata advances but the gate never passes; useful relocation progress versus churn and the common cause are inconclusive. | Small fixed range set, idle foreground, one joining member; then inject one bounded data-RPC delay/blackhole with a healthy metadata quorum. Poll every member's local and committed generation, heartbeat age, worker phase/timing and each move's durable cursor/bytes/watermark. Distinguish bounded progress, repeated retries and a persistent blocker. Assert a declared convergence bound and preserve failure. |
| ORIG-3: 404 logical failures cannot be individually diagnosed | Only 20 samples, no complete failed-attempt ledger. | A tiny deterministic harness fixture with one injected timeout, reset, stale route and recovered Get. Assert one lossless event per attempt/retry with operation ID/type, route, exact outcome and raw latency; verify counts against aggregates. Test evidence collection before another long run. |
| ORIG-4: named-leader Get lacks an explicit distributed read barrier | Source contract is local committed reads with cached epoch/leader/schema checks; stronger session/linearizable semantics unproved. | Pause a newly named leader before catch-up and exercise Get at the documented consistency level. Record its returned value/error and apply floor; independently specify and test whether it must reject, wait or serve a bounded stale result. Never silently reinterpret a missing acknowledged value as allowed staleness. |
| ORIG-5: load balance/capacity claim remains unproved | Count placement works; live byte/QPS feedback absent; capacity comparison aborted. | First a planner/host fixture with equal replica counts and unequal real byte/QPS inputs to expose which inputs/trigger operate. Separately repeat matched four/five traffic after actual convergence with a fixed data set, independent client and unchanged latency/error budget. Do not use joining-member success as throughput evidence. |
| ORIG-6: full-catalog persistence may dominate growth costs | Source rewrites retained catalog; 159 GB sampled process writes do not isolate cause. | Fixed topology and payload, vary retained command count only; measure clone/serialization/checksum/replacement/sync/RPC stages and process/device counters separately. Preserve fsync and crash assertions. A diagnosis requires this controlled measurement. |

No engine feature, consistency mode, recovery guarantee or infrastructure sizing
recommendation is introduced by this forensic report.
