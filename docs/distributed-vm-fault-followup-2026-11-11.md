# Distributed VM-fault follow-up: observed delay and diagnostic hypotheses

Runtime examined: **1.0.461-beta**, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`; optimized binary SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
Status: read-only source review of an isolated Azure guest-off probe; **root cause
not established, no engine change or production qualification**. This is separate
from the earlier four-to-five process-failure smoke and from later long trials.

## Observations and their limits

The strict controller-side boundary classification identifies five successful
calls that started after confirmed guest-off and completed before the first guest
start request. The first acknowledgment was observed **97.49 seconds after
confirmed off**, or **105.61 seconds after the off request**; the fifth was
observed 148.56 seconds after confirmed off. The selected range had been led by
the powered-off n4 and retained two surviving RF3 voters. Three calls had uncertain
client outcomes: subsequent point reads found one whole record present and two
absent. These are observed immutable-event outcomes, not a multi-record SQL
atomicity proof. The completed [probe evidence](evidence/azure-vm-poweroff-2026-11-11/README.md)
separately records the final targeted and earlier-ledger reconciliation; these
results do not establish a universal durability guarantee.

Successful probe writes during that bounded interval took approximately
11.55–12.11 seconds including SSH/client invocation, versus approximately 1.9
seconds at baseline. Earlier targeted calls reached the controller's 30-second
timeout. A timeout is not evidence that a transaction rolled back or was lost.

Two sets of survivor continuity snapshots were captured **after the first guest
start request**, despite earlier filenames containing `while-off`. They showed
the same owned BicDB PID/start-time between samples, increasing CPU ticks, later
port 9444 listeners and no recorded kernel OOM event. They exclude a restart
between those samples, not a transient listener/process stall inside the strict
off window. SSH reachability alone would not establish database health.
Untimestamped older log lines include earlier cluster process restarts; those
lines cannot be assigned to the guest-off interval as causal evidence.

Temporarily, all four surviving committed-status snapshots exposed the same
metadata generation 13094, commit index 2096, term 28 and metadata leader n2,
while derived liveness classified all five members dead and 64 ranges unavailable.
Unlike the later runtime snapshots, these metadata-status records have both
guest metric times and controller file receipt mtimes inside the strict off
interval. Cross-guest clock skew was not independently calibrated.
Later strictly off-window writes succeeded. Thus a stale/expired published
membership view and successful operations can coexist; this is not evidence that
all survivor operating systems or all metadata voters were actually dead.

**97.49 seconds is an observed first-response delay, not a certified general
RTO or an exact engine recovery duration.** It includes controller call scheduling,
whole-call start/completion, SSH startup, routing/retries and provider off-state
confirmation/polling. The service may have become capable of success before the
next successful call observed it. A late start poll also must not extend the
claimed off interval: the first actual start request is the conservative boundary.
No physical host/device power loss is established by an abrupt guest power-off.

## Source facts

| Source boundary at the named revision | Relevant implemented behavior |
|---|---|
| [`checkpoint_driver.rs`](../crates/bicdb-pgwire/src/checkpoint_driver.rs), `start_metadata_heartbeat_worker` | Metadata heartbeats run on a separate worker with transport timeouts capped to an election-sized budget. A simple claim that metadata heartbeats are on the same thread as every data operation is incorrect. |
| Same file, `replicate_metadata_round` | Metadata replication contacts followers concurrently in bounded batches. It is not a naive serial peer fan-out. |
| Same file, `start_distribution_supervisor` | Its main loop still synchronously calls placement/relocation `supervisor.tick`, local range-leader recovery and anti-entropy work. Member heartbeat/proposal publication shares this main loop, unlike the separate metadata heartbeat worker. |
| [`distribution_range_consensus.rs`](../crates/bicdb-core/src/distribution_range_consensus.rs), `recover_local_leadership`, `recover_leader_prefix`, `recover_leader_tail` | Recovery processes a bounded count of ranges, but calls current voters synchronously/serially. A range-count bound does not itself bound wall-clock delay below a liveness deadline. |
| Same file, `admit` implementation and `repair_voter` | Write admission repairs/probes and prepares remote voters serially. A healthy majority does not automatically skip all contact with the unavailable third voter. This creates a potential latency cost without implying failed quorum durability. |
| [`distribution.rs`](../crates/bicdb-core/src/distribution.rs), `ClusterNetworkTransportConfig::default` | Ordinary data transport defaults are 10-second connect and 30-second I/O timeouts. A blackholed/powered-off peer can consume those budgets, unlike a killed process returning immediate connection refusal. |
| [`distribution_transport.rs`](../crates/bicdb-core/src/distribution_transport.rs), `TcpClusterRelocationTransport::call` | Copies destination identity under the topology read lock, releases that lock, then connects and performs bounded mTLS/framed I/O. The network call does not simply hold that topology lock throughout. |
| [`cluster_events_command.rs`](../crates/bicdb-cli/src/cluster_events_command.rs), `client`, Append/Get branches | Fresh ordinary Append/Get commands read the local distribution store and invoke the event client directly; they do **not** unconditionally quorum-bootstrap on every call. Workload/Verify explicitly refresh and override transport timeout settings. |
| [`distribution_events.rs`](../crates/bicdb-core/src/distribution_events.rs), `request`, `refresh_members_until` | Failed routed calls can trigger refresh. Refresh visits known members serially and can try topology fallback after snapshot-fetch failure. A request budget may be exceeded by one in-flight transport call; retry/discovery time must remain part of client latency. |

## Diagnostic hypotheses, not a root-cause finding

1. **Membership publication can lag while metadata heartbeats continue.** Slow
   data RPCs in the main supervisor's recovery, relocation or anti-entropy work
   could delay member heartbeat/proposal processing. The independent heartbeat
   worker could retain metadata leader authority without publishing fresh
   topology membership timestamps. That is compatible with unchanged metadata
   generation/commit and expired derived liveness, but the snapshots alone do
   not establish which operation blocked the supervisor.
2. **One unavailable replica can add a full connect timeout to successful writes.**
   The serial voter repair/progress/prepare paths make an approximately ten-second
   additional delay credible even with two healthy RF3 voters. The observed
   11–12-second SSH-mediated calls are consistent with this mechanism; their
   duration does not prove it. Trace the actual destinations, stages and budgets.
3. **Routing refresh may compound uncertain-call latency.** After a refused or
   timed-out route, serial member discovery/fallback can spend further off-peer
   timeout budgets. This applies to retries, not an imaginary unconditional
   quorum-bootstrap step in every fresh CLI Append/Get command.

There is no independent thread-stack, per-RPC timing trace or deterministic
reproduction establishing the exact causal chain for this incident. No inference
here authorizes weakening quorum certification, fencing, schema checks or unknown-
outcome handling to obtain a faster response.

## Focused regression and qualification gates

A future regression should leave the database and healthy voter listeners live
while an already-established voter becomes genuinely unresponsive: delayed SYN,
accepted-but-unresponsive TLS/frame I/O, one-way partition and long response loss
must be distinct cases from fast `ECONNREFUSED`. It should establish:

- RF3 acknowledged commits survive with the selected leader/voter lost, and
  uncertain operations reconcile independently rather than being counted as loss.
- Metadata-quorum authority, member heartbeat publication and range-leader
  recovery each have separate timestamped progress witnesses. Background work
  cannot indefinitely make healthy members disappear from the published view.
- Range/operation limits also enforce time budgets and fair progress. Failure of
  one range or voter does not indefinitely block independent healthy ranges.
- Successful writes do not pay an undocumented serial unavailable-voter budget
  when the protocol can safely complete from a healthy quorum. Any optimization
  must preserve exact decision, repair-prefix and unresolved-tail proof rules.
- Admission, metadata and data RPC timing records distinguish network wait,
  local lock/storage wait, retry discovery, caller polling and SSH overhead.
  Collect direct warm-client latency separately from fresh-process SSH calls.
- Controller retry cadence does not become the asserted engine RTO. Repeat
  guest resets/partitions on an exact artifact and publish all attempts, failure
  counts, independent ACK ledgers, first-success observations and convergence.

These are future acceptance gates, not tests newly implemented or executed by
this document. The source review changed no engine, helper, VM or production
service. A later repair needs a minimized reproducer, scoped regression and new
artifact-specific failure evidence; this note neither diagnoses a proven engine
bug nor declares the distributed HA boundary qualified.
