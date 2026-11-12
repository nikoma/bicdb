# Isolated read-safety integration with the metadata candidate

Source-only integration `c48c804d6` merges the complete preserved independent
read branch `8096093ae7697836a7817512cf5b48375db3e45c` with independent metadata
PR162 head `ce02303676fe5fe730ffd7fc822a3a27ee8189aa`. The latter already carries
candidate source version1.0.485-beta; this work selects no new version and makes
no PR157 update, push, merge to main, binary or deployment claim.

The source byte comparison of `crates/bicdb-core/src` against809 is empty after
this merge. The only conflict was in `distribution_consensus.rs`: preserve
native bootstrap-domain validation, the matched-frontier follower commit cap,
and `persist_owned_append_response(matched_frontier)`, including the native
birth/StoreUUID/current-actor/body-digest boundary. Replacing that native producer
with the public generic response constructor would lose the native ACK seal.
The public candidate's generic bounded-frontier fix is already represented in
this native adaptation. Public candidate history, manifests and backup/Oracle
corrections remain intact rather than being removed to narrow a diff.

This is selected dependency composition, not the full native prototype. It
includes the original17,65-barrier and mid-round-retirement controls and their
accepted-peer progress correction. It excludes native AbortPromise/no-effect
runtime producers, joint-data activation, learner-birth RPC25, finite observed
learner enrollment, learner metadata transition and the new registration
controller169f80. Protocol27 remains the independent metadata ACK/read stack's
unpublished incompatibility fence; no feature25/26 request arm is imported.
The earlier selected-source document records the larger read dependency stack
and its distinct historical test results. Neither earlier results nor a clean
source merge validate this exact versioned composition.

## Read readiness and route audit

The audit found no integration delta in these boundaries:

- Actual local facts come from the same service's native physical lineage,
  current R/H, typed local visibility B, actual DB/store opens, health/schema,
  mutation stamps and catalogue commitment. The observer does not derive B from
  R or activate a recovery session/freeze per read. A raw CQ/pending command or
  invalidated native source refuses.
- The sealed data candidate consumes actual generic fresh range-leader metadata
  authority and request-bound actual local/authenticated peer observations.
  It needs the local point plus a distinct matching current-voter majority;
  known higher or conflicting history vetoes. An expired attempt is not a vote;
  its authenticated immutable point remains a bounded conflict constraint.
- The candidate alone is unadmitted. Enrolled consumption checks immutable
  metadata birth bindings against actual counted peer facts. Strong consumption
  additionally requires enforced writer1 for all range voters and the current
  actual installed writer guard for the exact coordinator/service/DB-open.
  Recreated roots, another hook, same-Arc reinstall and stale schema publication
  cannot be upgraded by a caller-supplied scope or cached recovered epoch.
- The actual installed coordinator's `active_ranges` map serializes the read
  with origin/background recovery. Reservation waits occur before mutation
  permits and without DB/service latches. Borrowed-DB native mutation admission
  uses the nonwaiting path. Every mutable admission invalidates observation
  stamps, including a later failed operation.
- Enrolled data quorum is consumed at the SAME service to capture an actual
  target-local MVCC snapshot at or above typed B. Snapshot object policy and
  exact collection/key-to-range mapping remain mandatory. A separately fresh
  final generic metadata quorum under the original budget must preserve range,
  voter identities/births, leader, schema/restore and native domain. Actual local
  proof, health and installed hook are rechecked before exposing an owned row
  or absence. Intermediate snapshots and audit DTOs are not permissions.
- Typed incoming Get dispatch precedes service/DB latches. Missing/expired weak
  dispatcher publication yields explicit unavailable. Actual born2 or a known
  enforced identity cannot fall through to bare legacy Get. Cached enforced
  destinations promote default requests to explicit Strong; explicit Strong
  cannot downgrade on born1. Legitimate born1 default Get and Scan remain
  explicitly eventual contracts.
- HostMQ supplies route DATA only. A value still requires generic leader MQ,
  data quorum and the snapshot/final-MQ consuming path. Client discovery/retry
  keeps one business ID and absolute whole deadline, uses a fresh attempt
  challenge, and validates typed response correlation. Tagged Present(null),
  Present({}) and Absent are distinct; unavailable/redirect/error never becomes
  successful absence. Write uncertainty handling is unchanged.
- Network and uncancellable local IO run in bounded owned workers. Cancellation
  fences a late verdict; real work retains its operation/context until exit.
  The protocol does not claim a hard OS/filesystem/DNS IO deadline.

## Bootstrap configuration is data, not a joining host

Source review confirms the existing generic override contract:

1. `run_remote_cluster_join` constructs the joining ClusterNode from explicit
   local ID/address/incarnation/capacity/labels and its own certificate pin.
2. `cluster_join_transport` replaces the source template's TLS configuration
   with explicit JOINER cert/key/CA paths, requires client certificates, and
   refuses missing explicit TLS for a remote target. It does not read source
   private-key bytes from BootstrapSnapshot.
3. `provision_cluster_member_directory` replaces template node identity/address,
   incarnation/pin/capacity from the committed joining node, uses the supplied
   JOINER transport, and writes only the caller-supplied local root. Existing
   mismatching files are refused. The preexisting focused control is
   `member_provisioning_keeps_target_specific_tls_identity`.

This helper is still generic directory provisioning, not an atomic native
factory or protected metadata-host constructor. A future native facade must
require explicit joining-host local DB/metadata roots, TLS identity and network
address, preserve them on retries, and refuse a missing local identity rather
than inheriting seed-local paths. Network timeouts may be template configuration
DATA, while local identity and local filesystem ownership may not. The separate
registration169 test registers a fifth CONFIG identity only; it creates no fifth
storage backend and supplies no evidence of cross-VM path correctness.

## Exact remote validation targets

No local Cargo, database open, process test or cloud action ran for this
integration. Local checks are `git diff --check`, rustfmt parsing of the resolved
file, source/exclusion/byte comparisons, and the synchronized version check.
A remote job must first compile all targets, the isolated `bicdb-workflow`
consumer and Core without default features. Existing focused workflow input is
`core_filter`; its nonempty selection check and full names must be retained.
Run these distinct nonempty filters on the final frozen head:

| Filter | Purpose |
| --- | --- |
| `metadata_append_actual_` | Both genuine public FSM divergent-tail/snapshot/empty-heartbeat controls; `native_` alone misses these |
| `native_initial_metadata_actual_tls_` | Original17,65-barrier continuation and real mid-round retirement |
| `native_read_` | Local typed-B/current-open observations and genuine TLS factual/enrolled candidate controls |
| `native_strong_point_` | Consuming snapshot/final-MQ, hook/publication/cancel, RF3/RF4 identity and actual same-voter transfer |
| `native_event_endpoint_` | Incoming typed value/null/object/absence, redirects, refreshed slot, mismatch/no fallback and whole deadline |
| `native_event_get_` | Tagged-presence/correlation codec negatives |
| `native_metadata_` | Protected owner/StoreUUID, actual lifetime/birth and metadata ACK dependencies |
| `member_provisioning_keeps_target_specific_tls_identity` | Existing explicit target TLS/path override helper contract |

Filters may overlap; execution totals must not be summed as distinct tests.
List selection and report exact names/counts and source for every result. A
zero-match filter is not a pass. Focused checks do not replace broad Core,
workspace, PostgreSQL/Oracle/application and compatibility gates before public
main delivery.

## Operator requirements still open

The private reader and endpoint components do not make ordinary native
Init/Join/Serve operational. Remaining producers include a public sealed host
facade and actual CLI/PGwire lifetime wiring; immutable native metadata-control
registration/queue consumption; fifth factory/schema transfer, actual observed
birth binding and owned metadata backend/catch-up/promotion; bounded automatic
leadership/configuration/recovery scheduling; full cross-process image repair
and restart continuation; old/new data-quorum handoff, placement/controller and
proof-authorized cleanup; sustained-history retention/compaction and complete
native transaction-effect coverage. This delivery intentionally lacks later
abort/joint/enrollment prototype code rather than silently claiming those gaps
closed. Same-store term/vote rollback protection beyond the selected actual-open
and missing/different-StoreUUID fences remains separate.

A real four-to-five operator demonstration must preserve acknowledged values
through membership movement, failed actions and restart on independent hosts.
No physical failure, Azure causality, stable8GiB throughput, SQL/PGwire native
qualification, automatic convergence or full Milestone1 completion is implied.

## Retained stale gateway: additional source-only regression

The existing
`native_event_endpoint_stale_route_retries_until_actual_new_leader_ready_under_original_budget`
uses a genuine application ACK, committed same-voter E2/new leader, fresh actual
writer policies, TLS Fence/Prepare/Complete, trusted **local** activation, and
actual installed new-coordinator recovery. Its gateway is constructed after the
transfer from a previously copied topology. Request traces prove old/new endpoint
attempts, while independent redirect tests assert structured replies. The
internal `native_strong_point_after_actual_same_voter_leader_transfer_and_recovery_reads_acked_value`
also exercises ACK2/current R2, but does not consume an external gateway route.
Those controls alone do not explicitly prove that a gateway which previously
returned the acknowledged value remains alive with its stale route across the
transfer and observes an actual structured rejection before its own refresh.

New focused source control:
`native_event_endpoint_same_stale_gateway_preserves_full_acked_value_after_real_transfer`.
It creates the gateway before transfer, obtains a real gateway Append ACK and
Strong value, and retains that **same object** throughout the real fsynced
metadata leader/E2 change. The acknowledged value contains more than16KiB of
payload plus nested array/object/null data; equality checks the entire value,
not just a record ID, timestamp, presence bit or digest. The old endpoint's
actual mTLS response must be a correlated Redirect with the current range,
leader, epoch/generation and real HostMQ context. A direct current-route call
while the new leader's actual slot is empty must return structured retryable
Unavailable, never a value or absence.

Without refreshing/reconstructing the retained gateway, the test starts its
normal bounded get_strong retry loop, waits for its actual unready new-leader
request, then drives the existing real recovery/activation helper. It requires
the original whole20-second budget, the full original acknowledged value,
consistent business correlation, fresh per-attempt challenges, old-epoch first
hop and at least two real new-leader attempts. The existing stale-route test
retains all its assertions and uses the same extracted producer helper. No
production logic, MQ/history evidence constructor, recording backend or weaker
read contract is added.

This is a same-voter leadership-transfer component case. The metadata change is
an actual fixture proposal/commit, not an automatically elected/operator-driven
transfer. Native activation still uses the trusted local Runtime API. It does
not move a replica, create a new voter, prove a placement/joint-data handoff or
exercise separate VMs. Those need the missing full old/new DATA-quorum bridge
and operator lifecycle, deliberately excluded from this delivery. Until those
producers qualify the changed placement, endpoint unavailable/refusal is the
only supported outcome; no fabricated placement proof or eventual-read fallback
is introduced to make a success assertion possible.

The new test remains uncompiled/unexecuted locally. Remote CI can select its
full `native_event_endpoint_same_stale_gateway_` prefix, followed by the unchanged
`native_event_endpoint_` cohort. Any earlier/frozen6c run is separate from this
later test-source delta; no pass count is transferred to it.
