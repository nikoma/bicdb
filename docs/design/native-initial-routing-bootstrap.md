# INITIAL DATA bootstrap routing under DATA contention

The actual expected-RED prerequisite is source
`05979463ef312fc1128a42903f908de0189c2a63`, run `43278532377`, job
`131393398627`: both real held metadata quorums passed, all three actual
BootstrapSnapshot requests returned DATA-service busy, and all FetchTopology
fallbacks were refused. After release, the same public client received the
second cross-leader APP ACK and exact Strong values before the final routing
acceptance failed. The separate portable receipt is docs-only commit
`00f413d6086e75e83b82028643f972107cc7d3c4`; original RED source/results remain
immutable. This establishes a component routing failure, not the cause of
old `3ee`, an Azure incident, lost ACK or false absence.

This source batch starts from full candidate
`4d468235b77c7239a4c8ef78a70ecfa51b0d8526`. Its new regressions have not run;
formatting/static review is not a Rust compile or runtime qualification.

## Actual producer and custody

`NativeMetadataRegistrationController::new` already captured and checked an
actual `ClusterServiceOpenGuard` against the same owned MetadataConsensusStore.
The controller now retains that guard. It has a Weak actual-open sentinel and
health/native identity; it does not extend the original DB open or provide a
quorum/read/write permission.

The sole real INITIAL DATA admission constructor captures the existing
`RangeWriteServiceSlotGuard` while its actual service mutex is already held.
Every admission lifetime check validates that live revocable slot generation
and its actual Store-open ID, in addition to existing owner life/writer/root
checks. This matters because `writer.require_store(root, cached_open_id)` alone
can succeed for a removed but still-retained Store and original writer. Slot
replacement revokes its old binding BEFORE swap, and restoring the old object
cannot revive that binding.

`bootstrap_initial_data` consumes that existing opaque admission, not a DTO.
Before and after reading, it checks the admission's Weak actual-service
pointer against the controller's real backend service, original Store ID,
current slot/life/writer, and retained actual-open lifetime. It takes only the
backend's bounded metadata mutex, calls `require_listener_database` against
the captured actual open, and clones/validates the ACTUAL committed metadata
snapshot. The same owned MStore/native identity and original caller deadline
are checked before returning. There is no DATA service/DB borrow, network
collection, metadata mutation, native WAL operation or role mint in this path.

The transport selects it only for the existing FetchBootstrapSnapshot request
on the actual immutable INITIAL DATA mode, after unchanged request identity,
mTLS, caller and mode checks. Generic/control-only/registered bootstrap keeps
its original method; FetchTopology and all other listener allowlists are
unchanged. An expired Weak controller cannot fall through into generic data.

## DATA contract

The response remains `ClusterBootstrapSnapshot`: committed routing data and
existing config/metadata-leader hints. It is neither a fresh metadata quorum
nor NativeReady, Strong, application permission, schema-snapshot authority or
data absence. Hints can become stale immediately; ordinary current route,
writer, role, QQ and Strong validators still govern every operation. There is
no cached-authority publication or old-proof deadline renewal here. Existing
frame bounds and response identity checks are unchanged.

## Focused acceptance

Filter `routing_discovery_tests` selects three source-only controls:

1. The positive sibling preserves BOTH real HostMQs while ALL three actual
   service latches are held, initial policies before ACK1, the same public
   client's phase budgets, n0 ACK/value then n1 null ACK/Strong, exact original
   ACK/history retention, unchanged actual topology, and explicit owner stops.
   It requires all three real BootstrapSnapshot responses to contain the full
   committed topology and no client FetchTopology fallback. A separate actual
   FetchTopology request WHILE HELD must still return the exact original
   unsupported-operation refusal.
2. Genuine different-service admission refuses while both service latches are
   held; an actually stopped/drained owner with retained opaque inputs refuses
   before service/DB latches. Actual protected native R/H/local B/commit sequence
   remain unchanged. These are refusal controls, not application ACKs.
3. An actual TLS snapshot first succeeds. Then the service Store is replaced
   with a different-root actual Store while the OLD actual object and native
   writer remain live. Holding the service and actual DB latches, the real TLS
   request must return the explicit current-slot-ended/changed refusal, not a
   timeout. Restoring the exact old Store still cannot revive the old binding;
   protected native point/sequence stay unchanged.

The cfg(test) input getter only clones already-produced actual controller and
INITIAL admission Arcs. It constructs no policy, authority or fake receipt.
No test serialization, RPC deadline extension, availability-waiver predicate,
blank learner permission or operator/physical qualification is introduced.
