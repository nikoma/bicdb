# CLOSED joint DATA-policy extension: concrete producer contract

Read-only design against `1a776947b2a13ce9fd59457da7b18d539803bc4d` (runtime
source through `ded3c412c`; later documentation only). No implementation or test
execution. This chooses a first architecture for the
[DATA policy integration gap](native-data-policy-transition-gap.md), without
changing existing writer, committed-decision or serving checks.
Paths below are relative to `crates/bicdb-core/src/`.

## Confirmed dependency cycle and usable lower seam

`distribution_data/native_joint_handoff.rs::observe_committed_native_joint_decision`
reads fresh committed metadata and selects the promoted descriptor, then calls
`require_native_writer_host_for_range(database, descriptor_NEW)` BEFORE
`database.read_native_joint_decision(...)`. With the original OLD policy still
installed, this refuses: original `NativeWriterHostState::require_range` is exact.
`prepare_local_native_joint_serving` in `native_joint_handoff/bridge.rs` likewise
requires a writer for the activation range before producing NEW Prepared.

The existing `CommittedNativeJointDecisionObservation` therefore cannot be the
input to a producer intended to install that NEW necessary policy. It is not
constructible yet on that path. The full local fixture breaks the cycle using
`refresh_hosts` after metadata commit: an ordinary enrollment installation makes
a NEW registration/StateArc before the committed observation and preparation.
That remains a declared component prerequisite, not an operational transition.

A lower actual producer already exists:
`db/range_image_native/serving_configuration/joint_handoff/decision.rs::`
`read_native_joint_decision(&NativeRecoveryAdmission, range, expected_sha)` reads
protected native current and immutable archive, verifies the actual birth/open,
reader fence, decision and local receipt, and returns `NativeJointDecisionReceipt`.
Its validation does not require a NEW-range host policy or NEW serving bridge.
It supplies a historical protected publication fact, not a live DATA quorum or
permission to activate. The complete decision originates from actual OLD closure
and NEW materialization quorums through `NativeJointDurableDecision` and its
native publication round; decoding its audit body cannot recreate those quorums.

## Chosen first producer: same writer, separate CLOSED proof

Introduce a PRIVATE non-serde `ClosedNativeJointPolicyTransition` on the actual
service, distinct from `CommittedNativeJointDecisionObservation`. Do not weaken
the existing observer or add a caller-selected bypass to it. Suggested stages:

1. Outside service/DB/journal guards, obtain independently fresh recipient-bound
   HostMQ under the ORIGINAL operation parent. Its committed pending handoff must
   be AwaitingActivation, exact final descriptor/OLD and NEW credentials, enrolled
   births/pins/schema/restore, same decision/session/nonce/cut and nonregressing
   post-decision metadata anchors. MQ alone is insufficient.
2. Under the SAME actual service/Store slot and checked DB, prove the installed
   OLD writer's actual StateArc, hook/registration generation, coordinator Arc/ID,
   DB-open, Store-open, root, birth, custody and exact OLD effective descriptor.
   This requires a dedicated origin check; asking that writer to authorize the
   NEW descriptor here would recreate the cycle. Reuse identity primitives,
   not a decoded generation or a generic "writer present" flag.
3. Read the actual protected `NativeJointDecisionReceipt` via the lower seam;
   verify current+archive and exact local receipt linkage. Consume actual local
   OLD closure/permanent promise and live Frozen/pin for a survivor; consume the
   actual finalized image/install owner for a target. Verify actual R/H/local B,
   namespace UID/schema and every inherited Commit/CQ/NoData/refusal obligation.
   The decision's original QQ audits are historical facts; fresh metadata must
   not relabel them as a renewed data vote or revive expired owners.
4. Mint the CLOSED proof only after these same-borrow checks, retaining original
   parent cancellation/deadline, actual owner identities, exact predecessor policy
   and decision bindings. It cannot be made from a caller receipt/hash or imported
   by serde. It contains no Ready/Strong/open grant and releases no freeze.
5. Publish an immutable effective-DATA-policy extension plus current pointer under
   the established admission-gate/native-journal order. Keep the ORIGINAL writer
   StateArc, registration, coordinator, policy record and ingress/admission Arcs.
   The extension binds old/new descriptor, exact actual decision/local closure or
   image evidence, prior effective-policy root, latest accepted metadata registry
   identity facts, actual open/registration IDs, fresh metadata audit and local
   R/H/B/UID. Read actual NEW pointer+archive before publishing its verified cache.
   No network or recursive native-journal acquisition inside that scope.

Short inspection borrows are not held while waiting for admission or network.
Use a nonwaiting admission acquisition and nonwaiting revalidation of the SAME
service/DB/slot before native publication, rather than invert the existing lock
order. Recheck actual decision current+archive and local owner facts under the
real publication journal, with final hook/schema/health/cancellation checks for
facts not serialized by it. A previously inspected receipt is not that recheck.

`NativeWriterHostState::require_range` would resolve necessary policy against
this actually verified effective map, rather than permanently consulting only
the first range map. Keep the first policy as immutable origin, with explicit
bounded predecessor continuity. This lets the UNCHANGED committed observer and
NEW preparation run afterward; it does not mean that extension publication alone
may satisfy an ordinary APP/Strong/readiness check. Existing current-command,
native lineage/effective joint domain, recovery, NEW preparation/quorum, protected
bridge, actual activation/open-generation and final freeze-release checks remain
mandatory. In particular, NEW commands cannot use an extension as native serving
authority, and OLD suffix decisions cannot escape their permanent promise.

Compose the effective DATA map with the already accepted metadata-registry facts
without treating either as authority for the other. Metadata-only refresh must
still reject arbitrary range edits. For this first 4→5 transition, complete the
fifth metadata enrollment/registry extension first; bind that exact accepted
registry in the DATA extension. Additional DATA transitions or registry changes
afterward need an explicit chained composition contract, not resetting the origin
or silently teaching the metadata-only refresh to accept changed ranges.

A single range's first transition is only a component milestone. The required
four-to-five expansion must finish every planned range movement and leadership
change, including multiple moves, retries and controller restarts. Before claiming
that gate, define effective-policy ancestry across independent ranges and repeated
transitions of the same range, with bounded retention and exact uncertain-write
reconciliation. Pending publication for one range must not become an unexplained
global admission outage. No one-range demonstration substitutes for convergence
of the complete placement plan.

Preserving this Arc avoids retargeting every live INITIAL dispatcher/admission
owner and the original proof windows. Rotation remains possible as a separate
typed handover, but ordinary reinstall is not the chosen publisher. A durable
extension followed by cancellation/cache-publication failure must fence new
admission and reconcile the EXACT stored fact under independently fresh authority
on the same original owner; it cannot restore an old grant or rewrite its audit.
Hook replacement/ABA, owner loss, missing archive and wrong local closure/image
refuse. Cold installation/rebinding of a writer from extension JSON is unsupported.

## Genuine fifth receiver is a distinct lifecycle

A real fifth receiver has no existing INITIAL writer StateArc to extend. It needs
an actual CLOSED first-writer installation from its OWN registered receiver/Store,
finalized target image, protected joint decision and fresh post-decision MQ, after
draining the immutable old listener. Retain the resulting coordinator/guard before
fallible checks; create the NEW admission/dispatcher binding while APP remains
closed. Do not run fresh genesis, overwrite target UID, cast it into INITIAL, or
derive permission from metadata promotion. Existing general installation mechanics
may be an internal last step only behind this typed handover producer. All NEW
members and the target leader must still complete activation before DATA ingress.

## Smallest genuine tests, in order

1. Preserve the existing full fixture. Add a separately typed no-refresh control
   reaching the actual quorum-durable decision/metadata commit with OLD guards
   retained. Confirm the committed observer currently refuses before native
   decision observation; capture actual policy IDs, ACK ledger, R/H/B/UID and
   permanent refusals. This is a necessary-policy component baseline only.
2. Drive the CLOSED producer with actual decision publication, actual local owners
   and fresh MQ; require original StateArc/registration/coordinator/open/ingress
   identity unchanged. No fixture guard-map substitution or ordinary installer.
   Reject metadata-only, wrong decision/image/closure, uncovered CQ, changed birth,
   expired original caller, foreign writer/slot and missing archive before grant.
3. Use real retained `NativeInitialDataHost` owners, not just EnforcedNetwork's
   guard map. After extension, unchanged committed observation/NEW preparation
   must succeed, but early APP/Strong stays closed. Complete all final replicas,
   release last, obtain target-leader next ACK and exact Strong prior/new values;
   old writer/domain remains refused. Cut before/after WAL/cache publication and
   after one member progresses; fresh reconciliation never revives old receipts.
4. Add the genuine fifth registered receiver's distinct CLOSED first-writer lane,
   then repeat through command-driven independent processes and real TLS with the
   SAME ordinary client. No child service Arc or low-level proposal injection is
   an operator result. Remote image/joint wire, automatic convergence, lower-R/
   missing-member recovery, cold cuts and full online 8 GiB qualification remain
   required; this proposed necessary-policy seam alone closes none of them.
