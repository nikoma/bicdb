# Actual image-target release in native joint handoff

This is an unexecuted source checkpoint based on `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`.
No Cargo, application acceptance, Azure operation, version bump, release, or
main merge was performed for this batch. The positive assertion is a required
acceptance condition, not a reported result.

## Missing consumer and narrow successor

The prior source can retain a completed target image through OLD closure,
NEW materialization, protected dual-quorum decision, committed metadata,
NEW preparation/publication and closed catalogue activation. Its only final
release method, `NativeJointHandoffRuntime::release_activated_new_member`,
requires an actual retained OLD-close session. The image target has no such
session. Its `NativeImageInstallFreeze::Recovery` cannot be extracted, and
ordinary Drop correctly returns the attempt-owned freeze to its recovery
runtime. Fabricating an OLD-close session would not establish target ownership.

`NativeJointOpenAdmission::retain_image_release_owner` now retains a dedicated
`NativeJointImageReleaseOwner` from that admission's actual local Image lease
and its original real catalogue activation receipt. Another member's receipt
cannot substitute. No decoded metadata or numeric installed position constructs
this object.

The release requires the original receipt's live budget, same actual service,
store/DB open, protected native decision and current image receipt, actual
birth/namespace IDs, complete R/H, local target B and exact intended NEW
configuration. Preparation, publication and image readers must relinquish
all other actual freeze owners. OLD overlap release is unchanged.

For a Recovery image lease, the narrow consumer checks the original transfer
attempt, S/N, same runtime session and absence of another active operation.
It retires only that obsolete *in-memory* session and consumes the actual
Frozen owner. It does not remove durable recovery/history/promise records or
mint normal recovery Ready. Ordinary Drop cannot reinstall the consumed owner.
The existing joint activation guard still remains closed until actual owner
release and explicit arm.

All fallible binding/cancel/busy checks precede owner consumption. The release
owner retains the original activation receipt when arm fails after real drop;
a same-open retry uses that exact proof and its original deadline. It cannot
renew authority after expiry. There is no persistence in the release operation;
ambiguous catalogue publication cannot produce a release receipt, and the
existing exact refsync path remains required first. Concurrent new freeze or
DB replacement invalidates the actual-open guard and refuses arm.

## Genuine acceptance source

The single test
`native_joint_actual_complete_image_target_becomes_leader_ack_and_strong_read`
uses the existing `EnforcedNetwork::new_with_metadata_members(4)`: four actual
from-birth writer-enforced DBs and owned metadata stores, RF3 {n0,n1,n2}, and
metadata-only native spare n3. It preserves that initial placement as a
fixture precondition. Later transitions use actual stores and opaque proofs:

1. Genuine selected-range ACK1 and fsynced external selected-range ledger;
   other-range ACKs separate source B from range R.
2. Actual committed learner plan, fresh TLS MQ/roles, all actual OLD constraints
   and closures. n0 supplies its full native history and completed capture.
3. n3's real recovery fence and nonextractable installer lease, bounded full
   history stage/audit, clear/copy/scan, native image publication and actual
   catalogue adoption/finalization. Local namespace identity is retained.
4. Actual survivor + target NEW materialization; the same complete decision is
   protected on both data sets. Real fsynced metadata stores commit certification,
   promoted-frozen union, and final NEW membership. Planning helpers cannot
   replace those metadata acknowledgments. All real Host writer policies refresh.
5. Actual NEW preparation, native publication and closed catalogue activation.
   n3 is the NEW data leader, not an overlapping old leader.
6. Wrong-member receipt, still-held owners, cancellation and held-service
   negatives precede release. A test-only error after real owner consumption
   verifies that the closed guard and original receipt survive for retry.
7. Actual image-target release plus unchanged overlap release, stale old-member
   refusal, real new-leader coordinator recovery and ACK2, all NEW native R/H and
   values, local target UID, fsynced selected-range ledger reconciliation, and
   the target's actual strong-point read authority/finalization path.

The test retains native and metadata roots and prints their paths. The metadata
fixture root also contains ephemeral test TLS material; do not publish it or
copy private key files into an evidence bundle. A later executed receipt must
report the first actual failure boundary and preserve the original assertions.

The image/decision orchestration uses same-process opaque-object integration;
it is not a handoff RPC/controller, fresh fifth-node join, automatic operator,
physical crash or deployment qualification. No test result is inferred from
formatting checks. Cold/expired release recovery, source deletion/cleanup,
new recovery after joint activation, and post-joint NoData across all six
previously audited seams remain separate open gates. The actual abort journal
4096-slot/64-MiB permanent capacity bound remains open.
