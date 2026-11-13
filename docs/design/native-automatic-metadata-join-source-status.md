# Automatic metadata join: source checkpoint

This private source batch implements metadata scheduling and one fresh learner
command. It has not been compiled or executed locally. Existing remote receipts
for the manual CLI and cold learner command do not validate this changed batch.
There is no data Serving, placement, rebalance, SQL, VM or physical qualification.

## Command and ownership

Four initial native metadata hosts retain their existing explicit preparation,
launch, election and immutable metadata-control listener transition. Once control
is enabled, default CLI scheduling starts the one actual-store metadata
supervisor and runs a separately bounded enrollment/promotion controller. The
supervisor owns elections/heartbeats; the controller does not create a second
election worker. `--manual-enrollment` disables all three automatic scheduling
activities for the original serial diagnostic regression. It never relaxes a
producer, persistence, owner, membership or admission check.

`cluster native-metadata join-fresh ROOT` supplies its OWN cluster/node identity,
endpoint, TLS files, seed route, capacity and local command/status paths. It
performs actual authenticated registration, then independently runs the fresh
factory/schema/own-MQ path. It never accepts seed paths as local configuration.
An existing root is refused before registration. TLS and current CONFIG still
authorize registration; a returned CONFIG is not backend ownership or readiness.
The command starts a provisional birth listener, observes actual committed birth
binding, starts the SAME-service owned metadata transition, then retains its
actual backend/listener/supervisor for the process lifetime. The command does not
accept a range/shard selector. Fresh and explicit cold-reopen constructors remain
Learner-only; neither accepts a blank or reopened factory under a Voter role.

Leader hints are routing DATA only. Learner refresh tries bounded actual known
peer snapshots and obtains a new actual OWN Host MQ under the original caller
budget. It does not assume the initial seed remains leader. The endpoint and
incarnation must still match the caller's own root/TLS identity. Cancellation or
IO completion cannot renew the original parent budget.

## Promotion

The controller visits one actual committed unassigned member per attempt, with
round-robin selection and bounded backoff. It uses at most three actual Append16
catch-up calls. An unreachable endpoint cannot retain the caller for unbounded
verdicts; any still-running worker keeps the controller's real operation slot,
launch, store and service until actual exit. No hard OS-IO wall-clock bound is
claimed. Numeric native metadata endpoints avoid DNS under this path. Each
attempt has a 900ms verdict budget; repeat attempts retain actual durable pending
state and do not create a new pending entry solely because a reply was lost.

A private `NativeLearnerPromotionReadiness` is produced only after recording a
genuine protected Append ACK from that participant. It binds the actual source
StoreUUID/term/committed C/index, native receiver StoreUUID, physical node
identity and original budget. A JSON copy of the ACK cannot recreate its skipped
native provenance. A distinct independently fresh actual owned-leader Host MQ
must match the exact C/term/readIndex before proposal. Existing exact persisted
pending-entry validation/refsync is shared with registration/enrollment. The
ordinary metadata proposal still requires genuine old AND new consensus
majorities. An already-promoted identity is never demoted, rebound or reset by a
retry.

A promotion proposal, queued response or global Voter role is not local voting
readiness. The controller sends the actual committed promotion snapshot/entries
to the target and validates a protected ACK through that entry on the SAME
receiver StoreUUID, then obtains another fresh leader MQ. Its retained report is
DATA, not a reusable authority. On the target, a separate private owned-member
observation validates the actual protected Registered store, current actual DB
open, original namespaces/schema, birth/restore/pins/incarnation, unassigned
state, local committed C/term/index and independently fresh OWN MQ. Only this
exact local state can report `owned_metadata_voter`. The status includes the
actual local voter set so configured role cannot conceal an unapplied entry.

## Added source controls

* The real CLI automatic test starts four processes with scheduling enabled and
  starts the fifth using only `join-fresh`. It never sends fifth diagnostic
  registration/enrollment/start/catch-up/proposal steps. It requires the actual
  local voter set, same protected receiver StoreUUID and installed frontier,
  namespace identities, and data refusal on all five real services.
* The prior manual CLI test explicitly opts out of automatic scheduling. Its
  wrong-address-before-root, cold-owner, quorum-outage and all data-refusal
  assertions remain intact.
* A real-store/TLS promotion component control rejects a decoded ACK and a
  canceled original proof, then requires a genuine old/new-quorum commit and
  local installed promotion on the same store.
* A second real-store/TLS control leaves a genuine promotion pending without
  replication, performs a real other-leader election and barrier, and refuses
  the held old-owner/term promotion proof without modifying the new state.

None of these new assertions has a runtime result at this checkpoint. Focused
filters are `native_metadata_promotion_`, `native_owned_metadata_supervisor_`, and
CLI target `native_metadata_operator_cli` (which contains both manual and default
automatic process cases).

## Remaining gates

This implements only metadata membership. The fifth has no data ranges, no
NativeReady/activation/strong-read permission and no application Serving. Full
initial data activation, catch-up/placement scheduling, joint data handoff and
operator qualification remain separate requirements.

Cancellation after an actual metadata listener binds but before the original
learner transition returns may retain a stopping owner. If the leader already
committed promotion meanwhile, the original Learner-only transition retry can
refuse. This checkpoint preserves that owner and refuses status rather than
resetting a UUID or blessing a Voter factory. A separate exact Registered-owner
catch-up/rebind producer and crash-cut tests are required to qualify that cut.
Likewise an all-owner cold reopen AFTER promotion remains unsupported by the
existing strict learner-only cold constructor. No bounded crash recovery or
leader-process-kill result is inferred from the live automatic source test.
