# Current joint recovery: source-only first batch

This branch is a proposed successor of the retained `b277260b0` post-joint
Prepare-only baseline. That baseline completed the actual joint release, target
leader ACK2 and Strong R2, then stopped at n3's ordinary NormalRecovery fence.
Safe refusal was not successful recovery. Its root, ledger and errors remain
separate. No local Cargo or executed result is claimed for this source.

The proposed operation uses private wire 28, range catalogue 16, and the local
`native_current_joint_recovery_v1` reader fence. It is not a rolling-upgrade
contract. Old purpose behavior and old omitted-field bytes remain unchanged.

## New authority versus old facts

`ClusterDataNodeService::observe_current_joint_recovery_purpose` reads the
actual protected native bridge/current archive and matching catalogue
activation, actual birth, registered native catalogue StoreUUID, immutable
physical base/namespace/history identity and original current writer object.
Its output is request intent data. It cannot construct a role or data quorum.
Each receiver independently obtains a fresh CurrentJointRecovery metadata
witness and revalidates the same actual native domain before fencing. The
protected NEW births, pins, schema, restore, exact epoch and leader must match.
No live old Prepared/MQ lease is revived.

The dedicated catalogue transition owns the newly drained N freeze and archives
the exact old fence, phase, certification map and retired nonces before replacing
it with a new refusal in one atomic durable catalogue publication. Old promises,
raw Prepared/Commit/CQ/Applied entries, sparse decisions and protected native
slot controls remain untouched. The retired-nonce set survives epoch changes.
The archive is bounded to 256 records and 8 MiB; overflow refuses, never truncates.
The original writer object is also captured in an EMPTY-on-reopen weak lifetime
map. A newly installed object with matching serialized generation cannot revive
the old operation. The reader fence is persisted before catalogue16. Any later ambiguous catalogue
write retains restrictive in-memory state and uncertainty.

Only a NEW-domain slot strictly above the original protected cut can use this
NoData path. The real candidate/no-effect producer, permanent per-member promise
and actual NEW data quorum remain mandatory. Canonical Aborted publication
inherits the actual predecessor's local data B and writes no application rows
or application WAL. Its native prefix binds the protected joint SHA; its actual
stored decision is checked against the independently loaded protected bridge,
including during prefix reopen validation.

Actual local readiness uses that effective NEW domain only after the saved16
scope and actual native domain have been checked. Every retained raw decision,
current and archived historical certification, permanent sparse obligation and
protected abort control must be covered by actual immutable native history.
The wire readiness domain is explicitly ProtectedCurrentJointAnchorV1.

A real ClosedReady quorum may publish a recovered serving record that separately
binds the protected joint decision and the unchanged physical base. A later
actual Completion quorum, durable activation and the newly owned Frozen/read
pin generate a new same-open serving generation. Ordinary command/read admission
then selects that verified new activation; it does not reuse the invalidated
original joint open guard. The original joint decision and activation remain
historical anchors. No naked promise deletion, old-source substitution, counter
cast, synthetic no-effect or generic normal-purpose bypass is introduced.

## Acceptance source and limits

`native_post_joint_actual_new_domain_prepare_only_recovers_then_ack4_and_strong`
uses actual enforced factories/owned metadata and real transport evidence. It
retains the genuine completed joint prerequisite, target n3 as NEW leader,
Prepare-only3/no ACK3, new-purpose discovery, full NEW fences and Abort3, all
three actual Ready/Complete/Activate operations, and real ACK4 plus Strong R4.
It asserts exact original refusal archival and unchanged native/raw/promise/UID
state at each new fence. This test is unexecuted at this source checkpoint.

This first producer intentionally requires the exact protected activation
configuration/epoch/leader, same-open writer registration and original frozen
operation. Higher epoch or leader replacement, changed membership, unresolved
OLD-domain tails, NEW-domain committed/CQ suffix historical adoption, unavailable
no-effect origin, minority promise reconciliation,
all-owner cold recovery and automatic operator recovery remain OPEN. A native
commit obligation must be repaired as committed; it cannot become NoData.

A catalogue persistence ambiguity is fail-closed. The existing exact retry can
re-fsync only once the actual-domain/durable-store preconditions hold; a general
uncertain/cold reopening controller is not supplied here. Unsupported cases are
refusals, not a bounded self-healing or physical HA qualification. Permanent abort
inventory capacity (4096 slots / 64 MiB) also remains a separate open gate.
