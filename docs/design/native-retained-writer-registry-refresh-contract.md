# Retained native writer registry refresh: approval contract

This is a **source design, not an implemented or executed repair**. Source is
`1d3976f9f8011f3ac7411494aa0dfb432763ce78`, a separate strict regression on
`ca7d481467c40cf6ac324ceb8c0c7e943d4d1cfc`. The ca7 receiver prerequisite passed
while its post-join read was unavailable; that does not qualify healthy DATA
continuity. The new six-case CLI run is independently frozen, with no result
claimed here. Earlier 3ee JSON-null uncertainty remains a separate observation.

## Exact existing seams

- `distribution_native_writer.rs:53,159`: `HostWriterPolicyFacts::from_actual`
  includes metadata voters in `needed` and requires their committed schema
  labels. `require_topology` rejects a changed metadata-voter set. Simply
  retrying the old enrollment publisher cannot repair both checks: ca7's n4
  has no committed schema label.
- `distribution_native_host/learner_birth.rs:392,551`: the sealed, non-serde
  `NativeLearnerBirthObservation` already binds authenticated actual birth,
  actual schema, current schema authority, requester/source MQ, pins,
  incarnation, original request and original deadline. Its schema accessor
  is not a raw registration label.
- `distribution_consensus/native_metadata_store/learner_enrollment.rs:8`:
  `propose_observed_native_learner_enrollment` currently commits the observed
  birth but omits the observed schema label; its exact persisted pending-body
  retry and independently fresh leader-MQ checks must remain mandatory.
- `distribution_native_host/provisional_listener/metadata_startup.rs:91`:
  `enroll_initial_writer_retained` constructs a new coordinator and installs
  a new registration. It is not a retained-owner refresh operation.
- `.../registration/initial_data.rs:182`: resetting `enrollment=None` would
  re-enter initial genesis capture. No refresh may do that or seed/clear
  recovered epochs, prefix state, freeze owners, slot controls or decisions.
- `db/range_image_native/writer_enrollment.rs:65,147,427`: current writer state
  has the original immutable native policy, exact hook/coordinator bindings,
  DB-open and Store-open checks. The normal installer deactivates/replaces
  registration. `distribution_data/native_joint_handoff/bridge/release.rs:42,112`
  explicitly compares the original writer **Arc**; preserving only UUIDs is
  insufficient.
- `distribution_consensus/host_read.rs:159`: Host MQ is opaque and bound to an
  actual authenticated exchange. Its evidence validates fresh old **and new**
  majorities (`distribution_consensus.rs:730`); `committed_topology()` alone
  does not expose whether a different joint candidate is still pending.

Paths above are relative to `crates/bicdb-core/src`. Line numbers are for the
frozen design source, not a claim about later delivery branches.

## Proposed producers and retained identity

First, when committing an actual learner birth, also commit its **sealed actual
schema SHA** while the node is still a Learner. Refuse contradictory labels;
include this field in exact pending-body comparison and actual quorum commit.
Promotion retains it. A raw registration/template label is insufficient.
Previously promoted missing-label roots remain refused unless a separate
actual-schema publication is proved; this design does not waive their check.

Add a narrow, non-serde `StableHostWriterRegistryRefresh` producer. It consumes
a new actual Host MQ under the original finite parent budget, then validates
the SAME retained DB/service/current Store slot, original installed guard,
coordinator, protected policy and catalogue custody. No network runs under
engine or service guards. A new opaque Host-MQ method checks its private
original evidence after `require_live_request`: both validated old/new quorum
**voter identity maps** must equal the current committed voter configuration.
An in-flight membership-changing joint candidate is refused. A pending
non-voter/load-statistics proposal is not itself a membership change: it can
be accepted only if those actual voter maps and all policy-relevant DATA,
birth/pin/schema/restore facts remain identical to committed C. No uncommitted
candidate supplies policy facts. A topology DTO's `metadata_voters()` alone
cannot prove these conditions.

Permit only metadata-voter additions. Require exact canonical DATA range IDs,
bounds, epoch, leader, voter/replica identities and placement; preserve every
existing member's birth, incarnation, pins, schema and restore domain. Newly
added metadata-only voters need actual committed enforced birth, pins and the
proved schema label. No data member/leader change, membership contraction,
restore/schema transition or unsupported joint DATA transition is admitted.

Keep the SAME `NativeWriterHostState` Arc, registration object/generation,
coordinator Arc/ID, DB-open and Store-open. Do not call the ordinary installer.
Retain the immutable original policy and introduce an append-only protected
registry-extension audit, not replacement of original Abort/CurrentJoint/
release provenance. Each record binds its predecessor and original policy
SHA, old/new semantic registry and full policy-relevant configuration hashes,
actual MQ context/term/read index, and original registration/coordinator/open
identities. Record the full bounded actual MQ audit; no voter-list DTO creates
one. An in-memory extension on that original state becomes usable only after
actual native-WAL publication/readback and exact-own revalidation. Existing
full database/hook/custody guards remain required; current-service Store-slot
identity must be checked explicitly, not inferred from a retained old Store
Weak or its cached open ID.

`require_topology` may recognize only that exact protected, verified extension.
It must still reject changed DATA quorum, foreign births, lost custody, writer
replacement/ABA and all unsupported domains. Extension DATA grants no role,
Ready, data quorum, snapshot or absence permission; each write/Strong request
retains its existing actual fresh authority and native-prefix checks.

## Publication, failure and ownership cuts

1. Acquire fresh stable Host MQ with no engine guards; under actual bindings,
   take a sealed snapshot of the necessary original installation and ancestry.
2. Before any native mutation, repeat current DB-open/Store-slot/hook/state
   identity and exact predecessor checks. Use the existing admission/journal
   order; no helper re-enters a native journal or waits for network/service
   while holding it. Busy or replaced ownership is refusal, not a retry grant.
3. Publish immutable history and current pointer in one native page transaction,
   with full readback and health/custody checks. Original hook and record remain
   intact. No catalogue CQ/everCommit/no-effect/promise record is cleared.
4. Publish the verified in-memory extension only under exact-own serialized
   binding checks and the original live operation budget. Writers/readers
   cannot slip between a transient publication and its late-cancel rollback.
   If the audit became durable but publication failed/expired, retain a
   refused/pending state. A new operation must obtain NEW actual MQ and verify
   the exact persisted predecessor/body before reconciliation; no old token
   is revived. No user ACK is inferred from successful policy persistence.
5. On cold reopen, neither the extension nor its audit installs a writer or
   reconstructs a DB/Store-open grant. Existing strict custody, birth and host
   lifecycle requirements apply. Unsupported cold-owner continuation refuses.

The new per-installation extension cache must not change original state Arc
identity or make original receipts refer to a new writer. Same-Arc hook
reinstallation still revokes the old registration. Cancellation, health or
publication errors cannot erase that revocation or re-enable an old owner.

## Bounds and required acceptance

Proposed bounds are 64 immutable extensions per original installation and
256 KiB of complete encoded audit per extension, plus a bounded current
pointer: at most 16 MiB logical history plus one pointer (storage overhead
separate). Exact idempotent repeats reuse the stored audit, including when the
new live MQ has another context. Additions are monotone from the last accepted
registry and the existing MQ limit is 64 voters; removal/replacement and
unbounded membership churn remain unsupported. Refuse size/count exhaustion
**before mutation**; no eviction, TTL reset or empty rebootstrap. This is not a
sustained arbitrary-churn or 8 GiB workload qualification.

Approval/implementation tests must preserve original assertions:

| Actual scenario | Required result |
| --- | --- |
| Frozen strict ca7 baseline, same gateway, unchanged DATA assignment | First establish old ACK/Strong; attempt BOTH post-join old read and fresh Append, retain all outcomes and five actual Child statuses; classify the first real failure |
| Fresh join with actual observed schema publication, stable final MQ refresh on existing DATA owners | Same gateway reads complete old ACK, receives fresh ACK and Strong exact new full value inside explicit 10s gate; original per-request deadline remains 2s |
| Pending joint metadata membership change, expired/canceled MQ, different requester/business or DTO-only audit | No extension/native serving grant; original controls retained |
| Pending stats-only proposal with identical actual old/new voter identity maps and identical policy-relevant facts | May use committed C only; no claim the pending proposal is committed or a membership change |
| Changed range leader/epoch/voters/placement, existing birth/pin/schema/restore or current Store slot | Refusal; no catalogue/native-prefix reset |
| Original writer replacement or same-Arc hook reinstall between capture, WAL and cache publication | Old operation refuses; no foreign hook clear or original-receipt rebind |
| Fault before WAL, durable WAL then returned error, post-WAL cancellation, cache publication cut | Exact pending audit retained where durable; no success from expired operation; independently fresh exact retry or explicit refusal |
| Strict same-root cold reopen, missing/foreign catalogue, previous extension replay, history/count bound | No implicit writer/Ready permission, no missing-file recreation or unsafe eviction |
| Abort controls and original CurrentJoint frozen/release owners coexist | Original Arc/audits and typed B unchanged; actual existing guards still enforce their domains |

The receiver remains an unassigned, DATA-refused prerequisite. Native image/
joint wire, automatic placement/controller, transfer, SQL, physical deployment
and full Milestone 1 remain separate gates. No production repair is included
in this document or the expected-RED test branch.

## Captured continuity baseline and compatibility refinement

The original policy fact layout did not record member address, lifecycle or
non-schema labels. NEW actual-MQ policy construction therefore records an
optional `member_continuity` map for every needed member. `None` is omitted
from serialization, preserving previous bytes/checksums and same-domain
original-policy reading. It cannot authorize retrospective baseline capture:
registry extension refuses an original None policy. Original policies remain
immutable; no cold writer grant or implicit migration is introduced.

Every extension compares against the LAST accepted protected extension's map,
not just the initial cohort. Earlier-added voters remain bound on later
additions. Metadata role is governed by the actual monotone committed-voter
proof, not frozen as an unrelated immutable member field. Address, lifecycle
and full-label equality is deliberately stricter than birth/pin identity:
endpoint drift, drain/replacement and administrative label changes require a
separately authorized transition and are not repaired by this first path.
Complete facts and full MQ audits remain bounded; no labels are truncated.

Additional unexecuted source controls use a genuine actual-MQ/native-WAL old
None profile as a negative, and an actual bound fifth-voter promotion/first
registry extension followed by a real five-voter committed label mutation.
The latter must fail against the previously added member's saved continuity
while retaining the exact accepted native audit. These are metadata-only
component controls, not a new operator/relocation qualification.

Exact member-continuity comparison applies ONLY when validating a new registry
extension against the last accepted map and binding the historical native
record to its actual MQ snapshot. Ordinary APP/Strong `require_topology`
retains the previous birth/incarnation/pin/schema/voter-domain checks; advisory
labels, health/lifecycle or routing data do not acquire a new blanket fence.
A genuine fifth-label-mutation control checks that the necessary ordinary
writer guard remains valid while a new registry refresh refuses that changed
baseline. It still does not manufacture DATA readiness.
