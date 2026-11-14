# Actual Joined NEW observation boundary

This source slice builds on original owner/lease custody at
`b811b56f65a6243f30f0ceba5029586e1f795a9d`. The sealed materialization view was
introduced by `f1dd8faea744277f05cb4c399d22620a5144839f`. Both are prototype
source; neither is physical qualification or proof of operator scale-out.

`ActualServiceNativeJoinedJointNewEvidence::observe` takes the actual host-retained
`NativeJoinedRemoteJointOwner`, its original `NativeImageInstallFreeze::JoinedRemote`
lease, a genuine remote OLD certificate, and separately acquired fresh Handoff
Role/OWN/current-MStore authority. It has no deserialization, parts or
`NativeJointMaterializedFacts` constructor. The evidence records the actual
selected native final/base/catalogue result and obtains a real source-capture
read pin under the original A. It grants no ordinary installed-writer permission.

The recipient checks the exact complete plan, authenticated original OLD close
record, all OLD member facts, target birth, source MF and source Finished seal.
The OLD native close-record digest and exported selected-prefix digest retain
their different meanings. The real close-record producer verifies their
relationship; equality of unrelated hash domains is not substituted for that
proof. Donor/certificate operations run outside target guards. The actual
MStore is captured and checked before and after the target native read; no
MStore mutex or network operation spans the target service/DB read scope.

`require_current` verifies historical same-open physical/owner/lease facts.
`require_issuance_current` separately requires the original real Role/OWN to
remain live and its captured MStore point to remain current. It does not renew
snapshot requests, reissue metadata authority or convert historical physical
facts into a fresh NEW vote. Any future NEW consumer must check both boundaries.
The original host still owns A after observation failure or drop. No native or
catalogue write is performed by this producer.

The sealed heterogeneous view for `Arc<NativeJoinedRemoteMaterializedOwner>`
returns its actual base/native/seal and invokes `require_joint_point` for current
materialization. Its explicit `joined_target()` accessor distinguishes Joined
provenance; `registered_target()` remains `None`. Existing legacy and Registered
implementations retain their own checks. No Registered/FIRST/local-source cast
or fake OLD quorum is added.

This slice deliberately does not add a Joined variant to
`NativeJointNewObservation`, remote NEW RPC participation, dual-quorum counting,
native DECISION publication, pin handoff/release, first policy or Serving. Those
consumers require their own actual producers and original-owner custody. The
view/evidence API alone does not establish that any later phase executes.

The next genuine required-CI acceptance cases should use the actual same target
that already returned R1 APP and full Strong results, then fully materialize R2
under its original State/A before observing NEW. Required checks include exact
native facts and both accessor kinds; a distinct genuine OLD certificate or
other actual requester must refuse; expired Role/OWN/current-MStore or revoked
original process/A must refuse; canceled old snapshot requests must not be
revived by fresh joint authority. The second range must not acquire ordinary
Serving merely by producing this evidence. No mocked evidence or copied facts
should replace these tests.

No Joined NEW runtime test, physical failure, RPO/RTO, application-transparent SQL
or sustained capacity result is established by this source slice. No new focused
Rust workflow or manual Rust job was dispatched. Validation here is formatting,
source-version consistency and static source checks only; existing required
repository CI remains the execution gate.
