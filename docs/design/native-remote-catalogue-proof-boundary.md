# Remote native decision and catalogue proof boundary

This inspection covers private source `f201b937e28bcc6c75364e99a793d3bfe593d265`,
tree `74b727c4be77ddf23a83909d423cb6650af9514c`. It incorporates the finite
FIRST/native-decision component from `77bffe7063bb3861f3a77fbd9e6d775770d23c36`.
Neither source is a merged release, executed acceptance result or physical
qualification. Later implementations must be assessed at their own revisions.

The independent inspection retained 28 exact source pins in
`first-f201-remote-catalogue-dual-durability-contract-source-review.json`,
SHA-256 `7ab90fcb85cbc275ee0fb85117ede05af36bfedb6d90490acee767878ec17015`.
That local report is source evidence, not an execution receipt.

## What this source actually produces

| Inspected boundary | Classification | Evidence and limit |
| --- | --- | --- |
| Remote native DECISION | Passed source-presence check; runtime not tested | The original recipient retains its complete expected decision, native attempt and actual successful sink return before outer reply checks. |
| Read-only known native outcome | Passed source-presence check; runtime not tested | A fresh request checks the same original committed XID, exact CURRENT/archive/body and protected provenance. A failed or unknown native sink has no retained successful return and cannot be promoted from visible bytes. |
| Remote range-catalogue confirmation | Failed source-presence check | The remote native sink returns a native fact without calling catalogue confirmation. |
| Remote OLD/NEW dual-durable aggregation | Failed source-presence check | The coordinator retains receipt DATA, which cannot construct the existing local durable-decision proof. |
| Remote conditional metadata certification | Failed source-presence check | The actual local proof consumer exists; there is no production adapter from the remote component. |
| Remote Prepare/Open/ALL-release and automatic Serving | Failed source-presence check | Later local components exist, but are not a connected remote operator. |
| Unchanged source/persistence inspection | Passed within source scope | Root checked the 16 changed files and the unchanged native persistence suffix; original ten test definitions remain unchanged. |
| Compilation, execution, crash recovery, physical scale-out or capacity gain | Not tested | No result follows from the presence of a type, API or source review. |

A failed source-presence check identifies a missing producer; it is not an
executed failing runtime test. No timing or original Azure cause is established
by this inspection.

The [FIRST coordinator](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_coordinator/native_decision.rs)
collects actual authenticated native replies. Its receipt map is historical
correlation, not a local native owner, a catalogue vote or a Serving grant.
The [recipient custody](../../crates/bicdb-core/src/distribution_range_handoff/joint/remote_new/participant/decision.rs)
retains the actual native attempt. The
[native sink](../../crates/bicdb-core/src/db/range_image_native/serving_configuration/joint_handoff/decision/remote.rs)
persists the decision and protected provenance, but does not confirm the range
catalogue's global decision fields.

The existing [catalogue confirmation](../../crates/bicdb-core/src/distribution_range_consensus/joint_handoff_state.rs)
and [Registered exact-candidate continuation](../../crates/bicdb-core/src/distribution_range_consensus/joint_handoff_state/registered_decision.rs)
require genuine local admission/prepared owners. A decoded remote receipt must
not be used to manufacture those owners. The
[local durable-decision round](../../crates/bicdb-core/src/distribution_range_handoff/joint/decision.rs)
counts actual native and catalogue completion on independent OLD and NEW
majorities, including the activation leader. Those local constructors remain
strict.

## Required real producer, before a remote metadata vote

The next implementation needs the original recipient to confirm its own
catalogue under fresh actual Role/OWN/current-MStore authority. It must retain
the exact original and candidate whole catalogues, encoded bytes, actual
Store/open/native/phase-owner binding and IO outcome before effects or outer
checks can fail. It must preserve unrelated ranges and unresolved obligations.
Generic reads and writes stay fenced while durability is uncertain.

A known native success is necessary but insufficient. Catalogue fsync, rename,
directory durability and exact readback must complete on that actual owner.
A lost successful reply may be observed read-only. An actual fsync error stays
uncertain; visible bytes alone are not a vote. Any permitted continuation must
prove and durably re-fsync the same retained candidate under fresh authority,
without replaying the native transaction or silently rebasing a whole head.
Missing RAM custody, another Store/open, a changed head or a revoked owner must
refuse. Cold recovery requires a separate real recovery producer.

Only an actual pinned outbound exchange may mint the distinct opaque remote
evidence. A new aggregate must count unique actual recipients against OLD and
NEW majorities separately, include the activation leader and require the same
complete decision. Mixed bodies, stale requests, duplicate members, wrong
identities and unknown catalogue outcomes cannot count. If a different metadata
leader needs that proof, it needs its own legitimate observation path; forwarded
serialized coordinator receipts cannot impersonate it.

The [metadata consumer](../../crates/bicdb-core/src/distribution/native_joint_handoff.rs)
must condition a proposal on the actual current committed topology, exact
decision/session/nonce/configuration, fresh HostMQ and current metadata Store.
The proposal then needs real consensus append, acknowledgments, quorum commit
and fresh exact committed readback. Returning a proposal or writing a local
topology file is insufficient. Native and metadata uncertain outcomes retain
their original identity; an API error does not prove rollback.

## Membership changes require a new phase proof

The current [remote request validation](../../crates/bicdb-core/src/distribution_range_handoff/joint/remote_new.rs)
and [plan role check](../../crates/bicdb-core/src/distribution_range_handoff/joint/plan.rs)
bind the original capture descriptor and pending relocation. Those checks
cannot authorize a later union/final epoch. They must remain unchanged.

After certification, each later stage needs a distinct recipient-local producer
bound to the same retained decision and original physical owner, plus fresh
HostMQ/current-MStore proof of the exact committed stage and current descriptor.
The [committed-decision observation](../../crates/bicdb-core/src/distribution_range_handoff/joint/committed.rs)
shows the existing local AwaitingActivation boundary. Preparation, native bridge
publication, Open, every-final-member release, writer policy and per-request
Strong readiness remain separate obligations. Membership alone grants none of
them.

The required deterministic controls use genuine normal hosts: complete native
and catalogue success, native-success reply loss with the same XID, catalogue
reply loss, actual catalogue fsync uncertainty, exact-candidate continuation,
changed whole head, foreign owner/open, duplicate/mixed/stale quorum inputs and
metadata proposal reply loss. Later epoch and partial-release cases need their
own controls. These are acceptance requirements, **not executed results**.

This necessary intermediate boundary does not narrow the goal to certification.
The finish condition remains automatic, fault-safe four-to-five-node relocation,
correct Strong reads, preserved acknowledged commits and full convergence;
cold recovery, permanent OLD-member loss and registry retirement remain open.
