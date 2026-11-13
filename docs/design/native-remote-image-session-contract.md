# Native remote image session: first executable contract

Source inspected: `3ee40c19b44affa0a481a66b278ded3af817e910`.
This is a proposed implementation contract, with no code, build or runtime result.
It narrows the [cross-process gap audit](native-joint-cross-process-gap-audit.md)
and the [operator audit](native-operator-four-to-five-gap-audit-2026-11-11.md).
A registered fifth receiver owns ingress; it does not yet move range data.

## Existing boundary and first deliverable

Paths below are relative to `crates/bicdb-core/src/`.

| Existing real producer | Ownership that must remain local |
| --- | --- |
| `distribution_range_handoff/joint/runtime.rs::close_old` and `joint/old_quorum.rs::NativeJointOldRound` | Per-OLD-node actual service, permanent promise, closed native history, freeze and read pin; every observed irreversible CQ/pending obligation |
| `distribution_data/native_joint_handoff.rs::prepare_native_joint_capture_inputs` and `ActualServiceNativeJointCaptureInputs::begin` | Actual source DB/open, immutable lineage, OLD closure, owned freeze/pin |
| `distribution_range_image.rs::RangeImageCapture` | Source DB, actual `R`, source-local reservation `B`, namespace/schema bindings, history and row cursors, content accumulator, one previous retry step |
| `distribution_data/native_image_install.rs` bounded begin/stage/audit/clear/copy/verify/adopt/finalize methods | Actual target DB/birth/open, Store, role, fence, safety/catalogue obligations, staged history proof and target freeze |
| `distribution_data/native_joint_handoff.rs::observe_native_joint_new_target` | Finalized image plus actual target lease; materialization is distinct from NEW voting/serving authority |

`TentativeNativeRangeImageSourceAuthorization<'_>` borrows the local capture.
`CompletedNativeRangeImageSource` owns it. The installer retains and rechecks
those objects, including through `db/range_image_native/history_stage.rs`.
Neither object can be reconstructed by decoding its seal or audit JSON.
`ClusterDataRequest` has no native source/image session family. Its legacy
`ExportSnapshot`/`ApplySnapshot` arms are not suitable native producers.

The first coherent implementation must transfer ONE nonempty range between
independent processes through final target verification and catalogue adoption.
It must not stop at an offer/status API. Start with a frozen source and target:
**bounded write unavailability is explicit; online copy/catch-up is unqualified.**
The target remains APP/Strong-unavailable until subsequent joint activation.

## Authority producers, in dependency order

1. The operator commits an exact pending learner relocation using fresh actual
   metadata authority. The target's immutable birth is already enrolled through
   the real JoinFresh path. A metadata-only voter is not a current DATA voter.
   Capture and intended activation descriptors, exact OLD/NEW voter identities,
   incarnation/pins, schema and restore lineage must all be bound separately.
2. Add authenticated OLD-close/status handlers. Each recipient obtains its OWN
   genuine `CommittedHandoff` role, verifies the committed pending plan and
   actual local native/Store domain, then invokes its local close producer.
   The handler owns the resulting freeze/pin and permanent promise in a bounded
   session registry. A wire plan/hash is request data, never a close permission.
3. Add a remote OLD-cut collector consuming sealed, request-correlated actual
   TLS observations from distinct expected OLD peers. It must preserve EVERY
   observed irreversible obligation and verify its inclusion in the selected
   source's immutable history. A maximum native counter is not a closed cut.
   Both the source offer producer and target acceptance need independently
   validated cut authority; forwarding a caller-provided quorum DTO is forbidden.
4. The source acquires its own fresh metadata role and actual OLD-cut evidence,
   prepares actual capture inputs under a short service/DB borrow, then starts
   the existing capture OUTSIDE those locks. It retains the real capture,
   freeze/pin and request lifetime; an opaque session ID identifies that owner.
5. The target obtains its OWN fresh committed-handoff role and OLD-quorum/cut
   observations. Its outgoing transport seals source replies to the original
   request, expected TLS peer/birth and session. A new private remote-source
   verifier consumes these actual observations plus complete validated history.
   Only that producer may mint a distinct remote install capability. Do not add
   Deserialize, numeric constructors or arbitrary hash constructors to local
   capture/lineage/Completed proof types.
6. Add a narrow kernel input alongside the local source-borrow input. The new
   capability must bind the exact source offer/cut, completed history and source
   session evidence to the target's actual role/fence/safety/catalogue context.
   Target-controlled scopes still mint the typed row grant. Every destructive
   step retains existing local identity, staged-proof and currentness checks;
   no network operation may execute while a local journal or DB guard is held.

This is additional authority implementation, not a trait that blindly trusts
remote fields. Fresh metadata alone does not prove DATA closure. A source
receipt hash alone does not establish native materialization. Handoff must not
be cast into `CurrentVoterRepair` to get the existing replacement-source lane.

## Bounded request and session family

Names below describe required operations, not existing public APIs.

| Operation | Receiver-owned action and evidence |
| --- | --- |
| `OldClose` / `OldCloseStatus` | Produce/read the exact protected local OLD closure; verify scope/current role and expose all irreversible constraints through bounded pages |
| `BeginImageSource` | Consume actual cut authority and create an owned capture; return sealed offer bindings and initial cursors, not a capture permit |
| `ImageHistoryNext` | Run existing `export_history_step`; preserve contiguous 0..R entries, H0/H and wire-chain commitments |
| `ImageRowsNext` | Run existing `export_step`; return canonical namespaces/records and exact previous/next cursor plus row chain |
| `FinishImageSource` / `SourceStatus` | Validate full source completion and retain the actual capture; expose completed seal or explicit ended/unknown status |
| `BeginTargetImage` | Recipient independently validates authority and creates actual target fence/intent/installer/freeze before acknowledging acceptance |
| `AdvanceTargetImage` / `TargetStatus` | Bounded pull, stage/audit, clear/copy, verify, adopt/finalize; return actual stored progress/receipt, never inferred readiness |

For the smallest path, the target pulls from the source using its own TLS
transport. The controller requests bounded advancement and reads progress; it
cannot push unsealed caller-supplied proof frames into the kernel. Source and
OLD-close transport observations need distinct sealed constructors. The
registered-replica immutable allowlist must explicitly add only these operations;
old metadata-only listeners remain closed. No implicit generic snapshot fallback.

Bind every operation to cluster/bootstrap, source/target identities and actual
births, plan/relocation/session/nonce/attempt, purpose, metadata configuration,
range/epochs/span, schema/restore, cut R/H0/H, source receipt and catalogue root.
Session lookup must also revalidate actual DB/Store open and owner registration.
Target namespace UIDs and target-local B come from the target; source B is audit
and source read visibility only. Neither B is a range index or a supplied floor.

## Frames, retries and cancellation

History export already bounds a page to 256 entries and 272 KiB. Row export
allows 8 MiB batches/records and a 9 MiB encoded step; transport configurations
can have only a 4 MiB frame. Check the COMPLETE serialized envelope before
publication. A single large record therefore needs bounded fragments, even
when record-count batching is one. Fragment frames bind transfer/step/record,
ordinal/total, byte count, whole-record hash and chain; persist/assemble only a
bounded complete record before the existing canonical record validator/import.
Duplicate bytes must match exactly. Reject missing/reordered/conflicting or
oversized fragments before mutation. Do not silently lower the supported value
contract, bypass frame limits or truncate history to fit an envelope.

Preserve existing exact-cursor idempotence: the source retains only the current
and one prior retry step. Unknown older cursors refuse; status does not rewind
or invent a durable source cursor. Target durably records accepted stage/copy
progress before a successful reply. A lost reply is reconciled with the exact
stored step/result; it cannot clear twice or change the accepted image.

Authority lifetime is at most the minimum of the original business parent,
source/target request bounds, actual roles and collected DATA-quorum evidence.
Transport decoding must retain original start/deadline/cancellation provenance;
a supplied duration or a retry cannot renew it. Capture's 300-second wait bound
is not a longer QQ/role lease. Each local operation checks its actual parent;
remote cancellation is a bounded best-effort stop request, not proof that a
promise vanished. Late fsync/timeout is unknown until actual readback, never an
ACK. Session expiry closes ingress and retains durable refusals. It must not
release permanent OLD promises, arm DATA or infer controller abandonment.

If an image cannot finish under those bounds, fail closed and measure the cut.
A distinct fresh-phase protocol would need newly acquired per-recipient role
and OLD-cut evidence plus exact retained capture/fence/intent/history identity,
while retaining the original business deadline/cancel. It is not implemented by
adding a refresh flag or replacing the token on an existing capture.

## Same-open streaming versus durable cold continuation

The first producer is SAME-OPEN: bounded live registries own all capabilities.
Read-only status can attest actual stored progress after a lost reply; an ended
or unknown session cannot mint a new capture, target freeze or permission.
Returning historical audit after expiry must be distinguishable from live
advancement authority. Keep registry count, bytes and per-session work bounded.

A process restart loses real source pins and target lease owners. Durable
intent, accepted cut, stage checksum or receipt JSON is restrictive evidence,
not a recovered live owner. Cold continuation requires a separate authorized
producer over actual protected accepted-cut facts, complete original/staged
prefix obligations and newly collected current role/DATA evidence. Source death
requires an explicit compatible replacement protocol; the existing current-voter
substitution capability does not make an unexposed learner a DATA voter. Initial
implementation must refuse these unsupported cuts without deleting stage or
resetting native Unknown to zero. Test the refusal and retain actual fault state.

## Deterministic acceptance and remaining gates

Use the existing four DATA child-process CLI harness and external fsynced ACK
ledger. Add ONE genuine fifth JoinFresh replica receiver, obtain a command-driven
committed learner plan, and drive every new operation over real TLS sockets.
No child service Arc, low-level test proposal or constructed quorum substitutes
for the production command path. Copy a nonempty R>=1 range with deliberately
different source/target B. Verify full old ACK values/history and exact target
UID/receipt/catalogue, no extra rows, while APP/Strong remain unavailable there.

Exercise wrong peer/birth/plan/nonce/attempt, missing OLD voter, uncovered CQ,
history fork/hole, duplicate/lost frame, large escaped single-record fragments,
expired/canceled original request and source/target session loss. Preserve exact
ACK ledger and durable refusal state before failing positive acceptance. Include
source post-fsync reply loss and target cuts before clear, after clear and before
catalogue finalization. Report cold/source-replacement refusals as open gates.

Image materialization is not completed 4→5. Next producers must carry remote
NEW materialization and the same complete decision to OLD+NEW DATA majorities,
then final metadata, fresh policies, NEW preparation/publication/open/release,
actual target-leader ACK and Strong verification of old/new ACKs, stale-old-writer
refusal and proof-authorized cleanup. Automatic placement/retry/convergence,
lower-R repair, origin-down/historical Commit adoption, online copying and
independent-VM fault qualification remain separate acceptance gates.
