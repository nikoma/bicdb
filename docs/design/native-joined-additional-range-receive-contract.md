# Additional populated range on an existing Joined target

Source base: `d6409423a8170074ef9adb41bd93dbff83ca512d`.
This change is the first source foundation, not a completed receive path or
qualification result. It was formatted/source-reviewed, not compiled or run.

## Existing boundary and preserved invariants

The existing FIRST path is intentionally a never-writer receiver. Its original
hook, B0, whole-native-inventory, unstarted abort catalogue and no-local-voter
requirements cannot be reused on an already serving database. The current
`Registered CLOSED` capability also refuses an existing nonlearner anywhere on
the target. Those producers remain unchanged.

Concrete base paths/functions:

- `registration/registered_replica/closed_fence.rs`,
  `NativeAssignedRegisteredClosedFence::require_live`: genuine blank target,
  learner and original no-writer lifecycle only.
- `distribution_range_consensus/recovery_state/registered_closed.rs`,
  `preflight_registered_closed_constraints`: whole initial catalogue and B0;
  it explicitly refuses populated history/obligations.
- `db/range_image_native/registered_receiver_domain.rs`: original B0 and whole
  protected native inventory. These FIRST constraints stay strict.
- `db/range_image_native/writer_enrollment/registered_no_domain.rs`,
  `registered_remote_attempt.rs`, `registered_remote_mutation.rs` and
  `registered_joint.rs`: actual original absent hook, not an installed writer.
- `distribution_range_consensus/registered_no_domain.rs`: selected raw/refusal
  preimage plus catalogue identity, but explicitly no installed State.
- `db/range_image_native/writer_enrollment/data_domains/*`: existing selected
  CLOSED/Serving audits on the same installed State. This is the extension
  mechanism, not whole host-policy re-enrollment or metadata-only refresh.

`registration/` above abbreviates
`crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/`;
other paths are relative to `crates/bicdb-core/src/`.

## First source foundation

`NativeJoinedRangeReceiveOrigin` is constructed only by the real retained
Registered-origin Joined host. It binds the original installed guard/State,
coordinator, service/runtime, database-open, Store slot, protected MStore and
Joined lifetime. It is identity, never FIRST/NoDomain/R0 or source authority.
It neither clears the dispatcher nor replaces the hook. Its per-call request
consumes actual Role/OWN, captures an actual member/version point outside engine
locks, and proves that this selected range is the genuine local target learner.
No other range is required to be empty or have B0.

`NativeJoinedReceiveStorageScope` borrows the actual installed hook/admission
and native journal. It validates the exact original native policy and birth,
actual selected Frozen and member/version identity, and can inspect selected
lower bounds or scan selected physical row absence. It cannot authorize a row
transaction, native installation, NEW vote or Open. Unknown lower bounds are
not a certified zero. The scope is deliberately distinct from all FIRST scopes.
Current-MStore checks occur outside engine locks; local version checks are not
a distributed lease or a replacement for native epoch/quorum fencing.

The Joined DATA-domain adapter retains the actual existing coordinator/guard
and the same transfer's attempt collection/cleanup owner. It accepts only the
existing typed joint lease and CLOSED/ALL-released admissions. It does not
enroll another host writer, close the original dispatcher or replace its first
range. Unresolved original attempts stay retained. This adapter alone does not
produce the missing image or joint admission.

## Remaining implementation before a genuine second-range case

1. Add a distinct selected Joined CLOSED-refusal producer using fresh actual
   Role/OWN/member/version and the installed-hook scope. Preserve every other
   range's raw history, abort decisions/slots, image obligations and native
   records. Do not call the blank Registered producer or synthesize R0.
2. Add a Joined preflight/proposal/mutation origin to the existing range import
   engine. It must verify selected native/raw/slot/row obligations and retain
   exact A/candidate/xids/pins/outcomes before writes. Only the selected range's
   data/history/index entries may change; normal imported-row WAL/index apply
   and restrictive pending-intent retention remain mandatory. Unrelated ranges
   and global business sequence B may advance between bounded steps.
3. Add a Joined image domain that binds both the original installed State and
   the actual new A/final-image/native-base/catalogue. Neither the no-writer
   Registered domain nor Installed's ordinary range-permission check can stand
   in for it. Preserve the authenticated OLD-record-to-native-prefix association;
   a Manifest/DTO is not an OLD proof. Keep remote/local quorum types distinct.
4. Execute real NEW/dual decision/metadata/Open/ALL release. Then extend only
   this range through the existing same-State CLOSED/Serving audit mechanism.
   Native attempts/outcomes remain owned before errors; no replacement xid or
   callback deadline renewal. Final publication needs lexical native/slot/hook
   ownership plus a short memory-only metadata-version check, fresh quorum
   proofs and range epochs. Each Strong independently proves its own barrier.
5. Route through the original fixed gateway/SDK with no shard hints. Keep the
   already-serving range's actual ACK/full-Strong, WAL/native and index state
   live throughout bounded receiving. New range ingress stays unavailable until
   its own actual activation; readiness observations are not future permissions.

## Smallest complete acceptance

Complete a real first handoff into Joined. Record the original State,
registration/coordinator, DB incarnation/birth/open and Store slot. Through the
original fixed-endpoint SDK, obtain fresh range1 ACKs and full Strong reads.
Then move a genuinely populated range2 into the same Joined database using the
actual hashing/routing and the new producer. At each bounded receive/native/
activation boundary, keep range1 APP and full Strong working under the original
2000ms calls. Reconcile every ACK, uncertainty, failed/retried attempt and exact
payload; verify range1 indexes/WAL/native lower bounds and the unchanged writer
identity. Prove range2 reads after its real release/Serving publication.

Required negative controls preserve existing assertions: selected preexisting
rows/native/abort obligations refuse; target Store/open replacement refuses;
actual metadata term/config change after capture cannot publish a usable vote,
Open or policy; unknown native outcomes retain the same candidate/xid and fence
retry. One genuinely stalled selected-range native I/O operation must not block
unrelated range operations. A held response after export tests only transport
independence and cannot replace that control.

This full scenario is **not tested/ready in this foundation**. Future Joined
source/donor hosting, cold recovery, controller convergence, physical four-to-five
scale-out and post-convergence sustained capacity are separate gates. No result
here changes the failed Azure qualification or production requirements.
