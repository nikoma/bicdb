# Joined receive engine / runtime and joint bridge interface

Interface frozen from the source successor of `cea500178` plus the privacy fix
`5c01e86e6`. This is an implementation contract, not executed evidence. Types
explicitly marked pending below have no constructor/grant yet.

A owns the receive/import engine. B owns runtime custody, the Joined joint
image domain and release/activation. Neither side casts a remote target into
FIRST, INITIAL, local OLD quorum or ordinary range permission.

## Implemented opaque pre-transfer inputs (A)

- `NativeJoinedRangeReceiveOrigin`: only the real Registered-origin Joined
  host constructs it from the exact existing State/installed guard/coordinator,
  service/DB-open/Store slot/MStore/lifetime. `require_live` is a full check;
  **it is not a final memory-only publication check**.
- `NativeJoinedRangeReceiveRequest`: only `origin.observe_request` consumes
  actual fresh Role+OWN and captures the actual member/version point. It is
  finite. Accessors: `role`, `member`, `cancellation`, `plan`, `origin_arc`;
  `require_metadata_current` runs outside engine/session locks.
- `NativeJoinedRangeReceivePreflight`: actual selected row/raw/native/slot
  constraints; only `request.observe_selected_preflight` constructs it from a
  real complete bounded cursor and the exact runtime Frozen. `with_current`
  runs outside Runtime.sessions and performs physical revalidation.
- `NativeJoinedRemoteImageIntent`: prepared DATA, not an installation fence.
  Only `store.prepare_joined_remote_image_intent` with the preceding actual
  preflight, request, source owner/frame and lexical storage scope constructs
  it. Its fields stay private; no getter casts it into FIRST authority.
- `NativeJoinedRemoteInstallPreflight`: only
  `request.prepare_remote_install_preflight(selected, source, frame, frozen)`
  constructs `Arc<Self>`, with no WAL/rotation. It retains the exact actual MF
  Arc, selected constraints, canonical prepared intent, A and read-only plan.
  Accessors: `scope()`, `attempt_id()`, `intent()`, `source()`, `native()`,
  `bindings()`, `safety()`, `selected()`, `readonly_plan()`, `service()`.
  Checks: `require_original_process()`, `require_runtime(&Arc<Runtime>)`,
  `require_original_writer(db, store)` (actual State, never DTO),
  `require_request(request, frame)`, `require_metadata_current(request)`.
  `require_before_rotation(request, frame, &Frozen)` is a **physical precheck**
  and MUST NOT execute under Runtime.sessions or a final version guard.

The host retains this exact preflight in its per-range owned attempt BEFORE
N-to-A transfer. The runtime must retain the same Arc before returning a moved
lease or post-transfer error. Re-obtaining that same retained lease after a
lost reply is not a new N, attempt, transaction or source capture.

## Physical preparation / FINAL memory boundary

Fixed public-to-the-team names, implementation pending in A:

```
NativeJoinedRemoteRotationPoint<'scope>
    ::prepare(...) -> Result<Self>       // actual I/O, OUTSIDE sessions
    .require_current_memory(
        store: &RangeWriteStore,
        actual_unique_n: &FrozenRangeMutationPermit,
    ) -> Result<()>                     // bounded memory only
```

`prepare` receives the exact `Arc<NativeJoinedRemoteInstallPreflight>`, current
request and live source frame and borrows the **actual** service/Store/DB/
hook/admission/native journal. It performs every source/selected/native/schema/
policy/canonical/hash/custody check before the final boundary. The point's
private representation must tie those checked facts lexically to the held
physical guards and exact immutable candidate. It is not a boolean or weak ID.

It captures `FrozenRangeMutationIdentity` (actual gate/private lease/owner),
NOT a borrowed or Weak permit Arc. The caller's temporary N Arc can then drop
so the runtime can obtain its unique actual N and rotate it. Reacquiring a
fresh physical scope after dropping that temporary Arc is allowed only if it
rechecks all actual facts under the new scope; a detached precheck is no grant.

After physical preparation, B takes its selected operation/session and actual
service/slot binding without performing I/O under Runtime.sessions. Final use
checks exact current original owners, unique N/private lease, same checked
State/slot and the actual member publication version. No full `require_live`,
canonicalize, fsync, native read, serde/hash, callback or destructor under the
short final guard. Fresh actual Role/OWN + local version + native epoch/quorum
fences compose; the local guard is not a distributed lease. Preparation may
need a callback borrowing the physical guard bundle rather than an escaping
owned value; the lexical requirement takes precedence over syntactic shape.

A and B must review the final lexical representation before enabling transfer.
No current type implements this final rotation point yet.

## Runtime custody and owned attempt (B/A)

B owns `JoinedRemoteImageInstallFreeze` and private runtime custody. It binds
exactly one `Arc<NativeJoinedRemoteInstallPreflight>` and actual N-to-A transfer.
Methods for A: `preflight()`, `try_as_frozen()`, `require_retained()`;
`as_frozen()` only for already validated internal borrowed use. No Clone,
raw-permit extraction, Drop-based A-to-N return or replacement N. Runtime
`transfer_joined_remote_install` must reconcile the exact retained preflight
on retry, and preserve custody on every post-transfer failure.

A owns `NativeJoinedRemoteInstallAttempt` (pending). Its private per-range
constructor retains preflight before transfer, and stores the returned exact
freeze infallibly before fallible phase work. Fields are private typed owners:
preflight, optional original freeze, proposal, Installing, image/native-base
boundary and catalogue publication. It retains original xids/outcomes before
writes; pending/unknown is neither rollback nor permission for a new Tx.

## Engine outputs and joint handoff (pending implementations)

A fixed output names:

- `NativeJoinedRemoteProposalPublication`: exact owned original native
  proposal candidate/xid/outcome; no public parts constructor.
- `NativeJoinedRemoteInstallingPublication`: actual selected catalogue
  Installing publication/preimage, preserving every other range.
- `NativeJoinedRemoteImageInstallation`: actual retained history/row/final
  engine, with restrictive original row-intent/WAL retention and exact retry.
- `NativeJoinedRemoteFinalImageReceipt`: actual protected complete profile2
  final image; getters `pointer`, `manifest`, `source_seal`, history/R/H0/H/B,
  and actual index/namespace integrity. It is not Serving.
- `JoinedRemoteBaseBoundary`: original exact native-base candidate/xid/outcome.
  Physical base output remains existing `NativeInstalledRangeImageBase`.
- `NativeJoinedRemoteCataloguePublication`: actual durable selected floor;
  `seal_joint_materialization` produces only after exact final/base/disk
  readback a `NativeJoinedRemoteCatalogueMaterialization`.
- `NativeJoinedRemoteCatalogueMaterialization`: original same-open catalogue
  floor; `store_open_id()` and `require_current(store, db, base)` preserve full
  actual preflight and current native/catalogue bindings.

B owns `NativeJoinedRemoteJointTarget` and `NativeJoinedRemoteJointOwner`.
A's host transition must verify complete image/base/catalogue before taking its
actual freeze, and retain B's new owner infallibly immediately after the take.
Target binds actual installed original State **and** actual new A/image/base/
catalogue. Its joint-only domain is distinct from no-writer Registered and
ordinary Installed range permission. `actual_writer` can return only the real
existing State; no ordinary new-range grant exists before actual activation.

B implements actual NEW/dual-decision/metadata/Open/ALL-release using this
Joined image domain and fresh real per-phase metadata/version proof. It must
preserve the authenticated OLD-record-to-native-prefix association. Release
consumes the original A only after real native/catalogue activation with
current release admission and lexical memory publication proof. Then the
existing same-State CLOSED/Serving audit extends only the selected range;
the same dispatcher/coordinator/hook continues serving original ranges.

No SOURCE/INITIAL bindings are created. Future Joined donor duties, cold
reconstruction, controller integration, disk-stall progress and physical scale
qualification remain separate. The complete two-populated-range acceptance is
not executed or implemented by this interface note.
