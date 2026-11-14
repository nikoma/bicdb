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

Implemented in A, not compiled or executed yet:

```
preflight.with_rotation_point(
    request, frame, temporary_n_Arc_BY_VALUE,
    |point: &NativeJoinedRemoteRotationPoint<'_>| { ... },
) -> Result<R>
point.require_current_memory(actual_unique_n: &FrozenRangeMutationPermit)
point.publication_guard() -> NativeMetadataPublicationGuard<'_>
```

The host helper performs current-MStore/runtime checks BEFORE service/DB
borrowing, and uses the exact physical service/Store/DB. The DB half in
`db/range_image_native/writer_enrollment/joined_receive/rotation.rs` acquires
actual admission/hook/journal guards and validates a transient borrowed storage
view against the exact preflight, native/policy/source/schema/catalogue facts.
Its private guard bundle does not borrow N. It captures the actual gate/private
lease identity, then explicitly drops the consumed temporary N Arc BEFORE
calling the runtime callback. The point cannot escape that callback.

After physical preparation, B takes its selected session and actual unique N.
The callback uses the point's actual member publication guard and memory-only
check, then consumes N into the original retained A. No full `require_live`,
canonicalize, fsync, native read, serde/hash or application callback occurs in
that final memory check. The check uses separate request/owner liveness,
sealed source-frame cancellation/deadline, actual private freeze identity,
installed State/registration and sticky Core/page/WAL health. Actual physical
guards remain lexically held throughout the callback. It never repeats an old-N
postcheck after the callback has consumed N. Fresh Role/OWN + local version +
native epoch/quorum fences compose; the local guard is not a distributed lease.

A and B must review the final lexical representation before enabling transfer.
The implementation is source only; runtime custody/rotation and acceptance are separate B work.

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

### Pending runtime custody has no host ownership backedge

The runtime owns the actual preflight and transferred A strongly. The retained
receive origin consequently stores weak references to the actual runtime,
backend, factory, admission, original FIRST continuation and coordinator. It
copies the actual database-open and Store-slot guards, node/incarnation DATA,
and retains the installed writer guard. It does not retain
`RegisteredAssignedParts`. In particular the FIRST continuation can indirectly
retain an image release lease/runtime and must remain weak here. The host keeps
its original strong owners; no runtime-owned custody is weakened.

Full physical operations upgrade those exact weak owners and retain each local
Arc while borrowing its locks. `service()` returns
`Result<Arc<Mutex<ClusterDataNodeService>>>`; consumers must bind the returned
Arc before locking it. These upgrades do not create replacements or extend an
expired request.

The final memory-only refusal path checks the actual Joined lifetime, copied
open/slot witnesses and whether the original weak FIRST owner has ended. That
weak-count check is only an additional refusal, never proof of authority. It
must not upgrade/drop a possible final complex owner under the publication
guard. The physical wrapper lexically retains the upgraded exact FIRST owner and
admission outside the gate; those owners drop only after the callback and
physical guards return. The lexical physical point still owns the checked State/hook/native
journal/DB/service guards and the sink independently requires the fresh request,
actual local metadata version and unique original N.

This source ownership correction has not independently exercised host teardown,
row import, second-range activation or stalled disk I/O. Those remain runtime
acceptance gates.
