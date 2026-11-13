# Joint handoff: cross-process delivery gap

This source audit covers `0934b108a5a8f06acd9ac8582845a45a59d18e79`
plus release-retry correction
`122bdc1475b2b3dcf692a1542203a23b1a44479d`. No build, database open,
process run or VM action was performed. Existing cohort results do not test a
remote joint handoff. The earlier [operator audit](native-operator-four-to-five-gap-audit-2026-11-11.md)
retains its historical source scope; this note does not update those results.

**No authenticated transport currently carries the complete joint-data chain.**
The joint proof collectors retain actual local service objects. Authenticating
metadata or serializing their audit fields cannot make those objects remote
capabilities. Paths below are relative to `crates/bicdb-core/src/`;
`joint/` abbreviates `distribution_range_handoff/joint/`.

| Phase | Actual source boundary | Missing cross-process producer |
| --- | --- | --- |
| OLD close and complete obligations | `distribution_range_handoff/joint/runtime.rs::NativeJointHandoffRuntime::close_old`; `joint/closed.rs::NativeJointOldObservation` owns `Arc<Mutex<ClusterDataNodeService>>`, real freeze and source pin; `joint/old_quorum.rs::record_constraints/record_closed` consume actual local evidence | Recipient-owned close/status sessions and sealed authenticated OLD observations; remote selected-source history membership checks for **every** observed CQ/pending obligation |
| Image source and target | `distribution_range_image.rs::RangeImageCapture` owns local DB/freeze/pin; `TentativeNativeRangeImageSourceAuthorization<'_>` borrows it; `CompletedNativeRangeImageSource` retains it. `distribution_data/native_image_install.rs::begin_native_range_image_install/clear_native_range_image_step/copy_native_range_image_step` require those actual source objects | Source-session offer, bounded history0..R and row export, completion and target operations whose authority comes from verified peer observations and recipient-owned state, rather than a decoded source seal |
| NEW materialization and dual durable decision | `joint/materialized.rs::NativeJointNewObservation` holds actual OLD survivors or target evidence plus a service Arc. `joint/decision.rs::publish_actual_member` calls these local services; `finish` requires protected receipts on OLD and NEW majorities, including the new leader | Authenticated materialization/decision publication and durable readback; receivers independently validate fresh exact roles, births and complete decision ancestry. A serialized `NativeJointDecisionData` is audit data only |
| Committed preparation and activation | `joint/bridge.rs::NativeJointPreparedServingObservation` owns actual evidence/service Arc; `joint/bridge/publication.rs::publish_actual_local_member/require_current` directly lock each local service | Purpose-specific NEW prepared and native-publication observations, then receiver-owned native/catalogue activation. Fresh HostMQ must follow the real collection/publication under the original budget |
| Release and cleanup | `joint/runtime.rs::release_activated_new_member` retains the original actual receipt and drops only its real local owner. `db/native_joint_activation.rs::arm_after_actual_release` checks actual drain. Topology has `AwaitingCleanup/Cleaned` states | Remote activation/release orchestration, qualified current NEW coverage, durable old-source retired tombstone/floor, bounded typed deletion and completion receipt. No joint cleanup producer currently closes this chain |

`distribution_transport.rs::ClusterDataRequest` has no joint close, decision,
image-session, NEW preparation, bridge, release or cleanup variants. Existing
`ExportSnapshot/ApplySnapshot/ExportCatchUp/ApplyCatchUp/CleanupSource` dispatch
legacy service paths; they do not consume joint proofs.
`distribution_supervisor.rs::TransportClusterRelocationDriver::drive_relocation`
still advances legacy snapshot/sequence/promotion/cleanup phases. The legacy
promotion/cleanup methods call `require_legacy_relocation_mutation`; changing
that refusal would not supply a joint decision.

Reusable real TLS surfaces include HostMQ, recovery-role/fence/historical
operations, ordinary native Apply, Ready/Complete, native read/Get, metadata
Raft and learner-birth observation. Wire25 and destination identity checks do
not add the missing joint operations. `NativeUnassignedLearnerHost` remains
provisional; its `stop_and_drain` gives an actual listener-drain fact, not data
promotion. A deliberate same-service enrolled-data listener transition is also
needed. Initial writer-policy installation alone leaves provisional listeners
metadata-only. The inspected CLI has no use of `NativeProvisionalHost`,
`NativeUnassignedLearnerHost` or `NativeJointHandoffRuntime`; host/operator
publication remains a separate delivery.

## Smallest next cross-process vertical

After local joint controls first compile and pass:

1. Extend `distribution_native_host/provisional_listener/tests/process_tests.rs`
   and `process_tests/startup_tests.rs` from four metadata/policy child processes
   to five independently owned DB/services. Keep the first four's RF3 data set
   exact, and create the fifth through its fresh empty-range factory and real
   learner-birth exchange. Install actual policies before the first user ACK.
   Current child controls intentionally reject application requests; add the
   explicit data-listener lifecycle rather than exposing a test DB handle.
2. Enable one range, real TCP native ACKs and Strong value/absence. Then commit
   its exact pending learner plan. Start with frozen copy and report its bounded
   write unavailability; this does not qualify online copy/catch-up.
3. Add the narrowly scoped joint RPC/session family in phase order from the
   table. Preserve actual birth/destination/request/challenge, configuration,
   original deadline and cancellation. Keep freezes/pins in their recipient
   processes. Verify full history0..R before target clear; do not truncate it or
   confuse target-local B with R or source B. Count only sealed, distinct actual
   peer observations, never caller voter lists or decoded local proof types.
4. Persist the complete same decision on OLD and NEW data majorities **before**
   committed metadata finalization. Follow with actual NEW preparation/native
   publication, post-quorum fresh metadata, catalogue activation and local
   owner release. Require a new-leader user ACK and Strong verification of both
   old and new ACKs, plus stale-old-writer refusal. The parent test driver must
   use sockets, not child service Arcs, to advance these phases.
5. Add proof-authorized cleanup and independently restart the coordinator and
   each child. Reconcile the external fsynced ACK ledger and exact values;
   verify convergence only after actual retired-source cleanup receipts.

This is a proposed single-range OS-process harness on one host. It precedes an
automatic controller/CLI candidate and five independent VM qualification; it
establishes neither by design alone. Source death, coordinator cuts before and
after metadata finalization, cold/expired reattestation, lower-R OLD repair,
missing-NEW-voter repair, origin-down recovery and post-joint historical Commit
adoption remain acceptance gates. Post-joint NoData is explicitly unsupported
and must refuse before native WAL, including retries. Preserve every observed
irreversible obligation and permanent promise; timeout is never a release rule.
