# Warm retirement and repeated remote movement: source contract

Inspected source: `8bbbb144f683f6442493610b94ef7e893b1441f7`.
Classification: **source inspection and missing-producer contract**. No new
garbage collector, repeated-movement implementation or executed acceptance is
provided by this document. The failed physical experiment remains failed.

## Independent boundaries

| Boundary | Actual source behavior | Consequence and scope |
| --- | --- | --- |
| FIRST coordinator ownership | `begin_finite_remote_first` requires a running INITIAL DATA host, retains one coordinator per range and refuses a ninth retained range. A different attempt for an already retained range is refused. | This finite remote entry point does not yet supply Joined-host re-entry or completed-coordinator retirement. This is not a claim about every separate local backend. |
| Remote preparation and observation retirement | `remote_serving` and `remote_reference_retirement` separately retain at most eight phases per host. | Clearing one map does not clear the other original owners, pending requests or uncertainty. |
| Writer DATA domains | `NativeHostWriterDataDomains::require_new` refuses a range retained in either its Closed or Serving map. | A second move of the same range needs a lawful predecessor/retirement transition; increasing a global phase count does not supply it. |
| Actual opened generation | `RangeWriteStore` retains the actual `NativeJointActivatedOpenGuard`; existing installation checks an existing guard with `require_not_armed`. | The old phase's released-generation UUID cannot replace the actual guard. Removing host bookkeeping does not install a new generation or permit reusing an old Frozen owner. |
| Durable native relocation history | Completed native tombstones cannot be pruned by the existing metadata preservation validator; the relocation map is bounded at 4,096. | This durable retention limit is separate from all warm limits and needs a separately proved compaction representation. |

Sources: [FIRST coordinator](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_coordinator.rs),
[remote serving owner](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/remote_serving_owner.rs),
[reference retirement](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/remote_serving_owner/reference_retirement.rs),
[writer DATA domains](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/writer_data_domain.rs),
[activation installation](../../crates/bicdb-core/src/distribution_range_consensus/joint_handoff_state/activation.rs),
and [metadata preservation](../../crates/bicdb-core/src/distribution/native_joint_handoff.rs).
No current execution exhausted these limits; the consequences above follow
from the inspected constructors and refusal conditions.

FIRST's Drop cancels its original process. Deleting a coordinator is therefore
also an operation-lifetime change, not merely a memory accounting adjustment.
An old unknown request or retained known result must not disappear under a
time-based eviction policy.

## Required warm-retirement producer

A future implementation needs a non-serialized proof minted by the same actual
host and selected operation owners. It must establish all of the following at
the final retirement boundary:

1. Actual committed metadata completion matches the original relocation,
   session, nonce, native decision, activation and retired-source receipt.
   `Completed` alone, a copied topology or a decoded digest is insufficient.
2. Every original owner has resolved its actual admitted requests. No queued,
   in-flight or uncertain mutation is discarded. A late reply remains tied to
   its original operation and cannot be attached to a new phase.
3. The actual installed writer, coordinator, service/open/Store slot, native
   lineage, listener and dispatcher are still the intended current owners.
   Current native history must genuinely extend the protected cut while legal
   writes continue; equality with an old whole-database sequence is not a
   quiescence or freshness substitute.
4. Removing obsolete host references preserves the actual current opened
   generation and its native Serving/archive obligations. The weak
   `NativeJointOpenContinuity` remains backed by its genuine retained guard.
5. A distinct next operation receives its own actual predecessor, freeze,
   closure, decision and opened generation. Neither unarming an old guard nor
   deleting a map entry creates that evidence.

Ordinary Serving validates its historical quorum audit and immutable native
archives; a metadata-stage change alone has not been shown to invalidate it.
The actual opened generation is a different live dependency. Relevant sources
are [open continuity](../../crates/bicdb-core/src/db/native_joint_activation/current.rs)
and [historical predecessor](../../crates/bicdb-core/src/db/range_image_native/writer_enrollment/data_domains/historical_predecessor.rs).
The predecessor producer must run before the next actual closure, using the
actual live writer and native history. Persisted DATA after closure cannot
recreate the missing original owner.

The final memory retirement must obey existing service/journal/version/queue
lock order. Dropping arbitrary ownership while a metadata or publication guard
is held can run cancellation or cleanup code; moving an owner out and draining
it afterward still needs the actual lifetime and stale-request proof. This
document introduces no constructor or permission from these requirements.

## Smallest genuine acceptance sequences

The remote activation, all-Policy ordering and source-retirement producers
must complete first. A mocked `Completed` topology or repeated construction of
fresh empty hosts cannot test warm retirement.

- Complete one actual remote movement, retain the original SDK's full ACKed
  values, retire obsolete owners through the real producer, and read them
  through the same gateway while normal writes continue.
- Complete more than eight distinct real movements on the same retained hosts
  without increasing caps or restarting owners. Require the ninth operation
  to enter through its genuine protocol and converge.
- Move the same range a second time, including when its leader is now a Joined
  host. Require a genuine new predecessor and generation; stale first-phase
  reads/mutations must refuse or refresh within their original contracts.
- Hold a real old queued operation or persistence uncertainty at the retirement
  boundary. Retirement must refuse without deleting that owner or its result.
  Release or resolve the same actual condition, then resume retirement.
- Interrupt controller/owner retirement and restart separately. Keep cold
  reconstruction distinct from a warm same-process continuation.

None of these combined sequences is currently executed or qualified. The
64-range four-to-five experiment must retain these prerequisites; shrinking
the workload below an eight-entry limit would establish only that smaller
scope. Physical membership retirement still retains rows, so the experiment
must distinguish active placement, obsolete copies and reclaimed bytes.
