# Remote native finalization and retained custody: source findings

Inspected source: `3d8eef4e611995db5f60dc2920ab34afe20aa0ee`. This is an
unpublished, uncompiled composition. These are source findings and acceptance
requirements, not executed HA, garbage-collection or scale-out results.

## Completion has separate producers

The local custody backend has a real completion chain. After actual release
and serving, it obtains `NativeJointActivatedQuorum`, records
`AwaitingCleanup`, asks the original source to persist its own retirement, and
records `Cleaned` / relocation `Completed`. The source retirement record
explicitly requires `rows_retained=true`. Completion of membership retirement
does not establish deletion of obsolete source records or reclaimed disk space.

The new remote Policy is a direct host path and has not supplied those
completion proofs or callers; it leaves the metadata record at
`AwaitingActivation`. Separately, the generic handoff driver's default
`records_completion=false` stops an unsupported backend at Serving; the
inspected local backend overrides it. The older partial
`RemoteJoinedHandoffBackend` uses that default, while the new direct Policy
path does not use that backend. The earlier source-export retirement
is a different resource operation. A successful local policy API or APP/Strong
pair cannot replace either missing metadata proof.

| Boundary | Source evidence | Classification |
| --- | --- | --- |
| Local activation and source retirement | [local_backend.rs](../../crates/bicdb-core/src/distribution_native_host/joined_data_handoff/local_backend.rs), [activated_quorum.rs](../../crates/bicdb-core/src/distribution_range_handoff/joint/bridge/publication/release_phase/activated_quorum.rs), [native_joint_handoff.rs](../../crates/bicdb-core/src/distribution/native_joint_handoff.rs) | Implemented local producer; this inspection does not execute its controls. |
| Remote metadata completion | [remote policy](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/remote_serving_owner/policy.rs), [handoff driver](../../crates/bicdb-core/src/distribution_native_host/joined_data_handoff.rs) | Failed source completion inspection: required remote proof/caller chain missing; runtime not tested. |
| Native source retirement | [retirement.rs](../../crates/bicdb-core/src/distribution_range_consensus/joint_handoff_state/retirement.rs) | Durable membership retirement retains rows. Physical source deletion/reclamation is not proved. |

The independent 13-file completion trace receipt is
`5775f5e0c205a63037c5f5eb4675e76a6c9040c1564b9bba1eee26861a7e818f`;
its complete trace SHA-256 is
`76d353ef4550238b2f53bee7505deaaa55be99f112248533e08dd15694bc3687`.
Those receipts cover source inspection only.
The frozen trace's attribution of `records_completion=false` to a remote
backend is corrected above: the actual remote Policy is the distinct direct
host path. The missing completion producer finding remains unchanged.

## Warm custody cannot be discarded by status

Both `remote_serving` and `remote_reference_retirement` retain at most eight
original phases per host. The inspected paths insert and preserve these owners;
they do not retire completed entries. The per-range
`NativeHostWriterDataDomains` also refuses a second retained continuation for a
range that already has a Closed or Serving attempt. Raising a cap, clearing an
unknown result, or deleting an old owner is not a completion protocol.

There is an important distinction in the actual serving path. The writer's
`ServingDomainRecord` validates its historical OWN/quorum audits and immutable
native archives. It does not require its old metadata snapshot to be the
current topology. Consequently a final metadata-stage update alone has not
been shown to invalidate ordinary serving.

Its `NativeJointOpenContinuity`, however, is a Weak observation of an actual
`NativeJointActivatedOpenGuard`. The live `RangeWriteStore` retains the Arc.
Any warm retirement must preserve that actual current guard and the verified
writer/native lineage, not replace them with the saved released-generation
UUID. The current activation installer refuses reuse of an already armed guard;
no actual next-phase supersession is supplied by deleting a host-map entry.
Repeated movement needs a lawful Closed/predecessor/frozen transition as well.

Relevant sources are [writer_data_domain.rs](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/writer_data_domain.rs),
[data_domains.rs](../../crates/bicdb-core/src/db/range_image_native/writer_enrollment/data_domains.rs),
[historical_predecessor.rs](../../crates/bicdb-core/src/db/range_image_native/writer_enrollment/data_domains/historical_predecessor.rs),
[current.rs](../../crates/bicdb-core/src/db/native_joint_activation/current.rs), and
[activation.rs](../../crates/bicdb-core/src/distribution_range_consensus/joint_handoff_state/activation.rs).
Classification: **missing warm retirement/supersession producer; runtime not
tested**. This is distinct from crash/cold reconstruction, which also remains
unqualified.

## Durable metadata retention is a different limit

`validate_native_joint_handoff_preservation` forbids removal of an original
native relocation even after `Cleaned`. The existing history-pruning loop
excludes native records, while the topology's relocation map is bounded at
4,096 entries. This is a different structure from configurable topology-change
history (default 64, maximum 4,096); that change-history setting is not the
cause of the native tombstone limit.
Therefore enough non-prunable native tombstones can eventually refuse new
allocations. This is a source-derived capacity consequence, not an executed
exhaustion test. It must not be repaired by deleting protected records or
silently increasing the limit. A separately specified durable compaction
representation and genuine recovery/snapshot checks would be required.

This persistent-history limit is not the eight-entry warm phase limit. Neither
is evidence that an automatic four-to-five expansion converged.

## Next deterministic acceptance

First prove the actual remote metadata owner can collect the required pinned
release evidence and commit activation through its protected proposer, including
when that metadata leader is outside the final data voters. A decoded digest,
generic topology setter or fabricated local quorum cannot supply this proof.
Then use the actual source's own fresh metadata read and permanent promise to
retire its membership and commit completion, retaining rows and old errors.

Warm retirement must independently prove no queued/in-flight/uncertain old
operation, current actual Installed/native/Store continuity, and observed
committed completion before releasing obsolete custody. Late old requests must
refuse through the original authority/version rules. Keep the unchanged SDK's
acknowledged full values readable across that retirement.

Run more than eight genuine completed phases without raising the cap, then
repeat a move of the same range through a new actual predecessor. Keep all
acknowledged, uncertain, failed and retried operations. Test restarts and
interruptions separately. These controls are not yet implemented or executed;
they cannot be replaced by map counts, `Completed`, or a passing source check.
The physical experiment must also distinguish active placement, retained
obsolete copies and actual reclaimed bytes. All original consistency,
durability, convergence and post-convergence capacity gates remain unchanged.
