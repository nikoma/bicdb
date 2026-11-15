# Remote native finalization and retained custody: source findings

Inspected source: `3d8eef4e611995db5f60dc2920ab34afe20aa0ee`. This is an
unpublished, uncompiled composition. These are source findings and acceptance
requirements, not executed HA, garbage-collection or scale-out results.

Later source-only composition adds the distinct
[metadata-owner activation primitive](native-remote-activation-metadata-owner.md)
from `15a453fb5` and its independently reviewed source-identity correction
`7d46e9eef`. That change supplies a finite actual ALL-released collection,
protected proposer and known selected-commit observation; the historical
missing-producer finding below is scoped to inspected source3d8. Compilation
and runtime acceptance remain **not tested**. Automatic all-Policy ordering,
remote source retirement, warm custody retirement and cold recovery remain
open; this primitive does not close the full completion gap.

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

## Later source trace: the retiring source already participates

Inspection of the composed source through `deb4f543c` distinguishes two
different custody facts. The metadata owner retains a certified decision
summary rather than the participant's full local D. That does not mean the
retiring source lacks its native decision. FIRST collection materializes every
original OLD member plus the target, and its native/catalogue member operations
admit both OLD and NEW. The actual metadata-owner certification independently
queries catalogue evidence from every OLD member plus the target before
proposing certification; failure of any exchange propagates. The normal-host
definition also publishes native/catalogue decisions to the retiring OLD source.

Sources: [native_decision.rs](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_coordinator/native_decision.rs),
[native_catalogue.rs](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_coordinator/native_catalogue.rs),
[first_catalogue_certification.rs](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_catalogue_certification.rs),
and [the actual OLD recipient](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/old_step/remote_new.rs).
This is source-path inspection, **not execution of the later source composition**.

The smallest missing remote retirement step should therefore use the actual
retired source's existing closed session, permanent promise, native decision
and healthy catalogue. Its own fresh committed metadata read must show the
recorded activation and its absence from final membership. It must issue its
own actual durable membership retirement receipt; neither the donor identity
nor the metadata owner's decision summary is a replacement proof.

The existing local source-retirement helper may fail a currentness postcheck
after the durable catalogue operation returned successfully. A remote actor
must retain the real known receipt before fallible postchecks, and retain the
original error separately. An ambiguous persistence result cannot become a
known receipt merely because bytes are visible, and a lost reply cannot justify
blind mutation replay. This is an outcome-retention requirement for the missing
remote producer, not evidence of data loss or a reproduced local failure.
Actual authenticated remote retirement, independent selected metadata
completion and interrupted/unknown-outcome controls remain **not implemented
or tested**. `rows_retained=true`, warm phase retirement and physical deletion
remain different boundaries.

## Warm custody cannot be discarded by status

Both `remote_serving` and `remote_reference_retirement` retain at most eight
original phases per host. The inspected paths insert and preserve these owners;
they do not retire completed entries. The per-range
`NativeHostWriterDataDomains` also refuses a second retained continuation for a
range that already has a Closed or Serving attempt. Raising a cap, clearing an
unknown result, or deleting an old owner is not a completion protocol.

Later inspection also identifies FIRST's separate eight-range coordinator
limit, one-attempt-per-range retention and INITIAL-only remote entry point.
These are additional constraints on this finite remote path, rather than
evidence about every local backend. The exact boundaries and required genuine
acceptance sequences are in the
[warm retirement and repeated movement contract](native-warm-retirement-and-repeated-movement-contract.md).

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
