# StrongGet integration review: authority, image and visibility

This independent source review compares runtime **1.0.463-beta**,
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, metadata-authority candidate
`be82a20f708de36644fa7fdd33b5dbf002a7840b`, and design candidate
`445cb5928c330a20964fed0e1e291718ae33d599`. The latter contains
`docs/explicit-strong-range-read-design.md`; it is a source identifier here,
not a claim that the design has merged. These findings identify integration
work and missing proofs. This review ran no engine tests, proves no new dynamic
defect, and does not identify the cause of either Azure observation.

Machine-readable findings and proposed tests are in
[findings.json](evidence/strong-get-integration-review-2026-11-11/findings.json).
All newly proposed tests remain unexecuted by this review. The frozen Azure
baseline does not contain these proposed interfaces or the metadata candidate.

## Smallest concrete integration boundary

Use a core `StrongRangeReadCoordinator` sharing the existing
`RangeWriteCoordinator` reservation state, with a separate host-provided
`MetadataAuthorityProvider`. These are proposed names, not existing APIs.
The provider obtains a request-scoped authenticated observation from the actual
metadata leader; the range coordinator establishes a fresh certified range
floor, verifies the installed image, materializes the value, then performs a
final fresh metadata check. Failure must return explicit unavailable/unsupported,
never eventual `Get` or successful absence.

1. **Acquire metadata authority across roles.** Candidate `be82` exports
   `MetadataConsensusStore::{begin_read_authority, read_authority_probe,
   record_read_authority_ack, finish_read_authority}` and opaque
   `MetadataReadAuthority`. Its `MetadataReadProbe` RPC is metadata leader to
   voters. It does not supply a range-leader-to-metadata-leader acquisition RPC.
   Add that authenticated request/reply boundary, binding a fresh application
   request/context and finite budget. An authenticated transport must turn the
   reply into a verified observation; deserializing a bare DTO grants no permit.
   Alternatively relay execution through the metadata leader, at the cost of
   another range-leader boundary. A healthy range leader that is a metadata
   follower must work. The provider must not call leader-only begin on a follower.
2. **Reserve once; release outer locks.** In `distribution_transport.rs`,
   `ApplicationEvent` currently enters `execute_cluster_event(&db.read(), ...)`.
   StrongGet requires a separate dispatch path that clones its installed
   coordinator handle under a brief lock and drops database/service locks before
   quorum I/O. In `checkpoint_driver.rs`, the listener starts before the range
   coordinator is constructed. Reorder construction or install an initially
   empty reader handle only after startup succeeds. `commit_admission_ready()`
   only reports that an admission object exists. Make reservation honor the
   remaining request budget: `reserve_range` currently waits up to 30 seconds.
   Do not reserve and then call `recover_one_range_leadership`, which reserves
   again. Split a fresh recovery inner method requiring the existing guard.
3. **Return fresh range evidence.** Reuse `recover_leader_prefix` and
   `recover_leader_tail`, but return a typed certified/applied floor and evidence
   bound to this request/configuration instead of `Ok(())`. Do not use
   `recover_local_leadership`'s cached `recovered_epochs` as a read barrier.
   Provisional entries, maximum `last_index`, and `resolved_through` counters
   without their image/application proof are insufficient. Preserve ambiguity,
   divergence, abort and snapshot-required refusal. Metadata quorum does not
   prove range decision quorum or reconfiguration safety.
4. **Persist the image-bound base.** `RangeLearnerApplyState` contains snapshot
   identity/hash and source database commit sequences, not a range-consensus
   floor. `finish_snapshot` sets the source watermark; catch-up advances source
   `CommitFrame` sequences. `complete_relocation` deletes that learner state.
   Snapshot installation/cleanup do not install a certified floor in the separate
   range-write catalog. A durable range-image record must therefore survive
   learner cleanup and bind the verified image/base to its covered certified
   range prefix, including abort decisions. Capture that relation at a fenced
   cut and prove crash-safe installation/extension. Adding a numeric learner
   field alone does not establish it. Compacted-history recovery especially
   needs a valid installed base rather than an endless request for another
   snapshot. Source DB commit sequences and range indexes remain distinct.
5. **Protect local visibility.** `active_ranges` covers foreground admission and
   leader recovery. Snapshot/catch-up, digest repair and source cleanup use the
   data-service mutex but do not share that reservation. Introduce a shared
   per-range image gate or generation protocol covering those paths and restore.
   Materialize the value from a snapshot proven to cover the certified floor.
   Keep an owned result and image stamp so outer database guards can be released
   before the final network round. `db.get`/page MVCC and
   `db.check_recovery_state()` supply local visibility/health, not quorum freshness.
6. **Revalidate semantics before return.** Compare the relevant range ID/token
   interval, authority epoch, leader, voter roles/identities, node incarnation,
   restore activation, namespace, active/pending schema and live verified
   fingerprint, negotiated strong-reader capability, image identity/generation
   and storage health. Record metadata term/index and topology generation for
   diagnostics. Heartbeat-only global generation increments must not force
   perpetual retries. Retained command epoch may be older than valid current
   authority epoch; do not rewrite it or require equality.

The coordinator/provider and image-floor methods above remain proposed. Prefer
an explicit `StrongGet` operation and distinct response/capability boundary:
older servers must reject unsupported strong reads without silent downgrade.
No persistent `strong_read_ready=true` bit or cached recovered epoch is a permit.

## Safety and liveness dependencies

The custom range protocol must preserve every earlier acknowledged decision
across the read quorum and old/new replica configurations. Fresh metadata
authority cannot repair missing durable range decision evidence. The separate
ACK-certificate cut and its candidate repair require their own validation.

Existing range recovery performs sequential network work and has count bounds,
not a complete end-to-end request deadline. The host must enforce remaining
budgets and bounded parallel quorum work; context expiry alone does not interrupt
blocked I/O. A TCP deadline candidate is separate from this review. Do not wait
for every unavailable voter once sufficient safe evidence exists. Finite refusal
is necessary but does not replace healthy successful reads.

## Proposed healthy positive cases

1. Different metadata and range leaders return the exact prior-ACK value and a
   genuinely absent key within a finite declared budget.
2. One voter is unavailable while required quorums are healthy; the read succeeds
   without waiting for all nodes.
3. Authority epoch E2 retains certified commands from E1; exact values remain
   readable after valid recovery.
4. A relocated/promoted image after more commands than retained history can
   serve using its certified base plus suffix, including after restart.
5. Aborts advance the resolved prefix while provisional data remains invisible.
6. Continuous heartbeat generation changes with stable range semantics do not
   starve successful reads.
7. Concurrent writes produce an in-interval state; an immutable event's earlier
   acknowledged value is visible. Validate actual payloads, not only counters.
8. Metadata leadership changes with stable range semantics; bounded retries
   eventually return exact values under a healthy new authority.

Required negatives include stale/minority authority; uncommitted metadata anchor;
one-sided joint quorum; wrong request/certificate/incarnation; unresolved or
conflicting tail; partial or mismatched image; catalog-ahead-of-visible-data;
schema/restore/reader-capability change before return; missing old/new range
intersection; deadline/lock contention; and unsupported wire versions. None may
fall back to eventual data or report an unavailable lookup as absence.

Existing component tests include
`new_leader_recovers_a_quorum_resolved_prefix_before_assigning_an_index`,
`new_leader_adopts_and_applies_a_quorum_provisional_tail`,
`new_leader_fails_closed_on_an_ambiguous_provisional_tail`,
`new_leader_rejects_divergent_quorum_resolved_histories`, and
`range_snapshot_completion_never_labels_a_commit_still_being_written`.
Candidate metadata tests cover fresh quorums and actual loopback transport.
Those are component evidence, not successful StrongGet integration tests or
physical/production qualification. Source paths and exact symbols are indexed
in the machine-readable findings.
