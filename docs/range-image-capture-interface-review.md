# Range-image capture: concrete interface review

Status: proposed implementation batch on source `c95400d1`/1.0.466-beta. No
image install or StrongGet API is implemented by this document. The independent
[late-ACK handoff fixture](range-image-base-protocol-design.md) and compacted
history case require both a correct final cut and guarded handoff/cleanup.

## The reserved-upper-bound proof

After range mutation exclusion has drained every relevant operation, obtain a
global reserved DB sequence `B=current_commit_seq()`. Wait boundedly until the
actual `replication_shippable_seq()>=B`; this is
`min(tx_log.written_seq,last_committed_tx)` in
[replication_live.rs](../crates/bicdb-core/src/db/replication_live.rs).
A timeout/cancellation/fenced database is explicit incomplete capture, never an
instruction to raise a transaction snapshot to `B`.

The [paged range exporter](../crates/bicdb-core/src/db/pitr_maintenance.rs) uses
`begin_transaction().applied_snapshot` and its paged MVCC snapshot. Each export
snapshot taken after the barrier includes all drained range mutations. Later
other-range mutations can raise DB sequences without changing frozen range
records. Every exported batch must check that its real applied DB snapshot is at
least `B`. Range-command cut `R`, reserved upper bound `B`, actual source DB
snapshot and destination installation sequence remain different typed values.

This proof requires the same range mutation exclusion in ordinary admission,
replica application, repair, catch-up/import, snapshots, anti-entropy mutation
and cleanup. An admission-only mutex is insufficient. Structural/schema changes
must invalidate the capture, including changes that return to the same digest.
The experimental deferred materializer defers page-WAL sync/markers rather than
record application; the image/native installation durability receipt still must
cover the configured native durability boundary.

## Types and construction boundaries

The following signatures describe crate-internal APIs. No public numeric
constructor grants authority. `RangeCutQuorumCertificate` is constructed only by
the coordinator from actual fresh authenticated data-voter responses and a
validated metadata authority context; it is not deserializable.

```rust
// Distinct values; wire decoding does not create the opaque proof types.
struct RangeCommandIndex(u64);
struct ReservedDbUpperBound(u64);
struct AppliedDurableDbSequence(u64);

struct RangeCutQuorumCertificate { /* private: identities, nonce, R, fence */ }
struct RangeMutationPermit { /* owned, Send, one operation/callback lifetime */ }
struct FrozenRangeMutationPermit { /* private gate identity + session */ }
struct RangeImageCapturePermit { /* consumes cut + frozen gate + barrier */ }
struct VerifiedRangeImageInstall { /* verified seal + target native receipt */ }

impl RangeWriteCoordinator {
    fn prepare_image_cut(
        &self,
        request: &CommittedRelocationImageRequest,
        authority: &MetadataReadAuthority,
        cancellation: &CancellationToken,
    ) -> Result<RangeCutQuorumCertificate>;
}

impl RangeImageCaptureService {
    fn begin(
        &self,
        cut: RangeCutQuorumCertificate,
        frozen: FrozenRangeMutationPermit,
        cancellation: &CancellationToken,
    ) -> Result<RangeImageCapturePermit>;
    fn export_step(
        &self,
        permit: &RangeImageCapturePermit,
        expected_cursor: &ImageCursor,
        limits: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<RangeImageStepV1>;
    fn verify_install(
        &self,
        manifest: &RangeImageSealV1,
        native: &DurableNativeImageReceipt,
        observed: &ActualInstalledImageRoot,
    ) -> Result<VerifiedRangeImageInstall>;
}
```

`begin` captures `B` only after the frozen permit is held, then waits for the
actual shippable prefix. It records source DB incarnation, schema authority
**generation plus digest**, namespace bindings and sorted reader capabilities.
Every export/seal checks that these bindings still hold. A changed source DB
incarnation restarts the session; it does not continue an old copy against a new
process by assuming matching names are the same namespace.

There is currently no persistent CollectionMeta namespace UUID. Live namespace
identities are Arc-backed, while `SchemaCompatibilityAuthority::generation()`
advances on schema invalidation. Use an explicitly session-scoped binding to
those actual identities plus source DB incarnation/generation. Do not label a
name/hash/assigned integer a durable namespace incarnation. Installation/reopen
must revalidate actual image/catalog lineage and fresh quorum evidence; otherwise
replica readiness remains closed. A durable namespace-lineage extension, if
needed, is a separately coordinated native-format change.

## Shared mutation gate and lock ordering

Use an owned ref-counted permit rather than a borrowed non-Send lock guard in a
`Send+Sync` coordinator. The gate is keyed by range and shared by the one live
service/coordinator owner. A freeze prevents new operations, drains existing
owned permits, and yields a private frozen token within cancellation/deadline
bounds. Failed/cancelled acquisition cannot leave an unowned pending freeze.

A leader admission permit lives through its local WAL callback; preserve it with
the pending ticket after irrevocable certification. Early definitive abort drops
it. Replica/repair/import operations acquire at their outer boundary and pass
the same permit to internal mutation helpers; internal calls must not reacquire
and deadlock on their own freeze. RPC entrypoints cannot pass an arbitrary
caller-created permit. A freeze seeing an unresolved replica tail must either
resolve it under trusted recovery authority or release/retry explicitly. It must
not deny completion RPCs forever while waiting for their tail to disappear.

Order: acquire/freeze the range gate without holding DataNodeService or DB
latches; then acquire service/DB guards for a bounded operation. Never wait for
range drain while holding the service mutex needed by the active command's WAL
callback. No synchronous nested export RPC may retain a source service lock
while acquiring the destination service lock.

Required entrypoint inventory before the gate can qualify a cut:

| Path | Permit lifetime |
| --- | --- |
| `RangeWriteCoordinator::admit` / `local_applied` | Through prepare/decision/certificate and actual local-WAL callback |
| Follower prepare/commit/certify/apply/abort | Complete local mutation; unresolved-tail handling remains explicit |
| Leader recovery / range-write suffix repair | Whole local suffix operation under current recovery authority |
| Snapshot and catch-up import | Native image/reader mutation plus durable checkpoint |
| Digest/anti-entropy repair | Full validated repair apply plus checkpoint |
| Source cleanup | Through deletion and durable cleanup progress; requires handoff permission |
| Schema/namespace/reader changes | Invalidate capture generation/binding; no ABA by equal final hash |

## Wire and native ownership coordination

Prefer a new `RangeImageStepV1`/`RangeImageSealV1` envelope around the existing
`RangeSnapshotBatch`, instead of changing that legacy struct while the concurrent
reader-capability change owns its format. The envelope binds session, exact
committed relocation, source/target member incarnations, range/span/config,
`R`, source DB sequence, reader/schema/namespace bindings, batch ordinal/cursor,
previous-chain hash and final image root. Apply reader capabilities before data.
Old snapshot progress cannot substitute for the new completion seal.

New begin/export/apply/seal RPCs require the existing TLS/member checks **and**
exact committed relocation plus intended destination. A metadata controller and
a range leader can be different nodes: delegate capture to the actual range
coordinator without allowing an arbitrary authenticated member to import data.
The source fast-path authorization is not a substitute for current committed
configuration validation. Fresh metadata authority must be obtained through an
actual fresh round; a cached `leader_id` does not serialize an opaque authority
proof across nodes. This delegation/round bridge needs explicit host plumbing,
not a forged deserialized metadata permit.

Planned files to coordinate before edits: `distribution_data.rs` for the shared
gate/session handle and image RPC entrypoints; `distribution_transport.rs` for
new authenticated envelopes/delegation; `distribution_range_consensus.rs` for
cut/fence promises and installed-base catalog; `db/pitr_maintenance.rs` only for
a narrow guarded export/install adapter after the reader-capability owner agrees.
A crate-internal `distribution_range_image.rs` owns typed manifests, bounds,
checksums and install journal validation. `distribution_supervisor.rs` integrates
the handoff gate; root owns that controller file.

## One coherent minimal batch and acceptance

Implement the gate, typed capture/barrier, sealed installation and guarded
handoff as one coherent batch, with focused tests while evolving. A manifest
parser or always-unavailable method alone does not close the defect. Quiesced
range copying is a declared limitation of the first correct batch; no online
scaling/performance qualification follows.

1. Actual interleaved A/B ranges: DB sequence differs from range index. Freeze A,
   capture `B`, let unrelated B continue, verify the exact A image and typed `R`.
2. Hold a lower reserved gap while later range applies exist. Capture times out
   without inventing visibility; release the gap and a healthy capture succeeds.
3. Exercise every mutator category against a frozen range, including replica
   repair/import/cleanup; unrelated ranges retain progress.
4. Actual1025-command relocation installs a verified base; compacted healthy
   quorum recovers exact values and continues writes. The8-command control stays
   healthy. Reject forged floor/chain/capability/incarnation/cursor inputs.
5. Reproduce the real late-ACK handoff schedule. Final drain/cut includes its ACK;
   cleanup cannot remove the old source until the new leader/current data quorum
   has the verified base and suffix through that cut. Test the unavailable old
   certificate holder and a stale old writer.
6. Interrupt every install/journal boundary; no premature readiness, and valid
   installed bases survive learner cleanup/reopen through native receipt/image
   verification. Healthy quorum recovery must actually finish and serve values.

No heavy build or physical operation was performed for this interface review.
