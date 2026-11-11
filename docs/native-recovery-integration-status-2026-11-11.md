# Native range recovery integration status — November 11, 2026

This is a working integration report, not a release qualification. Verified remote
main is `b0e2498842be7b01e355bb8da1cc249ff16be056`, source version 1.0.471-beta.
The private integration branch includes that main revision and its table/routine
fixes. A package version on a private branch does not identify a released binary.
The final change still requires a new unused source version, broad validation,
merge/push verification, and independent physical qualification.

## Evidence and scope

| Finding | Classification | Exact evidence | Remaining gate |
| --- | --- | --- | --- |
| Event-client silent-route budget and discovery retry | Passed | Main PR146, exact tested `c895ef71`, source merge `f42bdf94`/1.0.470-beta; six focused tests and all nine CI checks | Eventual client behavior only; does not establish strong-read readiness |
| Local physical prefix proof and actual local MVCC sequence | Passed component tests | Private `10a10ba6`; [32 distinct tests](evidence/local-native-readiness-2026-11-11/README.md), eight specifically exercise local readiness | New assembled source must be revalidated; no leader/serving or strong-read permit |
| Actual native image publication and full history beyond raw payload retention | Passed component tests | Private `8a7ddf6e`; [34 distinct tests](evidence/native-nonzero-target-install-2026-11-11/README.md), including R1 and R1025 | Actual current-voter recovery, activation, RPCs, faults and physical scale-out remain open |
| Current-quorum source compatibility and physical repair-cut producer | Not tested on current assembly | New private source in `distribution_range_recovery/image_history.rs`, `image_compatibility.rs`, and `distribution_range_image/source_cut.rs`; initial RF3 genesis/poisoned-round tests added | Compile and run against actual receiver observations, including nonempty positive and conflicting-history negatives |
| Replica/leader/strong-read readiness separation | Incomplete | Local physical proof is opaque and owns its actual read pin; permanent source promises and target refusal remain | Qualified serving configuration, complete historical recovery, leader recovery, and actual strong-read barrier/endpoint |
| Audit/sync/broker effects in distributed range decisions | Source-confirmed protocol coverage gap; dynamic failure not tested | Source analysis below | Exact original effects and replay identity must survive leader loss; ordinary row checks cannot qualify this |
| Four-to-five-node expansion and sustained gain after convergence | Failed qualification / not demonstrated | Original Azure evidence remains authoritative; no new campaign | Zero lost ACKs, correct consistency/routing, bounded fault-safe convergence, stable five-node throughput comparison |

These test sets cover different revisions and overlapping components. Do not add
their counts as independent guarantees or report them as CI on the private branch.
Actual local fsynced files and close/reopen are useful software evidence, not an
independent-host fault campaign, measured production RPO/RTO, or deployment.

## Repair-source invariant

A source with a larger R is not sufficient. Its actual full logical history must
agree with every irreversible observation collected from the fenced current data
quorum: each native H at its actual index, original pending native command hash,
retained CQ/Applied command hash, and durable current-session certification. An
unknown compacted prefix cannot become verified empty genesis. The source verifier
streams actual native history in bounded steps and must invalidate itself after
an error; retry must not skip a consumed conflicting obligation.

The source read pin must remain owned through verification. If it is released
before production capture begins, capture must atomically reacquire a pin and
revalidate the SAME native receipt, DB open, namespace identities, schema,
generations and history. Matching R alone is insufficient. Physical repair does
not establish a serving configuration, authorize a new leader, or permit a
successful strong read. Activation still requires independent old/new quorum
safety and a durable configuration ancestry receipt. The physical capture anchor
and intended future configuration must remain distinct.

The working global-catalogue digest is also too conservative for availability:
unrelated ranges can invalidate an otherwise frozen local proof. A separate
range-scoped digest change is being tested. This is a source/integration finding,
not an explanation established for the Azure convergence failure.

## Reproducible issue: distributed range decisions omit original effects

Scope: distributed virtual-range write admission. This finding does not classify
legacy/native whole-WAL HA, Cell replication, or application fleet behavior.

At the reviewed main and private source, `CommitAdmissionIntent` contains the
transaction ID and record mutations. `RangeWriteCommand` copies those mutations
and includes command/range/epoch identity and a checksum. It does not carry the
original signed record-audit events, sync/change-capture operations, or broker
publish envelope.

In `crates/bicdb-core/src/db/spatial_paged.rs`, the commit path constructs the
admission intent and invokes admission before computing `sync_ops` and
`record_audit_events`. It later includes those effects and pending broker publishes
in the local transaction WAL. Imported projection suppresses newly generated sync
and audit effects; original imported audit events are included only if separately
provided. Therefore row-command quorum durability does not establish quorum
durability of the original complete effect envelope. Replaying the rows while
suppressing duplicate effects cannot reconstruct effects absent from the survivors.

Relevant paths:

- `crates/bicdb-core/src/db.rs`: `CommitAdmissionIntent`, imported projection and
  audit fields, transaction-WAL effect encoding/recovery and outbox finalization.
- `crates/bicdb-core/src/db/spatial_paged.rs`: admission, effect construction,
  `encode_tx_commit_frames`, sync publication and record-audit/broker finalizers.
- `crates/bicdb-core/src/distribution_range_consensus.rs`: `RangeWriteCommand`
  fields and construction from admission mutations.

This is a source-confirmed absence of protocol coverage. No acknowledged audit
loss has been dynamically demonstrated by this report, and this is not a claimed
root cause of either Azure experiment. The original physical evidence must remain
unchanged. Queue and index/FTS guarantees require their own applicable contract and
reconciliation; no success is inferred from this row-only evidence.

The next smallest deterministic test is:

1. Use three actual fsynced data services and authentic admission/certificate ACKs.
   Configure a record-audited collection, sync capture and a transactional broker
   publish. Record the exact original effect IDs/content hashes independently.
2. Commit a same-range row transaction and capture its external ACK. Retain the
   actual source WAL and each survivor range command/CQ payload before faults.
3. First verify whether the original effect envelope is actually present on
   survivors. Then permanently remove the task-owned source process/storage and
   recover solely from the two survivors through the real recovery path.
4. Reconcile the acknowledged row, original audit identity/content, original sync
   operation and broker receipt/delivery contract separately. Report missing
   effect support and unavailable recovery as explicit failures or not-tested
   gates, never replace original audit identities with newly generated events.
5. Repeat crashes between replay, local WAL durability, prefix completion and
   publication to prove retry identity and prevent duplicate observable effects.

A bounded, versioned durable effect envelope with replay identity, or an
independently qualified equivalent recovery mechanism, is required before making
an acknowledged-full-transaction effect guarantee. Implementation changes for this
contract must remain separate from the original failed Azure evidence.

## Operational state

The original and instrumented Azure test resource groups were deleted and deletion
was verified. No new Azure allocation is authorized by this report. The original
USD25/eight-hour window has expired. Prepare and locally qualify a concrete pinned
binary and experiment plan before requesting a renewed spend/time window.
