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
| Current-quorum source compatibility and physical repair-cut producer | Passed initial genesis component scenarios; nonempty/fault cases open | Private `da378dd9`/1.0.471-beta; [56 distinct integrated tests](evidence/native-recovery-compose-471-2026-11-11/README.md), including actual RF3-configured genesis source verification/capture and poisoned retry | Nonempty quorum-compatible cut, authenticated conflicting-history negatives, actual repair completion and activation |
| Nonempty physical image catalogue adoption/finalization | Passed five component tests | Private `9e37d812`/1.0.471-beta; [38-test replay/adoption bundle](evidence/native-catalogue-historical-recovery-2026-11-11/README.md) | Actual Runtime-owned CurrentVoterRepair choreography and qualified completion/activation remain open |
| Historical native row/WAL replay | Passed two component tests | Same `9e37d812`; actual current certificate quorum, old-WAL cut/reopen/new local WAL and original command identity | Actual authenticated participant RPCs, normal replica Apply, recovery completion, new write and strong-read qualification remain open |
| Replica/leader/strong-read readiness separation | Not tested end-to-end; implementation incomplete | Local physical proof is opaque and owns its actual read pin; permanent source promises and target refusal remain | Qualified serving configuration, complete historical recovery, leader recovery, and actual strong-read barrier/endpoint |
| Audit/sync/broker effects in distributed range decisions | Failed source coverage against the complete-effect requirement; runtime loss not tested | Source analysis below | Exact original effects and replay identity must survive leader loss; ordinary row checks cannot qualify this |
| Four-to-five-node expansion and sustained gain after convergence | Failed original qualification; new physical campaign not tested | Original Azure evidence remains authoritative; no new campaign | Zero lost ACKs, correct consistency/routing, bounded fault-safe convergence, stable five-node throughput comparison |

Classifications are scoped to each stated assertion: passed, failed, inconclusive,
or not tested. Implementation status is separate from those results.

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

A verified native image can now be adopted as an explicit format-6 catalogue
snapshot, with the actual image R/H/receipt and birth/attempt identity retained.
This does not invent per-command Applied labels. Every image/recovery/promise
refusal remains; counter advancement is not activation. Five actual component
tests cover adoption, no new admission, idempotent retry without row replay,
persist-acknowledgment ambiguity/re-fsync, strict close/reopen/reissue, and sealed
finalization rejecting a foreign freeze. The two historical native tests also
establish actual local replay with a current certificate majority and fresh WAL
after cutting an older local WAL. Their explicit current metadata configuration
is a test precondition, not an automatic failover test. All result/source scopes
are in the separate 38-test bundle.

The earlier global-catalogue digest was too conservative for availability:
unrelated ranges could invalidate an otherwise frozen local proof. A range-scoped
digest is now integrated and focused tests at `da378dd9` preserve actual range
observations across other-range decisions and same-session provenance refresh.
This fixes a source/integration condition; it does not establish the historical
Azure convergence cause.

## Availability and remaining configuration boundary

The current native image path retains the old-quorum permanent promise and the
source freeze through the complete copy. User writes on that affected range remain
unavailable until actual new-configuration activation. A capture's exclusive source
read pin can also make another local-ready acquisition busy. Correct retryable
unavailability is not false not-found, but this does not meet a continuously
writable online-relocation contract. Legacy online snapshot/catch-up code and this
new native correctness path must not be treated as one qualified implementation.
A final campaign must measure the complete affected-range outage and request
outcomes. Improving this requires a qualified live snapshot/retained catch-up path
with a bounded final fenced cut; a full-copy write pause cannot silently become the
final product requirement.

An installed target's physical capture descriptor can still name old voters/epoch
E2 while its intended descriptor names future E4. The intended descriptor alone is
not proof of activation. Source repair after replacement requires an ACTUAL durable
configuration-transition/ancestry proof, prepared by the required native new-data
quorum and checked against committed final metadata. Keep the physical anchor and
current serving configuration distinct. Literal old physical voter IDs cannot be
silently treated as current, and replacing those IDs with metadata would discard
the required safety proof. That integration, catalogue adoption and strong-read
readiness remain open.

Ordinary remote native Apply is also unimplemented at the tested seam. The real
RF3 characterization at `da378dd9` gets an originating ACK/native R1 after actual
Prepare/Commit/Certify responses, while both remote services remain CQ1/native R0
and correctly refuse bare Apply. Those replicas cannot be advertised as readable
or caught up. The next fixes require an actual authenticated current-leader
instruction or separately proved historical certification, exact native/WAL
materialization and Store Applied progress; never a decoded voter list, equal-row
shortcut or fabricated ordinary certificate. Larger unresolved CQ backlogs also
need a bounded recovery/evidence strategy; the 1024-entry observation bound must
refuse excess rather than omit obligations. The RF1 R1025 image test is not an RF3
backlog/recovery qualification.

## Reproducible issue: distributed range decisions omit original effects

Tracked as [issue #148](https://github.com/nikoma/bicdb/issues/148), with immutable
main-source links and a deterministic survivor/effect reconciliation experiment.

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


## Additional bounded-recovery gates from source review

These are source-grounded limitations/hypotheses, not demonstrated Azure causes
or observed acknowledged-data loss:

- **Not tested / source contract gap:** historical command/entry transport has a
  2 MiB bound, while native origin admission did not enforce the same complete
  recovery-entry and configured transport-frame bounds before durable quorum
  work. A Native-mode pre-admission safeguard is being tested separately; it is
  a limited contract and does not qualify existing larger-value workloads.
- **Not tested / liveness hypothesis:** a clean donor prefix below a certified
  unresolved tail may be unable to provide an image, while a far-behind current
  voter rejects certifying that tail because its raw log is not contiguous. The
  smallest genuine characterization uses three ACKs, donor native2/CQ3 and a
  healthy peer native/raw0 after the original source becomes unavailable. Do not
  bypass the native floor or create fake Applied/ACK lists to make it pass.
- **Not tested:** actual normal writes to a DIFFERENT range during source capture
  still need a running-origin test. Source review finds normal admitted origin
  skips the unscoped database permit; direct unscoped writes intentionally remain
  blocked. Earlier high-B tests seeded other ranges before capture and do not
  prove this concurrent availability property.
- **Not tested:** transfer cancellation before any image intent needs a distinct
  unchanged-preflight return proof. After intent/physical changes, an exclusive
  attempted install must remain fenced and use actual resume/adoption evidence.
  A blind Drop cannot grant a return to ordinary recovery or user admission.
