# Native no-data abort-history recovery

Status: proposal only. No implementation, build, runtime test or production qualification is attached to this document. The design is based on private source `1cd28523c2f8a3970635dcae4ca43977727b1afe` and the separate failed-prepare fixture `eafa6949`. It is independent of the retained experiment receipts.

## The missing transition

A failed native prepare currently leaves a durable raw `Aborted` entry at index 1 with raw `resolved_through=1`, while the verified native prefix remains R0. There is no native pending apply and the rejected row is absent. The next origin selects index 2, then correctly refuses because its actual native predecessor is still R0. The missing work is a proven, durable **no-data decision at index 1**, followed by canonical abort-history publication and ordinary recovery. Resetting an index, trusting `RawAborted`, or committing an empty application transaction does not supply that proof.

Relevant current boundaries are:

- `distribution_range_consensus.rs::admit_with_database`: native base validation, local/remote Prepare, local provisional Commit, remote Commit, local certificate, remote certify, then native pending creation. `abort_prepared` broadcasts best-effort aborts and discards their errors.
- `RangeWriteStore::abort`: changes either `Prepared` or `Committed` to the same raw `Aborted` state. That loses the distinction between a prepare-only rejection and a possibly escaped decision vote.
- `db/spatial_paged.rs::commit_transaction`: distributed admission precedes application WAL sequence allocation, WAL enqueue and row/index/effect publication.
- `Transaction::commit_with_sequence` consumes the transaction; an admission error reaches its pending-transaction `Drop`. `Drop` releases locks and marks a generic observer outcome. That is not a durable, command-bound native no-effect receipt.
- `db/range_image_native/history.rs`: the canonical `Aborted` link already has a defined hash shape, but its constructor is test-only. There is no genuine native abort producer.

## Stage classification

The distinction must be represented by owned control-flow evidence and durable monotonic records. It cannot be inferred from an error string, a timeout, the number of replies received or the current raw state.

| Stage | Actual boundary | Permitted recovery classification |
| --- | --- | --- |
| Before Prepare | No range command or application WAL has been admitted | Ordinary local rollback; no range slot needs publication |
| Prepare only | Command prepared; neither local Commit nor any remote Commit may begin | A local no-commit candidate may be produced after actual transaction cleanup; global abort still needs fresh quorum promises |
| Commit may have escaped | Before the first local provisional Commit or any remote Commit dispatch | Outcome unknown on missing replies; do not invoke the prepare-only abort path |
| Decision/certificate exists | Actual commit-decision quorum or any durable CQ/Applied/sparse-commit evidence | Commit recovery using existing fresh current certification quorum; abort is forbidden |
| Application WAL may exist | Native pending/application WAL has started or its outcome is unknown | Retain recovery refusal and actual pending history; no no-data receipt from drop or row absence |
| Actual committed native callback | Actual application WAL receipt and certified prefix completion | Existing Applied path |

Before the first local Commit, the native origin path must durably cross a `CommitMayHaveEscaped` barrier. It must do so before an RPC can publish a remote Commit. A failed barrier write prevents dispatch and remains uncertain until re-fsynced/reopened. A local `Committed` state is already past this barrier even if no RPC was sent. Once past it, a lost Commit reply cannot be turned into a definite abort error.

The current `committed < quorum` branch is unsafe as a source of global abort evidence: a remote Commit can be durable although its reply was lost. A majority may therefore have committed provisionally even when the leader counted fewer than a majority. The proposed native path reports uncertainty and keeps the command for recovery from this point onward. Legacy non-native behavior is outside this batch.

## Durable facts and sealed types

All live proof types below have private fields, no `Clone` and no `Deserialize`. Wire/persistence projections are separately named observations or audit records. A decoded list of voters never reconstructs a live quorum or no-effect proof.

1. `NativeOriginAttempt`
   - Owned by the real native transaction/admission path; binds actual database open, birth/bootstrap, member incarnation, original local transaction ID, range/index/command checksum and actual range reservation.
   - Keeps a monotonic stage `PrepareOnly -> CommitMayHaveEscaped -> Certified` or `PrepareOnly -> NoCommitAbortIntent`.
   - A command cannot cross from `NoCommitAbortIntent` back to Commit under any retry, nonce or epoch. A new recovery authority can preserve or materialize its abort, not reuse its slot.
   - The stage barrier is durable before external consequences. Missing legacy stage evidence is unknown; migration cannot manufacture a positive proof from raw `Aborted`.

2. `NativeTransactionNoEffect`
   - Produced only inside the real transaction completion/rollback path after the original transaction is consumed, its actual record/index/constraint locks and deferred effect buffers are cleaned up, and no application WAL commit can still be queued by that attempt.
   - Binds the owned attempt, original transaction ID, current DB-open identity, actual native predecessor R/H and its typed local data visibility bound B.
   - Requires actual `PrepareOnly` admission rejection before any application WAL reservation/enqueue. If a later implementation admits withdrawal after WAL reservation, it needs an actual successful `abandon_unwritten` receipt for that exact queued attempt; this proposal does not infer withdrawal from an error.
   - Generic `TransactionOutcome::Aborted`, dropping an unrelated transaction, an empty transaction, missing rows, a raw counter and a caller-provided sequence cannot create it.
   - A durable no-data intent is published before returning the definite prepare-only rejection to the caller. An ambiguous intent publication reports uncertainty. A process death before this receipt exists leaves recovery unresolved; it does not prove rollback.

3. `NativeAbortCandidate`
   - Consumes the actual prepare-only rejection and `NativeTransactionNoEffect`, durably records `NoCommitAbortIntent`, and retains the exact original command identity.
   - A durable intent can be re-observed after reopen from the actual protected record/birth/lineage and independently fresh authority. A cached token from an older DB open cannot survive reopen.
   - It selects the slot for recovery; it grants no row operation or global abort decision.

4. `NativeAbortPromiseObservation`
   - Minted only from an actual local persisted promise or an authenticated challenge-bound transport reply.
   - Binds recipient identity/incarnation/pins, actual Store and DB opens/birth, range/index, candidate checksum, fresh role/config/bootstrap/schema/restore, recovery nonce, and the original absolute parent deadline.
   - Includes the actual no-commit observation and persisted refusal commitment. The vote is not a copied `RawAborted` enum.

5. `NativeAbortQuorum` / `NativeAbortDecision`
   - Built only by recording distinct current-voter observations and consuming a live original round under independently fresh metadata authority.
   - Applies only to the **same voter configuration** as the attempted command. Membership changes require a separately proven old/new quorum transition; current metadata membership alone does not substitute for the old quorum.
   - Its decision audit may be serialized, but only actual Store/native producers can use its durable record to reissue an authenticated decided observation. A naked audit DTO is not a quorum certificate.

6. `NativeAbortApplyAdmission` and `NativeRecoveryAbortedReceipt`
   - The admission combines the actual global abort decision, independently fresh recipient role, exact local durable abort marker and actual native predecessor/no-effect proof under the owned recovery freeze.
   - The receipt comes only from the actual no-data native publication described below. It binds previous/new R/H, original slot/decision digest, unchanged typed local data bound B, actual native metadata-WAL publication identity and native DB-open identity.
   - It cannot be converted to `NativeWalApplyReceipt`, `MaterializedNativeCommand`, NativeReady, Serving or a read permit.

The implementation needs a typed internal rejected-admission result/callback. The current generic `Result<CommitAdmissionTicket>` error loses the attempt identity before transaction cleanup. The native commit path must retain the owned rejection through cleanup and publish its no-effect intent before reporting a definite error. It must not parse `BicDbError` strings to recover the command.

## Why an abort quorum is necessary

A local origin receipt alone cannot close a globally writable slot. An old leader or delayed Commit RPC could otherwise race a new recovery coordinator. Conversely, one durable raw `Aborted` entry may have overwritten a prior provisional Commit and cannot prove that no decision quorum existed.

Each participant must preserve a **monotonic ever-Commit fact** for the slot, atomically with its first local Commit, before acknowledging it. Raw abort, epoch fencing, compaction and a new recovery nonce must not erase that fact. CQ, Applied, sparse committed records and materialized native pending work are irreversible conflicting evidence. Legacy compacted/erased history is unknown unless covered by actual verified native history or a retained decision record.

A participant may promise abort only after:

1. independently fresh normal-recovery role validation and actual same-config epoch fencing;
2. draining its actual owned normal/controlled mutation callbacks outside all service/DB latches;
3. checking actual native birth, Store-open scope, complete retained logical/sparse/native history and the relevant original attempt;
4. finding a genuine never-Commit state for the slot and no conflicting native/materialized history;
5. atomically persisting an irrevocable **slot-wide** refusal of all later Commit/Certify/Apply commands at this index, not just refusing one checksum.

Absent history above a verified retained last index can qualify only when the actual immutable lineage and writer-format rules establish that no vote was discarded. Compacted `None`, an unknown native floor, a reset database or a legacy `RawAborted` without its monotonic stage cannot supply that conclusion.

Collecting a majority of these promises intersects every possible old Commit majority. At least one such voter would have retained an ever-Commit fact and refused the abort promise. Promise persistence also prevents a delayed old Commit from forming a new majority after collection. This argument requires the old and current voting sets to match; the first implementation must explicitly refuse membership-transition cases.

Any observed CQ/Applied/sparse commitment, conflicting payload, native Applied history or materialized pending command rejects abort. A merely provisional minority past Commit may be mathematically abortable with additional complete evidence, but **that is outside this smallest protocol**. The initial implementation handles genuine prepare-only rejection and reports commit-maybe cases as uncertain.

The promise is permanent until its semantics are represented by the verified canonical abort history and a durable retained decision/checkpoint. Recovery completion, lease release or higher metadata epoch cannot garbage-collect it. Bound exhaustion is an explicit unavailable result; no eviction may reopen an aborted slot.

## Actual no-data native publication

The recipient acquires a counted controlled writer through its actual frozen owner. Native admission, actual prefix/birth/schema/UID/restore, catalogue scope and slot refusal are revalidated under the local native journal. Source pins must be absent; no network call occurs under these guards.

For a recipient whose actual native predecessor is R=i-1:

1. Verify exact prior native receipt and H; verify no local pending/materialized attempt can still publish application rows for i.
2. Persist an actual native **metadata page-WAL transaction** containing an immutable abort-decision binding, the canonical `Aborted` history link for i, and the new prefix receipt/pointer atomically. Retain the original decision audit outside the hash spine if needed.
3. Set R=i and H to the canonical abort link. Carry forward the previous actual typed data visibility bound B unchanged. The metadata page transaction's XID/sequence is recorded separately and is not an application materialization bound.
4. Read back and validate actual records/durability. Only then return `NativeRecoveryAbortedReceipt`.
5. The actual service-owned Store consumes this receipt to mark the ordered abort resolved. A failed catalogue write keeps refusal and retry state. No second Store or numeric `mark_applied` shortcut is allowed.

The existing canonical history shape deliberately hashes `Aborted` at the slot without an Applied command checksum. The durable abort decision/slot refusal still binds and audits the exact attempted command and forbids every competing command at that index. A conflicting attempted payload must not be silently selected because its final abort hash would happen to be the same.

No row transaction, audit event, outbox message, sync effect or application WAL commit is created. If the implementation needs a prefix receipt enum, it should have an explicit `Aborted { decision, no_data_publication, previous_data_bound }` arm. It must not fabricate an Applied receipt or an empty application transaction to satisfy the existing Applied-only constructor.

Already-ahead recipients verify their actual immutable history at i is the same canonical Aborted decision before acknowledging idempotently. Recipients behind i-1 first receive each certified prefix outcome in order or an actual compatible image. Unknown native floor is never coerced to zero. A missing raw payload does not establish an abort.

## RPC and coordinator shape

Proposed narrow operations are `PromiseNativeAbort` (no rows), `ObserveNativeAbortDecision` (read-only), and `PublishNativeAbort` (actual no-data native metadata publication). Incoming requests carry scope, slot/candidate identity, challenge and budget; they do not carry an authoritative voter list.

The receiver acquires fresh metadata role outside service/DB latches, rechecks the actual authenticated initiating leader, then performs local persistence with its actual service-owned Store. Each publication receiver independently collects/verifies the required genuine promise/decision observations; a coordinator's local opaque token cannot be serialized into recipient authority. A previously durable decided observation is a distinct recovery fact, not a fresh vote or NativeReady response.

Reuse the existing four-context/four-worker/64-member ceilings, bounded payload/record counts and original absolute parent cancellation/deadline. Drained workers retain their permits. Retry must preserve the durable slot decision and nonce rather than inventing a new session on each RPC. Old servers or missing mandatory versioned evidence refuse; there is no raw-Aborted fallback.

Failed native prepare invalidates ordinary recovered readiness and schedules this work after the caller releases its outer DB guard. It cannot synchronously self-RPC or freeze while holding the transaction's own mutation permit. The supervisor completes abort publication, then actual NativeReady/Complete/Activation before new user admission. No `recovered_epochs` seed is introduced. The next application transaction may be retried as NotReady while this bounded background work finishes.

## Crash and retry rules

| Cut | Required behavior |
| --- | --- |
| Before durable no-commit/no-effect intent | No abort authority; outcome remains unknown after crash; never infer it from a missing WAL record |
| Intent persisted, caller response lost | Same command reissues its actual intent; no competing Commit may start |
| Abort promise rename/dir-sync ambiguous | Recipient stays fenced; exact re-fsync/reopen validates refusal before returning a vote |
| Quorum decided, native publication not started | Retained actual decision can resume; no prefix counter has advanced |
| Native page-WAL abort record durable, reply/catalogue update lost | Actual immutable native record reissues receipt; Store marks the same abort idempotently |
| Store resolved but native publication incomplete/unknown | No Ready/new origin; this is a refused inconsistency until actual publication/repair completes |
| Process restarted | New Store/DB-open tokens; fresh MQ and actual native records reissue evidence, never reuse old live tokens |
| New leader/nonce/epoch or image lease | Permanent slot refusal and ever-Commit facts remain; exact current authority/config must be revalidated before work |

A serving/configuration grant is still separate. Canonical abort publication does not clear recovery, image or handoff refusals and does not authorize StrongGet.

## Three smallest genuine regressions

1. **Prepare-only failure, remote gap and the next ACK.** Reproduce the actual three-service native fixture: both remotes unavailable during Prepare, no Commit dispatch, definite no-effect intent, no row/effects. Restore remotes and run the real bounded recovery path without counter edits. Require genuine AbortPromise QQ, actual native `Aborted` at index 1 on the selected source and a previously empty remote, unchanged source B at this step, then real origin ACK2. Assert history `Genesis -> Aborted1 -> Applied2`, R2 with actual application B1 (or the measured actual B, never a cast), rejected row absent, no failed-attempt effects, and no cached-epoch seed.

2. **Commit reply loss cannot become abort.** Let actual services durably Commit on a majority, lose Commit replies so the origin counts less than a majority, and prohibit row Apply. Require `CommitOutcomeUnknown`, preserved ever-Commit evidence and no prepare-only no-effect intent. A fresh leader's actual abort-promise round must refuse; a local raw abort/row absence must not override the committed facts. Restore the required genuine history participants and recover through actual current certification QQ and real native WAL. Assert one logical outcome and no false canonical Aborted link. This is not a fake-ACK transport fixture: wrappers may drop real replies only after actual durable service work.

3. **Lost no-data publication and reopen.** On a genuine prepare-only candidate, inject one deterministic cut after actual native abort page-WAL commit but before the ordered Store result is durably acknowledged. Close every owning handle, reopen actual DB/Store, observe independently fresh same-session authority, and resume. Require the same immutable abort decision/H, unchanged actual data B, exact-once history entry, no row/effects and eventual genuine next ACK. An authenticated delayed Commit for the old slot and a fresh-nonce attempt must both refuse after reopening. A missing/corrupt protected abort record is a separate negative subcase that remains unavailable, not a reconstructed success.

Acceptance is scoped to these actual component/service tests. A later TLS participant case must verify exact peer/challenge/nonce/version bindings; a later physical test must separately establish restart/fault behavior. Neither is implied by this proposal.
