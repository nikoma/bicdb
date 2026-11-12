# Native joint all-frozen catalogue/open phase

This is a source-only continuation of the protected-publication component. The
prior integrated d194594b CI 43207167250/job 131145173584 passed its two
publication/caller-expiry tests in 23.58s, with all-target and isolated-consumer
checks passing. Those results do not test this new source. No local Cargo run,
operator feature, remote data handoff, activation release, ACK2, Strong, or
physical qualification is claimed here.

## Actual admission and immutable facts

`NativeJointPublishedServingQuorum::begin_fresh_open_phase` retains the exact
original caller token/deadline/cancellation and requires the full original and
final member/publication sets. Each actual participant independently acquires
real recipient-bound HostMQ and checks its SAME service, DB/store open, protected
native bridge, immutable preparation, writer registration and actual owned
freeze. A failed acquisition poisons the round. The local operation then needs
another independently acquired post-all-member HostMQ. Four-second phase and
five-second metadata maxima are unchanged. One phase selects one local operation;
its admission cannot retry itself.

The metadata observation does not establish membership. It must match the
already protected OLD+NEW data decision and all published native receipts. The
original preparation/audit and expired observations remain unchanged. A new
phase is not a cold-recovery purpose and accepts no substitute caller token.

## Exact target catalogue transition

An unactivated participant must still match its original complete opaque
catalogue observation. A participant already activated must have the exact
actual native-backed `NativeJointCatalogueActivation` in its current store,
with no image intent. Its native bridge must bind the original preparation
catalogue digest.

Only then does the checker recompute the complete current catalogue digest with
its epoch field compared to the original observation's captured epoch. The
actual current epoch must equal the protected activation epoch. Command IDs,
checksums, states, last/resolved/compacted watermarks and every retained
certification/sparse/Installing provenance remain in the comparison unchanged.
This supports the target's capture-to-activation epoch shift without treating a
metadata map, new epoch or matching numeric R as a data decision. A higher or
unresolved obligation invalidates this all-frozen phase; it is never discarded.

`NativeJointCatalogueActivationFact` is a non-deserializable actual catalogue
fact with no request authority/TTL. Its producer reads the current protected
catalogue and validates the actual native receipt. It is not a serving grant.
The original TTL-bearing activation receipt remains subject to its own original
expiry.

## One local locked validation scope

`NativeJointBridgeReadScope` borrows the actual DB and protected receipt and
owns its nonreentrant native journal guard. It validates current/archive native
and preparation records and actual source lineage, then keeps that journal
held only through the SAME service's local catalogue/preparation checks. It
cannot be cloned, deserialized or retained beyond its actual borrow. It carries
no metadata request, deadline or serving permission.

The frozen check still requires the exact protected preparation, native R/H and
actual local visibility B. Schema/restore/UID/base/history/generation validation
comes from the actual bridge and lineage checks. Actual writer registration is
rechecked within the local validation. The scope never spans network collection
or another service's lock. Final operational methods revalidate facts; there is
no cross-request authority cache.

The audited reader chain is protected bridge/current/archive → actual decision
reader → current source lineage → genesis/installed anchor/history/serving
readers. These readers do not acquire the journal. Native page-WAL, prepare and
install ownership writers acquire it separately. A page-WAL path must not call
this scope while already holding the same journal. The new scope is used by
read-only same-service checks and is dropped before local activation mutation.

## Unknown local catalogue completion

Before counting a participant, an independently fresh actual HostMQ may only
re-fsync an already present exact protected activation after ambiguity. It
checks current committed domain/requester, original native audit anchor, actual
native receipt, local writer policy, same store/root and real quiescent freeze.
It holds a counted controlled lease through the actual catalogue persist. It
cannot create an activation, rewrite the audit, clear a new intent, or discard
other catalogue obligations. Expiry/cancellation after persistence yields no
current permission.

A local open operation first recovers that exact stored activation when present.
It returns a distinct receipt under the new actual all-member/post-quorum
admission; the old receipt stays expired. Recovery does not rewrite catalogue or
native audit. Guard installation is idempotent: an existing exact unarmed guard
must still have its actual serving generation. It is not replaced by a new
generation. Missing installation after actual activation may be completed only
under the original actual frozen owner. Armed/released guards and invalidated
serving generations refuse this all-frozen path.

The new checked freeze accessors return an error for a consumed image recovery
lease. They never recreate a Frozen or dereference a consumed owner.

## Unexecuted regression and remaining work

`native_joint_fresh_open_phase_reconciles_exact_target_epoch_and_lost_catalogue_reply_while_frozen`
is drafted on the genuine born-v2 policy-before-first-ACK fixture. It first
publishes all actual native receipts, activates the image target before the two
survivors, drops its successful local catalogue reply, expires that original
receipt, and reconstructs the exact activation under new phases. It checks the
actual target epoch shift, immutable activation checksum, all three protected
activations, every retained freeze, and a real serving-generation invalidation
negative. The lost reply is a local aggregation fault after the actual fsynced
producer, not an executed remote socket-loss claim. Dropping a phase/controller
round is not a process/cold restart test. This new test has not run.

All final replicas and target leader remain mandatory. The original complete
joint test's ACK2/Strong assertions are not reduced. This new regression stops
while every owner remains Frozen. Actual release needs a distinct continuation
that drops preparation-owner Arcs, retains original caller provenance and exact
native/catalogue facts, and distinguishes actual retained Frozen, actual dropped
but unarmed owner, and current armed serving-generation/open state. After one
member releases, its old Frozen must never count in a later round. Higher current
R/H/B and complete irreversible obligations must remain current; they cannot be
reset to the original cut. Lost release reply, original expiry, owner/generation
ABA, controller restart and genuinely new cold recovery require their own
actual producers/controls. That work remains open, along with new-leader ACK2,
Strong, cleanup, fresh fifth learner, remote joint RPCs, lower-R/missing-member
repair, post-joint NoData, automatic controller and physical testing.

## First execution and opt-in custody follow-up

Integrated source 5056d857 CI 43209913124/job 131154586387 compiled (all-target
and isolated consumer passed), but the open case failed 0/1 in 37.31s with
three target timeouts. Its successful preceding all-three publication does not
prove catalogue activation. Original logs did not identify the exact open
substage or show a completed catalogue persist. The retained CI artifact
contained logs/source identity only: `TempDir::keep` paths on a discarded
runner were NOT persistent native-state custody. This lack of native cut
evidence remains explicit.

The follow-up adds test-only component timings and an opt-in
`BICDB_TEST_EVIDENCE_ROOT` panic capture. It actually stops/drains listeners and
checks runtime contexts, then holds the actual service, metadata, DB and native
journal locks. It copies only actual database trees and explicit actual
`strong-meta-*` directories. The private TLS root is never copied; credential
file extensions are rejected. Limits are 4096 files, 64MiB/file, 256MiB total.
Copies and manifest/directories are fsynced, each source file is rehashed and the
complete source file set is checked after copying. `BICDB_TEST_SOURCE_SHA`, if
provided by the runner, is retained separately from non-authoritative workflow
SHA. Partial/refused capture has an explicit error manifest.

The original engine owners remain alive. No checkpoint, engine reopen, repair
or floor mutation is invoked. This is an observed-stable, locked raw-files cut;
it is NOT claimed to be an atomic power-loss image or a verified replay/clean
shutdown. A later reconstruction must verify the manifest and replay a COPY.
The new capture hook itself is source-only until execution.

The lost-target fault is now keyed to the FIRST actual returned durable target
receipt (newly persisted or recovered), rather than lucky attempt zero. Three
pre-receipt phase attempts remain; one additional bounded phase is reserved
solely to reconcile that deliberately lost receipt. Every phase still uses the
unchanged four-second/original-caller bounds.

## Local execution-boundary refactor (unexecuted)

The 505 logs lacked open substage spans, so its exact before/after-persist cause
is not inferred. A separate diagnostic/custody checkpoint records those spans
without changing the phase algorithm. The following source refactor addresses
visible repeated work, and still needs its own compile/runtime feedback.

For a fresh open, all nonwaiting external-participant checks now happen at the
final kernel boundary before the local native journal is acquired. The same
checked DB/service borrow then performs the complete original local preparation
check under ONE actual native read scope, including exact old/new decision,
metadata anchors, writer-registration Arc, full catalogue digest (with only the
protected activation epoch transition), current/archive native preparation, real
freeze/read pin, schema/UID/history/generation and strict local R/H/B. This is
the same checker used by phase collection, not a weaker kernel substitute.

Every captured resolved catalogue decision is read directly from the actual
immutable native history under that scope. This replaces per-index source
export calls that each revalidated the entire source lineage; no decision is
filtered and no higher obligation may disappear from the actual catalogue
digest. The scope/journal stays held through controlled catalogue fsync and
exact receipt readback. It is dropped before installing the open generation.
There is no native page-WAL acquisition inside this already-held journal.

Recovery first checks whether any actual activation exists. An absence result
grants nothing; the new-publication branch still performs the complete final
checks. A present activation is fully verified under fresh full-set authority
and the same local scope, yielding the exact existing record without rewriting
it. Existing unarmed guards are checked and retained; new guards perform their
complete constructor check once. No private current-proof cache survives the
synchronous local call or becomes a serving permission.

Original APIs keep their original validation path and deadlines. This refactor
does not enlarge a phase, skip a participant, remove the target leader or satisfy
the release/new-ACK/Strong gates. If measurements show collection itself exceeds
the original window, a separate real bounded collection protocol is required;
repeating expired facts or silently increasing TTL is not a remedy.

## Executed retained-timing diagnosis and scoped candidate

The unchanged phase algorithm passed once at integrated source 656a8e6a
(CI 43212523246/job 131163486234, 1/0 in 32.96s). Successful stdout was suppressed,
so that run did not retain timing markers and did not exercise panic custody.
It is not evidence that the earlier timeout cause/stability was fixed.

A show-output rerun at exact source 399529dedeaba5d404098d091f733491c690067d
(CI 43213581356/job 131167066109) failed 0/1 in 36.00s. All three target attempts
exhausted their unchanged windows in activation/reconciliation, with durations
4075ms, 4075ms and 4139ms. Collection took about 1.1s, complete-round validation
about 1s, service prevalidation another 1s. The kernel entered with only
438–628ms left and repeated peer validations until expiry. No completed
catalogue-persist marker was emitted. Raw job SHA256:
`11ff4f55a212003d83aeaf168ca46a3755b7a67b0c92803875b27eaeefa7f8bf`.

This directly motivates the separate scoped execution candidate: preserve the
complete final kernel checks while performing the same local proof under one
real journal borrow and each external sweep once before that borrow. It does
not justify a larger timeout or fewer participants. The candidate remains
uncompiled/unexecuted locally; its first remote results must be recorded under
its own exact integrated source. Original API paths and all fixture assertions
remain. Higher protected abort/slot controls are also explicitly checked on the
exact activated-catalogue branch before accepting the old cut.

The 399 panic custody hook actually executed: manifest SHA256
`c12af7b77ab0007361d2da1e0b073481b179aafd81648ccb32088cf5623e9eeb`, status
`captured_observed_stable_with_live_engine_owners`. Artifact 13941814654 contained
71/75 manifest files (4,257,680 bytes verified by Root); upload-artifact omitted
the four zero-byte `databases/n{0,1,2,3}/.backup-write.lock` hidden files. The
archive is therefore PARTIAL, distinct from the hook's completed local copy.
No missing files were manufactured. Root separately enabled hidden-file upload
for a future unchanged-runtime reproduction; that does not retroactively repair
399 custody or establish replay/atomic-powerloss qualification.

## Retained concurrent fault-control failure and closure correction

At source `61eb4d92b5938349cf18a283e936eee45fa97e9d`,
[run 43225435854](https://github.com/nikoma/bicdb/actions/runs/43225435854),
Core job 131207285182 failed all three `open_adversarial_tests` in 38.19 seconds;
all-target and isolated-consumer compilation passed. In each fixture the new
scoped publication completed all three final members, including exact stored
lost-reply reconciliation. Opening then exhausted its unchanged four-second
phase before the intended fsync/cancellation or foreign-reenrollment fault cut.
Those fault invariants remain untested by this failed cohort.

The retained step-cancellation trace shows collection leaving 2,333 ms,
then a second complete sweep at round closure leaving 1,022 ms. The independent
post-quorum MQ left 946 ms; the required final foreign sweep exhausted the
budget before the local native journal and catalogue fsync. This is observed
CI validation cost, not a physical recovery measurement or Azure attribution.
Raw job-log SHA-256:
`39cc01a924073e23438497af2e30dcc82820ac36044e6ddde786ae53b79fb3f0`.

The correction closes the already collected round using original-caller,
per-member current MQ and actual writer-lifetime validation. It does not repeat
the full native reads there. No collected observation becomes an activation
permission: the distinct local post-quorum MQ, complete FINAL foreign-member
sweep and exact local journal-backed proof remain required at mutation. No
member, cancellation check, deadline or post-WAL validation is removed. The
corrected cohort must execute the actual fault cuts before it can pass; this
source correction has no executed passing claim yet.
