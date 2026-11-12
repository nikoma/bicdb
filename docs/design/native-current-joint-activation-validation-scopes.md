# CurrentJoint activation validation scopes

Source baseline: `6e00ba519715ee0ca392357394f2b0a9749a1358` (same activation
runtime as executed `f58d14fc8836e5cb14ba5251d4415d5ac47fd5b0`). This change is
source only until its own remote checks execute. It changes no deadline, worker,
retry count, fixture cap, wire format, catalogue format or permission purpose.

The two actual F58 failures are distinct. Activation-controls job 131502250718
(run 43309691294) passed seven controls but its heavy case failed first
RuntimeActivate after all three Complete calls returned. Decision WAL/readback
and catalogue fsync finished before the deadline; the final full receipt check
spent 986.139 ms with 746 ms remaining. Positive job 131502214629 expired earlier,
before decision WAL, after repeated full preflight checks. Neither reached
ACK4/idempotence. These observations do not establish a physical/network cause
or prove that the revised path fits four seconds.

## Actual exclusion and current authority

`NativeActivationReadScope` is a private-field, non-Clone/non-Serde, !Send borrow
created through an HRTB callback. It owns the actual native journal and one
actual source lineage; it borrows the SAME DB, Store, Prepared, completion QQ,
fresh mutation role, original caller and actual service slot. It is not an
observer-role conversion, cached currentness flag, durable grant or cold-open
constructor. Only the operational CurrentJoint path selects it. NormalRecovery
and standalone full receipt/physical APIs retain their full validation.

The actual service mutex and checked DB read exclude Store replacement and DB
object replacement. The actual owned Prepared keeps the physical pin/Frozen.
The native journal excludes other protected native publication during a scope.
It does NOT exclude hook replacement, registration revocation, schema/health
change or caller cancellation. Full original writer/native admission/physical
checks run on entry/exit; necessary actual hook/registration/open/schema/health,
slot, pin/Frozen, QQ and role/caller checks also run at internal boundaries.
The necessary-only writer/pin helpers grant nothing outside that contract.

The borrowed CurrentJoint route preserves the protected bridge current/archive,
actual native birth/open, local OLD closure/promise, catalogue joint activation,
original saved writer, and every saved/archived native-history floor. It reuses
actual lineage under the SAME journal, never a serialized binding as authority.
It adds no equation between bounded raw suffix and complete native history.

## Three independent scopes

1. Before catalogue publication, the actual Completed materialization, serving
   pointer/archive and actual QQ/fresh role are checked once in the pre-CAT
   scope. Native activation decision publication retains the existing page-WAL
   path, immutable old audits and current pointer. Its postwrite readback reads
   the ACTUAL NEW pointer and archive, validates the actual decision against
   owned materialization, and then full scope exit rereads physical/domain
   state. A prewrite lineage alone is not accepted as the new decision result.
   The exact existing-decision branch returns the stored audit without rewriting
   its original MQ/QQ provenance.
2. That journal is dropped BEFORE raw catalogue fsync. The catalogue write still
   sets persistence uncertainty first and clears it only after actual success.
   A NEW post-CAT journal scope independently revalidates native state. Activated
   is validated as its own phase: the saved Completed point/serving binding and
   recomputed Completed-phase catalogue digest must exactly match the protected
   transition. It is never passed through the Ready phase whitelist.
3. Runtime still proves actual session/Prepared/Frozen uniqueness before release.
   Late arm takes another independent scope and performs receipt validation plus
   actual serving-generation binding once. The existing generation guard is
   installed in the Store ONLY after successful scope exit. There is no fallible
   work added between the Runtime's final physical and Frozen drops.

No native journal crosses catalogue fsync, network work or a remote participant
wait. Existing standalone `receipt.require_current` and open-guard constructors
remain full; no scope token escapes to skip their checks later. Expiry/error
following durable WAL or catalogue publication returns no success/open. The
stored audit and retained actual Frozen session support only a distinctly fresh
actual retry, not renewal of the expired original QQ/caller.

## Source and acceptance

Primary changes are `serving_configuration/activation_scope.rs`, the typed
standalone/scoped native decision producer, Prepared's exact Activated binding
validator, and CurrentJoint's additive borrowed activation route. Data captures
its real service slot and passes the ORIGINAL Runtime caller. The unchanged full
post-joint positive remains the acceptance for canonical Abort3, all-member
activation, actual ACK4 (third business ACK), StrongR4 and idempotence.

New filter `native_current_joint_activation_scope_actual_` adds four actual
post-CAT cuts. Each requires genuine handoff/all3 release/ACK2/StrongR2, real
CurrentJoint fences and all3 TLS Prepared/Completed replies. A test-only hook
runs ONLY after actual catalogue fsync and validated Activated scope entry:
actual foreign DB/Store substitution, original caller cancellation plus fresh
exact retry, actual public hook replacement, and native-WAL deletion of the
actual decision's immutable archive. A mandatory reached assertion prevents an
earlier timeout from satisfying a negative. All data R/H/B/UID/slot/promise/ACK
projections remain exact; the expected Activated control-plane change is
asserted separately, and no failed cut arms/releases the real Frozen owner.

These tests are unexecuted at this checkpoint. They do not establish operator
handoff, higher-epoch/cold rebinding, native tail repair, large-history capacity,
8 GiB execution or physical qualification. A source-level removal of duplicated
work is not a latency or completion claim.
