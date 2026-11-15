# Original writer CLOSED historical read scope

Source baseline: `a5911ec99d0564c011d916309bda64b6ae62a9dc` (prototype source
1.0.502-beta). The original hosted run 43561072006 remains FAILED: 79 passed,
eight failed. Seven failures reached the survivor's selected DATA-domain CLOSED
publication and then demanded ordinary Serving permission from a joint historical
no-image/catalogue read. The eighth failure was the separate immediate actual
MStore `try_lock` refusal. This change does not reclassify that evidence.

The original transition marks its selected range pending before native audit
publication. Ordinary `NativeWriterHostState::require_range_id` correctly refuses
that range. The previous historical re-observation called
`closed_writer_local_promise` and `require_decided_joint_catalogue_base`, whose
ordinary no-image guard demanded that same Serving permission. This creates a
self-refusal after the transition has fenced itself.

A private, borrowed `NativeWriterClosedHistoricalReadScope` now comes only from
the actual retained `NativeWriterDataDomainTransition` and its fresh actual
`NativeWriterDataDomainPolicy`. It binds the original State, coordinator lifetime,
service/runtime/process, DB birth/open, Store slot, original registration/hook,
quiescent original lease, selected survivor range, complete protected joint
native decision, and current OWN/member/version point. It can inspect only the
same retained prewrite candidate, or the same attempted record after exact native
CLOSED CURRENT and immutable archive validation. A begun but unframed, missing,
changed, aborted or otherwise unproven native audit cannot use this path to
replace its native transaction or become a verified CLOSED domain.

Only joint historical no-image reads use this scope. They still require actual
image-intent absence, backup refusal, exact permanent LOCAL promise/native OLD
closure, complete native/catalogue decision, abort coverage and resolved log cut.
The generic no-image helper, ordinary writer range admission, APP and Strong
checks remain unchanged. Images retain the existing materialization checks.
There is no deserialization/parts constructor and no Serving or CLOSED-success
grant produced by the read scope. Verified CLOSED publication still requires the
native record and audit verification followed by the external actual MStore
postcheck. A pending transition remains unavailable to ordinary operations.

The actual issued native xid is retained as the first operation of the native
transaction closure, before scope checks and framing. The exact framed counter/archive/CURRENT bytes are retained in the same
attempt before any native put (at most 16 KiB plus two 256 KiB records).

The transition retains the actual native helper return before any health,
readback or liveness postcheck: `KnownCommitReturned` with its actual returned xid,
`ReturnedNoStore`, or `HelperErrorUnknown` with the actual issued xid when known.
Error provenance retains a UTF-8 prefix of at most 4,096 bytes and a streamed SHA-256
of the complete displayed error, with an explicit completeness flag. Health
observed after that return is separate; no helper error or TxStatus observation
is described as a successful abort API return. A helper outcome is DATA and does
not replace the actual protected CLOSED CURRENT/archive checks. Original unknown
or unframed attempts cannot obtain a replacement helper invocation. Unknown
outcomes keep the same attempt; this change adds no abort-retry protocol,
replacement xid, TTL, range-wide bypass or new source lineage.

The added genuine fifth-fixture case
`native_registered_fifth_remote_hot_closed_audit_native_reply_lost_same_attempt_retry`
uses the real initial survivor, real CLOSED native CURRENT/archive readback, and
an actual cancellation after that native publication. At that boundary it records
ordinary selected-range and no-image refusal, typed historical success, and
other-range scope refusal. It then requires fresh same-owner reconciliation to
retain the exact whole native audit record/xid, actual helper return, one helper
invocation and all three retained byte hashes. All original 87 selectors,
assertions and budgets remain. The new control is source only until executed;
formatting is not a compile/runtime result.

The independent immediate MStore contention failure is not fixed or causally
attributed by this leaf. Other open gates include later complete hot Serving,
unchanged-client APP/full Strong after movement, a second populated range on the
same actual target, restart/cold custody, normal controller/CLI integration,
fault-safe bounded rebalance and physical sustained capacity qualification.


The follow-on pre-Serving callgraph is source-pinned in
[native-writer-closed-joint-read-callgraph.json](native-writer-closed-joint-read-callgraph.json).
The actual Closed service producer now retains its existing Closed domain in the
committed observation; ordinary/Installed and Registered producers keep no such
domain. Only exact complete-decision equality is accepted. A persisted decision,
metadata snapshot or missing writer never constructs that domain. Historical
catalogue observe/revalidation checks this actual owner, independently fresh
committed OWN/native/MStore point, and exact original survivor Frozen allocation
before checking image-intent absence. A Weak reference retains allocation
identity without pinning the permit or affecting unique release. Existing image
materialization validation stays intact for image-owning paths.

The actualHost activation re-fsync sink also validates its caller's original
typed writer domain rather than demanding ordinary Serving from a Closed domain.
Its genuine native decision, current metadata, exact existing catalogue activation,
original frozen owner and actual fsync requirements remain. Fresh Open,
activation receipt recovery, release, source/OLD retirement and final Serving
policy were traced separately; their existing typed domain paths remain.
Ordinary recovery/Ready factory guards stay strict and must execute after the
actual Serving policy. This audit is source evidence only, not a claim that any
later phase or the physical operator path has passed.
