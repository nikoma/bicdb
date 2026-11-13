# Registered native-base real authority expiry and abort recovery

The original test-only leaf was `34dded55efef6ed989cdf7d141d5751d4f2a2391`
(unmerged source 1.0.494-beta). Its hosted run 43502925624 passed 1 / failed 1:
actual before-put Role expiry left owned native xid 21 Aborted with no visible
base/archive, and a fresh same-A request failed for a missing immutable base
publication. Actual after-commit Role expiry retained Committed xid 21 and a
fresh request reconciled that same xid. Those complete original artifacts stay
unchanged. This does not establish the original `e1ec` timeout's precise cause.

The runtime successor `c1e4b3a4e8e7ad33c688436830f9af10a4203cc5` implements
bounded same-open native-metadata abort resumption and adds safety controls.
Its hosted run [43510565054](https://github.com/nikoma/bicdb/actions/runs/43510565054)
compiled all targets and selected/executed exactly ten controls: **5 passed,
5 failed** in 88.92 s. Earlier passing results do not validate changed code;
HA remains unqualified.

The before-put control recovered actual Aborted xid 21 with fresh xid 22;
the after-successful-put control recovered Aborted xid 21 with fresh xid 23.
Both retained the exact old boundary under the original A. The committed-expiry
control reconciled the same xid 21. The default entry-budget control retained
nine actual aborted requests (eight old plus the current owner) and refused a
tenth allocation. Missing-final-image refusal also passed.

Both actual sync-failure controls reached `page_log.sync` once and retained the
same publisher with one native attempt and no retirement. The abort case
observed Aborted xid 21; the commit case returned `CommitOutcomeUnknown` with
same-open status InProgress. Fresh requests were refused by sticky
`WriteFailed`. Nevertheless, **both tests failed** because a fixture assertion
incorrectly required the health-gated ownership diagnostic to succeed on the
intentionally fenced handle. The later acceptance assertions were not reached;
independent reconciliation of retained observations does not relabel the tests.
Three existing controls timed out in prerequisites: final-image publication for
the cancellation case, and native-base publication for the normal and image
invalidation cases. Their precise timeout causes and native publication
outcomes remain **inconclusive**; their later named guarantees were not tested.
The complete failed archive remains unchanged.

The subsequent test-only observer correction is uncompiled and unrun. It keeps
the generic observer's exact `WriteFailed` refusal and independently compares
the actual host map, retained lease/frozen A, exact preflight and source IDs
before/after the fault and fresh request. The new snapshot reads memory only;
it does not check health, read native storage, grant admission or claim the
retained lease can currently serve. Publisher equality, actual I/O/status,
sticky health and no post-fault payload visibility claims remain mandatory.
The critical final/base calls now opt into the same original request-window
diagnostics and bounded CURRENT/archive/native-status observations. Fixed
`registered-native-image-critical-outcome.json` preserves each actual call
before its original unwrap/assertion; earlier outcomes are not overwritten.
Request ordering, actual metadata windows and the five-second parent are
unchanged. These diagnostics do not establish the causes of the earlier
prerequisite failures.

`RegisteredRemoteBasePublication` retains the actual issued native xid and
request identity before any fallible or liveness check after begin. It retains
the exact receipt/boundary/bytes before either put and validates the complete
current final image before writing. A failed allocation without an exact
boundary stays fenced; it cannot silently allocate again.

A replacement requires the original A/process/open/native birth/Store/MStore,
fresh actual Role and OWN context IDs, and the original source MF, Finished
frame, protected proposal/Installing/final image, current generations/schema/
readers/namespace IDs/index definitions and native irreversible preimage.
Classification holds the actual native exclusion and page writer serialization.
Core, page and WAL health must pass before and after classification; the
original privately issued xid must be terminal Aborted, with no commit-return
or verified base and no visible current base or exact old archive. Status or
absence alone grants nothing. A clean actual abort after successful metadata
puts is eligible because the helper completes its structural closure; failed
abort/put/sync or poisoned health remains fenced.

Before clearing any slot, the producer retains the exact original boundary Arc,
canonical bytes, original error/request and checked retirement provenance. The
same-open history is limited to 8 retired attempts and 8 MiB of canonical bytes.
Each original error must fit 4096 bytes; an oversized error is retained in the
current owner and refused rather than truncated into a new retirement proof.
Exhaustion returns a stable budget diagnostic and retains the current attempt;
there is no eviction. Each genuinely fresh request can create at most one new
native metadata xid, never reuse an xid or retry a logical row transaction.
Committed/unknown/in-progress outcomes never reset. Committed reply expiry
reconciles the same xid. This history grants no cold reconstruction authority.

The expiry selector is
`native_registered_fifth_remote_native_base_actual_expiry_`. Its before-put,
after-successful-first-put and after-commit controls retain the existing 5 s
parent, actual metadata windows and Role→OWN→Finished→host order. The first
two wait for the real original Role deadline and require a fresh same-A request
to finish with a different native xid, retaining the exact old Aborted proof.
The committed case requires the same xid and zero retired attempts. No fake
clock, deadline renewal or caller cancellation creates the expiry.

Additional `native_registered_fifth_remote_native_base_actual_` controls arm
actual `page_log.sync` failures at two distinct cuts: after the first put before
Role-expired abort/structural closure, and after the last owned-current check
before the actual native commit. They require sticky unhealthy/unknown refusal
without replacing the original owner, even after the I/O fault is disarmed.
The default entry-budget control uses nine actual separately requested healthy
before-put caller-canceled transactions, then proves refusal before a tenth
native attempt and retention of all eight old boundaries. The 8 MiB bound is
enforced in source; this small fixture does not exercise a large-receipt byte
exhaustion or establish suitable production budget sizing.

Fixed reports `registered-native-base-real-role-expiry-outcome.json` and
`registered-native-base-abort-recovery-outcome.json` precede acceptance asserts.
They retain primary/fresh outcomes, original/fresh windows, actual cut and I/O
counts, health/status, exact boundary/history, A, raw catalogue and payload
mapping. Diagnostic failures stay separate. The original Manifest/OLD/source
frame journal is preserved. Event-record observations are not ordinary SQL or
strong-read Serving proof. New report names need explicit inspection before
adding them to a public evidence whitelist; raw DB/key custody stays private.

There is no archived-engine open, source-process reconstruction, health reset,
row reissue, catalogue adoption, NEW, decision, Open, Ready or Serving grant.
Same-open native outcomes are not cold, physical power-loss, RPO/RTO or
production qualification. Source retirement, cap-eight source/OLD lifetime
gates, full 64-range 4→5 convergence and sustained throughput gain remain open.
