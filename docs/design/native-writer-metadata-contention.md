# Writer DATA policy: bounded original-OWN metadata contention

This successor addresses the separate immediate MStore `try_lock` refusal in
source-isolated a591 run 43561072006 (79 passed/eight failed). It is not the seven
CLOSED-domain historical self-refusals, a diagnosis of which thread held the
original lock, or a reclassification of the failed run.

The policy's actual MStore acquisition now waits in at most 1,024 local 5ms
`try_lock_for` slices. Before every slice and after obtaining the actual mutex it
checks the SAME already-issued OWN and original caller. The actual Store/member,
listener DB, and subsequent native point/version checks remain mandatory. The
post-engine MStore check uses the same bounded helper and the retained policy's
original authority. A slice bound exhaustion is an explicit refusal. No RPC,
metadata proof reacquisition, TTL extension, new logical transaction, native
attempt replacement, or engine/service/journal guard is introduced in this wait.
Actual request expiry can end the wait earlier; OS scheduling is not a measured
5ms or RTO guarantee.

Two added genuine fifth-fixture cases exercise the actual INITIAL survivor's
MStore mutex after real OWN acquisition. A separate thread holds that exact
metadata owner. One control releases it only after an actual failed lock slice
and requires the original OWN call to progress. The second keeps the mutex held
until the actual issued OWN deadline passes while the original 5s request and
process remain live; only the exact OWN-expiry or deadline error is accepted.
The failed call must not expose a verified CLOSED fact. The test then makes a
separate explicit fresh request for the same retained transition. Worker cleanup
releases/joins only after the original call has returned. All test cut callbacks
run on the waiter without a service/DB/journal/MStore guard, and production code
contains no callback.

Selectors:

- `native_registered_fifth_remote_hot_closed_actual_mstore_contention_same_own_progress`
- `native_registered_fifth_remote_hot_closed_actual_mstore_wait_expired_own_refuses`

These controls and the accompanying source are not compiled or executed yet.
They preserve original selectors, budgets and assertions. No physical failover,
Serving, throughput or current-main qualification follows from source review.
