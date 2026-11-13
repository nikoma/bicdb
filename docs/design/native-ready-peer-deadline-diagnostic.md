# Test-only correlated Ready/Completion observer timing

This unexecuted diagnostic successor starts from exact `432fc9bc5e9f681bb40e3bfe6b07fd043bcdf061`. Its [failed receipt](native-current-joint-432fc-closed-ready-timeout.md) remains unchanged: both heavy tests stopped at third Complete's closed-Ready collection before CurrentJoint activation. This is instrumentation, not a cost fix, new authority producer or passing recovery claim.

Every new helper, clock, context, map, extra job field and trace call is cfg(test). Ordinary evaluation is preserved where an existing Result must be named for logging: each witness/status/record/reservation/projection is still evaluated once, its old error propagation or intentional local-error ignore is unchanged, and no new validation is substituted. Original cancellation checks, deadline derivations, half-budget selection, remaining/(waves+2), max4 workers, max2 attempts and5ms polling remain identical. No network/native read/write/journal acquisition, sleep, retry or changed TTL is added.

## Correlation and clocks

`NATIVE_PROBE_TRACE` carries process ID, monotonic microseconds from a process-local anchor, phase (`ready` or `completion-status`), actual node, original request challenge/session/range/epoch/configuration hash, actual token deadline coordinate and remaining time. Queue records include the real child slice, transmitted request budget, attempt, parent remaining and collector original-parent context; workers include actual queue delay and parent deadline coordinates. No logged scalar is consumed as authority or used to construct a deadline. Different process clocks cannot be compared directly; the loopback TLS fixtures share one process. These are API call/handler clocks, not packet capture or proof of TCP arrival time.

The sender's transport-call event occurs only after its existing original child check. Worker return, collector receipt and record events retain every actual peer error rather than only last_error. Synchronous local status logs both success and the previously swallowed error, plus any record error. Completion selection logs its actual returned kind (Constraint/Quorum/Decision) without changing finish_selection or reinterpreting a Constraint as a successful Complete.

Each readonly receiver logs authenticated-dispatch entry, real observer-Operation reservation/error, handler entry, fresh readonly witness/context, actual role/request deadline, service acquisition, checked nonwaiting DB acquisition, full native-domain/physical/catalogue/binding validation and final observation sealing/error. Receiver operation tracing holds only a Weak diagnostic reference, never upgrades it and is declared BEFORE the original Arc owner, so its exit reports the actual strong count AFTER the original owner drops. It cannot prolong the operation or count as a capacity/readiness grant. Receiver handler and Operation exits are distinct from sender-worker/drain events; overlapping requests can be correlated by exact challenge and shared session/node.

A private !Send RAII thread-local context carries only copied diagnostic strings/UUIDs/Instant. It restores its predecessor on every error/panic, releases RefCell borrows before logging/nested work, retains no token, role, native receipt or Store/DB/freeze and never crosses a worker thread. Deep stages aggregate CurrentJoint domain, actual owned physical proof, catalogue identity, full binding and the existing serving-reader repeated physical/current-lineage/admission/receipt checks. They do not emit per namespace, genesis entry, history row or user record. NativeServing and standalone observer checks remain full and unchanged.

## Intended interpretation

| Observed timing | Supported distinction |
|---|---|
| Queue→worker start | Actual sender scheduling delay |
| Transport call→receiver entry (same PID) | Dispatch/TLS interval; not a pure network measurement |
| Receiver readonly-role duration | Actual independent metadata-witness cost |
| Service acquisition wait | Time in the existing bounded service-latch loop |
| Checked DB acquisition error/duration | Existing nonwaiting DB refusal/acquisition |
| Native-domain/physical/binding subspans | Existing validation cost; nested totals overlap |
| Receiver Operation exit after sender timeout/drain | Actual handler/owner overlap if observed, not inferred |
| Child deadline vs collector parent deadline | Actual allocation and remaining budget at that attempt |
| Collector record error | Actual proof rejection, distinct from missing transport response |

A span's completed=true means that wrapped statement returned Ok; it does not itself establish fresh authority, a returned Complete, persisted durability, Ready or quorum. Full protocol validators and original tokens still determine those outcomes. A receiver may finish expensive readonly work after a sender times out; trace ordering must not turn that into a successful reply. Incomplete/error spans and all original failures are retained.

Instrumentation formatting/timing adds overhead and can perturb tight budgets. Use correlated intervals and counts rather than adding overlapping totals, and distinguish evidence from inference. The unchanged original full positive remains the measured scenario. The separate genuine expired-delivery test establishes refusal/counting behavior only; it cannot replace that positive or explain432fc validation cost. No production optimization, observer cast, per-range isolation, operator HA, physical RPO or Azure claim is made by this source.
