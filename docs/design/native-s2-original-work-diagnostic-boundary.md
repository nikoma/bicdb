# S2 original-work diagnostic boundary

The original focused run `43550673003` at qualification
`3d7b6a74340c7068136f9f00b98573ad8e5df349`, runtime
`913bd4b4427711d542504d25d8d94c085a17a4c4`, remains **failed: 14 passed,
1 failed, 0 ignored, 223.38 seconds**. Compiler inventories matched exactly
15 selected S2 cases and all 97 native cases. The broader 97 were not executed.
Workspace all-targets, the workflow consumer without Core defaults, helper,
formatting, licensing and dependency-boundary checks passed on that qualifier.

The original raw Core log is 5,827,566 bytes, SHA-256
`1ecccbaecc02487d1ec8ab304a9df319ef2c1136d0549f1127e36cdcb4db97e2`.
Original full artifact `14096837323` is 7,006,805 bytes, SHA-256
`7bbdcb64911cb71c6923a64e9fd50dddcd2be00956d67b04d5a627abea5981d7`.
It includes generated fixture databases/TLS material and stays outside this
document. Local private custody does not imply owner-only confidentiality for
the original GitHub-hosted public-repository artifact.

Independent reconciliation retained all 30 source fixture APP acknowledgments,
separate from internal native metadata transactions. Three positive two-hop
cases reconciled the same MF/A/session and terminal outcome, removed only the
export capture/reference, retained the shared OLD pin/freeze and target pin,
and refused subsequent exporter frames. Both distinct callback controls reached
the actual guarded check with their other original scopes live. This remains
component evidence, not Serving, independent-machine or scale-out qualification.

## Original failure and limits

`native_registered_fifth_source_export_retirement_actual_original_own_expires_at_final_detach`
returned the actual client RPC error:

```text
Io(Custom { kind: TimedOut, error: "cluster RPC deadline expired" })
```

The fixture immediately attempted a source-memory snapshot and panicked on
`range image: discovery source busy`. Its intended after-state assertions were
unreached. The cut report proves a live OWN window of 1,275,566 microseconds
followed by that actual OWN deadline expiring. It does not capture final pin,
capture or terminal-publication state for this operation.

The relevant scopes differ:

| Scope | Retained configuration/observation |
| --- | --- |
| Transport I/O timeout | 1,500 ms |
| Retire request DATA budget | 4,000 ms |
| Fixture client parent | 5,000 ms |
| Actual server parent | Created at accept time with the I/O timeout |
| Final OWN | Capped by that actual server parent |

The old cut field `original_parent_live` referred to the captured **client**
parent. It did not prove server-parent liveness. OWN expiry in this scenario
may coincide with the original server request deadline; it is not an isolated
OWN-only refusal proof.

Cleanup recorded no errors. The actual cleanup path waits for the listeners'
active-connection counters to reach zero, so recipient workers drained by
teardown. That is not a tagged original-Retire completion timestamp or a
post-expiry pin-state observation. A generic broken-pipe log is likewise not a
request-tagged completion event. The original case's invariant poststate remains
**inconclusive**; no durable-data-loss, persistent-deadlock or controller-error
claim follows from that diagnostic panic.

## Test-only successor

The successor changes no production behavior or business deadlines. It retains
the original client RPC result in a fixed report **before** source/target state
diagnostics. Client-parent fields are explicitly labeled as client fields.

A private test observation records entry to the actual original recipient
function, binding exact request bytes/challenge, MF, A and source session. Its
RAII guard is the first function local and drops last, after source/session and
discovery-claim guards. The completion probe uses atomics only, without locks,
callbacks, serialization or I/O. It separately records the original server
parent's deadline/cancellation at completion. It is observation DATA and exposes
no token, Role, source, pin or admission handle to the fixture.

Only the failed OWN-expiry case waits for that exact original work to finish,
using a separate bounded two-second **diagnostic-only** scope. It does not
resubmit Retire, acquire new Role/OWN or renew any original token. The diagnostic
wait polls the tagged completion event, not an arbitrary sleep. Missing entry,
unfinished work or observation contention remains recorded and causes failure.
After completion, source state must be captured successfully: persistent Busy
is recorded and fails, never accepted as proof of safety.

The original test names, actual expiry cut and native/pin/state assertions
remain. New source/runtime validation has not run at this document's freeze.
The next focused check must confirm entry, completion after original guards
drop, actual cut expiry and the resulting no-detach/no-terminal state. All
remote decision/Open/OLD retirement, Serving, cold recovery, registry-cap and
physical capacity gates remain independent and open.
