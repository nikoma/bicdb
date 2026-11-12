# Exact local joint promise versus immutable plan request

This correction follows the actual first-fence failure at source `71af097e`:
run 43245592794, Core job 131277264375, 0 passed / 1 failed in 60.04s;
compilation passed. n1 refused before any new recovery fence or NoData action.
The genuine all-member release, target ACK2 and Strong R2 prerequisite passed.
This new source has not been compiled or executed locally.

## Retained evidence and exact difference

The original artifact 13958235525 is retained as
`71a-current-joint-positive-artifact13958235525.zip`; SHA-256:
`5825e57f2aa0569f8ab11854a5c61ff25eb9d39e7b8b1e45702390a53b6039e2`.
Root verified its 81 files, 5,501,667 bytes. A separate read-only comparison is
`71a-current-joint-promise-comparison.json` in the private evidence directory.
No retained engine root was opened.

On n1/n2 the exact durable local promise has `resolved_through = 1`. The complete
protected joint decision retains the original request with that field zero.
Every request identity/configuration field matches: session, nonce, plan,
relocation, range, old/activation epochs and both configuration hashes. The
actual selected joint materialized cut is 1. n3 has genuinely no old promise
and no local old-close pointer.

The n1 WAL bytes corroborate both representations: the old-close promise at
byte offset 705766 has local cut 1; the complete decision's scope promise at
742246 has request cut 0. These are JSON fragments read from the original
archived `n1/paged/store.wal`; this is **not independent WAL replay or page
validation**. Runtime producers/readers must verify actual protected records.

The source explains the difference: `NativeJointScopeData::from_actual_round`
clones the original request. `persist_native_joint_old_promise` derives the
LOCAL cut from the actual physical proof before durable promise publication.
`RangeHandoffPromise::same_request` deliberately excludes only that observed
local cut. Exact equality between those different lifecycle facts was wrong.

## Narrow protected proof

The new reader accepts matching request identity only when the actual LOCAL
old-close record and immutable archive agree in full. The record must bind the
actual local birth/incarnation, unchanged physical base/namespace identities,
history generation, schema, logical H0/H, actual local B and exact local promise.
Its scope must equal the protected global decision scope. The protected global
OLD materialization fact must match the local old-close receipt checksum,
local cut/H/B and birth. Its cut is covered by the actual global joint decision;
the corresponding immutable local native history entry must still match.

The OLD materialization fact's native-receipt SHA is the **old-close receipt
checksum**, not the earlier Applied-prefix checksum. The old-close record itself
authenticates that earlier prefix receipt. The actual catalogue old-close pointer
must match the protected current/archive receipt as well.

The exact LOCAL `Option<promise>` and old-close SHA are retained in the map16
binding, separately from the unchanged global request promise. Every later
operation revalidates them. The image target must retain real None; another
member's promise cannot be synthesized for it. A surviving OLD member cannot
lose, lower, raise or replace its original local promise. No old promise bytes,
cut, native history, rows or original global decision are rewritten.

## Controls and remaining limits

The full actual positive asserts exact LOCAL promise/closure-pointer archival,
including target None, while preserving its complete Abort3/ACK4/Strong contract.
The separate authority lane validates foreign-request, lower-floor,
raised-uncovered-floor, missing-survivor and synthetic-target CLAIM DATA against
an actual opaque native closure fact. These probes are read-only and mint no
role, quorum or mutation permission; exact native/raw/row/promise state remains
unchanged. They do not pretend to install a later handoff through a missing
controller. That actual transition remains separately closed by the existing
purpose/epoch guards.

All prior same-open/same-epoch qualifications and cold, higher-epoch,
committed-suffix adoption, minority and capacity open gates remain unchanged.
