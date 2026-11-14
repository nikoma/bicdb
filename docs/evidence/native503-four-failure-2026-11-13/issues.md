# Reproducible failures and smallest next controls

All four cases use the exact source/checkout trees and serial command retained
in the packet. Reproduce using each full test name in `case-boundaries.json`,
with the original configuration, two-second APP limit and original authority
and preparation deadlines. A narrower reproduction is a new experiment; it
must not overwrite this failed required-suite result.

## 1. Fresh metadata acquisition after actual normal recovery

**Failed:** `normal_serving_record_tests.rs:112`, before the current-damage
callback was installed. The aggregate error reports fresh joint quorum
unavailable within bounded probe attempts. Actual per-voter errors were
discarded, so transport, refusal and request-slice causes cannot be separated.
Native and catalogue completion/activation boundaries are present in the raw
trace. Three internal fixture commits are source-control-flow evidence;
neither their ledger bytes nor external client acknowledgments are retained.

Smallest next test: preserve bounded per-voter probe intents, identities,
contexts, terms, read indices, remaining original budget and exact error chains
at the existing failed acquisition. Require that a genuine aggregate failure
retains the actual errors and count, without extending deadlines or inventing
an all-peer outage. After proving that prerequisite, separately reach the real
damage callback and original refusal assertions. **Cause inconclusive;
damage control not tested in this run.**

## 2. CONFIG prerequisite before finite-open worker control

**Failed:** shared `initial_data_tests.rs:132` at
`prepare_common_initial_configuration`, with an actual RPC deadline error.
The initial configuration observation succeeded. The failing host/recipient
ordinal and native preparation stage were not recorded; some native effects
may have occurred. No APP call was reached.

Smallest next test: passively retain host and recipient identities, original
Role/OWN contexts, exact stage before/after each existing preparation call and
its remaining budget. A deterministic recipient-stage deadline control must
show original pending state retained and no APP/Strong grant. Do not warm up
Strong or renew the old preparation window. **Cause inconclusive; hot-worker
control not tested in this run.**

## 3. CONFIG prerequisite before actual Store ABA control

**Failed:** the same shared source line and error text as case 2, before DATA
opening, APP/Strong, reservation acquisition and real Store replacement.
Matching error strings are not proof of a common root cause.

Smallest next test: use the same bounded preparation diagnostics as case 2,
then independently prove the genuine DATA/held-reservation prerequisite before
replacing and restoring the actual Store. Keep the original slot-revocation
and preservation assertions. **Cause inconclusive; slot ABA control not tested
in this run.**

## 4. Mode-linked APP uncertainty before lifecycle drop/reopen

**Failed:** `initial_data_tests.rs:339`, first APP of an unrecorded mode 0, 1
or 2, with `QueryTimedOut`. Current metadata/control setup, all three DATA
opens and original SDK construction returned success. The current APP has no
returned acknowledgment; its durable effect is unresolved. Earlier modes may
have completed, producing zero to two prior returned APP acknowledgments.
Strong, materialization checks, current-mode fault/drop and cold same-root
readback were not reached in this failing iteration.

Smallest next test: before any existing operation, retain the actual mode,
logical operation ID, key, full expected payload/hash and exact request budget.
Use the existing original SDK transport observer to retain primary and refresh
attempts and complete errors. Preserve the API reply/uncertainty before its
original assertion; separately reconcile any timeout before teardown. No
Strong warmup, manual route refresh, inserted retry or fabricated readiness.
Keep previous modes' actual ACKs even when later calls fail. **Cause
inconclusive; complete three-mode lifecycle remains unqualified.**

## Shared limits

Host pressure and actual worker/context counts can be additional passive
observations, but do not alone prove timing or resource causation. These
source-reviewed controls remain proposals, not executed fixes or passed gates.
No production data loss is established or excluded, and no physical scaling
or HA qualification follows from the 208 other passing tests.
