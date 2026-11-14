# Direct consumption of an actual NEW collection

Source base: `4efcfc2cccc51307721777ad7d53a251e67bbbf3` (1.0.512-beta).
This successor has source checks only. It has not been compiled or executed.

The failed required Native suite, run `43590137719`, job `132432945598`,
reported 307 passed, 23 failed and one ignored. Seventeen failures stopped at
`hot_policy_tests.rs:483`, before decision publication. Their full-stream
`NATIVE_DECISION_PREPARATION_REFUSAL` records identify the original Registered
target `n4`, at `member_live_issuance`. Its Role and OWN were live on entry to
`prepare_decision`, then expired during the initial complete currentness sweep.
The outer request was still live. These records establish expiry at that
boundary; they do not identify a CPU, disk or scheduling cause.

For those seventeen records, the original target Role had 86,807–181,554
microseconds remaining at preparation entry. The initial currentness sweep took
195,131–204,383 microseconds. The original Role was 33,513–128,731 microseconds
overdue at refusal. An actual MStore point was retained, but its currentness was
not established by the failed issuance check: the Role check precedes OWN and
MStore validation. The intended later fault controls were not reached.

All three immediate callers previously obtained a fully checked materialized
quorum with `round.finish()` and immediately consumed it with
`quorums.prepare_decision()`, which repeats the complete currentness check.
The new consuming `NativeJointNewRound::prepare_decision` validates the same
majority, intended leader and target membership conditions, then transfers the
same actual observations directly into the original preparation implementation.
No unchecked materialized quorum escapes this method. The strict standalone
`finish` API retains its checks, with only its pure membership predicate factored
into the shared private helper.

The complete `NativeJointMaterializedQuorums::prepare_decision` body is unchanged:
initial actual OLD and NEW currentness, every original live issuance, the exact
decision body, capture-pin drain and final complete currentness check all remain.
There is no refreshed Role, renewed OWN, altered cancellation ancestry, additional
attempt, cached currentness claim or longer deadline. The product driver and the
two existing immediate fixture callers use this same consuming producer.

Two additional genuine Registered fixture controls act after actual NEW records
have been collected and before direct consumption. One crosses both original
Role and OWN deadlines while the outer request remains live; the other cancels
the actual original caller. They require the real cut to occur, the exact
corresponding refusal, the original host's same A, and an unchanged not-started
native decision attempt. Evidence is written after the real call and before
assertions. Copied deadlines and context identifiers are diagnostic DATA only.
These controls have not run.

This removes one redundant validation of an intermediate capability that the
caller never used. It is not evidence that all seventeen failures are fixed,
that a later collection fits its earliest deadline, or that independent range
operations progress during stalled I/O. All original failures, budgets,
application assertions and production qualification gates remain authoritative.
