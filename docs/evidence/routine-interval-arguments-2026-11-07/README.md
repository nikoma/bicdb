# Routine interval arguments — source regressions verified, EHR still unqualified

Implementation: `5f3cec922cf168ba9f13cadfe44b78fc0b1593c1`.
Parent/main compiler inputs: `c13ed335d05ccfd519853009488ac97dbf91b78a`.

A real hosted monitoring diagnostic emitted 5,001 occurrences with the same
single due date. Named make_interval arguments were normalized in queries/DML
but dropped from procedural scalar calls, silently producing a zero interval.
Routine expression parsing now applies the same existing scalar-call normalizer.
No clinical rule, operation authority, time limit or identity check was relaxed.

Causal proof: `before.txt` has zero passed/two failed tests; `after.txt` has both
passed. The tests assert live days/hours inputs and a recurrence that must emit
three distinct days and stop at its time horizon. The unchanged broken engine
instead emits eleven copies of the same date up to the test safety limit.
Command: `cargo --config profile.dev.debug=0 test -p bicdb-sql --test routine_interval_arguments -- --test-threads=1`.

All source regressions passed: 763 SQL library tests (four existing ignored),
41 routine/domain integration tests across 13 targets, 188 protocol tests plus
the procedural-migration test, and 140 pgwire library tests. Formatting/diff
checks passed. The raw scope list in status.json was inherited from the prior
batch; the exact change in this commit is routines.rs and the new interval test.

The development pair's source manifest has 1,412 inputs, SHA-256
`f60a6f25e40fda1acfaa74813bc0e3e6bf94d54fe39002ee2dcb60720a3c4ea1`.
Its cold paged fixture `/tmp/walknorth-ehr-bicdb-paged-_26oecku` passed all 168
migrations, replay, restricted API identity, patient/meal operations, tenant
refusal, encryption and engine restart. The actual hosted diagnostic at
`/dev/shm/ehr-bicdb-hosted-root-20261105/ehr-bicdb-hosted-q4NGry` started all four
packages and passed web/static/anonymous refusal, meal/mobile and acknowledged
email checks. Monitoring still FAILED at its original 45-second expectation.

An owned synthetic clone reproduced SQLSTATE 0A000 in the actual monitoring job:
the INSERT SELECT computing monitoring_evaluate / monitoring_missed_results
reaches the read-only row-routine write guard. A focused transaction-identity
case also reproduces that error without any data write. Further qualification
and a separate fix are required; this is not a qualified monitoring release.
The synthetic queue clock was adjusted only on the diagnostic clone to reclaim
its interrupted job; that replay is diagnostic evidence, not release acceptance.

No production cutover, HA acceptance, real-data import/recovery acceptance or
complete EHR acceptance is claimed. Clinical records and original keys remain
on production hosts. This build host contains public/synthetic material only.
