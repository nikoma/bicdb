# Sequence allocation while shared sessions wait on patient locks

WalkNorth's synchronization trigger holds a per-patient transaction advisory
lock before allocating an identity cursor. Another transaction can wait for that
lock inside a stored function while retaining shared database access. Previously
the lock owner needed the global writer guard to save its sequence; the waiter
prevented that escalation and the owner timed out. Removing the patient lock
would weaken cursor ordering and is not the repair.

Sequence mutation now uses a short independent core transaction through shared
database access. A read-committed stored reservation serializes that one sequence
row, preserving the observed version and latest sequence attributes. A durable
commit advances statement/session visibility without committing the outer
business transaction. `nextval`, bulk default allocation and `setval` share the
path. Bounds failures preserve the existing partial-allocation behavior;
permissions are checked before reservation. Describe does not persist changes.

The SQL regression allocates 200 values across eight shared sessions, rolls back
every outer transaction, checks uniqueness and verifies the next value after
reopen. It also exercises rolled-back bulk inserts and `setval`. The wire test
uses a 500 ms writer timeout and a second transaction waiting **inside** a stored
function for the first transaction's advisory lock. Both direct allocation and
bulk identity defaults must progress and release the waiter.

On engine base 13e36c7, removing the repair makes both shared-session tests fail
and the nested wire test fail. Restoring it passes all three. The source hashes
and linked artifacts are recorded in validation.json. Earlier tests on 9e1156c
also passed 717 SQL unit tests, the focused suite and 182 protocol tests; those
are intermediate results, not proof for the later base. Subsequent upstream
commits incorporated before this commit changed documentation only.

The final batch on the later engine base passes 717 SQL unit tests (four
intentionally ignored), 122 focused compatibility tests, 183 wire-protocol tests
and the three new sequence regressions. Attached logs omit Cargo's repetitive
compiler artifact/message JSON; validation.json retains full-log hashes, exact
commands and linked-artifact hashes. All negative controls compiled successfully
and completed with failing assertions before the repair was restored.

One attempt on the later base was interrupted by the task's free-space guard
(exit 99) while compiling the negative wire test. It is not a test failure or
passing gate. Superseded task-owned SQL libraries and interrupted objects were
retired, dependency caches and frozen native candidates retained, and the full
batch restarted. No guard or assertion was weakened.

An initial diagnostic used a top-level advisory-lock call, which the wire layer
handles outside the shared guard; it passed on the old engine and was not a
causal regression. Only the nested stored-function version above is retained
as the wire negative control. An early allocation implementation used a regular
update read and encountered 40001 under contention; the final stored-reservation
path passes the eight-session test without application retries.

This is engine regression evidence. The EHR's native hosted workflows, load,
background-job completion and production deployment must be rerun against a
new immutable unified CLI/supervisor build. Production EHR remains on PostgreSQL.
