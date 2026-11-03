# Blocking SQL row locks

A blocking `SELECT ... FOR UPDATE` previously inherited an internal retry budget
and could return a transaction conflict after about six seconds of legitimate
contention. A ten-second synthetic owner hold reproduces this in both native
and Oracle-preview profiles. Oracle and PostgreSQL 18.4 instead wait for release
and return the row.

SQL blocking locking reads now wait for release or query cancellation. They
register live row-lock dependencies to detect cycles rather than rejecting a
wait solely because of transaction age. Statement deadlines and explicit cancel
requests are checked inside the wait. A detected row cycle reports SQLSTATE
40P01; cancellation and statement timeout report 57014. READ COMMITTED continues
to refresh a row changed by its owner before returning it. Wait edges and
notification registrations are released on success and error.

Protocol metadata and result encoding also use the existing transaction-progress
read guard. A queued exclusive maintenance writer must not block Parse, Bind,
Describe or row encoding needed by a row owner to reach COMMIT. An active writer
still retains exclusive access. Ordinary query admission retains writer fairness;
metadata-only checks and read-only description fallback can make progress. The
fallback does not escalate metadata discovery to an exclusive write path.

Two deterministic regressions hold a shared reader while queuing an exclusive
writer, then exercise Parse, Bind, query/result Describe, row encoding and simple
COMMIT. Both profiles stalled at Parse before the change; both complete all
stages after it. Failure paths release the held guard before joining workers.

The SQL path uses this behavior in both profiles. Existing bounded low-level
lock APIs retain their contracts. NOWAIT and SKIP LOCKED retain their separate
nonblocking behavior. The regression checks exercise role visibility, privileges,
savepoints, joins, cancellation, rollback and alternate lock-notification settings.
No application, schema or table name selects engine behavior. Compiler version 24
and durable metadata format 5 remain unchanged.

The [source comparison fixture](../../../examples/oracle/reference/row_lock_waits.py)
uses owned synthetic tables. It compares a ten-second blocking read that returns
the newly committed value, explicit cancellation with connection recovery, and
a two-row cycle with one victim and one survivor. The PostgreSQL transport uses
prepared locking queries. Error comparisons normalize semantic categories;
complete Oracle error numbers/messages are not claimed.

The [Oracle comparisons](oracle-comparison.json) and
[PostgreSQL comparisons](postgres-comparison.json) each pass **3/3**. The retained
[number-format comparisons](retained-number-formats.json) pass **55/55** on the
same development binary. Local validation passes **316 transport tests**,
**36 focused core/SQL test executions across configurations**, and **58 Python
tests**. The focused core/SQL sources are unchanged in the final transport fix;
configuration repeats are identified in the evidence.

A longer target-only JDBC probe completed **360 reservations with twelve
workers**, verified distinct allocations and the expected final delta of 120
on each of three counters in a disposable copy of the migrated application
database. It uses PostgreSQL JDBC 42.7.8 with an updatable result set retained
through UPDATE and COMMIT. This is a bounded concurrency check, not full
application or production-performance qualification.

See [validation.json](validation.json) for exact source, binary, local test counts
and reference observations. This checkpoint does not qualify all UPDATE/DELETE
waiting behavior, metadata-lock interactions, all deadlock classes, full isolation
semantics, complete application workflows or production throughput.


The Oracle reference run selects in-band breaks with `--oracle-disable-oob`;
this uses the driver's documented
[disable_oob option](https://python-oracledb.readthedocs.io/en/latest/api_manual/connect_params.html).
Initial comparisons failed reference qualification with two- and five-second
cancellation observation windows. An independent source measurement took six
seconds on a reused connection, while a fresh-connection probe took three.
The final fixture uses a twelve-second observation window and records actual
response time. Cancellation must still finish while the owner holds the row,
and the connection must recover. These are semantic observations, not a
cancellation latency SLA. Earlier failed reference attempts remain recorded
separately from qualification.
