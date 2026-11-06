# Workflow savepoint recovery investigation

The original invoice diagnostic produced four `Update return 0` log messages
while its credit-stop case passed. Inspection of the unmodified upstream
`MWFActivity.java` identifies explicit recovery for this situation:

- `run`, around lines 1001–1006, rolls back the shared workflow savepoint after
  an exception.
- `setWFState`, around lines 311–317, handles a failed activity update by clearing
  its identifier and inserting the object again.
- `updateEventAudit`, around lines 373–379, performs the same recovery for the
  audit object. Both methods explain that savepoint rollback can remove the row.

In the retained private SQL trace, connection 6 creates savepoint 15 before
inserting the later activity and audit rows. It rolls back to that savepoint,
attempts the activity update, rolls back its object savepoint, and inserts an
activity again. It then does the corresponding update/rollback/insert for the
audit object. The four Java messages are two messages for each object, rather
than four distinct object updates in this part of the SQL trace.

This chronology is consistent with the application's explicit recovery path.
It is not evidence of a transaction bug by itself. The trace redacts keys and
values, and its `rows` field counts returned rows, **not affected DML rows**.
It cannot prove identifier equality or final audit contents. No warning was
suppressed and no application-specific database behavior was introduced.

The new [native regression test](../../../examples/oracle/idempiere/test_savepoint_recovery_native.py)
checks the underlying rule independently through PostgreSQL transport in both
`oracle-preview-v1` and `postgres` profiles. A rollback removes inserted parent
and child rows and restores an earlier update while retaining a change made
before the savepoint. Updating the removed rows returns exactly zero; reinserting
replacement rows returns one. Committing and restarting the native server
preserves the replacement rows and their foreign-key relationship. Both profile
tests passed against the candidate built from b984572; no engine change or new
binary was needed.

Run with a built compatibility-development server:

```sh
BICDB_ORACLE_SERVER=/path/to/oracle_server python3 -m unittest discover \
  -s examples/oracle/idempiere -p test_savepoint_recovery_native.py -v
```

The [validation record](validation-results.json) pins the inspected artifacts and
test scope. This closes the unexplained-warning investigation at the source and
transaction-rule level. It does not qualify complete workflow audit contents,
an original Oracle-backed application comparison, or whole-application recovery.
The original invoice class still needs repetition after complete source foreign
key installation; runtime provider packaging is also a separate open gate.
