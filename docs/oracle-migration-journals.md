# Migration journal publication

Importer phases and their verification tools publish JSON progress through the
shared `save_report` helper. Each publication creates an exclusive, uniquely
named staging file beside its destination, writes the complete JSON, flushes and
fsyncs that file, and then replaces the destination. Staging on the same
filesystem allows atomic replacement: readers see the prior or new complete
report. On POSIX, newly published files use owner-only permissions, even with a
permissive umask.

Existing siblings such as `progress.json.tmp`, including links to other inputs,
are left untouched. A caught serialization or replacement error removes that
attempt's staging file and preserves the prior destination. Errors are propagated
to the caller. Abrupt process/host termination and filesystem power-loss behavior
are separate from these exception-path checks; publication does not fsync the
parent directory or establish full crash-durability qualification.

Use one migration process per journal, retain the pre-upgrade backup, and follow
each phase's quiescence and resume requirements. Atomic replacement does not
provide a writer lease or certify successful migration. Inspect phase identity,
completed coverage and success flags, then run source-value, relationship,
application and recovery acceptance independently.

Run the focused publication checks without a database:

```sh
python3 -m unittest discover -s examples/oracle/idempiere \
  -p test_report_publication.py -v
```
