# PostgreSQL session timezone status

The server now reports `TimeZone=UTC` during startup and reports subsequent
session timezone changes before result metadata and rows. The same status path
tracks transaction-local changes, rollback and resets in simple and extended
protocol execution. Ordinary PostgreSQL and Oracle-preview profiles have
independent coverage within the regression fixture.

A default-profile reproduction required no application tables:
`SELECT NULL::timestamp` succeeded, while `SELECT NULL::timestamptz` terminated
the installed psycopg C client with SIGSEGV. The old connection had no `TimeZone`
parameter. With the patched server both queries return NULL normally and the
connection reports UTC. A wire regression failed on the missing startup status
before implementation, then passed with the fix. These results concern the
recorded client/server versions, not all driver releases.

The gap was found by full-table fingerprinting in the public BLOB importer.
A pure-Python psycopg control completed preflight, conversion and resume of all
16 source BLOB columns with complete affected-table fingerprints preserved.
That control was diagnostic; default C-driver acceptance is recorded separately.
The importer also refuses a missing TimeZone status before catalog/row decoding,
so an older target is rejected instead of reaching the known client crash.

This checkpoint establishes the recorded protocol status and driver boundaries.
It does not qualify every startup option, timezone region, Oracle NLS behavior,
whole application, crash-recovery path or production deployment. The accepted
application retains its previous binary until a separate runtime acceptance.


On the combined source candidate, all 181 general protocol tests, 48 Oracle
protocol tests and 11 JSON regression tests passed. Workspace/all-target checking
and the example server build passed. The public importer then passed 41 tests
with zero skips using the C driver, followed by full-snapshot preflight, apply
and resume for all 16 BLOB columns with all affected target table fingerprints
preserved. The owned database shut down successfully. The validation record
identifies exact source/binary hashes and separates the earlier diagnostic control.
