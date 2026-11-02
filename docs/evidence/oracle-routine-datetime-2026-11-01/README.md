# Declared datetime types in preview routines

New Oracle preview function definitions persist the explicit language marker
`bicdb_oracle_preview_v1`. Definition storage and routine resolution require the
durable Oracle preview database profile, rather than a mutable connection
setting. The language has its own pg_language identity only in that profile.
Native backend LANGUAGE plpgsql definitions retain their previous semantics;
existing translated definitions are not silently reinterpreted.

The preview compiler resolves procedural TRUNC using declared DATE/timestamp
parameters and locals before binding expressions. Initializers, assignments,
control conditions, nested calls, returns and exception-handler expressions use
the same traversal. Numeric TRUNC remains native. Embedded SQL and scalar
subqueries retain their own column namespace: a same-named numeric column is
not assigned the type of a datetime routine parameter.

The [reference comparison](reference-comparison.json) records sixteen values
matched with Oracle Free 23.26.3.0.0. Synthetic functions exercise day/month/year
truncation, local initialization and assignment, nested calls, repeated inputs,
leap days, fractional timestamps, NULL datetimes and numeric values. The first
comparison exposed an existing numeric TRUNC(NULL) error. The shared one-argument
numeric implementation now returns NULL; the rerun includes that case. Native
numeric and floating NULL regressions also pass. Temporary comparison functions
were removed from both databases.

The [older-reader check](older-reader.json) uses a separate database. The new
runtime stores and executes a function, then the previous runtime opens that
database and refuses to execute the unknown language with SQLSTATE 0A000.
This is execution rejection, not an incompatible database-open claim. The
application database was never opened by the older reader.

The [original iDempiere probe](application-probe.json) retains the original source
hash and uses only explicit destination qualification of the function name.
PAYMENTTERMDUEDATE now completes its datetime initialization and reaches the
cursor query. Execution fails with SQLSTATE 42P01 on the unqualified
C_PAYMENTTERM relation. The report records the Oracle result for that input,
not a matching BicDB result. The failed probe was removed, its absence checked,
and the existing GETDATE/CHARAT functions still execute. The three source
nullable UNIQUE constraints and their values passed read-only verification
after the development fixture restarted.

Routine namespace binding, general embedded SQL overload resolution,
record-field typing, source DATE/TIMESTAMP provenance and Oracle date arithmetic
remain separate requirements. This is not complete PL/SQL, full schema import
or application/workflow acceptance.

Validation passed: 355 SQL unit tests (three ignored), 416 SQL integration tests,
117 pgwire unit tests, fifteen Oracle-profile driver tests, 167 protocol tests
and fourteen Python migration-tool tests. Prepared datetime arguments alternate
after database reopen with timestamp result metadata. Formatting, license and
open-core boundary checks passed. The development example server was rebuilt
and the isolated fixture upgraded; no release or production deployment occurred.
