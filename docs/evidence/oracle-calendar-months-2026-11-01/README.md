# Gregorian ADD_MONTHS semantics

The preview implements a general Gregorian ADD_MONTHS operation. It truncates
month counts toward zero using exact numeric arithmetic, preserves the
whole-second time, and returns DATE. A source month-end maps to the target
month-end. Other source days clamp only when the target month is shorter.
NULL arguments return NULL. This differs from PostgreSQL interval addition,
which preserves fractional seconds and does not carry a February month-end to
the end of March.

Unqualified builtin calls are lowered through the explicit preview front end.
Qualified user-function names are preserved. Default PostgreSQL retains native
interval arithmetic and user functions named add_months. Independent engine
regressions protect these behaviors; profile-specific prepared driver calls
also exercise result metadata and database reopen. Version-3 routines retain
logical DATE results through subsequent arithmetic. Earlier routine compiler
versions are not silently reinterpreted.

The prepared regression exposed a native PL/pgSQL gap: bound evaluation lost
declared temporal parameter types before timestamp-plus-month-interval
arithmetic. Native PL/pgSQL routines now carry declared parameter, local-variable and
alias types into bound temporal operators, which reuse the existing native
operator implementations. DATE combined with INTERVAL promotes to a midnight
timestamp. Regressions also cover local initializers, NULLs, timestamp
differences, integer DATE offsets and anonymous blocks. The interpreter uses
declared types rather than guessing temporal types from string values.

The [public reference fixture](../../../examples/oracle/reference/routine_calendar.py)
uses independently specified expectations for scalar calls and generated stored
functions. It covers leap and non-leap February, century rules, month-end and
ordinary days, positive and negative offsets, fractional input seconds, NULLs,
text month counts and exact counts immediately below integer boundaries. The
runner records and requires the Oracle reference connection's Gregorian NLS
calendar. It creates and removes only its own generated functions.

The implementation bounds results to the preview's documented finite DATE
range. Alternate calendars, timezone conversions, BC boundary conformance and
Oracle-specific error codes remain unqualified. These development checks do
not establish full PL/SQL or application workflow acceptance.

The final [calendar comparison](calendar-comparison.json) matches all 36
scalar/stored-function cases against Oracle Free 23.26.3.0.0. The fixture's
expected values were also [independently checked on Oracle](oracle-fixture-validation.json).
The existing [29 temporal](temporal-recheck.json) and [24 namespace](namespace-recheck.json)
cases also pass under the same binary. No differences are ignored. Generated
objects were removed from both databases.

The [original payment-term function](application-source-recompile.json) was
recompiled from its unchanged source body, with only its destination namespace
specified. It matches the [five seeded terms plus an absent id](application-baseline.json).
Both [forced fixed-month branches](application-fixed-month.json) now match
Oracle, including the cutoff that moves a payment into the following month.
Input-row changes were rolled back and verified in both databases. Existing
GETDATE/CHARAT still execute; the read-only [constraint recheck](constraint-recheck.json)
confirms the previously qualified nullable UNIQUE policies and source values.
These function checks do not qualify the application or its business workflows.

The [previous runtime check](older-reader.json) uses a separate database. The
new runtime returns the expected month-end date; the previous runtime reports
SQLSTATE 0A000 when executing the calendar function. The application database
was never opened by the older binary. This is an execution capability check,
not a new durable database-format version.

Validation passed: 367 SQL unit tests (three existing ignores), 417 SQL
integration tests, 117 pgwire unit tests, nineteen Oracle-profile driver tests,
168 protocol tests and thirteen reference-harness tests. Formatting, licensing
and open-core boundary checks passed. The initial prepared-call regression
exposed the native temporal type loss described above; its failing behavior
was fixed and the original regression retained.

The development binary SHA-256 is
`3dc32c8ef3f99208106bade7a3dba914829d79f0c363e70263d48d019788c341`;
its target is `debug/examples/oracle_server`. The isolated development fixture
was upgraded to this binary. No published release or production deployment
is claimed.
