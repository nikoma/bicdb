# Logical DATE arithmetic in stored functions

The version-3 preview compiler retains Oracle DATE declarations separately from
physical timestamp storage. DATE arguments, assignments, casts and returns
discard fractional seconds. Explicit TIMESTAMP(0) rounds instead. DATE plus or
minus NUMBER returns DATE; DATE minus DATE returns exact NUMBER day fractions.
Explicit TIMESTAMP differences and known mixed DATE/TIMESTAMP differences return
intervals. Cursor numeric fields come from metadata inference, which does not
execute the query. Schema changes invalidate cached type resolution.

These are general type rules. Application, table and routine names do not
select behavior. Default PostgreSQL routines retain native casts, arithmetic
and metadata; the older preview compiler versions retain their previous rules.
The [public suite](../../../examples/oracle/reference/routine_temporal.py)
creates independent synthetic functions, with explicit expected values checked
against Oracle and BicDB. It includes leap-day boundaries, NULLs, exact day
fractions, second-fraction loss, TIMESTAMP(0) rounding, positive/negative
intervals, literals and wildcard cursor fields. The
[Oracle type matrix](oracle-type-matrix.json) separately records the source
database's types and values.

The first live run matched 26 of 29 values. All three interval cases failed
text driver decoding because the PostgreSQL handshake omitted IntervalStyle.
Binary decoding returned the correct interval but was not used as a workaround.
The wire server now advertises its default and reports effective setting
changes before results. Protocol regressions exercise startup, SET, SET LOCAL,
rollback, RESET and extended execution. Additional client checks exposed stale temporal settings within statement
batches and an unhandled parsed RESET statement. Each parsed statement now
uses the effective temporal settings and RESET follows the existing session
reset rules. Independent SQL and driver regressions cover these cases in
PostgreSQL and preview databases.

Logical DATE/TIMESTAMP provenance of physical timestamp columns and temporal
%TYPE anchors is unresolved. Operations requiring unknown provenance fail
explicitly. Timezone arithmetic, timestamps beyond six fractional digits,
calendar functions such as ADD_MONTHS, full record typing, general overload
resolution and Oracle AUTHID rights remain unqualified. Version-3 procedures
and dynamic SQL namespace binding remain unsupported. Routine names still
have the documented global normalized-key limitation.

A development example server and isolated fixture are used for these checks.
This is not a release, production deployment, full PL/SQL qualification or
successful application business workflow.

The final [temporal comparison](temporal-comparison.json) matches all 29 cases
with Oracle Free 23.26.3.0.0 through text driver decoding. All 24 existing
[namespace cases](namespace-comparison.json) also match under compiler version
3. Generated objects were removed from both databases. The
[wire-format check](interval-style.json) verifies that effective settings and
actual interval text agree in a multi-statement message.

The [older-reader check](older-reader.json) stores a version-3 function in a
separate database and verifies that the previous version-2 runtime rejects
execution with SQLSTATE 0A000. The application database was never opened by the
older binary.

The [original application function baseline](application-baseline.json) matches
all five seeded payment terms plus an absent id, using the original
PAYMENTTERMDUEDATE definition with only an explicit destination namespace. This
is partial acceptance. A [forced fixed-month branch](application-calendar-gap.json)
still fails while Oracle returns its due date. The test changes existing input
rows inside transactions and verifies rollback in both databases. ADD_MONTHS
remains the next calendar semantic dependency; the function is not fully
qualified. Existing GETDATE/CHARAT still execute, and a read-only
[constraint check](constraint-recheck.json) confirms the three previously
qualified nullable UNIQUE policies and source values after restart.

Validation passed: 363 SQL unit tests (three existing ignores), 417 SQL
integration tests, 117 pgwire unit tests, eighteen Oracle-profile driver tests,
168 protocol tests, fourteen Python migration-tool tests and eleven
reference-harness tests. Formatting, licensing and open-core boundary checks
passed. The development binary SHA-256 is
`ba8f21e172d3229821327be7f6d476de102488871ffb96083383f152db46bb16`;
its build target is `debug/examples/oracle_server`. The isolated development
fixture was upgraded to this binary. No published release or production
deployment is claimed.
