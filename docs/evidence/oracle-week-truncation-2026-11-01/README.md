# Week truncation and procedural precision

This batch implements general Oracle preview rules, selected by the explicit
profile and routine compiler rather than application names. W anchors weeks to
the first day of a month, WW to the first day of a year, and ISO-year formats
to the Monday of the week containing January 4. D/DY/DAY use the preview's
AMERICA default, with Sunday as the first day. Alternate NLS territories and
session configuration remain unsupported.

SQL and PL/SQL have distinct builtin argument contracts. SQL TRUNC(datetime,
NULL) returns NULL. A procedural expression with a non-NULL datetime and NULL
format raises ORA-01899 on Oracle; BicDB reports SQLSTATE 22023 through the
PostgreSQL transport. A NULL datetime returns NULL in either context. Numeric
TRUNC propagates NULL, but SQL truncates fractional precision toward zero while
PL/SQL converts that precision to an integer by rounding halfway away from
zero. Both implementations preserve exact decimal coefficients.

Compiler version 4 selects these procedural rules. Embedded SELECT statements
retain SQL behavior. Existing version-3 definitions retain their procedural
NULL-format behavior until recreated from source. The language versions have
separate catalog identities; native PostgreSQL routines keep native semantics.

Stored SQL and top-level preview SQL now share the one-row DUAL transformation.
Explicit aliases and CTE shadowing are retained. Independent PostgreSQL tests
protect native week truncation, NULL propagation and two-argument exact numeric
truncation with its integer precision and preserved display scale. Prepared
profile-specific calls also check timestamp metadata, binary results, error
recovery and database reopen.

The [public generated fixture](../../../examples/oracle/reference/routine_week.py)
has 102 assertions: Gregorian week and ISO-year boundaries, scalar SQL,
procedural expressions, embedded SQL, NULLs, empty literal formats and exact
numeric precision boundaries. Two explicit error assertions require the exact
expected code from each backend; no unexpected error or difference is ignored.
The runner verifies Gregorian/AMERICA reference settings before creating objects
and removes its own objects afterward.

The new numeric cases initially exposed incorrect truncation of the procedural
precision argument. The implementation was corrected to preserve Oracle's
integer conversion, and the failing cases were retained. These checks qualify
only their recorded inputs and versions. They do not establish full PL/SQL,
alternate NLS behavior, Oracle vendor error mapping, or application acceptance.

The final [week comparison](week-comparison.json) matches all 102 assertions
against Oracle Free 23.26.3.0.0. Its expectations also passed an
[independent Oracle-only check](oracle-fixture-validation.json). The existing
[36 calendar](calendar-recheck.json), [29 temporal](temporal-recheck.json) and
[24 namespace](namespace-recheck.json) cases pass on the same binary and
compiler version. All generated objects were removed from both databases.

The original FIRSTOF definition was [recreated from unchanged source](application-source-recompile.json),
with only its destination namespace specified. The original application date
functions match [all 23 selected assertions](application-date-functions.json):
FIRSTOF 14, ADDDAYS five and DAYSBETWEEN four. FIRSTOF's final assertion requires
the documented procedural NULL-format error from both backends. The previous
[nine-of-fourteen development result](application-date-functions-before.json) is
superseded for those selected inputs. This does not qualify all possible inputs
or the complete application.

The [six seeded/absent payment-term cases](application-paymentterm.json) and
[both fixed-month branches](application-fixed-month.json) remain correct. The
transactional input updates were rolled back and verified in both databases.
The read-only [nullable-constraint recheck](constraint-recheck.json) passes.

A [separate older-reader check](older-reader.json) confirms that the new runtime
executes a version-4 function and the previous runtime rejects its language with
SQLSTATE 0A000. The application database was never opened by the older reader.
This checks execution capability rather than defining a new storage format.

[Validation](validation.json) passed 1,099 Rust tests: 371 SQL unit tests (three
existing ignores), 417 SQL integration tests, 118 pgwire unit tests, twenty
Oracle-profile driver tests, 168 protocol tests and five integrated application
compatibility tests. All seventeen reference-harness tests, formatting,
licensing and dependency-boundary checks passed.

The development binary SHA-256 is
`77942c5fde7c9e8486324649279e2dda1c7cde469e3613b1b586cda3decd1a6e`;
its target is `debug/examples/oracle_server`. The isolated fixture was upgraded
to that binary. No published release or production deployment is claimed.
