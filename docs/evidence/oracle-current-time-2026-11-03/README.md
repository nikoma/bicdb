# Typed Oracle statement clocks

The durable preview profile lowers `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` and
`CURRENT_DATE` to statement-clock expressions. A statement shares one instant;
later statements in the same transaction obtain a new instant. The connection's
time zone determines the local representation. Precision zero through six rounds
timestamp values; Oracle DATE retains time of day and truncates fractional
seconds. Quoted columns, qualified references, literals and the exercised explicit
and implicit projection aliases retain their identifier/value meanings.

Shared result inference now preserves known datetime types during prepared
Describe. Previously, metadata-only inference could override a correctly typed
execution value with TEXT. Oracle timestamp expressions now use PostgreSQL OIDs
1184 or 1114; Oracle DATE uses 1114 to retain its time component. Native
PostgreSQL CURRENT_DATE retains OID 1082, and CURRENT_TIMESTAMP retains OID 1184
and its existing transaction-stable behavior. No application or table identity
selects these rules.

## Reference observations

The [public fixture](../../../examples/oracle/reference/current_time.py) compares
Oracle 23.26.3.0.0 and BicDB in UTC, Asia/Kolkata and America/Los_Angeles.
[All 75 cases match](oracle-comparison.json), including timestamp precisions,
non-NULL values, transport types, statement agreement, rounding, DATE truncation,
transaction advancement and legal aliases. The same fixture on the
[previous checked binary](oracle-baseline.json) matches 18/75, the alias cases.
Absolute times are checked against each execution's wall-clock window, rather
than comparing instants obtained from separate backends.

Three additional [Oracle declaration probes](declaration-oracle-reference.json)
accept unquoted clock keywords as column names and remove their nonce tables.
The SQL regression exercises those declarations together with insert targets,
primary-key columns and qualified reads, preserving their ordinary identifier
meaning. This is bounded identifier coverage rather than complete grammar parity.

Oracle Thin mode cannot directly decode TIMESTAMP WITH TIME ZONE values that
contain a named region ID. The fixture disables prefetch, describes the original
typed projection, and obtains timezone-bearing values with a TO_CHAR ISO-offset
projection. Paired clock comparisons obtain all their values in one statement.
Metadata and value queries are separate for those projections. This transport
method does not qualify Oracle-protocol timestamp decoding.

Independent [PostgreSQL 18.4 controls](postgres-controls.json) match 12/12 for
native CURRENT_TIMESTAMP, CURRENT_DATE, clock_timestamp and transaction stability
across the same zones. The [previous native baseline](postgres-baseline.json)
matches 0/12 because its datetime Describe types are TEXT. No mismatches are
excluded from these reports.

The [reader comparison](reader-comparison.json) passes 11 checks. Compiler
version 26 uses language OID 999425 while version 25 retains OID 999424. The new
binary executes an existing ordinary routine, and the previous binary rejects
new routines and views with SQLSTATE 0A000. Reopening with the new binary retains
their behavior. These checks use a disposable database; an older binary never
opens the application database. Durable metadata format remains 5. Original
routine declarations need explicit recompilation to adopt new lowering.

## Remaining qualification

The [validation record](validation.json) identifies the source, exact development
binary and completed local checks. This is source/binary evidence, not a
published release, production deployment or complete application qualification.

Local validation passes **1,490 Rust test executions** across 19 relevant
batches, with three existing ignores, and **58 Python tests**. Formatting, diff,
license and dependency-boundary checks pass. Earlier failed test expectations
and their corrections are retained in the validation record. The build used one
job, disabled incremental compilation and debug symbols, and an active memory
guard with 14 GiB start headroom and an 8 GiB floor. Minimum observed available
RAM was approximately 16.6 GiB. Removing 14 completed test executables reclaimed
approximately 4.1 GiB and left a 7.4 GiB build cache; frozen/current binaries and
reusable dependency artifacts were preserved.

The actual application previously passed the bounded row-wait point and then
failed in `PO.setProcessedOn` because `SELECT CURRENT_TIMESTAMP FROM DUAL`
returned NULL. Its order remained draft after rollback. A successful standalone
clock comparison does not prove that the application's Complete action succeeds;
that action must be retried and its committed effects independently verified.

Precision seven through nine explicitly remains unsupported pending nanosecond
storage/transport qualification. Full clock-family behavior, SYSDATE server-zone
rules, every identifier/declaration context, implicit output labels, PL/SQL
cursor timing, all temporal/NLS operations and the wider Oracle migration
programme remain open. Native statement_timestamp timing and other pre-existing
clock-family gaps are not qualified by these controls.

A separate [native type-relocation baseline](native-type-return-relocation-baseline.json)
reproduces a failure in the prior main commit's SQL CI job. After moving and
renaming a user type, loading the stored function declaration restores its old
return-type name, while its argument type displays the new name. This is an
actionable native compatibility follow-up, not a passing CI result or a behavior
qualified by the clock comparisons.
