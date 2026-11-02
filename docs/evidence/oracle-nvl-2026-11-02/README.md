# Eager NVL and timestamp precision qualification

The opt-in Oracle preview now lowers NVL independently of COALESCE. The new
compiler marker is `bicdb_oracle_preview_v11` (OID 999410). The implementation
uses declared operand types, evaluates both operands and required conversions,
then selects the first non-NULL value. PostgreSQL and historical stored-routine
contracts remain independently checked.

This is a bounded semantic qualification. The original Oracle-provider iDempiere
application is **not fully operational** on BicDB. The original definition
inventory remains **44/62**, with 18 blockers: thirteen procedure replacement/
compilation paths, one Oracle dictionary dependency, three package/autonomous
transaction dependencies, and one hierarchical-query dependency. Definition
acceptance does not establish successful compilation or execution.

## Measured Oracle behavior

[nvl-live-comparison.json](nvl-live-comparison.json) records **93/93** matches
against Oracle 23.26.3.0.0 under the fixed preview NLS configuration. Expectations
are frozen in the public reference fixture and checked again against the source.
Six cases use actual typed Oracle and PostgreSQL driver parameters.

The comparisons cover:

- Eager NVL, exactly-once side effects and unused conversion errors, with independent lazy COALESCE checks.
- Character/NUMBER/DATE/TIMESTAMP result selection, typed NULL versus empty
  character input, CHAR padding and unconstrained replacement length/scale.
- DATE fractional-second removal and fixed-default timestamp text conversion,
  including omitted time fields becoming midnight.
- Timestamp precision through columns, CTEs, derived queries, local variables,
  cursor records, SQL binds and anchored function returns.
- Oracle SQL timestamp displays with nine fractional digits, procedural values
  using declared precision, and anchored return rounding before SQL formatting.
- Declared native PostgreSQL projection OIDs: NUMERIC 1700, TEXT 25 and TIMESTAMP
  1114. The selected PostgreSQL transport does not claim Oracle wire identity.

Oracle documents the first-operand character conversion and numeric precedence
rules in its [NVL reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/NVL.html).
Eagerness, temporal precision and error observations here come from execution
against the recorded source version.

Binary floating point, national/LOB operands, intervals, other NLS settings,
complete Oracle optimizer/type-resolution diagnostics and nonzero fractional
values beyond microseconds remain unsupported or unqualified. These results do
not establish complete Oracle compatibility.

## Application and preservation checks

The upgraded development database runs the source engine commit recorded in
[validation.json](validation.json). Its temporal metadata remains at the existing
profile format; this compiler change introduces no storage-format transition.
A stopped-writer rollback snapshot verified 994 files before the upgrade.

[complete-target-value-preservation.json](complete-target-value-preservation.json)
records an independent comparison of **915 tables and 185,537 current rows**
against that snapshot. Every row multiset matches. Every table fingerprint also
matches the linked, already-public table vector from the previous qualification;
the report includes that vector's digest. The current rows include earlier
application writes and are not claimed equal to the original export's row count.

The unchanged PRODUCTATTRIBUTE source was explicitly recreated under compiler
version 11 and passes **6/6** original seed probes. A separate disposable import
of its four dependent tables also passes **6/6**. The prior 37 original-function,
21 configuration and 96 business-date probes still match Oracle. Application
values are published as counts and digests; connection details remain private.
The other installed source definitions retain their earlier compiler versions.

[reader-compatibility.json](reader-compatibility.json) records a new-reader run
and an older version-10 binary opening an isolated test database. The old reader
rejects each version-11 routine call with 0A000 and subsequently executes a normal
query. It never opens the upgraded application database.

## Validation

- **1,255 Rust tests passed**, with three existing ignored tests.
- **23 reference-runner** and **22 migration-tool** Python tests passed.
- All **845 retained generated Oracle cases** still match, for **938** total
  generated comparisons including the 93 new NVL cases.
- All **132 native PostgreSQL reference controls** still match PostgreSQL 18.4.
- Formatting, dependency license/source policy and the open-core boundary passed.
- Builds use one compiler job, disabled incremental compilation and an active
  available-memory guard. Resource and cleanup measurements are recorded in
  validation.json. Task-owned completed test executables are removed afterward;
  current and required older-reader binaries are preserved.

The complete application, deferred foreign-key validation, remaining source
routines, views, packages, operational behavior and end-to-end business workflows
still require qualification. The independently reproduced named-schema
primary-key UPDATE failure remains a separate general engine blocker.
