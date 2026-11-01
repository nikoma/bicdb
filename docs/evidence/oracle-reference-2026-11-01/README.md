# Initial Oracle reference comparison

An actual Oracle AI Database Free Lite `23.26.3.0.0` container imported the same
87-statement adapted HR fixture as BicDB. All seven source table counts match
the fixture, the view returns 106 rows, and Oracle reports no invalid objects.
The reference HR data was verified after container recreation using the same
named volume. No Oracle binary or private connection configuration is included
in this evidence directory.

The [query report](query-comparison.json) compares 27 queries on the source and
binary identified in [provenance](provenance.json): **25 match and two fail**.
The comparison command exited **1**. The remaining cases are `nvl2`
(unsupported) and `decode-null` (incorrect null result). These are unresolved
compatibility gaps, not ignored successes or general conformance results.

Initial investigation also found two defects that this source fixes:

- Oracle-profile `TO_DATE` discarded the time component by using PostgreSQL's
  date-returning function. The separate Oracle lowering now preserves time.
- View DDL traced types only through its first source, and view materialization
  dropped those types. Joined numeric columns now retain native numeric metadata
  and codecs. The regression covers type modifiers, same-named source columns,
  outer-join nullability, quoted identifiers and database reopening.

All original HR table values and all 106 projected view rows match Oracle in
this query fixture. Numeric comparisons preserve the full 38-digit sample
without rounding through a Python float or the default Decimal context.
Timestamp/date distinctions and character padding remain observable.

Local validation passed: 321 SQL library tests, 37 targeted SQL integration
tests, 318 pgwire tests, eight Python harness/preparation tests, the complete
workspace compilation check including tests, formatting, licensing and the
dependency boundary check. Three SQL library tests and two pgwire tests were
already ignored by their respective suites. The live comparison is reported
separately as failed because its two gaps remain.

Reproduce with the [reference instructions](../../../examples/oracle/reference/README.md).
The queries cover result values only, not full type metadata, error equivalence,
transactions, locking, PL/SQL packages, migration/cutover or operational
qualification. Oracle Free Lite is a newer, restricted edition and cannot
qualify all Oracle 19c or Enterprise workloads. The
[compatibility programme](../../oracle-compatibility-programme.md) remains open.
