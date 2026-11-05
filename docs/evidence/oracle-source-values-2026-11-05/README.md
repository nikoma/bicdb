# Strict source-export and resumed value verification

The [operator contract](../../oracle-source-value-verification.md) separates
export integrity, target equality, DDL preservation and workflow acceptance.
Source files now have their exact bytes, row widths and declared row counts
checked in one read before target SQL. Resumed verification rechecks every
completed table's values and saved digest; row count alone cannot re-certify it.
The comparison retains its existing exact-value and duplicate-multiplicity rules.

The [validation record](validation-results.json) reports **75 migration-tool
tests passed, none failed or skipped**, plus **10 JDBC SDK tests passed**. Native
cases cover PostgreSQL and Oracle preview profiles independently over PostgreSQL
transport, including exact 38-digit decimals under a precision-2 context, binary
content, timestamps, fixed-character padding, Unicode and duplicate rows. Oracle
CLI cases exercise initial/resumed verification, missing saved digests and a
same-count value change. New planning regressions reject malformed rows, wrong
counts, invalid projections, file digest mismatches, external paths and unknown
or inexact encodings before querying target values. The preceding verifier fails
the new row-count regression as expected.

The first complete Python batch passed 73 tests and failed two provider generator
tests because the isolated worktree lacked its JDBC SDK jar. Building the local
SDK, including its tests, supplied that dependency. The final complete rerun
passed all 75. Native engine source/binary identity is recorded separately from
the Python change and rebuilt SDK; the accepted application was not upgraded.
No GitHub workflows were changed or manually dispatched.

The strict reader also checked all **915 exported files / 185,471 source rows**:
file digests, row widths and declared counts match. This is source-export
integrity evidence, not proof that the current target equals the export.
A provisional sample of an already exercised target found selected dictionary
and archive tables matching, with differences in business/control tables. A
separate aggregate diagnostic retained only counts and changed column names.
Whole-source equality remains unqualified; a pristine target comparison or an
explicitly reviewed delta is still required. No data was repaired or comparison
rule weakened to obtain a passing result.

Relationship installation, full orphan comparison, remaining original business
classes, recovery/cutover and complete application qualification remain separate
programme gates. Host memory pressure interrupted the independently running
relationship migration; its clean shutdown and retained journal are operational
state, not a failing value-verification test.
