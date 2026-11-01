# Preview decimal division

All sixteen targeted NUMBER-division cases and one hundred deterministic
synthetic comparisons matched Oracle Free 23.26.3.0.0 through the PostgreSQL
driver. The thirteen existing datetime-offset cases also matched. Reports:
[targeted](comparison.json), [generated](generated-comparison.json),
[datetime regression](day-offset-comparison.json).

Preview integer/NUMERIC division uses a decimal quotient with twenty base-100
mantissa digits. The resulting 39/40-digit alignment is described by the
[Oracle datatype documentation](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html)
and was checked against the reference. The tests include signs, differing
exponent alignment, large/small representable quotients, derived columns, CTEs,
NULL and numeric text. Generated operands have at most thirty decimal coefficient
digits and deliberately keep the quotient within the recorded exponent range.

The previously failing datetime expression `timestamp + 1/86400` now adds one
second. Positive and negative half-second fractions composed through division
also match the reference. The earlier failing artifact remains in the
[day-offset evidence](../oracle-day-offsets-2026-11-01/known-gap.json).

Driver regressions check NUMERIC result metadata, declared integer parameters,
inferred NUMERIC parameters, preparing a division-by-zero query without executing
it, SQLSTATE 22012 at execution and subsequent connection recovery. Native
PostgreSQL integer division remains unchanged. The bounded HR profile was not
extended.

Validation passed: 341 SQL unit tests (three ignored), all 416 main SQL integration
tests, twelve Oracle-profile driver tests and 167 protocol tests. Formatting,
licensing and open-core boundary checks passed. The development example server
was rebuilt and restarted for the live comparisons. No release was published.

These results do not qualify the complete NUMBER datatype, all literal/input
range conversions, NLS, binary-floating operations, other arithmetic operators,
stored-program arithmetic, Oracle error equivalence or the complete application.
