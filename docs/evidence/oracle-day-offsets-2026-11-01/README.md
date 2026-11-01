# Numeric datetime offsets

All thirteen [day-offset fixtures](../../../examples/oracle/reference/date-offset-cases.json)
matched Oracle Free 23.26.3.0.0 through the PostgreSQL driver. Their
[comparison report](comparison.json) includes exact values and version metadata.

The implementation supports known date/timestamp operands with numeric day
values, numeric text and NULL. Timestamp numeric arithmetic discards fractional
seconds before adding the day offset. Offsets use exact decimal rounding to
whole seconds. Tests distinguish offsets immediately below and above half a
second, including negative offsets; literal tokens do not pass through floating
point. Out-of-range results fail.

Driver tests cover declared timestamp/integer parameters, inferred NUMERIC day
parameters and native timestamp result metadata. Typed timestamp subtraction
retains its interval overload. CTE metadata probes see resolved inner datetime
results. The application date-filter query that previously failed on `SYSDATE-1`
now matches the Oracle reference result. That application trace stays private.

At this revision, a separate [known-gap probe](known-gap.json) failed: an offset
expression `1/86400` evaluated with native integer division before datetime
addition. The subsequent [division follow-up](../oracle-number-division-2026-11-01/README.md)
resolves that composition. The original failing artifact is retained. The
thirteen matching cases here qualify offset values, not all NUMBER expressions.
Date-minus-date, time-zone/NLS behavior, stored-program arithmetic and complete
application workflows remain unqualified.

Validation passed: 340 SQL unit tests (three ignored), all 416 main SQL integration
tests, eleven Oracle-profile driver tests, 167 protocol tests and five Python
comparison-harness tests. Formatting, licensing and open-core boundary checks
passed. A native development server was rebuilt and restarted for comparison.
No release or production deployment is claimed.
