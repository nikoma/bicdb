# Prepared character-key lookup

The PostgreSQL wire text decoder previously reinterpreted numeric-looking
character parameters as numbers. An indexed iDempiere dictionary lookup returned
zero rows for a bound character value `1`, while the equivalent literal query
returned one row. Parameter inference correctly reported varchar; decoding lost
that type before execution.

Character types now retain their string representation in text-format binds,
using the same registry classification as the binary text codec. The regression
first reproduced the failure under the PostgreSQL profile. It now passes under
both PostgreSQL and Oracle preview profiles, with declared text/varchar and
inferred varchar parameters. Indexed lookups preserve `1`, `001`, `1.0`, ` 1 `,
`1e2` and `0`; projections retain their original spelling.

The actual imported application dictionary query was repeated after restarting
the native BicDB development server. All three Parse cases (unspecified, text
and varchar) returned one row, matching the literal query. This verifies that
lookup, not the complete application or its business workflows.

Validation passed: 117 pgwire unit tests, 166 protocol integration tests and ten
Oracle-profile driver integration tests. Formatting, licensing and open-core
boundary checks passed. The development example server was rebuilt; no release
or production deployment is claimed.
