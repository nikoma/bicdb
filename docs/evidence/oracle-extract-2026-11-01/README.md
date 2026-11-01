# Datetime extraction and dashboard aggregate

All eight synthetic extraction fixtures matched Oracle Free 23.26.3.0.0 through
the PostgreSQL driver. The cases cover year/month/day from Oracle DATE input,
hour/minute/fractional-second from timestamp input, a conditional aggregate and
an application column named `YEAR`. The field name is grammar; the column name
retains ordinary Oracle identifier folding. Invalid extraction fields still fail.

The previously failing iDempiere dashboard conditional aggregate was also
executed against the upgraded Oracle reference and the native BicDB application
copy. All three result values matched. Application data and the query trace
remain outside the repository. The complete desktop and its workflows have not
been qualified.

Validation passed: 339 SQL unit tests (three ignored), ten Oracle-profile driver
integration tests and 167 protocol integration tests. Formatting, licensing and
open-core boundary checks passed. The native development example server was
rebuilt and restarted for the reference comparison. No release was published.

The [synthetic report](comparison.json) contains the fixture hash, database and
driver versions, and exact normalized values. This qualifies those query values
only, not all Oracle extraction overloads, type metadata, error equivalence,
NLS/time-zone behavior or application completion. The bounded HR profile and
default PostgreSQL parser were not extended by this change.
