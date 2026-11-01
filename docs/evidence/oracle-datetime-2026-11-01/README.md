# Oracle datetime truncation reference

The 14 queries in
[`datetime-cases.json`](../../../examples/oracle/reference/datetime-cases.json)
matched Oracle Free 23.26.3.0.0 and BicDB's native development server over the
PostgreSQL driver with `oracle-preview-v1`. They cover default/day, month,
quarter, year, ISO week, hour, minute and century truncation of a leap-day
timestamp, plus an ISO week crossing a calendar-year boundary.

[`summary.json`](summary.json) records the query values. This is evidence for
these cases, not Oracle certification, complete datetime compatibility, or
iDempiere application acceptance. Connection credentials, application data and
third-party database binaries are not included.

With private reference connection configuration and an independently provisioned
BicDB preview database, reproduce the comparisons using:

```sh
PSYCOPG_IMPL=python python3 examples/oracle/reference/compare.py \
  --oracle-config /private/oracle-connection.json \
  --profile oracle-preview-v1 \
  --cases examples/oracle/reference/datetime-cases.json \
  --output /private/datetime-report.json
```

`BICDB_PG_DSN` supplies the target connection. Additional Rust tests cover typed
prepared timestamp parameters, NULL formats, column/CTE overload resolution and
the unchanged PostgreSQL numeric function.
