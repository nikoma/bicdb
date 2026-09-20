# PostgreSQL Window Functions

BicDB executes the common PostgreSQL window-function families on its
materialized row-query path:

```sql
ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC, id)
RANK() OVER (ORDER BY score)
SUM(amount) OVER (PARTITION BY account ORDER BY posted_at)
LAG(value) OVER (ORDER BY created_at)
LAST_VALUE(value) OVER (
  ORDER BY created_at
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

Supported functions are:

- `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, and `NTILE`
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`
- `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`

`PARTITION BY` and multi-key window `ORDER BY` are supported. Aggregate and
first/last-value functions use PostgreSQL's default behavior: the whole
partition without `ORDER BY`, or `UNBOUNDED PRECEDING` through the current
peer group with `ORDER BY`. Explicit `ROWS`, `RANGE`, and `GROUPS` frames
support bounded and unbounded preceding/following endpoints. Offset `RANGE`
frames accept a single compatible ordering key: numeric offsets for numeric
keys and interval offsets for date/timestamp keys.

Windows execute after grouping, so grouped aggregates can be used in window
arguments and ordering, including `SUM(SUM(amount)) OVER (...)`. Named windows
can inherit and extend earlier named windows. `LAG`, `LEAD`, `FIRST_VALUE`,
`LAST_VALUE`, and `NTH_VALUE` accept `IGNORE NULLS` and `RESPECT NULLS`.

Window-frame exclusion clauses and ordered-set aggregates used with `WITHIN
GROUP` remain unsupported. Unsupported window functions and forms return a
clear SQL error instead of being silently mis-evaluated.
