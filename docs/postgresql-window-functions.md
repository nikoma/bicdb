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

Ordered-set aggregates `percentile_cont`, `percentile_disc` and `mode` and
the hypothetical-set aggregates `rank`, `dense_rank`, `percent_rank` and
`cume_dist` (`rank(x) WITHIN GROUP (ORDER BY key)`, one ORDER BY key; NULL
keys take part by their null placement, as in PostgreSQL) are aggregates,
not window functions. `ORDER BY ... FETCH FIRST n ROWS WITH TIES` (with an
optional `OFFSET`) returns the first rows plus every row tied with the last
one, as PostgreSQL does; it uses `rank()` over the ORDER BY keys internally.

Frame exclusion `EXCLUDE CURRENT ROW | GROUP | TIES | NO OTHERS` after an
explicit `ROWS`, `RANGE` or `GROUPS` frame (inline or in a named window)
removes rows from the frame of aggregate and `FIRST_VALUE` / `LAST_VALUE` /
`NTH_VALUE` functions, as PostgreSQL does.

In FROM, `ROWS FROM (f(...), g(...)) [WITH ORDINALITY]` runs set-returning
functions in lockstep, padding the shorter ones with NULLs. Several
functions (or one with `WITH ORDINALITY`) are supported for the
single-column set-returning functions (`generate_series`, `unnest`,
`generate_subscripts`, `regexp_split_to_table`, `string_to_table`,
`json[b]_array_elements[_text]`, `json[b]_object_keys`); other lists are
rejected rather than evaluated differently.

Unsupported window functions and forms return a clear SQL error instead of
being silently mis-evaluated.
