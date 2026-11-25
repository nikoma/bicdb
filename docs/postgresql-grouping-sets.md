# PostgreSQL Grouping Sets, ROLLUP and CUBE

BicDB evaluates PostgreSQL grouping sets:

```sql
SELECT region, product, sum(qty), GROUPING(region, product)
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY region, product;

SELECT region, product, count(*) FROM sales GROUP BY CUBE (region, product);

SELECT region, product, sum(qty) AS total
FROM sales
GROUP BY region, GROUPING SETS ((product), ())
HAVING sum(qty) > 2
ORDER BY total DESC;
```

Supported:

- `GROUPING SETS (...)` with single expressions, parenthesized lists and the
  empty set `()`, and `ROLLUP (...)` / `CUBE (...)` inside it; top-level
  `ROLLUP`, `CUBE`, `GROUP BY ()`, composite elements such as
  `ROLLUP ((a, b), c)`, and any mix with plain grouping columns (the result
  is the cross product of the sets, as in PostgreSQL). At most 4096 sets
  (54001 beyond).
- In each grouping set, a grouping expression outside the set reads as NULL
  (aggregate arguments still see the input rows). `GROUPING(a, b, ...)`
  returns the PostgreSQL bit mask (a bit is set when the argument is not
  grouped in the row's set); a non-grouping argument fails with 42803.
- `HAVING` is applied per grouping set; `DISTINCT`, `ORDER BY` (by output
  column, alias, position or any expression), `LIMIT`, `OFFSET` and `FETCH`
  apply to the combined rows. The empty set produces its row even for empty
  input.

Execution evaluates one ordinary grouped query per set and concatenates the
results, so every set honours the same authorization and scan rules as a
plain `GROUP BY`.

Not supported (0A000): window functions over a grouping-sets query, `*` in
its select list, `DISTINCT ON`, and `GROUP BY DISTINCT`. The order of rows
without `ORDER BY` can differ from PostgreSQL's, as for any unordered query.
