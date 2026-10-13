# PostgreSQL application compatibility fixes (2026-10)

Found by loading a production PostgreSQL 16 application database (the WeWoBo owner API: 157 tables,
row-level security, PL/pgSQL, views from `pg_dump`) into BicDB and running the application against it.
Each item below now behaves as in PostgreSQL; regression tests are in
`crates/bicdb-sql/tests/pg_wewobo_compat.rs` and `crates/bicdb-sql/src/tests.rs`
(`pg_dump_nested_joins_plan_like_flat_joins`), delegation in `crates/bicdb-pgwire/src/delegation.rs`.

| Area | Before | Now |
|------|--------|-----|
| Partial unique indexes (`CREATE UNIQUE INDEX ... WHERE`) | predicate ignored: uniqueness enforced on every row, index build failed on valid data | predicate stored and enforced on build, insert, update, transaction-pending rows and `ON CONFLICT`; `pg_indexes` shows the `WHERE` clause. Supported predicates: `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE`, boolean columns, `NOT`, comparisons of a column with a constant (text/bool/integer/float columns), `IN` / `= ANY(ARRAY[...])` / `NOT IN`, `AND`, `OR`. A UNIQUE index with any other predicate is refused with a clear error instead of being enforced on all rows. |
| `ON CONFLICT (cols) WHERE predicate DO ...` | parse error | accepted; resolves to the partial index on those columns |
| `ORDER BY 1, 2` / `ORDER BY alias` | position treated as a constant; aliases ignored on several paths | resolved to the output column at every query level (subqueries, CTEs, UNION branches, views, `SELECT *`) |
| `GROUP BY 1` | grouped by a constant (one group) | grouped by that select-list expression |
| `FROM ((a JOIN b ON ..) JOIN c ON ..)` (pg_dump's view rendering) | evaluated as an opaque row set, 25-100x slower | flattened to the equivalent join chain and planned normally |
| `CROSS JOIN LATERAL unnest(t.arr) r(x)`, `t, unnest(t.arr)` inside UNION branches and views | "joins and grouped queries support only table references" | correlated `unnest` per outer row, `WITH ORDINALITY`, multiple arrays |
| Views over UNION | every column typed `text` (integers sorted as text) | result column types kept |
| Non-ASCII text in function bodies (`$$ ... 'Ä' ... $$`) | statement panic (contained) | works (byte-safe dollar-quote scanning in all rewrite scanners) |
| `translate()`, `left()`, `right()` | unsupported | implemented with PostgreSQL semantics |
| PL/pgSQL `LOOP`, `WHILE ... LOOP`, `EXIT [WHEN]` | not implemented | implemented; `EXIT` leaves the innermost loop |
| `ALTER DATABASE name SET setting = value` / `RESET setting` / `RESET ALL` | unsupported | stored per database; new sessions start with the value, `SET` overrides it, `RESET` returns to it |
| Transaction delegation for identities without a tenant | always refused | refused unless the operator passes `bicdb user delegate ... --allow-no-tenant` |
