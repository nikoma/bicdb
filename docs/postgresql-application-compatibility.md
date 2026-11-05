# PostgreSQL application compatibility fixes (2026-10)

Pgwire startup `options` accepts ordinary application settings in dotted,
unreserved namespaces through both `-c name=value` and `-cname=value`. Backslash
escapes preserve spaces and backslashes; values may contain Unicode. Settings
are visible to stored functions and survive transaction-local override rollback,
including lookup keys supplied through `PGOPTIONS`. Native identity, Carrier
authority and host/system namespaces remain excluded from client startup
settings. Existing `search_path` handling is retained. Unit and protocol
regressions cover these boundaries; PostgreSQL controls agree on application
setting values and escaping.

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
| `translate`, `left`, `right`, `strpos`, `lpad`, `rpad`, two-argument `ltrim`/`rtrim`/`btrim`, `repeat`, `reverse`, `starts_with`, `power`, `sqrt`, `cbrt`, `exp`, `ln`, `log`, `ceil`/`floor`, `regexp_match`, `regexp_split_to_array`, `age`, `make_interval` (named arguments), `date_bin`, `to_date`, `timezone`, `similarity` (pg_trgm) | unsupported | implemented with PostgreSQL semantics |
| `POSITION(a IN b)`, `EXTRACT`, `SUBSTRING(x FROM a FOR b)`, `CEIL`, `FLOOR`, `AT TIME ZONE` in a single-table SELECT list | "unsupported field expression" | evaluated (the record fast path now hands every non-field expression to the row evaluator) |
| `real`/`double precision` compared with a numeric literal (`r > 0.5`) | always false (operands treated as incomparable) | compared as double precision |
| `CREATE EXTENSION pgcrypto` (and other trusted extensions) by the database owner | superuser only | allowed for the database owner, as in PostgreSQL 13+; untrusted extensions still need a superuser |
| PL/pgSQL `LOOP`, `WHILE ... LOOP`, `EXIT [WHEN]` | not implemented | implemented; `EXIT` leaves the innermost loop |
| `ALTER DATABASE name SET setting = value` / `RESET setting` / `RESET ALL` | unsupported | stored per database; new sessions start with the value, `SET` overrides it, `RESET` returns to it |
| Transaction delegation for identities without a tenant | always refused | refused unless the operator passes `bicdb user delegate ... --allow-no-tenant` |
| `COALESCE((SELECT bigint_col FROM t ...), -1)` | typed `integer` from the literal; values over 2^31 failed with "integer out of range" | a single-column scalar subquery over a table (or without FROM) carries its column type, so the common type is `bigint` |
| `INSERT INTO t ... SELECT ... UNION [ALL] SELECT ...` (with `RETURNING`, `ON CONFLICT`) | "INSERT supports only VALUES or SELECT source" | set operations and parenthesized queries feed INSERT; `INSERT ... SELECT 1, 'x' ON CONFLICT ...` without FROM no longer reads `ON` as a column alias |
| `SELECT DISTINCT ON (lower(v)) v ... ORDER BY lower(v), ...` | "DISTINCT ON expression must appear in the select list" | keys need only match the leading ORDER BY items (42P10 otherwise); keys that are not output columns are evaluated on the source row. Remaining boundary: in a SELECT list that expands set-returning functions, DISTINCT ON keys must still be output columns |
| `to_jsonb(c.*)`, `row_to_json(c.*)`, `f(alias.*)` | "to_jsonb expects 1 argument(s), got 0" | `alias.*` as a function argument is the whole-row value, as `f(alias)` |
| `percentile_cont(f)`, `percentile_disc(f)` (also `ARRAY[...]` of fractions), `mode()` `WITHIN GROUP (ORDER BY x)` | unsupported | ordered-set aggregates with and without GROUP BY and with FILTER; NULLs ignored, empty input gives NULL. `percentile_cont` accepts numeric input only (not `interval`) |
| PL/pgSQL function called once per row (`UPDATE t SET slug = f(title) WHERE ...`, `SELECT f(col) FROM t`, `WHERE f(col) = ...`) | "unsupported constant expression title" / "function f is not supported without FROM" | the row evaluators hand the call to the session's PL/pgSQL interpreter (a child session over the caller's transaction, role, settings and cancellation), with the EXECUTE privilege check and SECURITY DEFINER; the body's queries see the transaction's own rows. Remaining boundary: such a per-row call may only read — a data-modifying statement (INSERT/UPDATE/DELETE/DDL, `nextval`) inside it fails the statement before anything is written, and `set_config` inside it does not reach the caller. Calls without FROM (`SELECT f(x)`) keep full write access |
| Table row types: `NULL::t`, `'(..)'::t`, `jsonb_populate_record(NULL::t, j)` (scalar and in FROM, also `json_`/`_recordset` in FROM), `(expr).col`, `(expr).*` | populate_record: "a column definition list is required" / "not registered"; `(expr).*`: parse error | every table's row type works as a composite type: populate_record takes its attributes and types from the table (JSON keys override the base record, unknown keys are ignored, untyped string literals are JSON input); `(expr).*` in a select list expands to one typed column per attribute, with the attribute names, including `(CASE WHEN .. THEN s ELSE jsonb_populate_record(NULL::t, ..) END).*` over a whole-row reference `s`. Remaining boundary: `(expr).*` needs a row type known without evaluation — a cast, a populate_record base, a table alias, or a CASE over those (not a CTE or subquery alias, not an anonymous `ROW(..)`), is supported only as a select-list item, and `expr` is evaluated once per attribute as in PostgreSQL. Single `(expr).col` items report no Describe type unless cast |
| `SELECT state, count(*) ... GROUP BY state ORDER BY 2 DESC` / `ORDER BY count(*) DESC` / `ORDER BY n DESC` (alias of an aggregate) | ORDER BY keys were evaluated on each group's first row, so aggregate keys became NULL and groups stayed in group-key order (with LIMIT: the wrong groups). The single-row `GROUP BY 1` result reported on an older build does not reproduce on current code | aggregate sort keys are evaluated over the group like select-list items |

## Rails 8.1.4 application qualification (2026-11)

The complete [Team Board application](../examples/rails/team_board/README.md)
runs with Rails' standard PostgreSQL adapter, Solid Queue, Solid Cache, and Solid
Cable. Its [qualification report](../reports/postgres-app-compat/rails-8.1.4/report.md)
records fresh schema loading, migrations and rollback, the Rails test suite,
concurrent job workers, pending-job recovery after SIGKILL, and real HTTP/WebSocket
requests. The report lists the generic SQL/catalog/constraint defects exposed by
the campaign and their regression tests. This is a tested application workload;
it does not imply unrestricted compatibility with other Rails applications.

## Routine row-count expressions

PL/pgSQL named variables and positional parameters in LIMIT/OFFSET are bound
before execution at every query level, including CTEs and nested subqueries.
Binding preserves the stored routine tree across successive calls. NULL counts
retain PostgreSQL semantics (unbounded LIMIT, zero OFFSET); negative counts fail
with 2201W for LIMIT and 2201X for OFFSET. The focused Carrier compatibility
suite covers pagination, repeated calls, null/zero counts, negative counts, and
FETCH with positional parameters, alongside a PostgreSQL control. The parser's
existing restriction on nonliteral named FETCH counts remains separate from
this LIMIT/OFFSET fix; the EHR sync function uses LIMIT.
