# PostgreSQL Expression Compatibility

BicDB supports a practical PostgreSQL-compatible baseline for common
application and ORM expressions in `SELECT`, `WHERE`, join predicates, grouping
keys, and simple `CHECK` constraints.

Supported expression forms:

- Numeric arithmetic with `+`, `-`, `*`, `/`, and `%`. `NUMERIC`/`DECIMAL`
  literals and columns are stored as canonical decimal text and arithmetic is
  evaluated with scaled integer math instead of `f64` drift.
- Date/timestamp plus or minus interval strings in the compatibility subset
  (`days`, `hours`, `minutes`, `seconds`), plus interval addition/subtraction.
- Interval scaling accepts numeric multipliers in either order and a numeric
  divisor after an interval. Uncast prepared parameters in these positions are
  described as `float8`, including expiry expressions such as
  `NOW() + ($1 * INTERVAL '1 second')`; explicit scalar casts retain their type.
  Multiplying or dividing two intervals returns SQLSTATE `42883`.
- String concatenation with `||`.
- Boolean `AND`, `OR`, and `NOT` with PostgreSQL-style unknown handling.
- `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, and `IS NOT DISTINCT FROM`.
- Searched and simple `CASE` expressions.
- `COALESCE(...)`, `NULLIF(...)`, and `date_part(...)` for year/month/day and
  hour/minute/second fields.
- `LIKE` and `ILIKE` with `%`, `_`, and a single-character `ESCAPE`.
- `SIMILAR TO` / `NOT SIMILAR TO` with an optional `ESCAPE` (empty or one
  character, otherwise SQLSTATE `22025`), and the SQL regular-expression
  substring `substring(x SIMILAR pattern ESCAPE e)` (also spelled
  `substring(x FROM pattern FOR e)`), whose two escape-double-quote separators
  delimit the returned part. As in PostgreSQL, `SIMILAR TO` runs as
  `x ~ similar_to_escape(pattern [, e])`; `similar_to_escape` and
  `similar_escape` are callable directly.
- Array subscripts on multi-dimensional arrays: `a[i][j]` yields the element
  type (also in the protocol's RowDescription) and any slice `a[i:j]` keeps the
  array type. Subscripting with fewer indexes than the array has dimensions
  (`(ARRAY[[1,2],[3,4]])[2]`) returns NULL as in PostgreSQL whenever the
  array's type is known; elements of `json[]`/`jsonb[]` arrays that are
  themselves JSON arrays are returned as elements.
- SQL/JSON query functions `JSON_VALUE`, `JSON_QUERY` and `JSON_EXISTS` with
  `PASSING`, `RETURNING`, `WITH [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER`,
  `KEEP|OMIT QUOTES`, and `NULL|ERROR|DEFAULT expr|EMPTY ARRAY|EMPTY OBJECT`
  (`TRUE|FALSE|UNKNOWN` for JSON_EXISTS) `ON EMPTY` / `ON ERROR`, with
  PostgreSQL's defaults and SQLSTATEs (`2203F`, `22034`, `22035`). The
  constructors `JSON_OBJECT`, `JSON_ARRAY`, `JSON_OBJECTAGG`, `JSON_ARRAYAGG`
  and the `IS [NOT] JSON [VALUE|OBJECT|ARRAY|SCALAR]` predicate are supported.
- `JSON_TABLE(context, path COLUMNS (...))` in FROM, including `FOR
  ORDINALITY`, typed `PATH` columns with ON EMPTY/ON ERROR, `EXISTS PATH`
  and one `NESTED PATH ... COLUMNS` per level, and joined laterally with
  earlier FROM items (`FROM t, JSON_TABLE(t.doc, ...)`). Sibling NESTED PATH
  clauses are refused with `0A000`; `PASSING` and a table-level `ON ERROR`
  clause are not parsed (syntax error).
- `ROWS FROM (f(...), g(...)) [WITH ORDINALITY]` over single-column
  functions, and multi-array `unnest(a, b, ...)` in FROM, zip the functions'
  rows and pad the shorter ones with NULL.
- Literal `IN` lists, single-column `IN` subqueries, `BETWEEN`, and
  comparison `ANY`/`ALL` over array literals.
- JSON access with dotted metadata paths, `->`, and `->>`.
- Common casts among booleans, integers, floats, exact textual/integer
  `numeric` values, text, JSON/JSONB, date/time-like strings, `interval`
  strings, UUID, regclass, bytea, and vector values.

`WHERE` filters treat unknown as not selected. `CHECK` constraints accept true
or unknown and reject only false, matching PostgreSQL's common NULL behavior.

Unsupported expression forms return PostgreSQL-shaped unsupported-feature
errors with SQLSTATE `0A000` when they reach execution. Forms listed as
unsupported in earlier releases (PostgreSQL regex operators, month-aware interval
calendar arithmetic, row constructors, array column types, enums, domains,
custom types, and custom operator classes) are covered by their own pages and
fixtures.

Differential coverage lives in:

- `fixtures/postgres-compat/023-expressions-operators-casts.json`
- `fixtures/postgres-compat/024-expected-difference-unsupported-expressions.json`
- `fixtures/postgres-compat/025-type-breadth-supported.json`
- `fixtures/postgres-compat/026-array-columns-supported.json`
- `fixtures/postgres-compat/027-expected-difference-enum-types.json`
- `fixtures/postgres-compat/028-expected-difference-domain-types.json`


## Predicate values in projections and ordering

NULL and truth predicates can produce boolean values in a SELECT list and in
ORDER BY expressions, including aliased and parenthesized forms. Computed
predicates use the expression evaluator while ordinary record fields retain
the field projection path. Three-valued logic is preserved for `IS NULL`,
`IS NOT NULL`, `IS TRUE`, `IS NOT TRUE`, `IS FALSE`, `IS NOT FALSE`,
`IS UNKNOWN` and `IS NOT UNKNOWN` in PostgreSQL.

```sql
SELECT id, (value IS NULL) AS missing
FROM samples
ORDER BY (value IS NULL) DESC, id;
```

The Oracle preview regression uses its NULL predicate for unknown boolean values,
matching the Oracle 23.26.3 reference observations. That reference rejects
PostgreSQL's `IS UNKNOWN` spelling. Separate regression
cases protect the two front ends.
