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
- Literal `IN` lists, single-column `IN` subqueries, `BETWEEN`, and
  comparison `ANY`/`ALL` over array literals.
- JSON access with dotted metadata paths, `->`, and `->>`.
- Common casts among booleans, integers, floats, exact textual/integer
  `numeric` values, text, JSON/JSONB, date/time-like strings, `interval`
  strings, UUID, regclass, bytea, and vector values.

`WHERE` filters treat unknown as not selected. `CHECK` constraints accept true
or unknown and reject only false, matching PostgreSQL's common NULL behavior.

Unsupported expression forms return PostgreSQL-shaped unsupported-feature
errors with SQLSTATE `0A000` when they reach execution. Current unsupported
forms include `SIMILAR TO`, PostgreSQL regex operators, month-aware interval
calendar arithmetic, row constructors, array column types, enums, domains,
custom types, and custom operator classes. Array values are supported only as
expression literals for selected functions and comparison `ANY`/`ALL`; generated-application
schemas should model repeated values as child tables or `JSONB`.

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
