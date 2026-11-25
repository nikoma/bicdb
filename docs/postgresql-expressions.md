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
- jsonpath values print as PostgreSQL prints them (operator precedence
  parentheses, folded signed literals, canonical numbers including
  hexadecimal, octal, binary and `_`-separated literals, string escapes
  `\b \f \n \r \t \v \xHH \uXXXX \u{X}`), scanner errors use
  PostgreSQL's texts (`trailing junk after numeric literal at or near "1a" of
  jsonpath input`), array subscripts take any expression with `last`, item
  method errors use PostgreSQL's texts, and `jsonb_path_*` accept
  `vars =>` / `silent =>` named arguments.
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
  earlier FROM items (`FROM t, JSON_TABLE(t.doc, ...)`). `PASSING value AS
  name` variables reach the row path and every column path, a table-level
  `ERROR ON ERROR` / `EMPTY [ARRAY] ON ERROR` applies to the row path and to
  columns without their own ON ERROR, and sibling `NESTED PATH` clauses
  produce PostgreSQL's union of their rows (the other branches' columns
  NULL). A NESTED PATH nested under sibling NESTED PATH clauses is refused
  with `0A000`.
- Postfix `x ISNULL` / `x NOTNULL`, `COLLATE` after a cast
  (`'b'::text COLLATE "C"`, also in index expressions) and typed window frame
  offsets (`RANGE BETWEEN '1 day'::interval PRECEDING AND CURRENT ROW`).
- User operators whose names the tokenizer would split (`===`, `!==`, prefix
  `~~~`), written infix, prefix or as `OPERATOR(schema.name)`.
- Assignment to array elements and slices, composite fields and jsonb
  subscripts: `UPDATE t SET a[2] = 5, a[1:2] = '{7,8}', c.f = 1, j['k'] =
  '2'` and `INSERT INTO t (a[1], a[3], c.f) VALUES (...)` (VALUES sources).
- Date and timestamp input is decoded as PostgreSQL decodes it (field
  splitting, the DateStyle field order MDY/DMY/YMD for ambiguous dates,
  two-digit years, `YYYYMMDD`/`YYMMDD`/`hhmmss` run-together fields, day of
  year `1999.008`, Julian `J2451187`, `BC`, `AM`/`PM`, text months and day
  names), with its errors (including the DateStyle hint). It accepts `now`,
  `today`, `tomorrow`, `yesterday`, `epoch`, `±infinity` and `allballs`,
  PostgreSQL's default time zone abbreviations (`PST`, `PDT`, `EST`, `CET`,
  ... with `DST`), full zone names in any case, numeric offsets and POSIX
  zones without daylight rules (`GMT+8`, `UTC-3`, `<+05>-5`), in input,
  `AT TIME ZONE` and `SET TIME ZONE`; timestamp without time zone checks and
  ignores a zone in its input. An untyped literal before `AT TIME ZONE` is
  a timestamptz.
- Output follows the session's `DateStyle` (`ISO`, `Postgres`, `SQL`,
  `German`, with MDY/DMY order) and `IntervalStyle` for date, timestamp,
  timestamptz and interval results over the protocol; `DateStyle`,
  `TimeZone` and `IntervalStyle` given at connection start (`PGDATESTYLE`,
  `PGTZ`, `-c` options) apply, and `RESET` returns to them.
- Infinite intervals (`'infinity'::interval`), timestamp arithmetic with
  them and `isfinite`; `(a, b) OVERLAPS (c, d)` over dates, times and
  timestamps (an interval as the second item) with PostgreSQL's NULL rules;
  `make_date`, `make_time`, `make_timestamp`, `make_timestamptz`,
  `timestamptz(date, time[tz])`, `date + timetz` and the three-argument
  `date_trunc(field, timestamptz, zone)`.
- `to_timestamp(text, text)` and `to_date(text, text)` follow PostgreSQL's
  template parser: every field (`YYYY`..`Y`, `Y,YYY`, `IYYY`, `CC`, `MM`,
  `MON`, `MONTH`, `RM`, `DD`, `DDD`, `D`, `DAY`, `DY`, `IW`, `ID`, `IDDD`,
  `WW`, `W`, `J`, `Q`, `HH`/`HH12`/`HH24`, `MI`, `SS`, `SSSS`, `MS`, `US`,
  `FF1`-`FF6`, `AM`/`PM`, `AD`/`BC`, `TZH`, `TZM`, `TZ`, `OF`), the `FM`,
  `FX` and `TH` modifiers, separator and whitespace skipping, and its error
  texts. `to_char` adds `CC`, `AD`/`BC`, `FF1`-`FF6`, the ISO week fields,
  `RM`, `TZ`, `TZH`, `TZM` and `OF`, and returns NULL for infinite values.
- `ROWS FROM (f(...), g(...)) [WITH ORDINALITY]` over single-column
  functions, and multi-array `unnest(a, b, ...)` in FROM, zip the functions'
  rows and pad the shorter ones with NULL.
- Literal `IN` lists, single-column `IN` subqueries, `BETWEEN`, and
  comparison `ANY`/`ALL` over array literals.
- JSON access with dotted metadata paths, `->`, and `->>`.
- Common casts among booleans, integers, floats, exact textual/integer
  `numeric` values, text, JSON/JSONB, date/time-like strings, `interval`
  strings, UUID, regclass, bytea, and vector values.

Casts from a string type (`text`, `varchar`, `char(n)`, `name`, an unknown-typed literal, or a
domain over one of them) to `int2`/`int4`/`int8` read the text with the integer type's input
function in every evaluation path: select list, `WHERE`, joins, `ORDER BY`, views,
`INSERT ... SELECT`, SQL and PL/pgSQL function bodies, and PL/pgSQL assignment (`i := t` for a
text `t`). `'34.5'::text::int` is `22P02 invalid input syntax for type integer: "34.5"`, as in
PostgreSQL; numeric sources still round half away from zero (`34.5::numeric::int` is 35) and
float sources half to even. A numeric `jsonb` value casts to an integer through numeric; other
`jsonb` values fail with `cannot cast jsonb <kind> to type integer` (`22023`).

Input errors carry PostgreSQL's DETAIL (and, for JSON, CONTEXT) lines: `json`/`jsonb` text
(`Expected JSON value, but found "]".`, `Token "01" is invalid.`, ... with
`JSON data, line 1: ...`), multirange literals (`Missing left brace.`, `Expected range start.`,
`Expected comma or end of multirange.`, `Junk after closing right brace.`), and `parse_ident`.

`to_number(text, text)` reads the number picture of `to_char` (`9`, `0`, `.`, `D`, `,`, `G`, `S`,
`MI`, `PL`, `SG`, `PR`, `L`, `TH`, `V`, `FM`, `RN`) in the C locale, with PostgreSQL's format
errors; `parse_ident(text [, strict])` splits a qualified name as PostgreSQL does.

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


## String constants

String constants are decoded exactly as PostgreSQL's lexer decodes them, before
any other processing, so every statement form sees the same value:

- `'...'` with `''` for a quote; with `SET standard_conforming_strings = off`
  plain constants take backslash escapes as in `E'...'` (the setting is
  reported to clients with ParameterStatus, and `SHOW`/`current_setting`
  return it).
- Escape strings `E'...'`: `\b \f \n \r \t \v`, octal `\ooo`, hex `\xhh`
  (byte escapes may form multi-byte UTF-8 characters), `\uXXXX` and
  `\UXXXXXXXX` with UTF-16 surrogate pairs, `\'`, `\\`, any other escaped
  character standing for itself. Invalid escapes report PostgreSQL's errors
  and SQLSTATEs (`invalid Unicode escape` 22025, `invalid Unicode surrogate
  pair` / `invalid Unicode escape value` 42601, `invalid byte sequence for
  encoding "UTF8"` 22021).
- Unicode-escape strings and identifiers `U&'...'`, `U&"..."` with
  `\XXXX`, `\+XXXXXX` and `UESCAPE 'c'`; rejected (0A000) while
  standard_conforming_strings is off, as in PostgreSQL.
- Constants continued across lines (`'a'` newline `'b'` is `'ab'`); two
  constants without a newline between them are a syntax error.
- Dollar quoting `$$...$$`, `$tag$...$tag$` is taken verbatim.
- `quote_literal` and `format('%L')` write a value holding a backslash as
  `E'...'` with doubled backslashes, as PostgreSQL does.
- With standard_conforming_strings off, a backslash escape in a plain
  constant gives PostgreSQL's WARNING (`nonstandard use of \\ in a string
  literal`, SQLSTATE 22P06) unless `escape_string_warning` is off. Statements
  sent with the extended protocol's Parse do not report it yet.

## Quantified operators

`x OP ANY (array)` and `x OP ALL (array)` also work for `@@`, `@>`, `<@`,
`&&`, `?`, `?|`, `?&` and custom operators, with PostgreSQL's result for
NULL elements, a NULL array and an empty array, and for a subquery in place
of the array. An array written out (`ARRAY[...]`, `'{a,b}'`) compares each
element with its own type; an array from a column is read with `unnest`,
whose elements carry no type, so an operator that needs typed operands
fails there.

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

## Text search operators and functions

- An untyped literal next to a text search value takes that value's
  type, as in PostgreSQL: `tsvector @@ 'a & b'`, `tsquery && 'c'`,
  `tsquery || 'c'`, `'a' <-> 'b'::tsquery` (the phrase operator) and
  `tsvector || 'b:2'`.
- `ts_lexize(dictionary, token)` works for `simple` and each built-in
  `<language>_stem` dictionary.
- The scalar functions `to_tsvector`, `to_tsquery`, `plainto_tsquery`,
  `phraseto_tsquery` and `websearch_to_tsquery` can be used in FROM
  (`FROM to_tsquery('english', 'x') AS q`).
- tsvector input follows PostgreSQL's parser: weight letters are
  case-insensitive, `*` means A, and a second weight on one position is
  an error. Syntax errors report SQLSTATE 42601.
- `ts_delete` and `setweight` ignore NULL elements of their lexeme
  arrays. `ts_filter` rejects NULL weights, and `array_to_tsvector`
  rejects NULL and empty lexemes, with PostgreSQL's messages.

- `ts_token_type('default' | 3722)` lists the default parser's token
  types; `ts_stat(query [, weights])` runs its query with the caller's
  privileges and returns `word`, `ndoc`, `nentry`.
- `SET`/`RESET`/`SHOW default_text_search_config` (default
  `pg_catalog.english`) choose the configuration of the one-argument
  `to_tsvector`, `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery`,
  `websearch_to_tsquery` and `ts_headline` forms and of
  `get_current_ts_config()`.
- `ts_rank_cd` leaves out positions whose weight the query operand
  excludes (`s:A`), as PostgreSQL's cover density does.

Limits: these fail instead of answering:
- `ts_debug`, `ts_parse` and `unnest(tsvector)`
- `CREATE TEXT SEARCH CONFIGURATION/DICTIONARY`
