# PostgreSQL maintenance statements, soft input errors, temporary tables and error positions

This page records PostgreSQL 18 behaviours BicDB reproduces for maintenance statements, the
non-throwing input functions, temporary tables, FROM-less SELECT evaluation and error positions.
Parity cases: `fixtures/postgres-compat/pgsql-parity/f.cases` (each case reads `same` against
stock PostgreSQL 18.4); focused tests: `crates/bicdb-sql/tests/pgsql_parity_f.rs`.

## VACUUM and ANALYZE

All PostgreSQL forms parse and validate as PostgreSQL does:

- `VACUUM [ ( option [, ...] ) ] [ table [ ( column [, ...] ) ] [, ...] ]`,
  `VACUUM [ FULL ] [ FREEZE ] [ VERBOSE ] [ ANALYZE ] [ table ... ]`,
  `ANALYZE [ ( option [, ...] ) | VERBOSE ] [ table [ ( column [, ...] ) ] [, ...] ]`.
- Options: `FULL`, `FREEZE`, `VERBOSE`, `ANALYZE`, `DISABLE_PAGE_SKIPPING`, `SKIP_LOCKED`,
  `INDEX_CLEANUP { AUTO | ON | OFF }`, `PROCESS_MAIN`, `PROCESS_TOAST`, `TRUNCATE`, `PARALLEL n`,
  `SKIP_DATABASE_STATS`, `ONLY_DATABASE_STATS`, `BUFFER_USAGE_LIMIT`, each with an optional Boolean.
- Errors with PostgreSQL's messages and SQLSTATEs: unknown option (`42601`), missing or
  out-of-range `PARALLEL` value, `FULL` with `PARALLEL`, a column list without `ANALYZE`,
  `ONLY_DATABASE_STATS` with tables or other options, `PROCESS_TOAST false` with `FULL` (`0A000`),
  unknown relation (`42P01`), unknown or repeated column (`42703`, `42701`), and
  `VACUUM cannot run inside a transaction block` (`25001`, also for a multi-statement query
  string). `ANALYZE` runs inside a transaction block.
- A relation the role may not maintain is skipped with PostgreSQL's `WARNING:  permission denied
  to vacuum "t", skipping it` (or `analyze`); a view is skipped with `skipping "v" --- cannot
  vacuum non-tables or special system tables`. Nothing is analyzed for a skipped relation.

Semantics: BicDB reclaims dead row versions with its store-wide reclaimer, automatically in the
background and on demand with plain `VACUUM` (a superuser operation that keeps its BicDB report
row). A table-scoped `VACUUM` has no per-table reclamation, freezing or rewrite to perform, so it
does what PostgreSQL makes visible to SQL: validation, the warnings above, and, with `ANALYZE`,
fresh statistics (`pg_stats`) for the named tables. `ANALYZE t (columns)` refreshes the statistics
of the whole table.

## pg_input_is_valid and pg_input_error_info

`pg_input_is_valid(text, type)` and `SELECT * FROM pg_input_error_info(text, type)` (columns
`message`, `detail`, `hint`, `sql_error_code`) run the type's input conversion, including enum,
domain (with its CHECK constraints) and other user-defined types. As in PostgreSQL:

- data errors of the input conversion (class `22`) and domain constraint failures (class `23`)
  are reported as values; valid input gives a row of NULLs;
- an undefined type (`42704`) and an error raised while evaluating a domain CHECK expression (for
  example a division by zero) are raised, not reported;
- NULL arguments give NULL.

The messages are BicDB's input-error messages for each type; where those differ from PostgreSQL's
the difference shows here as it does for a cast.

## FROM-less SELECT

A SELECT without FROM evaluates functions through the same function table as a SELECT with FROM
(database, session and transaction context included), so a function that works in one works in
the other. Aggregates, GROUP BY and HAVING without FROM read one implicit row:
`SELECT count(*)` is 1, `SELECT count(*) WHERE false` is 0, `SELECT sum(2), avg(3)` works.

Functions added in the shared table: `to_hex`, `to_oct`, `to_bin`, integer `gcd`/`lcm`,
`num_nulls`, `num_nonnulls`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`,
`getdatabaseencoding`, `isfinite`, `scale`, `min_scale`, `trim_scale`, `regexp_count`,
`regexp_instr`, `regexp_like`, `regexp_substr`.

## Temporary tables

For a session served over the PostgreSQL protocol, `CREATE TEMP[ORARY] TABLE t` (or `CREATE TABLE
pg_temp.t`) creates `t` in the session's temporary schema `pg_temp_<backend pid>`:

- the temporary schema is searched first for relations (or where `pg_temp` appears in
  `search_path`), so a temporary table hides a permanent table of the same name, and `DROP TABLE`,
  `DROP INDEX` and every query reach the temporary one first; `pg_temp.t` names it explicitly;
- new permanent objects never go to the temporary schema, and `current_schema()` does not report
  it;
- the temporary schema and everything in it are dropped when the connection ends, on
  `DISCARD TEMP` and `DISCARD ALL`, and at server start for schemas left by an earlier process;
- `pg_class.relpersistence` is `t`;
- `CREATE TEMP TABLE s.t` for a permanent schema fails with `cannot create temporary relation in
  non-temporary schema` (`42P16`), and `ON COMMIT` on a permanent table with `ON COMMIT can only
  be used on temporary tables` (`42P16`).

Not yet supported (fails with `0A000` instead of being accepted with different semantics):
`ON COMMIT DELETE ROWS` and `ON COMMIT DROP`. `ON COMMIT PRESERVE ROWS` is the default and works.
An embedded `SqlSession` (no connection) has no session lifetime, so `TEMP` tables it creates stay
permanent tables in their schema, as before.

## Error positions

ErrorResponse carries PostgreSQL's position field (`P`), so psql prints the `LINE n:` line and the
caret. Positions are reported where PostgreSQL reports them:

- syntax errors: at the offending token, or one past the end for `syntax error at end of input`;
- in DML statements (SELECT, INSERT, UPDATE, DELETE, WITH, TABLE, VALUES, MERGE): unknown columns
  (at the reference, or its qualifier), `column "c" of relation "t" does not exist` in INSERT and
  UPDATE, unknown relations, `missing FROM-clause entry`, unknown types and functions, and invalid
  literals (`invalid input syntax for type ...`, out-of-range values, malformed array, range and
  record literals) at the literal.

Errors PostgreSQL raises without a location (DDL name lookups, run-time errors such as division by
zero, and invalid data that does not come from a literal) carry none. A position is only sent
after it has been checked against the client's statement text.
