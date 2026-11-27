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
| `SET work_mem = '64MB'` and other planner/resource settings (`maintenance_work_mem`, `effective_cache_size`, `temp_buffers`, `random_page_cost`, `seq_page_cost`, the `cpu_*` and `parallel_*` costs, `enable_*` planner switches, `geqo`, `plan_cache_mode`, `constraint_exclusion`, collapse limits, `default_statistics_target`, parallel worker counts, `jit_*_cost`) | "SET work_mem is not supported" | accepted with PostgreSQL's validation and normalisation (`65536` -> `64MB`, invalid values and out-of-range numbers are `22023` with PostgreSQL's messages); `SHOW`, `current_setting`, `set_config` (which returns the normalised value), `RESET`, `RESET ALL` and `SET LOCAL` behave as in PostgreSQL. BicDB's planner does not read these knobs. Durability and isolation settings such as `synchronous_commit` remain refused rather than accepted without effect |
| `COMMENT ON TABLE t` / `COMMENT ON COLUMN t.c` then `obj_description(oid, 'pg_class')`, `col_description(oid, attnum)`, `pg_description` | comment discarded; functions returned NULL | stored with the table (transactional, removed by `IS NULL` or `''`), requires table ownership; missing tables are `42P01`, missing columns `42703`. Comments on other object kinds are still accepted without being stored, except types and domains |
| `has_table_privilege('pg_class', 'SELECT')` and other system catalogs | false, even for a superuser | a superuser holds every privilege; other roles may SELECT every catalog except the secret-holding ones (`pg_authid`, `pg_statistic`, ...). The two-argument form now answers for `current_user` (it used the bootstrap role), and NULL arguments return NULL |
| `ts_headline('english'::regconfig, doc, query [, options])` | the document was parsed as a tsquery | all overloads resolve by argument types |
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

## Frappe Framework v16 qualification (2026-11)

The [Frappe Framework](frappe-compatibility.md) runs in its PostgreSQL mode
(`bench new-site --db-type postgres`, psycopg2) against a BicDB cluster with
SCRAM authentication: site creation, login, document CRUD, custom DocTypes
created through the REST API, and a slice of Frappe's own test suite, checked
by `.github/workflows/frappe.yml`. The page lists the engine fixes, the test
results and the remaining gaps.

## SQL string formatting

`format()` accepts positional and sequential arguments, fixed widths and
widths supplied as arguments, and left or right alignment. `%I`, `%L`, and
`%s` use PostgreSQL identifier quoting, literal quoting, and string output.
Mixing positional and sequential references advances from the most recently
consumed argument. An explicit NULL variadic array supplies zero arguments.

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

Row-context operator result types resolve their operands from the row's schema
before applying context-free fallbacks. This preserves JSONB concatenation in
CASE branches rather than rejecting it as a text/JSONB mismatch. Temporary
row-limit rewrites also suspend routine-node type memos so an ephemeral AST
address cannot carry a different expression's cached type into a later call.
The Carrier compatibility suite covers direct row projection and the EHR-shaped
paginated JSONB feed with changed limits on repeated invocations. The regression
fails before the fix; 55 Carrier/type/JSON storage and composite checks pass.

COPY FROM preserves single-column blank records: text represents an empty string,
CSV represents NULL unless quoted, and a blank CSV header is consumed as a header.
An empty input remains zero rows and CRLF blank records remain real rows. The
original wire regression drops a row; all 17 focused COPY protocol checks pass
after the fix. This was exposed by the EHR transfer's count/hash reconciliation,
which refused the incomplete target instead of admitting it.

Parameter description for positional calls to stored functions reads the declared
input types, including calls inside a derived table. It does not compile or
execute the function body to discover those types. This preserves UUID, bigint,
date, JSONB and boolean parameter types in the EHR's 13-argument goal command;
previously every argument was described as unknown. The focused regression fails
without the fix and verifies that a volatile body's insert never runs during
description. All 8 type-consistency and 35 Carrier compatibility tests pass.
This is query-metadata qualification; the full hosted EHR goal workflow is
qualified separately and is not implied by these unit results.

Extended-protocol Bind also preserves a resolved JSON/JSONB parameter's SQL
identity after decoding its wire payload. A valid JSON object must not become a
SQL text value when the query omits an explicit cast. The regression covers
objects, arrays, numbers, strings, JSON null and booleans in text and binary
parameter formats. It fails before the fix; all 179 protocol tests pass after
restoration. This complements stored-function argument description; it does not
claim that the remaining hosted EHR goal and nested-query failures are resolved.

Unfiltered scalar command wrappers of the form `SELECT ... FROM (SELECT f(...)
AS result) q` execute their inner command once in the owning session rather than
the read-only per-row host. Its returned fields retain their logical types and
the complete outer statement owns transaction effects, so an outer projection
error rolls back the command. Explicit transaction rollback, denied routine
EXECUTE and an outer LIMIT 0 are covered. The new regression fails with the
read-only-host error before the fix; all 36 Carrier compatibility and 8 type tests
pass after restoration. Joins, lateral/correlated sources, filtered wrappers,
nonzero outer row-count clauses and projection set-returning sources retain their
existing execution path. This adds the scalar command shape used by the EHR;
the complete clinical workflow and deployment are qualified separately.

`DROP FUNCTION` with an explicit signature checks the current routine's input
argument types before ownership or deletion. A cleanup for an obsolete two-input
signature therefore preserves a current three-input table function. Wrong types
and an empty signature also remain misses; `IF EXISTS` is harmless, otherwise
the missing function reports 42883. Matching drops still require ownership and
remain transactional. Builtin type aliases match by OID; OUT slots, names,
defaults and typmods do not change input identity. The regression reproduces the
EHR reflection function being deleted before the fix, then passes with all 61
focused Carrier/type/assignment/ownership checks. This does not add general
routine overloading or change procedure resolution. A database already migrated
with the old deletion bug must be recreated or repaired deliberately; replaying
existing successful migration receipts does not restore the missing function.

## NOTICE and WARNING messages

BicDB sends the notices PostgreSQL sends for these statements, with PostgreSQL's
severity and SQLSTATE:

- `DROP TABLE/VIEW/INDEX/SEQUENCE/TYPE/DOMAIN/SCHEMA/ROLE/COLLATION/FUNCTION/PROCEDURE
  IF EXISTS`, `DROP TRIGGER/POLICY IF EXISTS ... ON t`, `ALTER TABLE IF EXISTS`,
  `ALTER TABLE ... DROP COLUMN/CONSTRAINT IF EXISTS`: `... does not exist, skipping`.
- `CREATE TABLE/INDEX/SEQUENCE/SCHEMA IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`:
  `... already exists, skipping`.
- `DROP ... CASCADE`: `drop cascades to view v` (or `drop cascades to N other objects`
  with one DETAIL line per object); `TRUNCATE ... CASCADE`: `truncate cascades to table "c"`.
- `COMMIT`/`ROLLBACK` outside a transaction block: `WARNING: there is no transaction in
  progress`; `BEGIN` inside one: `WARNING: there is already a transaction in progress`.
- PL/pgSQL `RAISE WARNING` and `RAISE INFO` keep their severity; `RAISE LOG`/`DEBUG` are
  not sent at the default `client_min_messages`.
- Notices raised by triggers during `COPY FROM STDIN` and by deferred triggers at
  `COMMIT` are sent before the command completes.

## INSERT defaults, output column names, rules and error messages (2026-11)

- `INSERT INTO t VALUES (...)` and `INSERT INTO t SELECT ...` without a column
  list fill the leading columns and give the remaining ones their defaults (or
  NULL), as in PostgreSQL. Too many values fail with 42601 `INSERT has more
  expressions than target columns`; an explicit column list that does not match
  the values fails with 42601 `INSERT has more target columns than expressions`
  (or the reverse), and VALUES rows of different lengths with `VALUES lists
  must all be the same length`.
- Result columns are named as PostgreSQL names them in RowDescription:
  `?column?` for operators and literals, the function name for calls, the type
  name for casts of literals (`'1'::int` is `int4`), `case`, `exists`, `array`,
  `row`, a scalar subquery's own column name, the column name for qualified
  references, and `column1..N` for VALUES. Derived tables, CTEs and view
  columns use the same names. Databases with the Oracle profile keep naming an
  unaliased expression by its text.
- Rules: `CREATE RULE ... DO ALSO` (the default) and `DO INSTEAD` with one or
  more actions or `NOTHING`, on INSERT, UPDATE and DELETE, for tables and
  views. Actions run once per affected row with `NEW` (INSERT, UPDATE) and
  `OLD` (UPDATE, DELETE) bound to its values; an INSERT runs before its rules'
  actions and an UPDATE or DELETE after them, and an INSTEAD rule suppresses
  the statement. A `NOTIFY` action is delivered at commit. Conditional
  (`WHERE`) rules, and `UPDATE ... FROM` / `DELETE ... USING` on a relation
  with rules, are not supported (0A000). Actions run with the caller's
  privileges.
- Error messages on the PostgreSQL protocol use PostgreSQL's wording (embedded
  callers keep BicDB's `Display` text):
  - missing relation: 42P01 `relation "t" does not exist`, also for INSERT,
    UPDATE and DELETE targets (checked before privileges, as in PostgreSQL);
    `DROP TABLE` of a missing table: 42P01 `table "t" does not exist`;
  - missing column: 42703 `column "c" does not exist` / `column t.c does not
    exist` in queries, `column "c" of relation "t" does not exist` for
    INSERT and UPDATE targets (an UPDATE now checks its SET targets even when
    no row matches);
  - missing function: 42883 `function f(integer) does not exist`;
  - permission: 42501 `permission denied for table|view|materialized view|sequence x`
    without BicDB's `authorization denied:` prefix;
  - syntax: 42601 `syntax error at or near "X"` / `syntax error at end of input`;
  - duplicate relation: 42P07 `relation "t" already exists`;
  - a referenced parent row: 23503 `update or delete on table "p" violates
    foreign key constraint "c_p_fkey" on table "c"`;
  - unsupported features lose the `unsupported SQL:` prefix (still 0A000).

## Result types, statement-end foreign keys and COPY (2026-11)

- Result descriptions carry PostgreSQL's types and type modifiers: casts keep
  their modifier (`'x'::varchar(3)` describes as `character varying(3)`,
  `numeric(5,2)`, `timestamp(0)`, `bit(3)`), `date_trunc` on a date returns
  `timestamp with time zone` (its value too), math functions with float8 and
  numeric forms (`ceil`, `floor`, `round`, `trunc`, `sqrt`, `power`, `exp`,
  `ln`, `log`, `cbrt`) return numeric for a numeric argument and double
  precision otherwise, `random()` is double precision, `generate_series` takes
  its argument type, `to_number` is numeric, `to_timestamp` timestamptz,
  `make_date` date, `localtimestamp` timestamp, `position` integer and
  `sum(real)` real. A bare `VALUES ...` or `TABLE name` statement can be
  described.
- Non-deferred foreign keys that reference their own table are satisfied by
  rows the same INSERT or UPDATE writes (`INSERT INTO tree VALUES (2, 1),
  (1, NULL)`), as PostgreSQL checks them at statement end.
- `COPY ... TO STDOUT (HEADER)` writes the header line (CSV and text); CSV
  output quotes an empty string so it reads back as text. `COPY ... FROM STDIN
  (HEADER MATCH)` checks the header names (22P04). `COPY ... (FORMAT binary)`
  and the `BINARY` keyword read and write PostgreSQL's binary COPY format for
  built-in types; HEADER with binary is refused (42601).
- Server-side COPY: `COPY t FROM 'file'`, `COPY t|(query) TO 'file'` and
  `COPY ... FROM/TO PROGRAM 'command'`, with every format and option COPY
  STDIN/STDOUT takes, PostgreSQL's privilege checks (`pg_read_server_files`,
  `pg_write_server_files`, `pg_execute_server_program`) and its file and
  program errors (42602, 58P01, 42809, 38000 with the exit status). It is off
  unless the server is started with `--copy-server-files-dir DIR` (files, only
  below DIR, no symbolic links) and, separately, `--allow-copy-program`;
  otherwise 0A000. Only top-level client statements can use it; COPY in a
  routine is refused. See [server-side COPY](server-side-copy.md).
- COPY leaves generated columns (stored and virtual) out of its default column
  list: COPY FROM computes them and COPY TO does not write them. Naming one in
  the column list is refused before COPY starts (42P10), and so is a
  `COPY ... FROM ... WHERE` condition that reads one.
- COPY options are read in both PostgreSQL syntaxes (the legacy list after
  `WITH`, `USING DELIMITERS`, and the parenthesized list) and checked as
  PostgreSQL checks them, with its errors and option positions. DELIMITER,
  NULL, QUOTE, ESCAPE, FORCE_QUOTE, FORCE_NOT_NULL, FORCE_NULL and HEADER
  [MATCH] work for text and CSV in both directions, with PostgreSQL's
  backslash escapes in text. ENCODING takes UTF8 and its aliases, SQL_ASCII,
  and LATIN1 (converted both ways); other encodings are refused with 0A000
  before COPY starts. No option is accepted and ignored.
- COPY FROM rules, as in PostgreSQL:
  - `ON_ERROR IGNORE` skips rows a column type's input refuses (domain
    constraints included) and reports `N rows were skipped due to data type
    incompatibility`; `LOG_VERBOSITY VERBOSE` adds one NOTICE per skipped
    row, `SILENT` none; `REJECT_LIMIT n` fails the COPY once more than n rows
    were skipped. Missing or extra fields and table constraint violations
    still fail the COPY.
  - `DEFAULT 'marker'`: a field equal to the marker takes the column default
    (22P04 "unexpected default marker in COPY data" for a column without
    one).
  - `COPY ... FROM ... WHERE condition` loads only the rows for which the
    condition is true. Subqueries, aggregates, window and set-returning
    functions are refused as in PostgreSQL.
  - `FREEZE` is accepted only where PostgreSQL accepts it: in a transaction
    block, on a table the current subtransaction created or truncated (55000
    otherwise; 0A000 for a partitioned table). BicDB has no separate frozen
    row visibility, so where it is accepted it changes nothing: the rows are
    visible exactly as other rows the transaction wrote.

## Statistics aggregates, partitions and lock privileges (2026-11)

- `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`
  return numeric for integer and numeric input (PostgreSQL's scale rules)
  and double precision for real and double precision input. `corr`,
  `covar_pop`, `covar_samp` and every `regr_*` aggregate use PostgreSQL's
  Youngs-Cramer accumulation, so results match to the last digit;
  `regr_count` is bigint. `bit_and`, `bit_or` and `bit_xor` keep their
  integer input type. The window forms of these aggregates are not
  supported (0A000).
- An aggregate without FROM (`SELECT sum(1)`, `SELECT var_pop(1.0)`) reads
  one row. `make_date`, `make_time` and `make_timestamp` are available (BC
  years as negative years, 22008 for out-of-range fields).
  `pg_typeof('a')` is `unknown`. A `VALUES` list keeps a type modifier
  shared by every row in its description.
- A partition can itself be partitioned (`CREATE TABLE p1 PARTITION OF p
  FOR VALUES ... PARTITION BY ...`, also with DEFAULT). Rows route through
  every level. `pg_partition_tree`, `pg_partition_ancestors` and
  `pg_partition_root` report the tree. Siblings in `pg_partition_tree` are
  listed by name, not by OID.
- ATTACH PARTITION matches columns by name, so a partition may order its
  columns differently from the parent. A mismatch fails with PostgreSQL's
  message (42804): a column not in the parent, a missing column, a different
  type or a missing NOT NULL. Rows that break the bound fail with
  `partition constraint of relation "x" is violated by some row`, and an
  overlap with `partition "x" would overlap partition "y"` (42P17). Bound
  expressions that reference a column, an aggregate, a subquery or a
  set-returning function fail with PostgreSQL's messages. Expression
  partition keys are supported (see below).
- LOCK TABLE follows PostgreSQL's privilege rules: UPDATE, DELETE, TRUNCATE
  or MAINTAIN allow any mode, INSERT allows modes up to ROW EXCLUSIVE, and
  SELECT allows ACCESS SHARE. TRUNCATE needs the TRUNCATE privilege (or
  ownership).
- COPY FROM STDIN finds its table the way the session does (search_path).
  If the table cannot be found, the statement fails before COPY starts. A
  simple-query client that sends something other than COPY data during COPY
  gets an error and the connection returns to idle; it no longer waits for a
  Sync that such a client never sends.
- A role with ADMIN OPTION on a role, held directly or through a role it
  belongs to, can grant and revoke that role, as in PostgreSQL. No role holds
  ADMIN OPTION on itself. Other roles get 42501 `permission denied to grant
  role "x"`, with PostgreSQL's DETAIL. Superusers and CREATEROLE roles keep
  their existing rights. `current_role` works in a SELECT without FROM.

## Partition keys, row movement, partition indexes and predefined roles (2026-11)

- Partition keys may be expressions: `PARTITION BY RANGE ((b + 0))`,
  `LIST (lower(a))`, `RANGE (a, abs(b), c)`, `HASH ((a + 1))`. Rows route,
  direct inserts into a partition are bound-checked, ATTACH validates existing
  rows and bounds are typed by the expression. A cast is an expression key
  (`(a::text)` orders as text). `((a))` is a column key. CREATE fails with
  PostgreSQL's errors for an unknown column (42703), a constant key, a mutable
  function such as `random()` (42P17), an aggregate (42803), a window
  function, a subquery or a set-returning function. Functions BicDB cannot
  guarantee to be immutable, user-defined ones included, fail with 0A000.
  A PRIMARY KEY or UNIQUE constraint on such a table fails with 0A000 and
  PostgreSQL's DETAIL. A key column cannot be dropped or retyped (42P16).
  `pg_get_partkeydef` prints `RANGE (a, ((b + 0)))`. `pg_partitioned_table`
  shows `partattrs` 0 for an expression and a non-null `partexprs` (it holds
  the expression's SQL text, not a node tree).
- UPDATE moves a row to the partition its new key belongs to, across levels
  and only within the table it names. A row that fits no partition fails the
  whole statement (23514). CHECK and NOT NULL errors name the partition that
  receives the row. `RETURNING tableoid` gives that partition.
- CREATE INDEX on a partitioned table creates an index on every partition,
  at every level, named `<partition>_<columns>_idx`. New partitions and
  attached partitions get these indexes; ATTACH uses an equivalent index the
  table already has. DETACH keeps the partition's indexes. A partition's
  attached index cannot be dropped by itself (2BP01); dropping the parent's
  index drops them all. A UNIQUE index must include the partition key
  (0A000). Indexes of partitioned tables have relkind `I`. `pg_inherits`
  lists index parents.
- CHECK constraints and SET / DROP NOT NULL added to a partitioned table are
  added to every partition, and existing rows are validated. ATTACH PARTITION
  requires every CHECK constraint of the parent on the table, with the same
  name and definition (42804 `child table is missing constraint "x"`).
  A routed row is checked against the partition that receives it first, so
  CHECK, NOT NULL and foreign key errors name that partition. A partition key
  that lists more than one column for LIST, an unknown strategy, a row or
  untyped literal key, or a user-defined function in the key fails with
  PostgreSQL's errors. A user-defined IMMUTABLE function in a key that reads
  a column fails with 0A000.
- `GRANT/REVOKE USAGE|ALL ON TYPE|DOMAIN` work. `ON DOMAIN` with a non-domain
  fails with 42809, and an array type fails with 0LP01. USAGE is checked for
  CREATE TABLE, ALTER TABLE ADD COLUMN / ALTER COLUMN TYPE, CREATE DOMAIN,
  composite attributes, and function argument and result types. The error is
  `permission denied for type x` (42501).
- The predefined roles (`pg_read_all_data`, `pg_write_all_data`,
  `pg_maintain`, `pg_monitor`, `pg_read_all_settings`, `pg_signal_backend`,
  ...) exist and cannot log in. `pg_read_all_data` grants SELECT on every
  table and `pg_write_all_data` grants INSERT, UPDATE and DELETE. Neither
  bypasses row security or gives TRUNCATE. `pg_maintain` allows LOCK TABLE.
  ANALYZE still needs ownership. Only a superuser or a holder of ADMIN OPTION
  can grant or revoke a predefined role. CREATEROLE is not enough.
  `pg_`-prefixed role names are reserved (42939), and predefined roles cannot
  be dropped (2BP01) or altered. A connection that names a predefined role is
  refused.
- Not yet supported: `Failing row contains ...` / `Partition key of the
  failing row contains ...` DETAIL lines, GRANT on large objects,
  `has_largeobject_privilege`, the `MAINTAIN` privilege keyword, and
  `GRANTED BY` (still refused with 0A000).

## PL/pgSQL exceptions, partitioned foreign keys and more privileges (2026-11)

- `EXCEPTION WHEN` accepts every PostgreSQL condition name (all of
  errcodes.txt), `SQLSTATE 'xxxxx'` and `OR` lists. A category condition
  (`integrity_constraint_violation`, `SQLSTATE '22000'`) traps its whole
  class. `OTHERS` never traps `query_canceled` or `assert_failure`. An
  unknown name fails with 42704 `unrecognized exception condition`. A
  handled block's writes are undone and the outer block's writes stay.
- `ASSERT condition [, message]` raises P0004. `RAISE condition_name USING
  ...`, `RAISE USING ...` and `RAISE 'format', ...` default to EXCEPTION.
  `RAISE ... USING HINT/COLUMN/CONSTRAINT/DATATYPE/TABLE/SCHEMA` send those
  diagnostic fields.
- `GET STACKED DIAGNOSTICS` supports RETURNED_SQLSTATE, MESSAGE_TEXT,
  PG_EXCEPTION_DETAIL, PG_EXCEPTION_HINT, COLUMN_NAME, CONSTRAINT_NAME,
  PG_DATATYPE_NAME, TABLE_NAME and SCHEMA_NAME. PG_EXCEPTION_CONTEXT is
  always empty. Outside a handler it fails with 0Z002.
- `FOR target IN EXECUTE command [USING ...]` iterates rows from a dynamic
  statement, including `EXPLAIN` and data changes with `RETURNING`. Dynamic
  SQL sees only the explicitly supplied parameters. A non-row-returning
  command is rejected before it runs.
- The postfix `x ISNULL` / `x NOTNULL` operators work in PL/pgSQL
  expressions and conditions.
- A foreign key that references a partitioned table protects rows in every
  partition, also when a partition is named directly. A blocked delete names
  the partition and its derived constraint (`<name>_<n>`). `pg_constraint`
  has one row per referenced partition. A foreign key declared on a
  partitioned table is reported by that table. Partition rows have a non-zero
  `conparentid`. ATTACH PARTITION checks the rows against the key and adds the
  key to the partition.
- `GRANT/REVOKE {SELECT|UPDATE|ALL} ON LARGE OBJECT` work. Only the owner or
  a superuser can grant. `has_largeobject_privilege` works and returns NULL
  for a missing object. `pg_largeobject_metadata` lists large objects and
  their ACL.
- The `MAINTAIN` table privilege can be granted and revoked, and `ALL` on
  tables includes it (`m` in ACLs). The owner, a MAINTAIN grantee or a
  `pg_maintain` member can run ANALYZE. Statistics stay behind `pg_stats`,
  which lists a column only to roles that can SELECT it (a column grant is
  enough). REFRESH MATERIALIZED VIEW still needs ownership.
- `GRANTED BY` accepts only the current role (0A000 `grantor must be current
  user` otherwise), as PostgreSQL does for object privileges.
- `DROP TYPE ... CASCADE` reports the columns it drops (`drop cascades to column c of
  table t`); without CASCADE it fails with PostgreSQL's DETAIL and HINT.
- An identifier longer than 63 bytes in statement text is cut to 63 bytes, as
  PostgreSQL's lexer does, with `NOTICE: identifier "..." will be truncated to "..."`
  (not inside string literals, comments or dollar-quoted bodies). Statements built
  through the embedding API's parsed-statement entry point are not cut.
## EXPLAIN in PostgreSQL's plan layout (2026-11)

`EXPLAIN` with an option list (`EXPLAIN (COSTS OFF) ...`, `(VERBOSE)`,
`(FORMAT JSON|YAML|XML)`, `(ANALYZE, TIMING OFF, ...)`) prints the plan in
PostgreSQL's node vocabulary and layout: `Seq Scan on t`, `Index Scan using i
on t` with `Index Cond:`, `Filter:`, `Sort` / `Sort Key:`, `Limit`,
`Aggregate` / `HashAggregate` with `Group Key:`, `WindowAgg`, `Unique`,
`Append`, `HashSetOp`, `Hash Join` / `Hash Cond:` over `Hash`, `Nested Loop`
with `Join Filter:`, `Subquery Scan`, `CTE Scan`, `Function Scan`,
`Values Scan`, `Result` / `One-Time Filter:`, InitPlans and SubPlans, and
`Insert on t` / `Update on t` / `Delete on t` (with ON CONFLICT resolution and
arbiter indexes). Qualifications are printed as PostgreSQL deparses them
(`(a = 1)`, `(b = 'x'::text)`, `(c = '1'::numeric)`, `(a = ANY
('{1,2}'::integer[]))`, `~~` for LIKE, dates in the session's DateStyle).
The plan describes what BicDB executes, from the decisions of the executor's
own choosers, so it differs from PostgreSQL's where the planners differ. Node
names follow what runs: the row-set index locator collects matching row ids
and then fetches the rows in row-id order, a `Bitmap Heap Scan` over a
`Bitmap Index Scan` (a `BitmapOr` when it unions OR branches, `Recheck Cond:`
with the indexed terms); an `Index Scan` is a read in index order or of at
most one row (primary key, full-key unique equality); joins appear in the
executor's join order with its join method (a `Nested Loop` whose inner
side is looked up per outer row, a `Nested Loop` over a `Materialize`d inner
side read once, or a `Hash Join`); an uncorrelated one-column `IN
(subquery)` is a `hashed SubPlan`, and a top-level `EXISTS (SELECT ... WHERE
inner.k = outer.c ...)`, `NOT EXISTS` or `IN` answered from a hash table of
the subquery's keys is a `Hash Semi Join` / `Hash Anti Join`. A node type the
session turned off with an `enable_*` setting is marked `Disabled: true`
(BicDB's executor does not read those settings). There are no Index Only,
Memoize, Merge Join or parallel nodes; derived tables are Subquery Scans
over their materialized rows. `tests/explain_matches_execution.rs` checks
every scan, join and subplan node against the path recorded while the
statement executes. INSERT, UPDATE,
DELETE, CREATE TABLE AS and set operations are explainable, and EXPLAIN
requires the explained statement's privileges. The option list is parsed as
PostgreSQL parses it, with its errors (`unrecognized EXPLAIN option`,
`EXPLAIN option TIMING requires ANALYZE`, ...).

Not yet covered: estimated costs (`COSTS ON` prints the nodes without cost
figures), `Output:` lists under VERBOSE, `EXPLAIN ANALYZE` of plans with more
than one scan (refused with `0A000`; single-scan plans and INSERT/UPDATE/DELETE
over one scan report their actual rows), and `EXPLAIN EXECUTE`. A bare
`EXPLAIN [ANALYZE] SELECT ...` keeps BicDB's native access-path report
(`FullScan`, `IndexScan ...`, `EstimatedRows`, `ActualRows`), which tools and
tests read; statement shapes that report does not cover fall back to the
PostgreSQL layout.

## Temporary objects (2026-11)

- `CREATE TEMP VIEW`, `CREATE OR REPLACE TEMP VIEW`, `CREATE VIEW pg_temp.v`
  and `CREATE TEMP SEQUENCE` create session objects in the session's
  temporary schema (`pg_temp_N`), dropped with the session. A view over a
  temporary relation becomes temporary with PostgreSQL's NOTICE; in an
  explicit permanent schema it is 42P16, and a materialized view over one is
  0A000. Rules of temporary relations go with them.
- Temporary tables are found, the temporary schema first, by `CREATE TABLE
  ... (LIKE t)`, `INHERITS`, `COMMENT ON`, `has_table_privilege`, rule
  actions, `nextval`/`currval`/`setval` and regclass (which prints a visible
  temporary relation unqualified), as well as by queries and DML. A permanent
  table cannot inherit from (42809) or reference (42P16) a temporary one.
- `CREATE TEMP TABLE ... PARTITION OF` and `CREATE TEMP TABLE t(cols) ON
  COMMIT ... AS query` work; ON COMMIT DROP / DELETE ROWS / PRESERVE ROWS
  apply as before.
- Not yet: domains, types and operators in `pg_temp` (refused: "schema
  pg_temp does not exist"), and a temporary function named like an existing
  permanent one (refused as already existing).

## Foreign-data wrappers, servers and user mappings (2026-11)

`CREATE / ALTER / DROP FOREIGN DATA WRAPPER`, `SERVER` and `USER MAPPING`,
`GRANT / REVOKE USAGE ON FOREIGN DATA WRAPPER | FOREIGN SERVER` (with
`WITH GRANT OPTION`, `GRANT OPTION FOR` and `CASCADE`), `COMMENT ON` both and
`IMPORT FOREIGN SCHEMA` follow PostgreSQL 18.4: generic `OPTIONS` lists with
`ADD` / `SET` / `DROP`, the built-in `postgresql_fdw_validator` and its
`invalid option` hints, superuser-only wrappers, owner and USAGE checks for
servers and mappings, `DROP ... CASCADE` dependencies and their notices, and
transactional rollback. The catalogs `pg_foreign_data_wrapper`,
`pg_foreign_server`, `pg_user_mapping` (superusers only), `pg_user_mappings`
(options shown only to the roles PostgreSQL shows them to),
`pg_options_to_table()`, `has_foreign_data_wrapper_privilege()`,
`has_server_privilege()` and the information schema views
`foreign_data_wrappers`, `foreign_data_wrapper_options`, `foreign_servers`,
`foreign_server_options`, `user_mappings` and `user_mapping_options` report
them. A role named by any of these objects cannot be dropped.

BicDB has no wrapper handlers: `HANDLER` naming a function is refused
(`0A000`, or PostgreSQL's errors for a missing or wrongly typed function),
user-defined validators are refused with `0A000`, and `IMPORT FOREIGN SCHEMA`
fails with `foreign-data wrapper "..." has no handler`. Foreign tables are
not supported yet.
