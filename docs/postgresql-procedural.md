# PostgreSQL procedural objects

BicDB executes a subset of SQL and PL/pgSQL routines. The earlier v0 description
of all routines and triggers as metadata-only is obsolete. Stored definitions
remain visible through `pg_catalog.pg_proc` and `pg_catalog.pg_trigger`.

## Execution and authority

Supported paths include scalar stored functions, `CALL` procedures, anonymous
`DO` blocks, and row triggers. PL/pgSQL execution includes declarations,
assignments, conditional branches, loops, embedded SQL, dynamic `EXECUTE`,
`PERFORM`, return values, and supported exception conditions. Unsupported
language constructs fail explicitly; accepting a stored definition does not
prove that every statement in its body can execute.

SQL EXECUTE privileges apply to stored functions. Invoker functions retain the
caller's SQL authority; SECURITY DEFINER functions use their recorded owner's
SQL role and restore the caller's role after success or error. Neither path
replaces a verified delegated end-user identity. See
[transaction-scoped delegation](transaction-delegation.md) for identity and pool
reuse, and [role membership](role-membership-options.md) for SQL privileges.

Triggers execute in the statement's transaction and enforce their checks during
INSERT, UPDATE, and DELETE. They are not limited to a special notification
pattern. The authorization integration suites cover routine owners, trigger
execution, and inherited object privileges.

## Function call forms

Calls of SQL and PL/pgSQL functions resolve as PostgreSQL resolves them:

- Named notation `f(b => 2, a => 1)` (also `:=`) and mixed notation
  `f(1, b => 2)`, with defaults filling any omitted parameter. Positional
  arguments must come first (`42601` otherwise); an unknown parameter name, a
  parameter given twice or a missing non-default parameter is `42883`
  ("function ... does not exist").
- `VARIADIC` parameters: trailing arguments are collected into the array
  (`f(1, 2, 3)`), and `f(VARIADIC ARRAY[...])` passes the array itself. A
  non-variadic overload that takes the same arguments is preferred. As in
  PostgreSQL, named notation cannot target the variadic parameter without
  `VARIADIC`; the combined `VARIADIC name => array` form is not supported.
- OUT and INOUT parameters: a function declared without RETURNS returns its
  single OUT parameter's type, or a `record` whose fields are the OUT
  parameters, so `(f(1)).c`, `SELECT * FROM f(1)` and `RETURNS TABLE`
  functions in FROM work for SQL and PL/pgSQL functions.

The call is rewritten to its positional form before execution (named
arguments moved into place, defaults inserted, variadic arguments gathered),
then overload resolution proceeds as for any positional call. Overloads that
would place named arguments differently are refused with `0A000`.

Set-returning functions in a select list follow PostgreSQL 10+ (ProjectSet):
`SELECT srf(3)`, `SELECT id, srf(id) FROM t`, `SELECT (rt(2)).sq` over a
`RETURNS TABLE` function, `SELECT rt(2)` (one record per row) and nested
uses such as `generate_series(1, 3) * 10` repeat the row once per returned
value, several functions advance in lockstep with the shorter ones padded
with NULL, and a row whose functions return nothing disappears. User
functions and nested calls run from the FROM clause (`LATERAL`); in a
grouped, DISTINCT or windowed query they are left as written, and several
record-returning functions in one select list are refused with `0A000`.
PL/pgSQL `RETURN NEXT expression` appends a row to a `SETOF` result; in a
function with OUT parameters it is refused with `42804` as in PostgreSQL.

## User-defined aggregates and casts

`CREATE [OR REPLACE] AGGREGATE name (arg types) (SFUNC = f, STYPE = t
[, FINALFUNC = f] [, INITCOND = 'v'] [, COMBINEFUNC = f] ...)` and `DROP
AGGREGATE [IF EXISTS] name (arg types) [, ...]` define aggregates usable with
GROUP BY, DISTINCT, ORDER BY inside the call, FILTER and as window aggregates
(`OVER (...)`), with SQL or PL/pgSQL transition and final functions and
PostgreSQL's strictness rules (a strict transition function skips NULL inputs
and, without INITCOND, starts from the first non-NULL input). An aggregate is
recorded as a routine (`prokind = 'a'`), so `regproc`, EXECUTE privileges,
ownership, transactional DDL and restart persistence are those of functions,
and `pg_aggregate` lists it. A call is collected with the built-in `array_agg`
(which supplies grouping, ordering, filtering and window frames) and then
folded through the transition function; COMBINEFUNC, the moving-aggregate
options and SORTOP are recorded but not needed for that. Ordered-set and
hypothetical-set aggregates, `FINALFUNC_EXTRA` and aggregates over `*` are
refused with `0A000`.

`CREATE CAST (source AS target) WITH FUNCTION f[(types)] | WITHOUT FUNCTION |
WITH INOUT [AS ASSIGNMENT | AS IMPLICIT]` and `DROP CAST [IF EXISTS] (source AS
target)` are catalogued in `pg_cast` and require ownership of one of the two
types (or a superuser). Explicit casts (`CAST(x AS t)`, `x::t`) of a source
type a user cast converts use it; `AS IMPLICIT` casts also convert arguments of
a user function call whose only candidate of that arity needs them, and `AS
ASSIGNMENT`/`AS IMPLICIT` casts convert `INSERT ... VALUES` items to the column
type. Implicit user casts do not take part in operator or overload resolution
among several candidates.

User-defined operators (`CREATE OPERATOR`) also bind `<->`. `ORDER BY expr
USING <` / `USING >` order ascending / descending; other operators in `USING`
and operator symbols the parser cannot tokenize as one operator (`@@@`, `===`,
prefix `~~~`) are refused with a syntax error.

## Routine DDL, extended statistics and REINDEX

`ALTER ROUTINE` and `DROP ROUTINE` resolve each named signature to the
function or procedure of that name; a missing one fails with `42883`
(`function name(args) does not exist`), or is a notice under `IF EXISTS`.
`ALTER FUNCTION`/`ALTER PROCEDURE` accept `RENAME TO` (refusing a duplicate
signature with `42723`), `OWNER TO`, and the characteristics `IMMUTABLE`,
`STABLE`, `VOLATILE`, `[NOT] LEAKPROOF`, `CALLED ON NULL INPUT`, `STRICT`,
`SECURITY DEFINER|INVOKER`, `PARALLEL`, `COST` and `ROWS`, which are
reflected in `pg_proc` (`provolatile`, `proisstrict`, `prosecdef`). `SET`
configuration clauses are refused with `0A000`.

`CREATE STATISTICS [IF NOT EXISTS] [name] [(kinds)] ON columns-or-expressions
FROM table` is stored in `pg_statistic_ext` (default name
`table_columns_stat`, default kinds `d`, `f`, `m` plus `e` for expressions)
with PostgreSQL's errors (`42P16` for a single column, `42703`, `42P07`,
`42P01`), table ownership required. `ALTER STATISTICS ... RENAME TO | SET
STATISTICS | OWNER TO | SET SCHEMA` and `DROP STATISTICS [IF EXISTS]`
complete the lifecycle; the planner does not use the objects.

`REINDEX [(options)] INDEX | TABLE | SCHEMA | DATABASE | SYSTEM
[CONCURRENTLY] name` validates its target as PostgreSQL does (`42P01`,
`42809`, `3F000`, ownership) and is otherwise a no-op, because BicDB indexes
are maintained transactionally and are never invalid. `REINDEX SCHEMA`,
`DATABASE`, `SYSTEM` and any `CONCURRENTLY` form fail inside a transaction
block with `25001`.

Temporal keys (`PRIMARY KEY`/`UNIQUE` with `WITHOUT OVERLAPS`) are refused
with `0A000`.

## Exported snapshots

`pg_export_snapshot()` in a REPEATABLE READ transaction returns an identifier
that another session's REPEATABLE READ transaction adopts with `SET
TRANSACTION SNAPSHOT 'id'` before its first query; it then reads exactly the
exporter's snapshot. Identifiers are valid while the exporting transaction is
open; others are `22023` (invalid snapshot identifier). Importing into a READ
COMMITTED or SERIALIZABLE transaction, and exporting from a READ COMMITTED
one, are refused with `0A000`.

## SQL cursors

`DECLARE name [BINARY] [ASENSITIVE|INSENSITIVE] [[NO] SCROLL] CURSOR
[WITH|WITHOUT HOLD] FOR query`, `FETCH` and `MOVE` with every direction
(`NEXT`, `PRIOR`, `FIRST`, `LAST`, `ABSOLUTE n`, `RELATIVE n`, `n`, `ALL`,
`FORWARD`/`BACKWARD [n|ALL]`), `CLOSE name|ALL`, the `pg_cursors` view and
`UPDATE/DELETE ... WHERE CURRENT OF name` behave as in PostgreSQL:
DECLARE without HOLD outside a transaction block is `25P01`, missing
cursors `34000`, duplicates `42P03`, backward motion of a non-scrollable
cursor `55000`, and a `WHERE CURRENT OF` on a cursor that is not on a row
`24000`. A cursor reads with the declaring transaction's snapshot and the
declaring role's privileges and row security: its query runs at DECLARE and
its rows are kept, which is also what makes a `WITH HOLD` cursor survive
COMMIT. COMMIT closes cursors without HOLD and ROLLBACK closes every cursor
the transaction declared; `ROLLBACK TO SAVEPOINT` closes the cursors declared
after the savepoint. A cursor declared without SCROLL or NO SCROLL
can move backward when its query is a plain scan of one table or function
(PostgreSQL decides this from the plan). `WHERE CURRENT OF` needs a cursor
over one table with a primary key (others are refused with `0A000`), and a
`FOR UPDATE` cursor locks its rows at DECLARE rather than as they are
fetched.

`pg_notify(channel, payload)` queues a notification exactly like `NOTIFY`:
inside a transaction block both are delivered at commit and discarded on
rollback, and identical channel/payload pairs of one transaction are delivered
once. Empty or over-long channel names and payloads of 8000 bytes or more are
`22023`.

## Migration control flow

Procedural IF matching distinguishes actual statement boundaries from SQL
`ADD COLUMN IF NOT EXISTS`, `DROP INDEX IF EXISTS`, and `SELECT ... FOR UPDATE`.
Comments, quoted identifiers, strings and dollar-quoted bodies do not introduce
nested control blocks. Integer FOR, query FOR and FOREACH loops support unlabeled
`CONTINUE` and `CONTINUE WHEN condition`; they skip only the innermost iteration.
CONTINUE outside a loop and labeled CONTINUE are rejected explicitly.

Declarations can contain constant two-dimensional arrays, and `FOREACH ...
SLICE 1 IN ARRAY` iterates their rows. The pgwire `procedural_migration` regression
executes this form with nested IF checks and dynamic table renames/compatibility
views twice, preserving data through both names.

Dynamic `EXECUTE expression [INTO [STRICT] targets] [USING values]` supports a
single SELECT or DML statement. USING binds typed values; SQL identifiers must
still be quoted in the command expression. Local variable names are never
substituted inside dynamic SQL. GET [CURRENT] DIAGNOSTICS supports ROW_COUNT for
the preceding SQL command. Dynamic EXECUTE updates that count without changing
FOUND. Other diagnostics and utility commands with INTO/USING fail explicitly.

PostgreSQL scalar stored-function effects belong to their entire calling SQL
statement. An error in a later expression rolls them back, and a failed statement
in an explicit transaction preserves earlier transaction work. Read-only row
functions retain the caller's pending snapshot and cannot acquire write authority.
These paths do not imply full application migration compatibility: unimplemented
PL/pgSQL constructs still fail explicitly. Opening an existing database tests
storage upgrade compatibility, not migration compatibility.

## Returning rows and strict lookups

PL/pgSQL `RETURNS TABLE` functions support `RETURN QUERY` in a single FROM item
and correlated inner/left joins, including SELECT CTEs feeding an UPDATE.
Successive `RETURN QUERY` statements append rows; plain `RETURN` ends the
function. The statement transaction includes writes performed by every function
invocation. An error in the outer statement rolls those writes back when the
statement owns an implicit transaction.

`SELECT ... INTO STRICT` requires exactly one row: zero rows raise `P0002`
(`NO_DATA_FOUND`), and multiple rows raise `P0003` (`TOO_MANY_ROWS`). Supported
exception handlers can catch those conditions. Ordinary INTO retains its
first-row assignment behavior.

The current row-returning subset does not implement `RETURN NEXT`, dynamic
`RETURN QUERY EXECUTE`, WITH ORDINALITY, or anonymous record results without
named output columns. Recursive and more deeply nested query shapes may still
require additional SQL engine support. BicDB does not claim complete PostgreSQL
PL/pgSQL or extension compatibility.

Regression coverage includes `plpgsql_set_functions_*` in the SQL library,
`routine_owner_execution_identity`, `trigger_authorization`, and the pgwire
delegation tests. Hub deployment acceptance must additionally complete its
migrations, grants, and live API tests.

## Catalog provisioning and application SQL

Pgwire advisory-lock recognition only parses top-level SELECT candidates. A
lock-function name in an anonymous block, catalog string literal, or routine
body does not cause the block to be rejected by the generic SQL parser. Nested
record FOR loops over pg_proc/pg_namespace, regprocedure identities, and dynamic
GRANT EXECUTE work through the ordinary interpreter and privilege checks.
Record-loop `regprocedure`/`regproc` values resolve as functions rather than
relations, and pg_proc reports the actual number of defaulted input arguments.
The authored-function grant block no longer bypasses normal SQL GRANT handling
through a marker-specific shortcut.

Function GRANT/REVOKE statements accept line breaks and tabs between keywords,
including before `TO`/`FROM`. Quoted role names are preserved and execution
privileges are still enforced. A redundant `WITH GRANT OPTION` is accepted for
owners and superusers; granting that authority to other roles remains unsupported
until dependent-grant tracking is implemented.
Function identity lists, including commas within argument signatures, apply to
every named routine in standalone statements, anonymous PL/pgSQL blocks and
migration batches. All routine
existence, ownership and grantee checks precede any privilege changes; a missing
or unauthorized later target cannot leave an earlier target granted.

`pg_timezone_names` and `pg_catalog.pg_timezone_names` expose IANA names and
aliases from the same chrono-tz database used by AT TIME ZONE, with current
abbreviation, interval offset, and DST status. Scalar `IN (SELECT ...)`/`NOT IN`
works without a FROM clause and preserves NULL/empty-set semantics.

`hashtextextended(text, bigint)` supports UTF-8 deterministic text hashing with
PostgreSQL-compatible little-endian results, including nonzero and negative
seeds. Nondeterministic collation hashing is not claimed. Existing `hashtext`
retains its historical BicDB hash values; do not assume its output is the low
32 bits of `hashtextextended`.

Direct pgwire advisory-lock arguments are evaluated as SQL expressions in the
caller's session/transaction, including parameters, concatenation, functions,
and verified identity context. They acquire real connection/transaction-scoped
locks. Preparing/describing these calls does not evaluate arguments or acquire
locks. These capabilities do not replace SQL role grants or signed-operation
checks, and do not by themselves certify an application's complete deployment.

## Deferred foreign keys

Foreign keys support `DEFERRABLE INITIALLY IMMEDIATE` and `DEFERRABLE INITIALLY
DEFERRED`, including `ALTER TABLE ... ALTER CONSTRAINT` inside a `DO` block.
`SET CONSTRAINTS name [, ...] DEFERRED|IMMEDIATE` changes transaction-local
foreign-key timing; schema-qualified names and the first matching schema in
`search_path` are supported. `pg_constraint` and `pg_get_constraintdef` expose
the recorded default timing. Altering a constraint requires table ownership.

Checks use the final row state at commit or when switched back to IMMEDIATE.
Failures preserve SQLSTATE 23503 and roll back the write. Savepoint rollback
restores timing and pending checks. Parent-key reservations still serialize
conflicting referential writes. RESTRICT actions remain immediate; NO ACTION
can be deferred. Autocommit validates at statement end.

The native application host also finalizes deferred SQL effects before commit,
with the authenticated actor's security context. A direct core commit refuses
unprocessed SQL effects. Stable constraint identities prevent renaming a
constraint or relation from dropping its pending validation.

This timing implementation covers foreign keys and `DEFERRABLE` unique
constraints (see [constraints](postgresql-constraints.md)). Named timing
changes for other constraint classes fail explicitly. `SET CONSTRAINTS ALL`
covers both and refuses when constraint triggers are present, since changing their firing
mode through ALL is not implemented. Existing INITIALLY DEFERRED constraint
triggers still fire at commit. Finalization grants no new mutation authority.

## PostgreSQL application refusal handlers

Named EXCEPTION conditions `raise_exception` and `insufficient_privilege` match
P0001 and 42501 respectively. SQLSTATE and SQLERRM are available while their
PostgreSQL handler runs, including embedded SQL. Nested handlers restore the
outer diagnostics; they are not retained outside a handler or injected into
Oracle's diagnostic namespace. Regressions cover the success path, selective
matching, unrelated error propagation, nested diagnostics and rollback of failed
body changes before handler writes. The Carrier compatibility suite passes all
35 checks; the original EHR-shaped refusal wrapper fails before this support.
