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

This timing implementation covers foreign keys. Named timing changes for other
constraint classes fail explicitly. `SET CONSTRAINTS ALL` covers foreign keys
and refuses when constraint triggers are present, since changing their firing
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
