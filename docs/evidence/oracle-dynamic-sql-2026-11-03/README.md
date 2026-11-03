# Bound dynamic statements and nested SQL text

Oracle preview compiler version 16 adds a bounded `EXECUTE IMMEDIATE` contract
for standalone routines. SQL text is parsed at execution under the routine's
recorded namespace. Scalar IN values remain typed parameters rather than being
inserted into SQL text. Caller locals cannot become dynamic SQL column names.
The original Oracle-provider iDempiere application remains incomplete. This is
a development qualification; the running application is not upgraded.

## Reference comparisons

[oracle-dynamic-statements.json](oracle-dynamic-statements.json) records **36/36**
matches against Oracle 23.26.3.0.0. Cases cover repeated positional placeholders,
missing/excess binds, USING IN, NULLs, scalar INTO, zero/multiple rows, invalid
identifiers, trailing semicolons, multiple-statement rejection, DML rollback,
unreached branches, namespace isolation and unfetched SELECT behavior. An
unfetched SELECT does not evaluate volatile projections or division by zero;
the supported metadata path still checks names and read permissions.

SQL rendering preserves apostrophes, adjacent apostrophes, Unicode and SQL-like
text across literal, routine and nested dynamic-statement layers. Character
host expressions preserve their type through supported concatenation forms.
At a version-16 Oracle function's SQL call boundary, unhandled NO_DATA_FOUND
returns NULL. Procedural handlers still receive it.
[postgres-literal-controls.json](postgres-literal-controls.json) records **13/13**
independent PostgreSQL 18.4 controls, including native P0002 propagation and
native dynamic SELECT evaluation. Oracle changes do not select native behavior.

PG-driver checks exercise prepared text binds, Describe without autonomous
side effects, empty-string normalization and reopen. The persisted compiler
identity remains versioned.
[reader-and-restart.json](reader-and-restart.json) records a version-15 reader
refusing a version-16 routine with 0A000; the current reader executes the dynamic
INSERT once, and its committed row survives restart.

Run the generated comparison using matching pre-existing dedicated schemas:

```sh
python examples/oracle/reference/dynamic_sql.py \
  --oracle-config /private/reference.json \
  --output /private/dynamic-statements.json
```

Set BICDB_PG_DSN privately to the dedicated Oracle-preview target. The runner
creates and removes its own fixtures. For independent native controls, set
ORACLE_CONTROL_PG_DSN to real PostgreSQL and BICDB_PG_DSN to a native-profile
target, then run:

```sh
python examples/oracle/reference/postgres_dynamic_literals.py \
  --output /private/postgres-literal-controls.json
```

## Original routines and application boundary

[original-record-comparisons.json](original-record-comparisons.json) records
**8/8** comparisons of the unchanged original TORECORDID body with minimal
dictionary and record tables in generated schemas. Ordinary-name UUID lookup
returns matching IDs for ordinary, SQL-like and Unicode text; a missing row
returns NULL. The source's long-name discovery branch returns NULL in all four
tested cases under the reference's binary collation: its lowercased column name
does not match an uppercase `UU` suffix. The target mirrors that source result;
these checks do not establish working long-name lookup as a business feature.

[original-sequence-comparisons.json](original-sequence-comparisons.json) retains
**9/9** checks of unchanged AD_SEQUENCE_NEXT with minimal dependencies.
[original-counter-call-diagnostic.json](original-counter-call-diagnostic.json)
records a **failing** valid-input call with unchanged GET_SYSCONFIG, NEXTID and
NEXTIDFUNC bodies. Oracle returns 10; BicDB returns 0A000 because the national-
character TABLENAME variable is not typed for concatenation. This replaces the
earlier dynamic-SQL namespace rejection but is not a passing application call.
The fixture stops at that error; subsequent planned calls are not qualified.

[application-copy-diagnostics.json](application-copy-diagnostics.json) records
**60/62** original definitions on a disposable imported application copy, with
the existing NULL-input procedure diagnostics still **1/13** successful.
TOTABLEOFVARCHAR2 still needs hierarchical queries/collections; AD_COLUMN_SYNC
still lacks its Oracle dictionary anchor. Accepted definitions and isolated
function comparisons do not establish application startup, JDBC integration or
business workflows. No application-specific engine exceptions were added.

OUT/IN OUT binds, RETURNING/BULK operations, record targets, dynamic DDL,
transaction control, dynamic PL/SQL blocks and metadata-only CTE/set-query
binding remain unsupported. The bound scalar type set and query metadata path
need broader qualification. National-character types, NLS behavior, packages,
dictionary objects, deferred foreign keys and the other programme workstreams
remain open.

## Retained validation

[retained-oracle-controls.json](retained-oracle-controls.json) records
**1,165/1,165** earlier generated matches on the same candidate, giving
**1,201/1,201** in the passing generated Oracle corpus.
[retained-postgres-controls.json](retained-postgres-controls.json) records
**214/214** earlier controls, giving **227/227** independent PostgreSQL controls.
Original application-function checks are separate from those generated totals.
[existing-semantic-gap-observations.json](existing-semantic-gap-observations.json)
retains five mismatches among seven separately tracked type/conversion probes;
their outcomes are unchanged and are outside the passing corpus.

[validation.json](validation.json) records local checks, source/binary identities
and monitored resource limits. The integrated wire run initially failed because
its cache fixture used GRANT ON FUNCTION for a procedure. The fixture now creates
objects as its calling role and retains its repeated-CALL/cache assertions.
Formatting failures inherited from the integrated upstream commit are corrected.
Builds use one compiler job, no incremental cache and an active memory guard.
A post-integration attempt was interrupted when available host RAM fell below
8 GiB; only the task-owned process group was stopped, and validation was resumed
when headroom returned. That interruption is recorded separately from tests. Two further compiler attempts
received SIGTERM while sufficient host RAM remained; the resource guard did not
terminate those groups. These incomplete attempts are also recorded separately
from the completed regression run.
Task-owned temporary data and obsolete executables are removed after validation;
current binaries and older compatibility readers are preserved. A built binary,
merged source, published release and verified application deployment remain
separate states.
