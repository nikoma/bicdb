# Scoped Oracle routine grammar qualification

Compiler version 13 (`bicdb_oracle_preview_v13`, OID 999412) adds conjunctive
Oracle `(+)` join lowering in static queries and cursor declarations, and
catalog-based owner binding for column lists in stored `FOR UPDATE OF` queries.
Shared parsing accepts PostgreSQL FOR UPDATE/SHARE target lists and mixed-case
procedural loops with whitespace between END and LOOP. Oracle preview DELETE
statements may omit FROM. Engine behavior is selected by syntax and compiler
version, without application-specific branches.

**The original Oracle-provider iDempiere application is not fully operational on
BicDB.** The last published application definition baseline remains **57/62**.
This qualification does not update the application database or claim acceptance
of the remaining definitions, procedures or complete business workflows.

## Original procedure execution

[original-sequence-comparisons.json](original-sequence-comparisons.json) records
**8/8** comparisons of the original `AD_SEQUENCE_NEXT` body against actual Oracle.
The procedure declaration is namespace-qualified for an isolated schema; its
body is unchanged. Oracle uses a fresh test user and BicDB a disposable database.
The dependency is a minimal synthetic AD_SEQUENCE table, with a primary key and
the columns read or updated by this procedure. It is not a full application
schema or a production data migration.

The checks cover standard and system numbering, successive allocations, disabled
system numbering, a non-unit increment, NULL input branch selection, counter
state, setting Updated, output NUMERIC metadata, and explicit transaction
rollback. Application data is not used in this artifact. Both the temporary
Oracle user and disposable target are removed after execution. Failure paths,
concurrent allocation and full application dependency behavior remain unqualified.

## Grammar and PostgreSQL controls

[oracle-grammar-comparisons.json](oracle-grammar-comparisons.json) records
**14/14** new matches with Oracle 23.26.3.0.0.
[retained-oracle-controls.json](retained-oracle-controls.json) records all
**964/964** earlier generated cases rerun against the candidate, for **978/978**
passing generated Oracle comparisons in total. The eight controlled original
procedure comparisons above are separate from that generated corpus.

[postgres-grammar-controls.json](postgres-grammar-controls.json) records **9/9**
new independent native matches with PostgreSQL 18.4.
[native-controls.json](native-controls.json) includes those and the retained
145 controls, for **154/154** matches. BicDB's existing compact native CALL
extension remains verified separately from PostgreSQL equivalence.

The generated Oracle runner is available as:

```sh
python examples/oracle/reference/routine_namespaces.py \
  --suite grammar --oracle-config /private/reference.json \
  --output /private/grammar-comparison.json
```

Set BICDB_PG_DSN privately to a dedicated Oracle-preview BicDB database. The
fixture creates and drops only its uniquely named objects. The join cases check
null extension, optional-side versus WHERE filters, unqualified ownership, chained
join dependencies, original wildcard column order, keyword-named columns and a
cursor loop. Procedure calls compare Oracle output binds with PostgreSQL-driver
output rows for column lock lists, including NOWAIT. These are uncontended lock
cases; they do not establish concurrent locking equivalence.

The independent PostgreSQL runner is:

```sh
python examples/oracle/reference/postgres_grammar.py \
  --output /private/postgres-grammar.json
```

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately to dedicated PostgreSQL and
native BicDB databases. This checks native mixed-case loops, native lock lists
and modifiers, NULL concatenation, literal marker text, and rejection of Oracle
`(+)` and DELETE-without-FROM syntax. It checks result values and declared wire
types independently of the Oracle fixture.

The compiler scope is restored after parsing errors and native compilation
explicitly disables inherited Oracle grammar. Older compiled routines retain
those earlier language identities; recreate an original Oracle definition to
select version 13. An older reader must reject new calls rather than execute a
body with the wrong compiler contract.
[reader-compatibility.json](reader-compatibility.json) verifies successful new
calls and actual 0A000 rejection by the previous version-12 binary on an isolated
database. The old reader then succeeds on a normal query. It never opens the
application database.

## Application diagnostics and validation

[original-procedure-call-diagnostics.json](original-procedure-call-diagnostics.json)
repeats all thirteen NULL-argument diagnostic calls on a separate disposable
application copy. The definition baseline remains **57/62** and **none of the
thirteen calls succeeds**. Some now reach later blockers: RAISE_APPLICATION_ERROR,
DBMS_OUTPUT, procedural labels and unresolved concatenation operand types.
Dynamic SQL and additional grammar also remain. These diagnostics do not invoke
original procedures on source Oracle and are not business-workflow comparisons.
They are separate from the successful controlled AD_SEQUENCE_NEXT comparison.

[validation.json](validation.json) records **1,264 passing Rust tests**, three
existing ignored microbenchmarks, **25 reference-runner Python tests** and
**22 migration Python tests**. Formatting, dependency licensing and the open-core
dependency boundary pass. Builds use one compiler, disabled incremental
compilation and an active memory guard. Peak owned build/test RSS was
4,050,415,616 bytes; observed available host memory stayed above
21,440,032,768 bytes during the guarded validation. Cleanup reclaimed
2,676,316,472 bytes of completed test executables, leaving a 5.5 GiB build cache.
The development server binary is built; no release is published or application
upgrade performed by this qualification.

## Limits and remaining work

Only supported conjunctive WHERE predicates are lowered. Mixed ANSI/legacy join
syntax, marked OR/IN expressions, ambiguous ownership, cyclic dependencies and
unsupported marker positions are rejected. This does not claim all Oracle join
syntax or matching diagnostics. See the [Oracle join rules](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Joins.html).

Oracle FOR UPDATE OF names columns; native PostgreSQL names relations. The
lowering resolves each Oracle column to its owning base table and preserves
NOWAIT/SKIP LOCKED modifiers. Existing BicDB row-locking restrictions, including
the primary-key requirement, remain. PostgreSQL allows repeated locking clauses;
see [PostgreSQL SELECT](https://www.postgresql.org/docs/18/sql-select.html) and
[Oracle SELECT](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/SELECT.html).

The [explicit semantic gaps](existing-semantic-gap-observations.json) were rerun:
the same five of seven observations still differ from Oracle. They are excluded
from all passing totals. This change does not claim to fix NVL constant propagation,
PLS_INTEGER, formatless TO_NUMBER, function invocation purity, Oracle packages,
autonomous transactions, dynamic SQL, implicit SQL cursor attributes,
transaction-control statements, dictionary completeness, deferred foreign-key
timing or full application workflows. The existing named-schema primary-key
UPDATE issue is also outside this qualification.

The running application database remains on the previous verified version-12
binary. Its previously verified 915-table / 185,537-row preservation evidence
is linked from the [previous qualification](../oracle-procedures-2026-11-02/README.md);
no new live-data preservation or deployment claim is made here.
