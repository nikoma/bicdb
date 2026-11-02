# Nested exception-block reference contracts

The initial [six independent reference cases](reference-contracts.json) ran on
Oracle Free 23.26.3.0.0 and PostgreSQL 18.4 to establish implementation
requirements. That initial report did not execute BicDB. The subsequent bounded
BicDB qualification is recorded below, separately from wider transaction and
application acceptance.

An update before a handled nested-block error remains visible in Oracle. In
PostgreSQL it is rolled back to the value at entry to that block. Both references
retain the changed local variable. A failing multirow INSERT leaves no partial
rows in either reference; Oracle retains the preceding successful UPDATE, while
PostgreSQL rolls it back with the block. An error raised inside a selected
handler propagates to the outer handler instead of selecting another handler
from the same block.

The [public runner](../../../examples/oracle/reference/nested_blocks.py) uses
fresh connections, unique synthetic tables and explicit expectations for each
backend. It verifies rollback after each case and drops its tables. The measured
PostgreSQL instance was disposable, used a cached image with 256 MiB memory and
one CPU, and was removed after validation. Source connection settings and
credentials are not published.

Set `ORACLE_CONTROL_PG_DSN` to a dedicated PostgreSQL reference connection and
provide a private Oracle configuration in the reference harness format:

```sh
python3 examples/oracle/reference/nested_blocks.py \
  --oracle-config oracle-reference.json \
  --output nested-block-reference-contracts.json
```

The [Oracle exception contract](https://docs.oracle.com/en/database/oracle/oracle-database/26/lnpls/unhandled-exceptions.html)
and [PostgreSQL block contract](https://www.postgresql.org/docs/18/plpgsql-control-structures.html)
explain why sharing one rollback policy would be incorrect.

The implementation requirements are scoped block parsing and variables, nested handler
selection and propagation, condition/error mappings, and profile-specific undo:
statement rollback for the tested Oracle case versus block rollback for
PostgreSQL. Execution must carry the routine/block's profile identity; native
LANGUAGE plpgsql inside a preview database must retain PostgreSQL behavior.
The same synthetic scenarios must then be run through BicDB's PostgreSQL driver
with the appropriate expected results, including verification after rollback
and reopen. Packages, autonomous transactions and full exception semantics
require additional acceptance beyond these cases.

The shared interpreter now executes nested BEGIN/END and DECLARE blocks with
shadowed values, declared assignment types and cursors restored on every exit.
Five additional [PostgreSQL 18.4 reference cases](scope-reference.json) establish
cursor/value shadowing, loop control, declaration-initialization errors, handler
variables and returns. An initialization error is handled by an enclosing block;
an error inside a selected handler does not select its sibling handler.

New Oracle routine definitions use compiler version 6 (language OID 999405).
They retain successful statements preceding a handled error, undo partial
writes from a failed statement and apply strict SELECT INTO cardinality.
Native LANGUAGE plpgsql and versions 1–5 retain their previous rollback policy.
Anonymous preview blocks carry their language identity and bind their SQL to the
selected application namespace before execution. The shared routine cache now
isolates database instances even when their catalog generations match, including
negative lookups. The adaptive compiler falls back to the interpreter for blocks.

The [live driver qualification](live-qualification.json) runs the three write
contracts independently through BicDB's PostgreSQL transport in each profile.
All six match their reference expectations, verify rollback and remove their
synthetic tables. Constant routine variables work beside aggregate expressions,
including empty inputs; real columns still require grouping, including ambiguous
join columns. BicDB's reported PostgreSQL server version is transport metadata;
the tested binary's SHA-256 is recorded separately.

The [first driver run](initial-live-qualification.json) exposed DECLARE being
treated as a cursor command and aggregate projections rejecting routine
constants. The next run exposed the preview namespace pass attempting to parse
raw DO source as an ordinary SQL statement. These paths now preserve raw block
source and bind its scoped SQL during execution. Native regressions separately
retain grouping requirements rather than accepting arbitrary ungrouped fields.

The [ROWNUM reference](rownum-reference.json) and the live driver record check
15 literal-integer prefix predicates over ordered subqueries. Same-block sorting
or grouping, projected ROWNUM, disjunctions, dynamic bounds and broader Oracle
row numbering remain explicitly unsupported. This bounded rule unblocks the
unchanged GET_SYSCONFIG source: [original application rechecks](original-routine-rechecks.json)
match all 13 selected comparisons plus eight existing-configuration cases after
recreating that definition under version 6. App result rows are retained only
as counts and digests. The [fresh catalog inventory](original-definition-inventory.json)
still accepts 35 of 62 source routine names; foreign keys and
business workflows remain outstanding.

The [validation record](validation.json) distinguishes local tests, live
comparisons, the selected older-reader fence and the development binary from a
published release or production deployment. This evidence does not qualify full
exception state/diagnostics, all rollback boundaries, implicit DDL commits,
autonomous transactions, packages or the complete application.
