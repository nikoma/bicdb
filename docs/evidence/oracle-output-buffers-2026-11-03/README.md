# Versioned session output operations

Compiler version **25**, language OID **999424**, adds ENABLE, DISABLE, PUT,
PUT_LINE, NEW_LINE and GET_LINE to the Oracle preview. Their buffers belong to
the logical connection and survive statement, transaction and savepoint rollback.
Nested routines, SQL-called functions and autonomous routines share the caller's
buffer. Other connections have independent state. Connection disposal and
DISCARD ALL clear it; it is not durable application data.

This is **partial supplied-package support**. GET_LINES, its collection types,
general package declarations, package ACL management, broader OUT destinations
and the other DBMS/UTL APIs remain open. The full acceptance fixture must continue
to report failure until those remaining cases pass.

## Semantics and transport

Output is disabled initially. Arguments are evaluated normally before the
disabled path; this is not an application-specific logging no-op. The compiler
uses declared scalar types for procedural character conversion. Native PostgreSQL
procedure names retain their normal resolution. New lowering is restricted to
the registered compiler version; existing version-24 routines retain their prior
behavior until explicitly recompiled from their original declarations.

The implementation retains complete and partial lines, NULL/empty-line identity,
Unicode byte limits and read-before-write discard behavior. ENABLE honors the
most recent limit and resets byte accounting while retaining buffered text. Line
overflow discards an unfinished line without refunding its charged bytes; buffer
overflow leaves that partial line available for a smaller append. Those details
are measured against the Oracle reference, rather than inferred from a successful
application startup.

The selected PostgreSQL transport provides a result row for OUT arguments:

```sql
CALL DBMS_OUTPUT.ENABLE(NULL);
CALL DBMS_OUTPUT.PUT('hello ');
CALL DBMS_OUTPUT.PUT_LINE('world');
CALL DBMS_OUTPUT.GET_LINE(NULL, NULL);
```

The final call returns `line` and `status`; it uses the PostgreSQL CALL convention
for OUT placeholders. Oracle's own driver instead binds OUT variables. This does
not add unchanged Oracle-driver protocol support or qualify every JDBC callable
statement and catalog interface.

Metadata description does not append, consume or clear output, and does not
evaluate the output operation's argument expressions. Runtime operations observe
cancellation. Session state transfer and savepoint restoration share the same
nontransactional buffer, while ordinary settings keep their existing rollback
behavior. No application-specific data, schema or table name selects this behavior.

## Resource accounting

ENABLE(NULL) removes the API's byte limit, while the host's memory budget still
applies. `BICDB_ORACLE_OUTPUT_MAX_BYTES` sets that per-connection allocation budget
in bytes; its default is **64 MiB**. Exhaustion reports PostgreSQL SQLSTATE
`53200`, separately from Oracle's ORU line/buffer errors. The budget includes
container capacity and empty lines, not only text bytes. Allocation accounting
takes constant time; geometric queue growth takes amortized constant time per
line, apart from copying text. Clearing a buffer releases its contents. Allocations are included in opaque
session memory estimates.

## Qualification

The [executable fixture](../../../examples/oracle/reference/output_buffers.py)
now includes the original 27 cases plus additional ENABLE/overflow accounting,
autonomous execution, exceptions, SQL-called functions and connection isolation.
The original [failing baseline](../oracle-output-buffer-baseline-2026-11-03/README.md)
is retained with its original fixture digest and runtime identity.

The final development candidate matches **32/38** cases against Oracle
**23.26.3.0.0**; all 38 expectations pass on that reference. The six remaining
cases call GET_LINES. The [full fixture report](oracle-comparison.json) retains
`success: false` and exit status **1**, rather than counting missing behavior as
passed. All helper objects were removed and supported buffer resets completed.

Local validation passes **1,430 Rust tests**, with three existing ignores, and
**58 Python tests**. Separate [PostgreSQL controls](postgres-controls.json) pass
**23/23**. The [reader matrix](readers-comparison.json) passes **13/13**, including
preserved version-24 identities, rejection of version-25 routines by the earlier
reader, and successful reopening afterward. See [validation.json](validation.json)
for identities, the allocation regression/fix, other validation history and memory
headroom. These are bounded local checks, not production or CI qualification.

Prepared procedure DDL is a separate observed interface gap. Reader fixtures use
unprepared procedure creation and quoted native catalog names; those choices do
not qualify the missing prepared-DDL path or all native catalog interoperability
under Oracle identifier rules. Scalar OUT destinations are the current bounded
procedural contract. Record/collection destinations, further implicit/NLS
conversions and broader resource-failure behavior require additional acceptance.

The application workflow remains separate. On the earlier development runtime,
the [draft header](../idempiere-sales-order-draft-2026-11-03/README.md) saves and
reopens, but stock-function calls block product selection with unresolved NVL
aggregate operand types. The same failure reproduces on a synthetic table;
no application-specific fallback is introduced. Full order processing,
accounting, operational qualification and the complete Oracle programme remain
open. This work is not a published release or vendor certification.
