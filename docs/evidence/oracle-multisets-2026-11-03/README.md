# Named nested-table multisets

This checkpoint implements general Oracle-preview collection semantics and
independent PostgreSQL row-set multiplicity. It does not qualify the full
application, production operation or Oracle conformance.

The public fixtures are
[`multiset_operators.py`](../../../examples/oracle/reference/multiset_operators.py)
and [`postgres_multiset_controls.py`](../../../examples/oracle/reference/postgres_multiset_controls.py).
The Oracle reference comparison matches **42/42** cases; PostgreSQL controls
match **12/12** against PostgreSQL 18.4. The previous frozen binary matched 7/12
of those native controls, with INTERSECT ALL and EXCEPT ALL unimplemented.
The unchanged original intersection function matches **11/11** source calls,
and the original CSV collection function retains **5/5**. Original application
DDL and credentials are not included in the public fixtures.

## Semantics and scope

Named nested-table UNION, INTERSECT and EXCEPT support default/explicit ALL and
DISTINCT. INTERSECT has tighter precedence. Whole NULLs propagate; NULL elements
participate in multiplicity. IS EMPTY and IS NOT EMPTY distinguish NULL from an
empty collection. Numeric comparison ignores scale, fixed character comparison
ignores trailing spaces, and variable text retains its exact value. Distinct
named collection types are rejected even when their element declarations agree.
Result order is not a contract.

Type provenance is bounded to constructors, explicit named casts, scalar stored
function results and typed procedural variables/parameters outside SQL queries.
SQL collection columns, variables embedded in SQL queries, object elements,
TABLE(collection), sparse methods and edition/type replacement are unqualified.
The source type import adapts CREATE OR REPLACE EDITIONABLE TYPE to fresh CREATE
TYPE; it does not implement editioning or replacement.

Combined input limits are 100,000 elements and an estimated 8 MiB; canonical-key
work has a separate estimated 16 MiB budget. Invalid element shapes and unknown
operations are refused. Resource exhaustion uses SQLSTATE 54000.

Compiler version 21 uses language OID 999420. Version 20 retains OID 999419.
Old routines preserve their semantics, unknown future languages remain refused,
and older readers refuse new routines and lowered-view helpers. Twelve reader
checks cover these boundaries without changing durable metadata format 5.

Native PostgreSQL INTERSECT ALL and EXCEPT ALL consume matching rows once per
occurrence, including NULLs. They reuse existing typed row equality. The existing
common-type resolution limitations and quadratic comparison costs remain;
this checkpoint does not qualify broader coercion or large-set performance.
MINUS ALL remains unsupported.

See [validation.json](validation.json) for the final binary identity, local
regression counts, retained reference checks and monitored memory evidence.
A development binary, merged source, published release and verified application
deployment are separate states. Complete application workflows remain open.
