# SQL BLOB reads and declared SQL-function argument types

The Oracle preview profile supports read-only SQL GETLENGTH and ISTEMPORARY
calls over directly resolved logical BLOB columns. Table aliases, SYS-qualified
package names and named arguments are covered. Nonempty, empty and NULL values
remain distinct; prepared metadata describes BIGINT results even when no rows
match. Provenance checks reject ordinary binary or text columns, including
NULL-only rows. Static inference does not invoke a volatile WHERE function.

General SQL-function argument types now reach constant and row evaluation.
For a function returning `octet_length(b)+5`, a one-byte BYTEA argument returns
six; text containing the four characters `\xab` returns nine. Both scalar and
table-returning functions are covered. A local text column retains precedence
over a same-named BYTEA argument. PostgreSQL user functions named
`"DBMS_LOB"."GETLENGTH"` keep ordinary function semantics.

The [validation record](validation.json) reports **119 native/tool tests passed,
three external-reference tests skipped; four focused SQL tests passed; and 717
SQL library tests passed with four ignored benchmarks**. Native tests include
COPY/full-value verification, prepared metadata and clean restart. The focused
SQL tests cover transaction/savepoint and observer visibility, NULL provenance,
metadata-only inference, and the PostgreSQL namespace control. Test log and
candidate source/binary digests identify the tested build.

The [storage contract](../../oracle-blob-storage.md) defines the supported
operands and remaining CLOB/BFILE, streaming, expression-lineage and persistent
locator lifecycle work. The [reference fixture](../../../examples/oracle/reference/sql_blob_reads.py)
matched **all six cases against Oracle 23.26.3.0.0**, recorded in the
[live comparison](reference-comparison.json). These include metrics,
SYS-qualified/named calls, a positive-length predicate, NVL, an empty selection,
and writer/observer visibility across a savepoint and full rollback. Both sides
removed their nonce tables; the native comparison database shut down cleanly
and was removed, the source owner was dropped, and the reference container was
restored. Complete application business acceptance, production qualification and
an accepted deployment upgrade remain separate gates.
