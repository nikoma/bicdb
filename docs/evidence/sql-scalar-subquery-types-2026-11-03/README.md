# Qualified scalar result types and diagnostic records

Scalar-subquery type inference now resolves SQL-qualified tables with the same
relation identity used by execution. Public qualifiers are removed for storage
lookup; other schemas use their encoded identity. Previously metadata lookup
used the SQL spelling directly, failed to find the relation, and could advertise
text or integer types for a numeric scalar result. The existing CTE-shadow
check still uses the SQL name and is not bypassed. No application/table special
case or metadata/compiler version change is involved.

The [executable fixture](../../../examples/oracle/reference/scalar_subquery_types.py)
checks six aggregate shapes in prepared text and binary result formats: a
correlated sum, empty sum, NULL sum, empty outer result, MAX and a scalar wrapper.
The Oracle reference must report NUMBER; the selected PostgreSQL transport must
report NUMERIC OID 1700 independently of values. PostgreSQL reference metadata
is checked against that same OID. This is bounded single-relation aggregate
coverage, not general subquery/CTE/join type qualification.

## Source results and regression checks

The exact development binary matches [12/12 Oracle cases](oracle-comparison.json)
and [12/12 native PostgreSQL controls](postgres-controls.json). Oracle is
23.26.3.0.0; PostgreSQL is 18.4. Both references independently validate every
explicit value/type expectation. All helper objects were removed in the passing
runs. The [Oracle](oracle-baseline.json) and [PostgreSQL](postgres-baseline.json)
prior-binary baselines each match 0/12. They retain wrong type OIDs, decoding
errors, and a connection closure during a binary case; SQL cleanup errors in
that failing target run are retained too. The disposable target database was
removed by the runner afterward. Those baseline failures are not counted as
successful acceptance.

Rust regressions cover quoted schemas, native int4 preservation, metadata for
empty results, and CTE shadowing. Driver tests check prepared Describe/Execute
and empty result types in both profiles. Local validation passes **1,443 Rust
tests**, with three existing ignores, and **58 Python tests**. Formatting, diff,
license and open-core checks pass; CI was not run in this local qualification.
The five Oracle/five PostgreSQL column-binding controls and nineteen Oracle/twelve
PostgreSQL aggregate controls also pass again, as recorded in
[validation.json](validation.json).

## Concurrent diagnostic logging

The previous application trace had [20 malformed JSON lines](query-trace-baseline.json)
because formatted appends could write the payload and newline separately.
Structured logging now formats the full record and terminator before appending.
The existing slow-query event format and redaction are shared with pgwire through
one event conversion. Pgwire adds the server connection ID and transaction-active
state after execution, allowing transaction boundaries to be correlated.

A core regression validates 800 records from eight concurrent appenders. The
[live tracing check](query-trace-validation.json) validates every JSON line while
four driver connections each execute twenty transactions. It also verifies
connection identities and both transaction states. This is diagnostic logging,
not a claim of a crash-durable audit trail or arbitrary multi-process guarantees.
Logging remains disabled unless explicitly configured.

## Remaining application dependency

The header-total gap is still open. A separately reproduced
[indexed pending-write baseline](indexed-pending-baseline.json) uses generic
parent/child tables with NUMERIC keys and amounts. After updating an existing
child, Oracle returns 185.25 from direct/correlated sums and the parent update.
The target matches without a secondary index but returns zero when the child's
join key is indexed. This reproduces the application's zero-total pattern;
it is not fixed by the result-type changes in this checkpoint.

The next dependency is using committed indexes' canonical typed key projection
when filtering pending transaction candidates, with equality/range, insert,
update, delete and savepoint regressions. Disabling the index or inserting an
application-specific repair would not establish that contract. The saved line
and four correct inventory calls remain partial application evidence. Processing,
accounting, recovery and the complete compatibility programme remain open. This
checkpoint provides merged source and a development binary, not a release,
production deployment or complete application qualification.
