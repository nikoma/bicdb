# Session output buffer acceptance baseline

The executable [output buffer fixture](../../../examples/oracle/reference/output_buffers.py)
checks **27 explicit expectations against Oracle 23.26.3.0.0**. All 27 pass on
that reference. On the current BicDB development runtime, **0/27 match**: each
case stops at the initial DBMS_OUTPUT.DISABLE call because that procedure is
unavailable. These are 27 blocked cases, not 27 independently exercised target
semantics. This is a retained failing acceptance baseline, not an implementation
or a compatibility qualification.

The fixture exercises disabled output, partial and complete lines, NULL/empty
strings, embedded newlines, Unicode, numeric input, reads followed by writes,
repeated enable/disable, rollback and savepoints. It also checks UTF-8 line-byte
boundaries, line/buffer overflow and recovery, unlimited buffering, and batched
GET_LINES through the package's CHARARR type. Intermediate reads, errors and
final values have explicit expectations. PG transport error messages are checked
for Oracle error numbers and ORU categories; complete diagnostic payload parity
is not asserted.

The [comparison report](oracle-comparison.json) records the blocked BicDB cases,
the independently qualified source cases, and the development binary identity.
Both helper procedures were removed from each database. BicDB's final buffer
reset also fails because DISABLE is unavailable; `buffers_reset` and overall
`success` remain false. No schema cleanup error was observed. The disposable
BicDB server and database were stopped and removed after the probe.

Run with a dedicated Oracle schema and an explicit Oracle-preview BicDB target:

```sh
python examples/oracle/reference/output_buffers.py \
  --oracle-config /private/reference.json --output /private/output-buffers.json
```

Supply `BICDB_PG_DSN` privately. The fixture requires python-oracledb and psycopg;
it returns nonzero while either backend fails an expectation. Source connection
settings and credentials are excluded from the report. See Oracle's
[package reference](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_OUTPUT.html)
for the API contract; the measured reference version is stated above rather than
assuming that every Oracle release was tested.

## Implementation dependencies and remaining acceptance

- Keep output buffers per logical connection and outside transaction, statement
  and savepoint rollback snapshots. Propagate the same state into nested SQL,
  triggers, SQL-called routines and autonomous transactions without sharing it
  with other connections. Clear it on connection disposal and DISCARD/reset where
  appropriate, and include allocations in session resource accounting.
- Implement ENABLE, DISABLE, PUT, PUT_LINE, NEW_LINE, GET_LINE and GET_LINES as
  supplied-package operations, with declared argument modes, named/default
  arguments, type conversion and writable OUT destinations. Do not install an
  application-specific logging no-op.
- Resolve package-owned CHARARR and the separate SQL DBMSOUTPUT_LINESARRAY type, retain their
  different collection contracts, and expose OUT/INOUT results through PostgreSQL
  drivers. The current batch probe covers bounded CHARARR retrieval only.
- Fence new compiler lowering and stored-routine semantics explicitly. Preserve
  old-reader rejection and independent native PostgreSQL behavior, including user
  procedures with names resembling supplied packages. Honor schema/package name
  resolution and authorization rather than bypassing them by text matching.
- Extend source comparisons for independent connections, autonomous calls,
  exception handlers, triggers, connection pooling, resource exhaustion,
  Describe-without-effects, restart/reset and larger collections. Qualify the
  implementation against those cases before closing the supplied-package stream.

This baseline does not implement the package, qualify other DBMS/UTL APIs, or
complete the Oracle compatibility programme.
