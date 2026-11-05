# Persistent BLOB ownership: native source observations

The [source probe](../../../examples/oracle/reference/persistent_blob_ownership.py)
ran eight cases twice on Oracle 23.26.3.0.0. All 16 observations matched their
explicit expectations. The [observations](source-observations.json) retain
operation-specific reads, copy/append errors and resulting rows. The owned table
was dropped and its absence checked. No original application objects changed.

This is source evidence for an unfinished implementation. It does not compare
BicDB results, implement persistent locators, qualify an application or establish
production compatibility. Temporary BLOB behavior has a separate
[implementation checkpoint](../oracle-temporary-blobs-2026-11-05/README.md).

Every case changes the owner in the transaction that acquired the `FOR UPDATE`
locator. Independent read locators and changes committed by other sessions have
separate snapshot controls; these observations do not redefine those rules.

| Change after acquiring a locked locator | Read/copy through the old locator | APPEND through it |
| --- | --- | --- |
| DELETE, before COMMIT | Old bytes readable and copyable into another row | ORA-22275, both with and without the intervening copy |
| DELETE then COMMIT | Read and copy both raise ORA-22922 | ORA-22922 |
| DELETE then ROLLBACK | Old bytes readable/copyable; original row restored | ORA-22920 because rollback released its lock |
| Replace payload, before COMMIT | Old bytes readable and copyable | ORA-22922 |
| Replace payload then COMMIT | Read and copy both raise ORA-22922 | ORA-22922 |
| Move primary key, before COMMIT | Old locator remains readable/copyable; copied row gets independent value | Succeeds against the moved row; the copy remains unchanged |
| Move primary key then COMMIT | Old locator remains readable/copyable | ORA-22920 until the owner is locked again |

## Consequences for the implementation

A locator is not a row address or a byte string. A missing current owner is not
one uniform error condition: uncommitted deletion, replacement and a committed
invalid value have distinct effects in these observations. An uncommitted
invalidation must not discard an old readable snapshot or prevent a permitted
SQL value copy. A copied value needs its own writable identity. A primary-key
move must preserve the original identity independently of its physical record
identifier.

Transaction outcome/write receipts provide part of the lifecycle tracking.
They do not identify whether a value was replaced/deleted, resolve its current
owner or authorize mutation. Those responsibilities require SQL value identity,
operation provenance and the existing transaction/lock/authorization paths.
Read validity and mutation validity must be evaluated separately.

## Reproduce

Use a dedicated non-administrative reference schema with permission to create
and drop its own tables. Configuration uses the existing Oracle reference keys:
`user`, `host`, `port`, `service_name` and `password_file`. Relative password-file
paths resolve against the configuration file's parent. Credentials are not
included in the report.

```sh
python3 examples/oracle/reference/persistent_blob_ownership.py \
  --oracle-config /path/to/reference.json \
  --output /path/to/observations.json
```

The probe needs `python-oracledb`. It records each operation inside its own
exception block, so an INSERT failure cannot be misreported as an APPEND failure.
It does not require a BicDB connection. PostgreSQL bytea semantics are outside
this native Oracle source probe; the future implementation must protect them
with independent controls.
