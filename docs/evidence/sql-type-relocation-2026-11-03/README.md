# Stored function declarations after type relocation

Moving or renaming a user type now updates the typed positions in dependent
stored function declarations as well as their catalog metadata. Previously,
loading a declaration could restore the old custom return-type name after
`ALTER TYPE ... SET SCHEMA` or `RENAME TO`. This reproduced the prior main
commit's failing `user_type_management` CI test.

The fix parses the declaration and updates matching argument/return types,
including array elements and quoted names. Function body literals retain their
original values. Metadata loading continues to recover older custom-type
declarations and security attributes; it reads the current declaration after a
type relocation. No application identity selects this behavior.

The [native PostgreSQL fixture](../../../examples/oracle/reference/postgres_type_relocation.py)
[matches 7/7 PostgreSQL 18.4 cases](postgres-controls.json), improving the
[3/7 prior-binary baseline](postgres-baseline.json). The cases cover transaction
rollback, committed moves/renames, argument and return metadata, execution,
preserved body literals and a fresh connection. All nonce functions/types are
removed. Two namespaces are explicitly provisioned fixture infrastructure.

Local validation passes **598 Rust test executions**: the six type-management
integration tests and 592 SQL unit tests, with three existing unit-test ignores.
The integration tests include database reopen/execution and quoted-name coverage.
The unit suite independently retains Oracle custom-type/version behavior.
[Oracle clock controls](clock-oracle-controls.json) retain **75/75** and
[native clock controls](clock-postgres-controls.json) retain **12/12** on the
new binary. Formatting, Python reference tests, licenses and dependency-boundary
checks pass. See the [validation record](validation.json) for exact provenance.

The initial reference collector matched all seven behavioral cases but returned
`success=false` because schema cleanup received two `0A000` errors. Both the
[cascade attempt](schema-cascade-cleanup-baseline.json) and
[ordered-schema attempt](schema-cleanup-baseline.json) are retained. The fixture
now removes its nonce objects in dependency order inside provisioned namespaces.
It does not test or claim schema removal. DROP SCHEMA and schema-cascade
qualification remain separate gaps; the earlier cleanup failures are not green
comparison results.

The routine compiler remains 26 and durable metadata format remains 5. This is
a bounded native metadata correction and development source/binary qualification.
It is not a published release, a production deployment, complete type-management
coverage, full Oracle DDL compatibility or complete application qualification.
