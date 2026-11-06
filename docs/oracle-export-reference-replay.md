# Oracle reference counts from a saved export

A live Oracle schema can change after an export. Do not use current orphan
counts as evidence for an older snapshot without checking dataset identity.
The reference replay tool restores every exported row into isolated Oracle
global temporary tables, verifies every complete row multiset against the saved
export, and then counts unmatched rows for every supported source foreign key.

This is data replay for independent reference counts. It does not recreate the
application's constraints, indexes, defaults, triggers, routines or execution
environment. It does not qualify a BicDB target merely because the Oracle replay
matches its input. Full target/export equality and application acceptance remain
separate gates.

## Run in a separate test schema

Use a dedicated, non-administrative Oracle reference owner permitted to create
and drop its own temporary tables. The tool refuses the exported source owner,
SYS, SYSTEM and PDBADMIN. It also checks the actual session owner/schema and
requires explicit `--isolated-reference-schema`. Application source tables are
never updated or repaired.

Use the same private connection JSON format as the Oracle exporter: `user`,
`host`, `port`, `service_name` and `password_file`. Keep credentials and source
data outside the repository. From the repository root:

```sh
python examples/oracle/idempiere/replay_oracle_reference.py \
  --snapshot /private/export \
  --oracle-config /private/oracle-reference.json \
  --report /private/oracle-export-reference.json \
  --isolated-reference-schema
```

The Oracle Python driver is used only for reference work. BicDB's selected
application transport remains the PostgreSQL driver with SQL-profile opt-in.

Source files must pass manifest digest, row-width and row-count checks. Source
column metadata must match each exported projection. Supported base types are
replayed with their declarations, including character length units, LOBs, RAW
byte widths and bounded temporal declarations. Unsupported types and virtual
columns fail explicitly. The tool inserts all projected values and verifies
them again through Oracle before counting relationships; it does not compare
only key projections or omit payload columns.

The shared export/reference reader preserves numeric timestamp offsets
explicitly rather than using the driver's naive timestamp result. LOB locators
preserve empty CLOB/BLOB values separately from NULL; replay uses Oracle's empty
LOB constructors for those values. New exports record `value_encoding.version:
2`. Older exports remain readable, but their source fidelity must be established
independently. This checkpoint covers the supported temporal precision and
numeric offsets; named time-zone identities and broader temporal declarations
remain outside its qualification.

The exporter now checks all base-table timestamp declarations before publishing
catalog or row files. Precision 7–9 and `WITH LOCAL TIME ZONE` fail explicitly,
so the microsecond codec cannot silently shorten those values. Supported base
projections remain unchanged; view-only declarations are handled by the separate
view migration workstream. Oracle supports fractional precision through 9, as
described in [Oracle Data Types](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html).
Full precision-9, local-zone and named-zone support remains an open programme
requirement; rejecting them here is a bounded export contract, not completion of
that support.

## Cleanup and interruption recovery

Each attempt uses a fresh nonce namespace. Its report records every possible
owned table name before DDL. Temporary rows belong to the loading session; after
that session closes, a separate connection drops the table definitions. Oracle
documents the distinction between temporary row lifetime and persistent table
definitions in [CREATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-TABLE.html).

A successful run requires complete row verification, every relationship count,
zero unmatched rows under source-validated relationships, and successful cleanup.
Legacy orphans under source `NOT VALIDATED` relationships are retained as counts;
they are not repaired. Counts across relationships are not distinct row counts.

If a process or connection failure interrupts cleanup, retain the report and
original manifest, then use the same reference endpoint:

```sh
python examples/oracle/idempiere/replay_oracle_reference.py \
  --snapshot /private/export \
  --oracle-config /private/oracle-reference.json \
  --report /private/oracle-export-reference.json \
  --isolated-reference-schema --cleanup-only
```

Recovery validates manifest/endpoint identity and the deterministic nonce map
before attempting drops. Missing tables count as already removed; other cleanup
errors are reported. Cleanup alone never promotes an incomplete inventory to
success. A failed replay is rerun with a fresh report/namespace after cleanup;
there is no partial data-replay resume.

Reports contain declarations, row digests, counts and cleanup metadata, without
passwords or row values. `live_source_compared`, `target_inventory_compared` and
`application_qualified` remain false. The [qualification record](evidence/oracle-export-reference-2026-11-05/README.md)
records the tested snapshot and distinguishes reference results from native
application acceptance.
