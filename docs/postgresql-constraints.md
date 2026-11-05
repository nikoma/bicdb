# PostgreSQL Constraints

Table renames preserve incoming foreign-key references, including self
references, deferred validation and cascading deletes. References follow the
renamed base table when a compatibility view reuses its old name. Transaction
and savepoint rollback restore the previous dependency metadata, and committed
references persist across reopen. Regression coverage is in
`crates/bicdb-sql/tests/renamed_foreign_keys.rs`.

BicDB enforces a practical PostgreSQL-compatible subset of table constraints for
SQL tables created through `CREATE TABLE` or added later with supported
`ALTER TABLE ... ADD CONSTRAINT` forms.

Supported constraint classes:

- `NOT NULL` on columns, including primary-key columns.
- Basic `UNIQUE` constraints on one or more simple columns.
- Basic `CHECK` constraints whose expressions use existing BicDB predicate
  support, such as comparisons joined with `AND` or `OR`.
- Basic `FOREIGN KEY` constraints over simple columns, including `ON UPDATE` and
  `ON DELETE` `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`, and
  `SET DEFAULT` when the referencing columns have BicDB-supported constant or
  sequence defaults.

Foreign keys support `DEFERRABLE INITIALLY IMMEDIATE`, `DEFERRABLE INITIALLY
DEFERRED`, and transaction-local `SET CONSTRAINTS` timing. Savepoints restore
timing and queued checks; invalid final references abort commit with 23503.
Other constraint classes remain immediate. See [deferred foreign keys](postgresql-procedural.md#deferred-foreign-keys)
for the supported timing commands and constraint-trigger limits.

`ALTER TABLE ... ADD CONSTRAINT` validates existing rows before committing the
catalog change by default. Add `NOT VALID` to skip that initial scan for
`UNIQUE`, `CHECK`, or `FOREIGN KEY` constraints during large backfills; new
writes are still checked. Run
`ALTER TABLE ... VALIDATE CONSTRAINT constraint_name` after backfill to scan
existing rows and mark `pg_catalog.pg_constraint.convalidated = true`.

Constraint violations return PostgreSQL-shaped SQLSTATE categories:

- `23502` for not-null violations.
- `23503` for foreign-key violations.
- `23505` for unique violations.
- `23514` for check violations.

Constraint metadata appears in:

- `information_schema.columns` through `is_nullable`.
- `information_schema.table_constraints`.
- `information_schema.key_column_usage`.
- `information_schema.check_constraints`.
- `pg_catalog.pg_attribute` through `attnotnull`.
- `pg_catalog.pg_class` through `relchecks`.
- `pg_catalog.pg_constraint` with `contype` values `p`, `u`, `c`, `f`, and
  generated not-null rows using `n`. `convalidated` reflects `NOT VALID` and
  later `VALIDATE CONSTRAINT` state.

Use `bicdb integrity check <path>` or `bicdb check <path>` to combine
storage-frame verification with SQL integrity validation for primary keys,
unique constraints, foreign keys, check constraints, indexes, and SQL
schema/catalog consistency. Add `--json` for the machine-readable report:

```bash
bicdb integrity check ./erpdb --json
```

Current limits:

- Constraint columns must be simple column identifiers; expression indexes and
  ordered constraint columns are not supported as table constraints.
- `CHECK` expressions are limited to BicDB's existing predicate evaluator.
- `SET DEFAULT` foreign-key actions require a supported default on each
  referencing column; otherwise the action sets `NULL`, which can still fail
  `NOT NULL` or foreign-key validation.
- Deferred timing is implemented for foreign keys. Named timing changes for
  other constraint classes, and ALL when constraint triggers are present, fail
  explicitly.
- `ALTER TABLE DROP CONSTRAINT IF EXISTS` removes persisted `UNIQUE`, `CHECK`,
  and `FOREIGN KEY` table constraints by name. Dropping primary keys is not
  supported.

## JSON-key partial unique indexes

A JSONB column may be tested for a top-level key in a partial-index predicate:

```sql
CREATE UNIQUE INDEX monitoring_occurrence_key
ON assignments ((payload_json->>'occurrence_key'))
WHERE payload_json ? 'occurrence_key';
```

Object keys count even when their value is JSON null. String array elements and
an equal scalar JSON string also match, following JSONB `?` semantics. Missing
keys, SQL NULL, nested-only keys and other scalar values stay outside the index.
Predicates compose with supported AND/OR, tenant comparisons and soft-delete
checks. Index membership is updated with writes and survives reopening the DB.
Runtime uniqueness failures return SQLSTATE 23505 and the index's constraint
name; native host callers receive a conflict rather than an internal error.

New JSON expression-index definitions retain the column root: for example,
`payload_json->>'occurrence_key'` indexes `payload_json.occurrence_key`. Older
builds could persist a path that omitted `payload_json`. Those existing SQL
indexes need to be recreated from their SQL definitions under the corrected
runtime; rebuilding stored index contents alone retains their old field paths.
Recreating a unique index must validate existing records and refuse duplicates.
