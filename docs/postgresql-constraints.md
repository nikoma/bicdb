# PostgreSQL Constraints

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

Constraints are immediate. BicDB accepts explicit `NOT DEFERRABLE` metadata,
but rejects `DEFERRABLE` and `INITIALLY ...` constraint characteristics with
SQLSTATE `0A000` instead of pretending to support deferred transaction-time
validation.

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
- Foreign-key enforcement is immediate and non-deferrable; deferred constraint
  transactions are explicitly unsupported.
- `ALTER TABLE DROP CONSTRAINT IF EXISTS` removes persisted `UNIQUE`, `CHECK`,
  and `FOREIGN KEY` table constraints by name. Dropping primary keys is not
  supported.
