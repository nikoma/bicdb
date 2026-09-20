# BicDB Schema Migrations

BicDB provides a conservative production migration runner for greenfield transactional
deployments:

```sh
bicdb migrate dry-run /data/erpdb --dir ./migrations --lock-timeout-ms 30000
bicdb migrate apply /data/erpdb --dir ./migrations --lock-timeout-ms 30000
bicdb migrate status /data/erpdb --json
bicdb migrate rollback /data/erpdb --down ./rollback/0007_down.sql
bicdb migrate repair /data/erpdb
```

Migration files are `*.sql` files sorted by filename. The migration id is the
filename stem, for example `0001_initial` for `0001_initial.sql`.

## Transactional DDL Contract

The runner classifies every statement before applying any pending migration.
If a statement is unsupported or may rewrite rows, `dry-run` and `apply` fail
before mutation.

Transactional metadata DDL:

- `CREATE TABLE`, `CREATE VIEW`, `CREATE SEQUENCE`
- supported routine, trigger, and role metadata DDL
- `DROP TABLE`, `DROP INDEX`, `DROP SEQUENCE`, `DROP VIEW`
- routine and trigger drops

These operations commit through BicDB atomic catalog or system-record writes.
If the process stops before the atomic write, reopen sees the old committed
catalog. If it stops after the write, reopen sees the completed new catalog.

Online-safe DDL:

- `CREATE INDEX`, which builds from committed records before publishing the
  index definition
- `ALTER TABLE ADD COLUMN` when the column is nullable and has no default
- `ALTER TABLE ADD CONSTRAINT` and `DROP CONSTRAINT`
- `ALTER TABLE ALTER COLUMN SET/DROP NOT NULL`
- `ALTER TABLE ALTER COLUMN SET/DROP DEFAULT`
- row-level-security metadata toggles
- `ANALYZE`

Row-rewriting DDL is rejected by the migration runner and must be decomposed
into online phases:

- `ADD COLUMN` with `DEFAULT` or `NOT NULL`
- `DROP COLUMN`
- `RENAME COLUMN`
- `RENAME TO`
- `ALTER COLUMN TYPE`
- generated-column rewrites

The direct SQL engine still supports several row-rewriting ALTER forms for
developer and fixture compatibility, but they are not migration-transactional
and should not be used against production transactional databases.

## Metadata and Recovery

The runner persists migration state in BicDB system collections:

- `__bicdb_schema_version`: current version, latest migration id, and applied
  count.
- `__bicdb_migration_history`: migration id, file, checksum, status, timestamps,
  statement count, and failure text.

These collections are ordinary durable BicDB records, so backups and restores
carry migration history with the database. `migrate status` reads those records
after reopen or restore.

During `apply` and `rollback`, the runner writes a lock record and a pending
history record before executing statements. A crash therefore leaves an
observable pending migration instead of an untracked half-applied state.
`migrate repair` marks pending records as failed and clears the lock after an
operator has inspected the schema and chosen whether to retry or write a repair
migration.

Rollback requires an explicit `--down` SQL file. BicDB does not infer inverse
DDL because safe rollback depends on application semantics and retained data.

## Online transactional pattern

Use this sequence for production application schema changes:

1. Add a nullable column with no default:

   ```sql
   ALTER TABLE invoices ADD COLUMN approval_status TEXT;
   ```

2. Backfill in application-controlled chunks using bounded DML. Keep batches
   small enough for normal write latency and retry conflicts at the job layer.

3. Add indexes with `CREATE INDEX`. BicDB builds the index from committed
   records and publishes it after verification.

4. Add or validate constraints. For not-null, run an application validation
   query first, then:

   ```sql
   ALTER TABLE invoices ALTER COLUMN approval_status SET NOT NULL;
   ```

5. Deploy application code that depends on the stricter contract only after
   `bicdb migrate status` reports the migration as applied.

Before every application release:

- run `bicdb migrate dry-run` in CI;
- take and verify a backup;
- apply to a restored staging copy;
- run compatibility, pgwire, transaction, backup, and restore smoke tests;
- record the migration status output with the release evidence.
