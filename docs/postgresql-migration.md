# Migrating a PostgreSQL workload

Use a rehearsed **logical** migration. PostgreSQL data directories, physical
backups and WAL are not BicDB storage formats. PostgreSQL drivers alone do not
prove application compatibility. The starting point is the
[operations comparison](postgresql-operations-comparison.md).

1. Inventory the source version, schemas, types, extensions, functions/triggers,
   sequences, partitioning, ownership/grants/RLS and application SQL. Include
   transaction isolation, lock/retry assumptions, timezone/collation behavior,
   jobs and authentication integrations. Test required behavior against the exact
   BicDB binary. Treat unsupported extensions and native functions as porting work.
2. Establish a dedicated test target with [configuration](server-configuration.md),
   durable storage, TLS and appropriate SQL users. Keep credentials out of exports
   and logs. Do not point a trial migration at production BicDB data.
3. Export schema and data logically using the PostgreSQL client tools appropriate
   for your source. A plain SQL export can be inspected and transformed; custom
   format `pg_restore` compatibility is not assumed. For example:

   ```sh
   pg_dump --dbname "$SOURCE_DSN" --schema-only --no-owner --no-privileges > schema.sql
   pg_dump --dbname "$SOURCE_DSN" --data-only --no-owner --no-privileges > data.sql
   ```

   These are separate snapshots unless coordinated: use a consistent snapshot or
   quiesce writes for a real migration. Follow PostgreSQL's
   [logical backup guidance](https://www.postgresql.org/docs/18/backup-dump.html).
   Export files can contain sensitive application data; restrict access.
4. Review and adapt schema SQL first. Preserve an explicit record of changes;
   never silently skip failed statements. Restore reviewed SQL through `psql`
   against the target, for example `psql "$TARGET_DSN" --set ON_ERROR_STOP=on
   --file reviewed-schema.sql`. Load data using compatible SQL or text/CSV COPY;
   binary COPY is unsupported. Large imports should use bounded batches with
   reconciliation rather than one unbounded transaction.
5. Reconcile row counts, primary/foreign keys, sequence positions and representative
   value hashes, including NULLs, precision, bytea, timestamps and text. Rebuild
   and inspect indexes. Recreate and review ownership/grants/RLS deliberately:
   the sample exports omit ownership/ACLs, and PostgreSQL password catalogs are
   not a portable provisioning mechanism.
6. Run full application workflows, concurrency/retry tests, reporting, connection
   pooling, backup/restore and failover under representative load. Check isolation
   and locking differences explicitly. Keep an acceptance record with failures;
   successful import is not complete migration.
7. Rehearse cutover. Stop or fence source writes, take the final consistent export
   or reconcile a separately qualified change-capture process, load and verify,
   then redirect clients. No general PostgreSQL logical-slot/CDC migration
   connector or zero-downtime cutover is promised by this guide.
8. Keep the source available under a documented rollback plan. Once the target
   accepts new writes, switching back requires reconciling those writes; changing
   a connection string alone does not provide lossless rollback.

See [production deployment](production-deployment.md) for resource, storage,
security and recovery checks before accepting authoritative traffic.
