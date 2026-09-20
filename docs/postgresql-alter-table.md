# PostgreSQL ALTER TABLE Compatibility

BicDB supports the common `ALTER TABLE` forms used by ORM migration tools for
single-table schema evolution:

```sql
ALTER TABLE patients ADD COLUMN email TEXT;
ALTER TABLE patients ADD COLUMN active BOOLEAN DEFAULT true;
ALTER TABLE patients ADD COLUMN profile JSONB DEFAULT '{"tier":"gold"}'::jsonb;
ALTER TABLE patients DROP COLUMN active;
ALTER TABLE patients RENAME COLUMN name TO full_name;
ALTER TABLE patients RENAME TO people;
ALTER TABLE people ALTER COLUMN age TYPE TEXT;
ALTER TABLE visits DROP CONSTRAINT IF EXISTS visits_patient_id_fkey;
ALTER TABLE visits
  ADD CONSTRAINT visits_patient_id_fkey
  FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE;
ALTER TABLE visits
  ADD CONSTRAINT visits_patient_id_fkey
  FOREIGN KEY (patient_id) REFERENCES patients(id) NOT VALID;
ALTER TABLE visits VALIDATE CONSTRAINT visits_patient_id_fkey;
ALTER TABLE patients ENABLE ROW LEVEL SECURITY;
ALTER TABLE patients FORCE ROW LEVEL SECURITY;
ALTER TABLE people ALTER COLUMN age SET NOT NULL;
ALTER TABLE people ALTER COLUMN sequence SET DEFAULT nextval('people_sequence_seq');
ALTER TABLE people ALTER COLUMN status SET DEFAULT 'active';
```

Supported `ADD COLUMN` defaults are constant scalar and JSON expressions that
can be evaluated without reading other columns. Existing rows are backfilled
with the default value. Adding a `NOT NULL` column without a default fails when
existing rows would contain nulls.

`DROP COLUMN` removes the column from schema/catalog metadata and from stored
record metadata. Dropping primary key columns is not supported.

`RENAME COLUMN` updates schema/catalog metadata, stored record metadata, and
simple constraint/index column references. Renaming primary key columns is not
supported.

`RENAME TO` copies records into the new collection name, recreates secondary
indexes for the new table, updates owned sequence metadata, and removes the old
collection/schema entry. Because this rewrites collection state, the production
migration runner rejects it; use a planned copy/cutover migration instead.

`ALTER COLUMN ... TYPE` supports practical casts that BicDB can apply directly
to existing stored values, such as integer-to-text and text-to-integer when the
values parse cleanly. `USING` expressions and generated-column rewrites are
reported with PostgreSQL-style unsupported-feature SQLSTATE `0A000`.

`ALTER COLUMN ... SET DEFAULT` supports PostgreSQL sequence defaults in the
form `nextval('sequence_name')` and constant scalar/JSON defaults that BicDB
can evaluate without reading other columns. `DROP DEFAULT` clears the default.
`ALTER COLUMN ... SET NOT NULL` validates existing rows first and fails with
SQLSTATE `23502` if any row contains a null value for the column.

`ADD CONSTRAINT` supports the same simple `UNIQUE`, `CHECK`, and `FOREIGN KEY`
constraint subset as `CREATE TABLE`, including basic `ON DELETE`/`ON UPDATE`
referential actions for foreign keys. Existing rows are validated immediately
unless `NOT VALID` is specified; `NOT VALID` constraints are enforced for new
writes and can be scanned later with `VALIDATE CONSTRAINT`. `DROP CONSTRAINT IF
EXISTS` removes persisted table constraints by name; dropping primary keys is
not supported. `DEFERRABLE` and `INITIALLY ...` constraint characteristics are
explicitly rejected with SQLSTATE `0A000`.

`ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` are accepted and
persisted. BicDB exposes the flags through
`pg_catalog.pg_class.relrowsecurity` and
`pg_catalog.pg_class.relforcerowsecurity`, and enforces the generated-application
policy expression subset described in `docs/security.md`.

For production transactional deployments, use [`bicdb migrate`](schema-migrations.md).
The runner accepts metadata-only and online-safe ALTER forms, and rejects
row-rewriting ALTER forms such as `ADD COLUMN DEFAULT`, `DROP COLUMN`,
`RENAME COLUMN`, `RENAME TO`, and `ALTER COLUMN TYPE` before any pending
migration mutates the database.
