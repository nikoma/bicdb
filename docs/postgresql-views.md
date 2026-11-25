# PostgreSQL Views Compatibility

BicDB supports a focused PostgreSQL-compatible logical view surface for client
DDL, reads, and introspection. These SQL views are stored query definitions and
are evaluated at read time. They are not BicDB materialized projections.

## Supported

```sql
CREATE VIEW patient_names AS
SELECT id, name FROM patients;

SELECT id, name FROM patient_names ORDER BY id;

CREATE VIEW patient_appointments AS
SELECT patients.name AS patient_name, appointments.doctor AS doctor
FROM patients
JOIN appointments ON patients.id = appointments.patient_id;

DROP VIEW patient_appointments;
DROP VIEW patient_names;
```

View definitions must be SELECT queries within BicDB's supported SQL subset.
Simple views and views over supported joins can be selected like tables.

Column aliases in `CREATE VIEW name (column, ...) AS ...` are stored and exposed
through introspection.

## Introspection

Views are exposed in the practical catalog layer:

```sql
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_name = 'patient_names';

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'patient_names'
ORDER BY ordinal_position;

SELECT relname, relkind
FROM pg_catalog.pg_class
WHERE relname = 'patient_names';
```

`information_schema.tables.table_type` is `VIEW` and
`pg_catalog.pg_class.relkind` is `v` for SQL views.

## Unsupported

Materialized views are not implemented:

```sql
CREATE MATERIALIZED VIEW patient_names_mat AS
SELECT id, name FROM patients;
```

Updatable view behavior is also not implemented:

```sql
INSERT INTO patient_names (id, name) VALUES ('p1', 'Ada');
UPDATE patient_names SET name = 'Ada';
DELETE FROM patient_names;
```

These unsupported operations return SQLSTATE `0A000` with explicit errors.

## CREATE TABLE AS and SELECT INTO

```sql
CREATE TABLE recent AS SELECT id, name FROM patients WHERE created > now() - interval '1 day';
CREATE TABLE named_copy (patient_id, patient_name) AS SELECT id, name FROM patients;
CREATE TABLE IF NOT EXISTS shape_only AS SELECT * FROM patients WITH NO DATA;
CREATE TEMP TABLE scratch AS TABLE patients;
SELECT id, name INTO archive FROM patients WHERE NOT active;
```

The new table gets the query's output column names (`?column?`, function and
cast names as PostgreSQL figures them) and types, including type modifiers
(`varchar(10)`, `numeric(8,2)`, `timestamp(3)`). A column-name list renames the
leading columns; too many names fail with `42601` and duplicate names with
`42701`. The table has no key and no constraints. The query runs once with the
creator's privileges and row-level security (SELECT on every source is
required); its rows are written through the normal write path in the same
statement transaction as the table, so a failing query leaves no table. The
command tag is `SELECT n`, or `CREATE TABLE AS` for `WITH NO DATA` and a skipped
`IF NOT EXISTS`. `SELECT ... INTO` inside a subquery fails with `42601`.
`CREATE TABLE ... AS EXECUTE` and `SELECT ... INTO UNLOGGED` are not supported
(`0A000`). Temporary tables follow BicDB's existing `CREATE TEMP TABLE`
behavior.
