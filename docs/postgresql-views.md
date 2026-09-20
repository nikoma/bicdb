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
