# PostgreSQL CTEs

BicDB supports a focused PostgreSQL-compatible subset of common table
expressions for `SELECT` queries.

Supported:

```sql
WITH adults AS (
  SELECT id, name FROM patients WHERE age > 40
)
SELECT id, name FROM adults;
```

CTE column aliases replace the output column names for the CTE, matching basic
PostgreSQL visibility rules:

```sql
WITH renamed(patient_id, years) AS (
  SELECT id, age FROM patients
)
SELECT patient_id, years FROM renamed;
```

Multiple non-recursive CTEs may be declared in one `WITH` clause. CTEs are
materialized in memory for the containing query and can be referenced as row
sources by later CTEs and by the final `SELECT`.

Unsupported:

- `WITH RECURSIVE` returns PostgreSQL-shaped unsupported-feature SQLSTATE
  `0A000` with a clear recursive CTE error.
- `WITH ... INSERT` and `WITH ... UPDATE` return unsupported-feature SQLSTATE
  `0A000`. Plain `INSERT ... VALUES` and simple `UPDATE` remain supported.
- Data-modifying statements inside CTE definitions are not implemented.

