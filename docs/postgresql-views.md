# PostgreSQL Views Compatibility

BicDB supports PostgreSQL views, automatically updatable views with
`WITH CHECK OPTION`, materialized views with `REFRESH MATERIALIZED VIEW`, and
`CREATE TABLE AS` / `SELECT INTO`. The cases in
`fixtures/postgres-compat/pgsql-parity/ctas-views.cases` are compared against
PostgreSQL 18.4.

## Views

```sql
CREATE VIEW patient_names AS
SELECT id, name FROM patients;

SELECT id, name FROM patient_names ORDER BY id;

CREATE VIEW patient_appointments AS
SELECT patients.name AS patient_name, appointments.doctor AS doctor
FROM patients
JOIN appointments ON patients.id = appointments.patient_id;

DROP VIEW patient_appointments;
```

Views are stored query definitions evaluated at read time. Column aliases in
`CREATE VIEW name (column, ...) AS ...` are stored and exposed through
`information_schema` and `pg_catalog` (`relkind` `v`). By default a view reads
its tables with its owner's privileges and row-level security; a view created
`WITH (security_invoker = true)` uses the caller's.

## View options

`CREATE VIEW ... WITH (option [= value], ...)` and `ALTER VIEW ... SET
(option [= value], ...)` / `RESET (option, ...)` accept PostgreSQL's view
options, each with its PostgreSQL meaning:

- `security_invoker`: the view reads its relations with the privileges and
  row-level security of the querying role instead of the view owner's.
- `security_barrier`: predicates from outside the view are never pushed into
  it, so a leaky function in a query cannot see rows the view filters out.
- `check_option = local | cascaded`: the same check as `WITH [LOCAL |
  CASCADED] CHECK OPTION`.

An option written without a value is `true`. Boolean options take
PostgreSQL's boolean spellings (`true`, `on`, `1`, ...); anything else fails
with 22023 `invalid value for boolean option`, and an unknown option with
22023 `unrecognized parameter`. `pg_class.reloptions` lists the options as
written, in order (`{security_invoker=false,security_barrier=true}`), plus
`check_option=...` from a `WITH CHECK OPTION` clause. `CREATE OR REPLACE
VIEW` replaces the options. Only the view's owner may change them (42501).
Storage parameters of materialized views are refused with 0A000.

## Automatically updatable views

`INSERT`, `UPDATE` and `DELETE` (with `RETURNING`) work on views that follow
PostgreSQL's rules: exactly one table or view in `FROM`, and no `DISTINCT`,
`GROUP BY`, `HAVING`, `LIMIT`/`OFFSET`, `UNION`/`INTERSECT`/`EXCEPT`, `WITH`,
aggregate, window or set-returning function at the top level.

```sql
CREATE VIEW active_patients AS
SELECT id, name AS display_name, upper(name) AS shout
FROM patients WHERE active;

INSERT INTO active_patients (id, display_name) VALUES (7, 'Ada');
UPDATE active_patients SET display_name = 'Ada L.' WHERE id = 7 RETURNING *;
DELETE FROM active_patients WHERE id = 7;
```

- Columns that are plain references to a base column are updatable; writing
  any other column fails with `0A000` (`cannot insert into column "shout" of
  view "active_patients"`). A view that is not updatable fails with `0A000`
  and PostgreSQL's detail (`Views containing DISTINCT are not automatically
  updatable.` and so on).
- An `UPDATE` or `DELETE` only reaches rows the view shows. An `INSERT` may
  add a row the view does not show, unless the view has a check option.
- Columns of the base table that the view does not expose cannot be named
  (`42703`), and the base table is not in scope under its own name (`42P01`).
- Views over updatable views work level by level.
- `WITH [CASCADED | LOCAL] CHECK OPTION` (and `WITH (check_option = ...)`)
  is enforced for `INSERT` and `UPDATE`: a new row that does not satisfy the
  condition (false or NULL) fails with `44000` `new row violates check option
  for view "v"`, naming the innermost failing view, and the whole statement is
  rolled back. `CASCADED` also checks the conditions of the views below;
  `LOCAL` checks its own condition and those of lower views that have their
  own check option. A check option on a view that is not automatically
  updatable is refused with `0A000`.

Privileges follow PostgreSQL: the caller needs the privilege on the view; each
underlying table or view is checked as the owner of the view above it (or as
the caller for a `security_invoker` view). The caller needs no privilege on the
base table. Row-level security of the base table applies to the role that
writes it: for a `security_invoker` view or a view owned by the caller, the
policies apply exactly as on the table. Writing through a definer view owned
by a different role into a table with row-level security enabled is refused
with `0A000` instead of evaluating the policies for the wrong role.

Not supported (`0A000`): `INSERT ... ON CONFLICT` on a view, `UPDATE ... FROM`
and `DELETE ... USING` on a view, an unqualified reference inside a subquery to
a renamed or computed view column when the subquery's own relations cannot be
resolved. A CHECK OPTION check needs SELECT on the base table for the role
that writes it.

## INSTEAD OF triggers

```sql
CREATE TRIGGER pairs_write INSTEAD OF INSERT OR UPDATE OR DELETE ON pairs
FOR EACH ROW EXECUTE FUNCTION pairs_write();
```

A view with an INSTEAD OF trigger for the statement's event (updatable or
not) hands each affected row to its INSTEAD OF triggers in name order, with
`NEW`/`OLD` as view rows and `TG_WHEN = 'INSTEAD OF'`; the base relation is
not written by the statement itself. A trigger returning NULL skips the row,
which is then not counted and not returned. `RETURNING` projects the row the
last trigger returned (`OLD` for DELETE). The rows an UPDATE or DELETE affects
are read through the view, so the caller needs SELECT on it. PostgreSQL's
restrictions apply: INSTEAD OF triggers only on views (`42809` on a table),
FOR EACH ROW only, no WHEN condition or column list, and views take no
row-level BEFORE/AFTER triggers (`42809`). Statement-level triggers on views,
and `ON CONFLICT`, `UPDATE ... FROM` and `DELETE ... USING` on a view with an
INSTEAD OF trigger, are not supported (`0A000`). Rules (`CREATE RULE ... DO
ALSO | INSTEAD`) are described in `postgresql-application-compatibility.md`.

## Materialized views

```sql
CREATE MATERIALIZED VIEW visit_counts AS
SELECT doctor, count(*) AS visits FROM appointments GROUP BY doctor;

REFRESH MATERIALIZED VIEW visit_counts;
REFRESH MATERIALIZED VIEW visit_counts WITH NO DATA;
REFRESH MATERIALIZED VIEW CONCURRENTLY visit_counts;
```

A materialized view stores the rows its query returned when it was created or
last refreshed; reads return that snapshot (`relkind` `m`). The query runs with
the view owner's privileges. `WITH NO DATA` (on `CREATE` or `REFRESH`) leaves
the view unpopulated, and reading it fails with `55000` `materialized view
"visit_counts" has not been populated`. `REFRESH` requires ownership (`42501`),
is transactional, and fails with `42809` on a relation that is not a
materialized view. `CONCURRENTLY` requires a populated view with a unique index
without a `WHERE` clause (`55000` otherwise) and produces the same result as a
plain refresh. A materialized view cannot be written (`42809` `cannot change
materialized view`).

The snapshot is kept in the view's catalog descriptor, which suits small and
medium result sets. Only views whose columns all have built-in scalar types
keep a snapshot; a populated materialized view with an array, composite,
user-defined or spatial column still evaluates its query at read time. Materialized views created by earlier BicDB versions have
no snapshot and keep evaluating their query at read time until they are
recreated.

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
