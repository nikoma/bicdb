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

A table without a declared `PRIMARY KEY` has none, as in PostgreSQL: rows
carry a hidden internal identity, and no column becomes unique or `NOT NULL`
implicitly (earlier releases promoted a column named `id` to the primary key;
tables created by those releases keep their stored definition). `REFERENCES t`
without a column list names t's primary key and fails with 42704 when t has
none, and `ON CONFLICT (columns)` on a table with no unique arbiter fails with
42P10.

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
See [deferred foreign keys](postgresql-procedural.md#deferred-foreign-keys)
for the supported timing commands and constraint-trigger limits.

`UNIQUE` constraints (column or table form, and `ALTER TABLE ... ADD
CONSTRAINT`) also accept `DEFERRABLE [INITIALLY IMMEDIATE | INITIALLY
DEFERRED]`, with PostgreSQL's timing:

- A non-deferrable `UNIQUE` constraint is checked row by row, so
  `UPDATE t SET a = a + 1` over keys 1 and 2 fails with 23505, as in
  PostgreSQL.
- A `DEFERRABLE` constraint in IMMEDIATE mode is checked when the statement
  ends, so the same `UPDATE` succeeds.
- In DEFERRED mode (`INITIALLY DEFERRED` or `SET CONSTRAINTS name|ALL
  DEFERRED`) the changed rows are queued and their final state is checked at
  `COMMIT`, or when `SET CONSTRAINTS ... IMMEDIATE` switches the constraint
  back. A duplicate left at commit fails with 23505 and the transaction rolls
  back. Savepoint rollback restores the queue and the timing.
- `pg_constraint.condeferrable`/`condeferred`, `pg_get_constraintdef`,
  `information_schema.table_constraints` and `pg_index.indimmediate` report
  the timing.
- `ON CONFLICT` refuses a deferrable arbiter with 55000, as PostgreSQL does
  (without a conflict target every unique constraint is an arbiter).

The storage index behind a deferrable constraint is non-unique, because the
key may be duplicated until the check runs. The SQL write path enforces the
constraint and reserves each checked key until the transaction ends, so
concurrent transactions cannot both commit one key. Writes made through the
native record API bypass SQL constraint checks and are not covered.

`PRIMARY KEY ... DEFERRABLE` is refused with 0A000: the primary key is the row
identity in storage, so two rows can never share one even transiently.
`DEFERRABLE` unique constraints on partitioned tables are also refused with
0A000. Other constraint classes remain immediate.

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
- Deferred timing is implemented for foreign keys and unique constraints.
  Named timing changes for other constraint classes, and ALL when constraint
  triggers are present, fail explicitly.
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

## Violation DETAIL lines

Constraint violations carry PostgreSQL's DETAIL line in the error response:

| violation | DETAIL |
|---|---|
| unique / primary key (`23505`) | `Key (a, b)=(1, x) already exists.` |
| `CREATE UNIQUE INDEX` / `ADD UNIQUE` / `ADD PRIMARY KEY` over duplicates | `Key (a)=(1) is duplicated.` (the smallest duplicated key, as PostgreSQL's sorted build reports it) |
| foreign key, referencing row (`23503`) | `Key (x)=(5) is not present in table "p".` |
| foreign key, referenced row (`23503`) | `Key (a)=(1) is still referenced from table "c".` with PostgreSQL's `update or delete on table ...` message |
| exclusion (`23P01`) | `Key (r)=([3,8)) conflicts with existing key (r)=([1,5)).` (plain-column constraints) |
| CHECK (`23514`) and NOT NULL (`23502`) | `Failing row contains (1, null, abc).` (every column in order, each value cut at 64 bytes) |

Values are printed by their types' output functions (`t`/`f`, `null`, PostgreSQL float
and jsonb text). As in PostgreSQL, values are shown only to a role that may read them:
a superuser, or a role with SELECT on the relation the values come from when row-level
security does not apply to it. Otherwise the key DETAIL is omitted and a foreign-key
violation says `Key is not present in table "p".` / `Key is still referenced from table
"c".`. Unique keys over expressions other than `lower()`/`upper()` of a column are
reported without a DETAIL.

`DROP TABLE` / `DROP VIEW` without CASCADE refuse a relation that views or foreign keys
depend on (`2BP01`, with a DETAIL line per dependent object and PostgreSQL's HINT); with
CASCADE they drop the dependent views and constraints and report `drop cascades to ...`
NOTICEs. An Oracle-profile database keeps its views when a table is dropped.
