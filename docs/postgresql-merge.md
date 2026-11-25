# PostgreSQL MERGE

BicDB executes PostgreSQL 18 `MERGE`:

```sql
MERGE INTO inventory t
USING (SELECT sku, sum(qty) AS qty FROM receipts GROUP BY sku) s
ON t.sku = s.sku
WHEN MATCHED AND s.qty = 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty
WHEN NOT MATCHED AND s.qty < 0 THEN DO NOTHING
WHEN NOT MATCHED [BY TARGET] THEN INSERT (sku, qty) VALUES (s.sku, s.qty)
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET stale = true
RETURNING merge_action(), t.*, old.qty, new.qty;
```

Supported:

- Source: a table, a subquery, or `VALUES` with an alias.
- Any number of `WHEN` clauses, each with an optional `AND condition`. Each
  joined row takes the first clause of its kind (`MATCHED`, `NOT MATCHED [BY
  TARGET]`, `NOT MATCHED BY SOURCE`) whose condition holds; a clause after an
  unconditional clause of the same kind is rejected (42601, as PostgreSQL).
- Actions: `UPDATE SET ...`, `DELETE`, `INSERT [(columns)] VALUES (...)`
  (including `DEFAULT`), `INSERT DEFAULT VALUES` and `DO NOTHING`.
- `NOT MATCHED` clauses see only the source and `NOT MATCHED BY SOURCE`
  clauses only the target; other references fail with 42P01.
- `RETURNING` with `merge_action()`, target and source columns, `*` (source
  columns, then target columns) and the `old` / `new` row images.
- The command tag is `MERGE n`, counting inserted, updated and deleted rows.

Semantics:

- The source and the ON join are evaluated once, from one snapshot, before
  any change. A target row may be changed (updated or deleted) by only one
  source row; a second change fails the statement with 21000 "MERGE command
  cannot affect row a second time". Repeated matches that reach `DO NOTHING`
  or no clause are allowed.
- Every action is executed by the ordinary UPDATE, DELETE and INSERT
  executors, so privileges (checked up front for every action named in the
  statement, as PostgreSQL does), CHECK / NOT NULL / unique / foreign-key
  constraints, defaults, identity and generated columns, and row triggers
  (with `TG_OP` set to the action) behave as for those statements. Rows are
  processed in source order; consecutive rows of one clause run as a batch.
- The statement is atomic: any error rolls back every action.
- Concurrency follows UPDATE ... FROM and DELETE ... USING: the chosen
  target rows are written through the same conflict checks as those
  statements (concurrent-update behaviour under each isolation level is
  theirs; it has not been separately tested for MERGE).

Not supported (fails with 0A000 instead of running with different
semantics): MERGE into a view, a partitioned table or a table with row-level
security when the statement has more than one clause or a non-UPDATE action
(a single `WHEN MATCHED THEN UPDATE` clause works on them), and changing a
primary-key column in `UPDATE SET`. `WITH ... MERGE` and statement-level
triggers are not available. The relative order of row triggers across
different clauses can differ from PostgreSQL's interleaved join order.
