# PostgreSQL Transactions

BicDB supports single-node transactions through the SQL engine and PostgreSQL
wire server. The implemented isolation behavior is `READ COMMITTED` style:
statements never see uncommitted writes from other transactions, and later
statements can see rows committed by other transactions. application mode relies on
this visibility plus optimistic conflict detection; it does not require or
claim PostgreSQL `REPEATABLE READ` or `SERIALIZABLE`.

Accepted PostgreSQL isolation command forms:

```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN ISOLATION LEVEL READ COMMITTED;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;
START TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

`READ ONLY` and `READ WRITE` transaction access modes are parsed for client
compatibility and currently do not change execution behavior.

Unsupported isolation levels are rejected instead of being silently accepted:

- `REPEATABLE READ`
- `SERIALIZABLE`
- `READ UNCOMMITTED`
- `SNAPSHOT`
- transaction snapshots

These return SQLSTATE `0A000` through PostgreSQL-shaped error responses on the
wire protocol. BicDB does not implement serializable validation or repeatable
read snapshots.

Concurrent writes use per-record locks and optimistic conflict checks. A
transaction conflict is reported as SQLSTATE `40001`; the application must retry
the complete transaction. The core has bounded waiting and configurable conflict
policies, so accepting concurrent writers does not imply PostgreSQL-identical
blocking or deadlock behavior. After an error, pgwire reports failed transaction
status until `ROLLBACK` or a valid `ROLLBACK TO SAVEPOINT`.

See [TRANSACTIONS.md](../TRANSACTIONS.md) for storage guarantees and remaining
transaction limits.

Schema migration DDL has a separate production contract. Metadata-only DDL is
committed through atomic BicDB catalog/system-record writes, while row-rewriting
ALTER forms are rejected by `bicdb migrate` and must be decomposed into online
steps. See [`docs/schema-migrations.md`](schema-migrations.md).

## Triggered mutations

INSERT, UPDATE, DELETE, and data-modifying CTE statements with enabled row
triggers run inside a transaction, including their authored trigger bodies.
COPY also runs transactionally. Trigger rejection rolls back the statement's writes and deferred effects. Pgwire COPY
FROM uses one transaction across spilled/replayed input batches, so a later
failure also removes earlier batches. Triggers cannot issue transaction-control
commands to commit around a failing guard.

The existing requirement for signed mutation grants on native-policy protected
collections inside transactions also applies to triggered mutations and COPY.
Trigger-free INSERT/UPDATE/DELETE retain their existing autocommit authorization
path; adding a trigger to an unrelated table does not change it.

For an existing transaction, the embedded SQL session restores the failed
mutation's rollback mark and transactional settings; sequence consumption and
currval/lastval remain nontransactional. Pgwire additionally maintains its failed
transaction status and requires rollback as described above.

## Locking reads

`SELECT ... FOR UPDATE`, `NOWAIT`, and `SKIP LOCKED` acquire transaction row
locks shared with INSERT/UPDATE/DELETE. Supported targets are base tables with
primary keys, including inner joins and `FOR UPDATE OF alias`. Locking selects
inside a derived table also execute their locks (for example, an outer
`string_agg` over a limited queue selection). `SKIP LOCKED` excludes contended
rows before applying LIMIT/OFFSET. SQL SELECT and UPDATE privileges and ordinary
row visibility apply before locking; hidden tenant rows are not reserved.

Locking queries evaluate ordinary output expressions after acquiring locks and
applying LIMIT/OFFSET. Skipped, unreturned rows do not execute those expressions.
An output expression referenced by ORDER BY must run before sorting; its result
is reused for output rather than evaluated again. Output aliases and positional
ORDER BY refer to the visible select list, including wildcard expansion. Rows
skipped by OFFSET remain locked, while LIMIT 0 emits no output effects. These
rules follow PostgreSQL's [SELECT evaluation and locking semantics](https://www.postgresql.org/docs/18/sql-select.html).

For a joined candidate skipped by SKIP LOCKED, BicDB releases the read locks
newly acquired for that candidate. Locks held before the candidate—including
those retained for earlier returned rows—remain held. This prevents an empty
join result from leaving new locks on an earlier relation in the join.

Locks survive individual statements and are released by commit, rollback, drop,
or rollback to a savepoint created before the lock. Implicit transactions release
them at statement end. Extended-protocol Describe resolves locking-query metadata
without executing the query or acquiring locks.

BicDB's contention contract differs from PostgreSQL's blocking default:
`FOR UPDATE` returns retryable SQLSTATE `40001` on contention or a stale selected
row; retry the entire transaction. `NOWAIT` reports `55P03` for an occupied lock.
`SKIP LOCKED` skips occupied locks, but a row changed since the statement snapshot
can still require a `40001` retry. The executor does not wait for another commit
while holding its shared database latch. Applications must handle these retries.

FOR SHARE, tables without primary keys, outer-join locking, DISTINCT/grouped/window
locking queries, projection set-returning functions with row locks, and FETCH
with row locks are explicitly unsupported. Selection
currently materializes eligible candidates before locking; this is correctness
coverage, not a throughput claim for large work queues.

An explicit `BEGIN; ...` simple-query message now retains connection transaction
state across messages, including SET LOCAL. Subsequent batches in that transaction
execute their boundaries through the same connection state.
