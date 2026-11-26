# PostgreSQL two-phase commit

BicDB implements `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`,
the `pg_prepared_xacts` view and the `max_prepared_transactions` setting with
PostgreSQL semantics. The state lives in the core (`bicdb-core`,
`db/prepared_transactions.rs`); pgwire and the SQL session only translate
statements.

## Enabling

`max_prepared_transactions` defaults to `0`, as in PostgreSQL, and is fixed at
server start (`bicdb serve --max-prepared-transactions N`, or the environment
variable `BICDB_MAX_PREPARED_TRANSACTIONS`). With `0`, `PREPARE TRANSACTION`
fails with PostgreSQL's error (`55000 prepared transactions are disabled`, hint
`Set "max_prepared_transactions" to a nonzero value.`). The regression suite's
`prepared_xacts` test therefore follows its skip variant on a default server,
exactly as on a default PostgreSQL server. Prepared transactions that already
exist in a store are always restored and can always be resolved, whatever the
setting.

## Design

A BicDB transaction buffers its writes and holds per-row write locks and
constraint-key reservations until it ends. Preparing turns that live state into
durable, ownerless state:

1. **Validate as COMMIT would.** Delta-repair writes are resolved, write/write
   conflicts are checked against each write's snapshot, unique keys are validated
   against committed data, and a PostgreSQL SERIALIZABLE transaction takes its
   SSI commit decision. Every failure here aborts the transaction like a failed
   COMMIT, so a successfully prepared transaction can always commit.
2. **Claim.** Every unique key the write set introduces is reserved exclusively,
   so no other writer can commit the same key before resolution (an SQL writer
   waits for it, as in PostgreSQL). Row locks (writes, `FOR UPDATE`/`FOR SHARE`)
   and foreign-key reference reservations are already held.
3. **Persist.** One row in the internal collection `__bicdb_prepared_xacts`
   (key = gid) records the gid, owner, database, prepare time, isolation, the full
   write set (record images and the snapshot each write was validated against),
   and every lock and reservation key. It is committed through the ordinary
   commit path, so it is durable (fsync, sync-quorum wait) under exactly the
   commit durability rules before `PREPARE TRANSACTION` returns. It survives
   checkpoints, WAL truncation and vacuum like any committed row, and it reaches
   standbys through ordinary replication.
4. **Detach.** The session's transaction ends. Its locks, reservations, catalog
   observations (which keep DDL off its tables) and SSI participant move into an
   in-memory registry entry keyed by gid; nothing is released.

`COMMIT PREPARED` rebuilds a transaction with the same transaction id from the
entry, adds the deletion of the gid row to its write set and commits it through
the normal commit path. The data and the removal of the prepare record are one
atomic commit, so a crash at any point leaves either the prepared transaction or
its committed result, never both. `ROLLBACK PREPARED` durably deletes the gid row
and then releases the locks. Both work from any session, after a restart and
after failover, and an interrupted resolution is simply retried.

**Recovery.** When a store opens, every row of `__bicdb_prepared_xacts` is
restored before the database accepts work: a fresh transaction id takes over its
row locks and constraint reservations, its tables are pinned against catalog
changes, and a SERIALIZABLE one is re-registered as a prepared SSI participant
whose reads are conservatively taken to cover every relation (its precise read
set is not persisted, like PostgreSQL's summarized conflicts after recovery).

**SSI.** A prepared participant is treated as committing later than anything
else: its writes are never visible to readers, so every overlapping reader gains
an edge to it; it can never be chosen as a victim, so the other member of a
dangerous structure, or the committing transaction, aborts instead.

**MVCC and vacuum.** A prepared transaction's writes are not in any version chain
until COMMIT PREPARED, so the sweeper cannot reclaim anything it needs. The rows
it overwrites stay locked, so their latest committed version cannot change.

## Permissions and errors

Only the role that prepared the transaction (the current user at PREPARE) or a
superuser can finish it (`42501 permission denied to finish prepared
transaction`). Errors match PostgreSQL: duplicate gid `42710`, unknown gid
`42704`, gid of 200 bytes or more `22023`, resolution in progress `55000`,
limit reached `53200`, another database `0A000`. `PREPARE TRANSACTION` outside a
transaction block warns `there is no transaction in progress`; in a failed
transaction block it rolls back. Savepoints are allowed.

## Refused with 0A000

These are refused rather than given different semantics:

- transactions that ran DDL (BicDB applies DDL eagerly with an undo journal) or
  that fell back to statement replay;
- `NOTIFY`/`LISTEN`/`UNLISTEN`, temporary tables, cursors `WITH HOLD`, and
  transaction-level advisory locks (PostgreSQL's own errors where it has them);
- distributed/range-quorum commit admission and other native commit authorities;
  application transactions with mutation grants, commit validators or broker
  publishes;
- PREPARE on a read-only standby (`25006`).

## Replication, HA and mesh

The prepare record and the resolution are ordinary commits, so a standby holds
the same prepared transactions as its primary but never exposes their rows. HA
promotion takes effect on restart, and recovery then restores the prepared
transactions with their locks, so failover neither loses nor commits them
silently (subject to the configured replication durability, as for any commit).
The internal collection is never mesh-synced; prepared writes reach mesh peers
only when committed.

## Known differences

- `pg_prepared_xacts` lists the prepared transactions of the current database
  only. Each database has its own store.
- After a restart, `transaction` shows a new transaction id.
- Inherited from BicDB's row locks: `FOR SHARE` takes an exclusive row lock,
  so a second `FOR SHARE` of a row a prepared transaction holds waits for it.
  `lock_timeout` is not applied to these waits; use `statement_timeout`.
- Inherited from BicDB's SSI: a serialization failure is raised at PREPARE (or
  COMMIT) instead of at the conflicting statement.
- A server with prepared transactions in its store restores them even when it
  starts with `max_prepared_transactions` set below their number; PostgreSQL
  refuses to start.
