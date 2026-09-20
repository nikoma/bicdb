# BicDB Transactions

BicDB supports local transactions in embedded and server deployments, including
concurrent write execution and ordinary concurrent commits. SQL isolation support
and core transaction capabilities are distinct; see the isolation section below.
For replication and consensus boundaries, see [High Availability](docs/high-availability.md).

## Guarantees

- `BEGIN` allocates a durable `TransactionId` and appends `TxBegin`.
- `insert`, `update`, and `delete` append `TxWrite` frames to
  `transactions.log`.
- `COMMIT` appends `TxCommit`; recovery applies only transactions with a commit
  frame.
- `ROLLBACK` appends `TxAbort`; aborted and pending transactions are ignored by
  recovery.
- Partial trailing transaction frames are truncated during open, using the same
  frame checksum recovery path as record, event, and sync logs.
- Uncommitted writes are invisible to ordinary reads and snapshots.
- `db.snapshot()` returns a stable view of committed records as of snapshot
  creation.
- Simple write conflicts are detected with per-record write locks for
  concurrent local transactions.

## API

```rust
use bicdb_core::{BicDb, Record};

let mut db = BicDb::open("./testdb")?;
db.create_collection("patients")?;

let mut tx = db.begin_transaction()?;
tx.insert("patients", Record::new("p1"))?;
tx.commit()?;

let snapshot = db.snapshot()?;
let patient = snapshot.get("patients", "p1");

# Ok::<(), bicdb_core::BicDbError>(())
```

Rollback:

```rust
let mut tx = db.begin_transaction()?;
tx.insert("patients", Record::new("p2"))?;
tx.rollback()?;
assert!(db.get("patients", "p2")?.is_none());
# Ok::<(), bicdb_core::BicDbError>(())
```

## SQL And Pgwire

The SQL layer supports:

```sql
BEGIN;
INSERT INTO patients (id, name) VALUES ('p1', 'Ada');
UPDATE patients SET name = 'Ada Lovelace' WHERE id = 'p1';
DELETE FROM patients WHERE id = 'p1';
COMMIT;

BEGIN;
INSERT INTO patients (id, name) VALUES ('p2', 'Rollback');
ROLLBACK;

BEGIN;
SAVEPOINT nested;
INSERT INTO patients (id, name) VALUES ('p3', 'Nested');
ROLLBACK TO SAVEPOINT nested;
RELEASE SAVEPOINT nested;
COMMIT;
```

The PostgreSQL wire server returns `ReadyForQuery` status `T` while a
transaction is open, `E` while a transaction is failed, and `I` when idle.
`ROLLBACK TO SAVEPOINT` can recover a failed transaction back to status `T`.
Prepared statements can run inside a transaction and inside savepoint scopes.

## Write concurrency

BicDB is not universally single-writer within a database server:

- Eligible pgwire DML executes with a deferred transaction under a shared database
  guard. Ordinary buffered commits also use a shared guard, including explicit
  `COMMIT`. Different transactions can execute and commit concurrently.
- Write admission bounds outstanding work. It does not itself acquire an
  exclusive database lock. DDL, non-transactional mutations, and fallback paths
  can still require that lock. Autocommit shared execution has bounded conflict
  retries before falling back to exclusive execution.
- Core commits coordinate per-record write conflicts, unique keys and indexes,
  ordered WAL publication, and the MVCC visibility watermark. Shared resources
  and conflicting rows can still cause contention; concurrency does not imply
  PostgreSQL-equivalent scaling or isolation semantics.
- The global commit mutex is off for ordinary transactions by default.
  `BICDB_GLOBAL_COMMIT_LOCK=1` enables it for comparison. Native
  `ApplicationInvariant` commit validators also acquire this mutex. Core
  serializable transactions use exclusive serializable commit admission.
- The pgwire shared commit path releases its database guard before waiting for
  WAL durability, allowing apply work and durability work to overlap. Durability
  guarantees depend on the configured WAL/fsync mode.

Implementation references (prefer executable paths over older comments):

- [Pgwire execution](crates/bicdb-pgwire/src/lib.rs):
  `execute_server_db_sql_for_state_inner`.
- [Explicit commit](crates/bicdb-pgwire/src/sql_parsing.rs):
  `commit_buffered_transaction`.
- [Admission and exclusive locking](crates/bicdb-pgwire/src/server_cluster.rs):
  `try_admit_write` and `write_db_with_admission`.
- [Core commit](crates/bicdb-core/src/db/spatial_paged.rs): `commit_transaction`.
- [Concurrency tests](crates/bicdb-core/src/db.rs):
  `concurrent_disjoint_writers_commit_in_parallel_via_shared_path` and
  `same_row_writers_conflict_on_shared_commit_path`.

“One writable primary” in HA documentation describes the replication topology,
not the number of concurrent transactions on that primary. Connection identity
is another separate concern; see [identity and pooling](docs/database-identity-and-pooling.md).

## Isolation

The SQL/pgwire interface accepts `READ COMMITTED` and the explicit
`REPEATABLE READ READ ONLY` combination. Writable `REPEATABLE READ`,
`SERIALIZABLE`, `READ UNCOMMITTED`, explicit transaction snapshots, and `SNAPSHOT`
isolation are rejected with SQLSTATE `0A000`. Internal core serializable support
does not mean SQL `BEGIN ... SERIALIZABLE` is supported.

```sql
BEGIN ISOLATION LEVEL READ COMMITTED;
ROLLBACK;
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
ROLLBACK;
```

See `validate_transaction_modes` in
[SQL transaction parsing](crates/bicdb-sql/src/schema_meta.rs) and
`pgwire_transaction_isolation_commands_accept_read_only_repeatable_read` in
[protocol tests](crates/bicdb-pgwire/tests/protocol.rs) for the accepted forms.
Do not infer complete PostgreSQL transaction compatibility from command parsing
or from concurrent commit support.

Readers do not see other transactions' uncommitted writes. Buffered transactions
perform conflict validation before commit; a conflict can return SQLSTATE
`40001`. Ordinary disjoint-row writers are not universally rejected merely
because they touched the same table. Per-record locking, index constraints,
and operation-specific validation determine conflicts.

## Crash Recovery

On open, BicDB replays normal record segments first, then replays
`transactions.log`:

1. `Committed` transactions are applied in transaction-id order.
2. `Aborted` transactions are ignored.
3. `Pending` transactions are ignored.
4. Corrupt or partial trailing frames are truncated.

This means a process can crash after `TxWrite` frames but before `TxCommit`
without exposing partial data after restart.

Run strict verification before recovery when operators need corruption to fail
closed:

```bash
cargo run -p bicdb-cli -- check ./testdb
```

See `docs/crash-safety.md` for the full repair and restore matrix.

## Benchmark

```bash
cargo run -p bicdb-cli --features bench -- bench transactions --records 100000 --batch-size 1000
```

The benchmark reports:

- single insert transaction throughput
- batch transaction throughput
- rollback throughput
- recovery time after many transactions
- snapshot scan overhead

## Limitations

- Single-node only.
- No distributed transactions.
- HA and consensus capabilities are separate from local SQL transaction semantics.
- SQL/pgwire `SERIALIZABLE` is not supported; core capabilities are separate.
- Transactional writes are durable in `transactions.log`; segment compaction is
  future work.
- Sync-log and event-log grouping with transaction commits is still planned.
