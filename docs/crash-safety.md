# BicDB Crash Safety And Corruption Checks

BicDB persists canonical records, transaction frames, events, and sync-log
entries as append-only frames. Each frame has a magic header, version, kind,
payload length, and CRC32 over the stored payload. Encrypted databases also use
authenticated encryption for frame payloads.

## Commands

Use `check` before opening a database when you need strict corruption detection:

```bash
cargo run -p bicdb-cli -- check ./testdb
```

`check` validates:

- collection segment frames under `segments/*.seg`
- `transactions.log`
- `events/events.seg`
- `sync.log`
- encryption metadata and encrypted frame authentication when a key is supplied
- SQL integrity for primary keys, unique constraints, foreign keys, check
  constraints, secondary/vector indexes, and schema/catalog consistency
- optional backup artifacts passed with `--backup`

Encrypted databases use the same key options as `verify`:

```bash
cargo run -p bicdb-cli -- check ./securedb --key "$BICDB_KEY"
```

Use `--json` when the integrity report is release evidence or needs to be
ingested by automation:

```bash
cargo run -p bicdb-cli -- integrity check ./testdb --json
```

`bicdb integrity check` is the database-only form of the same strict checker.

For a localized `server_paged` MVCC cycle or version-chain safety-limit fault,
use the guarded workflow in
[MVCC version-chain diagnosis and recovery](mvcc-chain-recovery.md). These
faults are diagnosed separately from checksum, torn-page, page-type, and
short-read corruption.
The JSON report has `storage` and `sql` objects. `sql.violations[]` entries are
machine-readable and include `check`, `table`, `object`, `record_id`, and
`message` fields.

Backups are checked explicitly because they can be stored outside the database
directory:

```bash
cargo run -p bicdb-cli -- check ./testdb \
  --backup ./testdb-full.bicbackup \
  --backup-key "$BICDB_BACKUP_KEY"
```

Use `backup verify` to validate one archive or an ordered full/incremental
chain, and `backup drill` to rehearse restore into an isolated path:

```bash
cargo run -p bicdb-cli -- backup verify ./testdb-full.bicbackup ./testdb-inc.bicbackup
cargo run -p bicdb-cli -- backup drill ./testdb-full.bicbackup \
  --json-out ./backup-drill.json
```

`verify` remains an alias for database integrity verification. `check` is the
production-oriented command and can also verify backup files.

## Recovery Behavior

Startup recovery is conservative:

- a partial trailing frame header or payload is truncated to the last valid
  frame boundary;
- a trailing checksum mismatch is truncated to the previous valid frame;
- transaction recovery applies only transactions with a valid `TxCommit` frame;
- pending or aborted transactions are ignored;
- a compaction checkpoint recovers through either durable compacted record
  segments plus checkpointed logs, or compacted records plus replay of the old
  transaction log if the process stopped before the log checkpoint phase;
- in-memory B-tree and spatial indexes are rebuilt from canonical records and
  `indexes.json`;
- graph, analytics, and HNSW sidecars are derived structures and should be
  rebuilt with their existing rebuild commands if missing or corrupt.

Use `check` when you want corruption to fail closed instead of being repaired by
startup recovery.

## Repairability

Repairable:

- partial trailing frames in record, transaction, event, and sync logs;
- interrupted compaction/checkpoint work that leaves a temporary compact file
  behind before the atomic rename;
- missing derived sidecars such as graph projections, analytics sidecars, and
  HNSW vector indexes;
- stale in-memory indexes, which rebuild from canonical records on open.

Requires rebuild:

- corrupt derived sidecars;
- missing HNSW, graph, or analytics sidecars when the application expects them;
- missing `indexes.json` definitions, if the deployment has an external schema
  manifest from which indexes can be recreated.

Requires restore from backup:

- corruption inside committed non-trailing canonical record frames;
- corruption inside committed transaction frames needed to reconstruct durable
  transaction state;
- corrupt or missing collection catalog metadata;
- corrupt encrypted envelopes or wrong encryption keys;
- corrupt backup archives or incremental backup chains.
- failed restore drills, missing PITR audit-event coverage, or a drill report
  whose restored database does not pass strict integrity checks.

Fatal until restored:

- silent application-level data loss with no clean backup;
- conflicting or manually edited metadata whose intended schema cannot be
  determined;
- encrypted databases without the required key material.

## Certification Tests

The focused regression suite covers:

- crash-before-commit and crash-after-commit recovery for small and large
  transactions;
- strict detection plus startup truncation for partial record, transaction,
  event, and sync frames;
- intentional frame corruption detection through `verify_path` and `bicdb check`;
- partial and corrupted encrypted backup rejection;
- ordered backup chain validation and restore drill evidence generation;
- transaction-log recovery that ignores pending writes and applies committed
  writes after reopen.
