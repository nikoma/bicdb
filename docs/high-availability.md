# BicDB High Availability

> This document begins with the general/legacy server HA path. The hardened,
> one-Cell Phase-5 path is a separate capability boundary documented in
> [`cell-runtime-phase5.md`](cell-runtime-phase5.md). Do not use the commands
> below as substitutes for Cell writer epochs, replica leases, Cell-bound
> encryption, Recovery quorum, or regulated-workload admission.

BicDB uses a single-primary high availability model. One primary accepts writes.
That primary can execute and commit concurrent transactions; this topology does
not impose a single transaction writer. See [write concurrency](../TRANSACTIONS.md#write-concurrency).
Native ordered commit-stream replication is the preferred production direction;
the original whole-file shipping path remains supported as a legacy/fallback
bootstrap and repair mechanism.

For zero-data-loss replication between servers, use
[synchronous streaming replication](#synchronous-streaming-replication-bicdb-serve).

Native Raft-style consensus support exists for ordered commit coordination.
It is separate from this legacy file-shipping HA path and does not make BicDB
multi-writer. Operators must still fence the old primary before promoting a
standby until the server daemon owns automatic failover end to end.

The 1.0.322-beta Cell runtime adds a separate, callable HA path: threshold
writer/replica authority is verified before Cell key release, primary commits
cross a locally revocable database fence, native commit objects are signed and
Cell-encrypted for one destination, and backup/restore authority is
Recovery-quorum scoped. Its keyless failover supervisor forbids route
publication before old-writer fencing and new-epoch activation. It currently
supports asynchronous replication with a declared commit RPO and
Auditor-measured drill evidence. It does not claim continuous RPO enforcement
at commit admission. A `SynchronousQuorum` policy is explicitly rejected
because zero-RPO quorum commit replication is not yet implemented.
Regulated-data admission remains denied.

Distributed range placement uses a different data-plane protocol: starting in
1.0.9-beta, row transactions are epoch-fenced and majority committed through
the per-range command log described in
`docs/distributed-cluster-operations.md`. Do not confuse that synchronous
range quorum with the legacy whole-database streaming/HA commands below.

## Synchronous streaming replication (`bicdb serve`)

`bicdb serve` can run as a replication primary or standby itself. A primary
streams every commit to its standbys over long-lived authenticated connections.
With `--synchronous-standbys K`, a client's commit is acknowledged only after
K standbys have made it durable (RPO = 0). Standbys keep serving read-only
queries while they apply the stream.

```bash
# primary: accepts writes, waits for 1 standby on every commit
bicdb serve /srv/primary --port 5432 \
  --replication-cluster-id prod --replication-node-id db1 \
  --replication-listen 10.0.0.1:5940 \
  --allow-standby db2=<sha256 of db2's certificate> \
  --allow-standby db3=<sha256 of db3's certificate> \
  --synchronous-standbys 1 \
  --replication-cert db1.crt --replication-key db1.key --replication-ca ca.crt

# standby: read-only, follows the primary
bicdb serve /srv/standby --port 5432 \
  --replication-cluster-id prod --replication-node-id db2 \
  --standby-of 10.0.0.1:5940 --primary-server-name db1.internal \
  --replication-cert db2.crt --replication-key db2.key --replication-ca ca.crt
```

| Flag | Meaning |
| --- | --- |
| `--synchronous-standbys K` | Standbys that must confirm each commit (0 = asynchronous) |
| `--synchronous-timeout-ms N` | How long a commit waits for confirmations (0 = forever, the default) |
| `--synchronous-on-timeout fail\|degrade` | `fail` (default): the client gets an error; the commit stays durable on the primary and replicates when a standby reconnects. `degrade`: acknowledge and replicate asynchronously until the standbys catch up again, then synchronous commit resumes automatically |
| `--allow-standby ID[=SHA256]` | Standbys allowed to connect. Fail-closed: an empty list authorizes nobody. With TLS each identity must be pinned to its certificate |
| `--replication-dev-plaintext` | Plaintext on loopback addresses only, for development and tests |
| `--advertise-sql HOST:PORT` | This node's client address. The primary announces it to standbys, which name it when they refuse a write |

### Guarantees

- **No acknowledged commit is lost.** Every commit path (explicit `COMMIT`,
  autocommit statements, extended-protocol execution, DDL, `COPY`) holds the
  client's acknowledgement until the synchronous standbys have the commit
  durably. If the primary fails, a synchronous standby has every commit any
  client was told succeeded. Killing the primary with `SIGKILL` mid-workload
  was verified to lose no acknowledged write.
- **Reads never wait.** Only statements that committed wait for standbys.
- **Exact, ordered stream.** Every commit sequence number is shipped in order,
  including commits that wrote nothing. Catalog changes (create, drop and
  rename of tables, the recycle bin) travel in the same stream, so a standby
  never mixes a dropped table's rows into a re-created one. Collection
  policies (tenant scoping, field masking) and flashback settings follow the
  primary.
- **Standbys do not fall off.** Checkpoints never cut the transaction log past
  a connected standby's acknowledged position, so a standby that lags or
  reconnects resumes where it stopped. Commit sequence numbers never repeat
  across restarts.
- **Standby positions survive restarts.** Each standby's acknowledged
  position is persisted on the primary (`ha-node.json`), so a standby that is
  offline while the primary restarts still finds its log. Standbys removed
  from `--allow-standby` stop holding log back at the next start.
- **Epoch fencing.** Every primary term has an epoch, carried by every
  replication message. A primary that hears of a newer epoch fences itself at
  once and durably: it refuses writes, fails commits still waiting for
  confirmation instead of acknowledging them, and stays fenced across
  restarts. Standbys refuse a stream from a primary with an older epoch.
- **Clients are told where to write.** A refused write on a standby or a
  fenced node fails with SQLSTATE `25006` (read-only transaction) and, when
  the primary's `--advertise-sql` address is known, a hint naming it:

  ```text
  ERROR:  25006: database is read-only standby: insert records is refused on a hot standby/read replica; current primary: 10.0.0.1:5432
  HINT:  send writes to the primary at 10.0.0.1:5432
  ```
- **Shutdown never hangs on a missing standby.** Stopping the primary fails
  commits that are still waiting for confirmations; they are durable on the
  primary and replicate when a standby connects.
- As with other databases' synchronous replication, a commit becomes visible
  on the primary slightly before its acknowledgement returns.

### Failover (manual in this release)

1. Make sure the old primary is stopped and cannot come back (fence it).
2. Restart a synchronous standby as the primary (any synchronous standby has
   every acknowledged commit): drop `--standby-of`, add
   `--replication-listen` and `--allow-standby` for the remaining nodes. It
   builds its secondary indexes at start.
3. Point clients at it. Re-create the old primary as a standby from a fresh
   copy.

### Adding a standby, and standbys that cannot continue

Start a new standby from an empty directory. When it cannot continue from its
own position, because the primary's log no longer reaches back that far, or
because it is a former primary whose history diverged from the current
primary's (detected from the epoch history), the primary sends it a consistent
snapshot instead of the commit stream. The standby stages the snapshot next to
its directory (`<dir>.bootstrap`), restarts itself, installs it on open and
then streams normally. A crash at any point leaves either the old or the new
database whole. Commits on the primary pause while the snapshot archive is
written locally; the network transfer runs without holding anything.

Not yet available: automatic failover. Standbys do not build secondary indexes of their own; reads
on them stay correct and use scans.

## Architecture

- Primary: normal BicDB server or local database path with `role=primary`.
- Standby/read replica: copied BicDB path with `role=standby`; server writes
  are refused and local BicDB write APIs return a read-only standby error.
- Native streaming path: export committed transaction frames by `commit_seq` and
  apply them to a standby in order. See `docs/replication-streaming.md`.
- Shipping/apply fallback: `bicdb ha ship <primary> <standby>` opens and flushes
  the primary, copies durable catalog, segment, transaction, event, sync, index,
  planner-statistics, graph, vector-index, encryption, and server-user files,
  then writes standby HA metadata last.
- Replication mode: asynchronous. Re-run `bicdb ha ship` after primary writes
  or from an external supervisor/timer.
- Read replica: start pgwire on the standby path. `SELECT`/`SHOW` queries are
  served, while `INSERT`, `UPDATE`, `DELETE`, DDL, `COPY FROM`, and `COMMIT` of
  buffered write transactions are rejected.

## File-Shipping Commands

Create or refresh a standby:

```bash
cargo run -p bicdb-cli -- ha ship /srv/bicdb/primary /srv/bicdb/standby
```

Inspect lag and readiness:

```bash
cargo run -p bicdb-cli -- ha status /srv/bicdb/standby
cargo run -p bicdb-cli -- ha status /srv/bicdb/standby --json
```

Promote after fencing the old primary:

```bash
cargo run -p bicdb-cli -- ha promote /srv/bicdb/standby
```

If the standby reports non-zero lag or an apply error, promotion fails unless
`--force` is passed. Use `--force` only after manually verifying the old primary
cannot accept writes and the data loss window is acceptable.

## Health and Readiness

Offline status is available from `bicdb ha status`. A running pgwire server also
exposes:

```sql
SELECT * FROM bicdb_ha_status;
```

The status row includes `role`, `read_only`, `ready`,
`source_checkpoint_bytes`, `applied_checkpoint_bytes`, `lag_bytes`,
`last_apply_at`, `last_apply_error`, `promoted_at`, and
`last_durable_checkpoint_bytes`.

`bicdb server status` also prints the HA role, read-only state, lag bytes, and
durable checkpoint bytes for offline inspection.

## Failover Runbook

1. Fence the old primary at the process, host, volume, or load-balancer layer.
2. Run `bicdb ha status <standby>` and verify `ready=true`, `lag_bytes=0`, and
   no `last_apply_error`.
3. Promote with `bicdb ha promote <standby>`.
4. Start the pgwire server on the promoted path.
5. Repoint clients or the supervisor endpoint to the promoted server.
6. Rebuild the old primary only from the new primary; do not restart it as a
   writer with stale data.

For planned maintenance, run one final `bicdb ha ship` after stopping writes,
verify zero lag, promote the standby, and then move clients.

## Limitations

- Streaming replication is asynchronous whole-database commit-stream
  replication. The `consensus run` interface is retained for compatibility;
  distributed `bicdb serve` data safety comes from the synchronous per-range
  command log, not from asynchronous post-commit frame export.
- File shipping is async fallback replication, not streaming consensus.
- BicDB has a durable consensus state machine for leader election, but no
  automatic failover supervisor in `bicdb serve` yet.
- BicDB has no multi-writer mode.
- Operators are responsible for fencing to prevent split brain.
- The lag metric is byte/checkpoint based for the last completed ship, not a
  transaction LSN compatible with PostgreSQL.
- Shipping should run when no external process is writing directly to the
  standby path.
