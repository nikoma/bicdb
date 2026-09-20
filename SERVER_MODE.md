# BicDB Server Mode

BicDB can run embedded in-process or as a single-node PostgreSQL-compatible
server. Server mode keeps the same local append-only storage engine and exposes
it over the pgwire protocol for local tools, LAN clients, smoke tests, and
simple application development.

Server mode does not automatically run a cluster supervisor. BicDB has a
separate Raft-style consensus core, but `serve` remains a pgwire server process
unless explicitly paired with replication/consensus orchestration.
Run `bicdb consensus run` beside `bicdb serve` when a node should participate
in leader election and quorum commit coordination.

## Start A Local Server

```bash
cargo run --release -p bicdb-cli -- serve ./testdb --host 127.0.0.1 --port 5433
```

`serve-pg` remains an alias:

```bash
cargo run --release -p bicdb-cli -- serve-pg ./testdb --port 5433
```

Connect with `psql`:

```bash
psql -h 127.0.0.1 -p 5433 -U bicdb -d bicdb
```

### Server identity and compatibility

BicDB tells clients which engine they are connected to while keeping the
PostgreSQL compatibility target separate:

```sql
SELECT version();
-- BicDB <bicdb-version> (PostgreSQL 18.4 wire compatible)

SELECT bicdb_version();
-- <bicdb-version>

SHOW server_version;
-- 18.4
```

The pgwire handshake and `server_version` settings default to PostgreSQL 18.4.
If a client requires a different compatibility identity, override it for
`serve` or `serve-pg`; this does not change `bicdb_version()`:

```bash
bicdb serve ./testdb --postgres-server-version 16.7
```

`--postgres-server-version-num` is derived automatically (`160007` above).
When supplied explicitly, it must match `--postgres-server-version`.

Smoke queries:

```sql
SELECT 1;
SELECT COUNT(*) FROM wearable;
SELECT * FROM patients LIMIT 5;
SELECT * FROM bicdb_server_connections;
SELECT * FROM bicdb_server_stats;
SELECT * FROM bicdb_ha_status;
```

For production-style metrics, health checks, slow-query logging, doctor
bundles, dashboards, and alert thresholds, see
[`docs/production-observability.md`](docs/production-observability.md).
For deployment examples, see [`docs/examples/`](docs/examples/). For recovery
procedures and DR rehearsals, see [backup and recovery](docs/backup-recovery.md).

## LAN Mode

Bind to all interfaces:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb --host 0.0.0.0 --port 5433 --require-auth
```

BicDB refuses no-auth non-local server mode by default. For an explicit
development exception:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb --host 0.0.0.0 --allow-remote-no-auth
```

That mode is not appropriate for production or shared LANs.

## Authentication

Create a local server user:

```bash
cargo run --release -p bicdb-cli -- user create admin --password change-me --path ./testdb
```

Start with required auth:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb --require-auth --port 5433
psql -h 127.0.0.1 -p 5433 -U admin -d bicdb
```

Use SCRAM-SHA-256 instead of cleartext password exchange:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb \
  --require-auth \
  --auth-method scram-sha-256 \
  --port 5433
```

Passwords are stored in `server_users.json` using Argon2id-derived hashes and
SCRAM verifier material with per-user salts. Plaintext passwords are not stored
or logged.

Current auth scope:

- PostgreSQL cleartext password exchange over local/LAN TCP.
- Argon2id at-rest password verification.
- SCRAM-SHA-256 SASL authentication when `--auth-method scram-sha-256` is set.
- Existing users created before SCRAM fields existed should be recreated before
  using SCRAM auth.

## TLS

The CLI accepts TLS flags and upgrades PostgreSQL SSLRequest connections using
Rustls:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb \
  --tls-cert cert.pem \
  --tls-key key.pem
```

Both `--tls-cert` and `--tls-key` must be provided. Add `--require-tls` to
reject clients that send a plaintext StartupMessage instead of first negotiating
PostgreSQL SSLRequest:

```bash
cargo run --release -p bicdb-cli -- serve ./testdb \
  --tls-cert cert.pem \
  --tls-key key.pem \
  --require-tls
```

PostgreSQL client certificate authentication for pgwire clients is not
implemented. Supplying `--tls-client-ca` fails server startup instead of
silently ignoring the setting. This does not apply to native node-to-node
streaming replication, which requires TLS/mTLS for non-localhost transport.
Both `serve` and `serve-pg` accept `--channel-binding=require|prefer|disable`
with `--auth-method scram-sha-256`:

| Policy | SCRAM mechanisms advertised and accepted |
| --- | --- |
| `require` | Only `SCRAM-SHA-256-PLUS`; TLS and authentication are required. |
| `prefer` | PLUS first, then plain `SCRAM-SHA-256` when a TLS binding is available; otherwise plain SCRAM. |
| `disable` | Only plain `SCRAM-SHA-256`, including over verified TLS. |

Omitting the flag preserves the secure SCRAM transport default: **require PLUS
when TLS is configured**, plain SCRAM otherwise. In the Rust API,
`PgWireConfig.channel_binding = None` selects this default; set
`Some(ChannelBindingPolicy::Prefer)`, for example, to choose explicitly.
An explicit `require` setting without certificates or authentication is rejected
at startup. An explicit channel-binding flag with cleartext authentication is
also rejected rather than silently ignored.

```bash
bicdb serve-pg ./testdb --require-auth --auth-method scram-sha-256 \
  --tls-cert cert.pem --tls-key key.pem --require-tls --channel-binding require
```

`prefer` permits clients that only implement plain SCRAM over verified TLS.
`disable` changes the SCRAM mechanism, not certificate verification or transport
requirements. None of these modes relaxes the existing TLS policy: when server
TLS is configured, SCRAM clients must negotiate TLS. Without server TLS,
`prefer` and `disable` allow plain SCRAM only where existing network policy
permits it.

A client selecting PLUS must finish that exchange with the correct binding;
there is no retry as plain SCRAM after a mismatch or bad proof. If the server
advertised PLUS, a plain-SCRAM client claiming that binding was unavailable
(the GS2 `y` flag) is rejected as a downgrade, following
[RFC 5802 section 6](https://www.rfc-editor.org/rfc/rfc5802.html#section-6).

The `tls-server-end-point` binding hashes the loaded leaf certificate with its
signature hash, as specified by [RFC 5929 section 4](https://www.rfc-editor.org/rfc/rfc5929.html#section-4).
SHA-384 certificates use SHA-384; MD5 and SHA-1 signature hashes use the required
SHA-256 fallback. RSA-PSS uses its message-hash parameters, including the SHA-1
default, independently of the MGF1 hash. SHA-224, SHA-256, SHA-384, SHA-512 and the
SHA-512/224 and SHA-512/256 variants are supported. Unsupported or undefined
signature-hash mappings (including Ed25519) are never silently assigned SHA-256.
`require` rejects them at startup; `prefer` logs that PLUS is unavailable and
advertises plain SCRAM, and `disable` uses plain SCRAM without a binding. This
mapping does not relax TLS certificate validation or permit legacy signatures that the TLS implementation rejects.

Only the first certificate in the configured chain contributes to the binding.
Certificate and binding bytes are loaded together at server startup; replacing
the PEM file requires restarting the server to activate the new certificate.
An incorrect binding is rejected even when the password proof is valid.

## Runtime Behavior

Implemented server hardening:

- Shared `BicDb` runtime for all connections.
- Bounded Tokio runtime for plain TCP accept/idle connection scheduling.
- Bounded blocking pool for active frontend message and SQL execution.
- Multiple concurrent client connections without one idle OS thread per plain
  connection.
- Monotonic connection IDs.
- Connection lifecycle logs.
- Graceful disconnect and `X` terminate handling.
- Idle timeout.
- Race-free max connection admission.
- Separate max active query/read/write execution limits.
- Bounded pending query/read/write queues with overload rejection and timeout.
- Request byte limit.
- Result row limit.
- Query timeout errors.
- Per-connection memory estimate.
- Background flush loop.
- Background checkpoint timestamp loop.
- Background metrics log loop.
- Clean shutdown marker at `server.shutdown`.

Configuration flags:

```bash
--max-connections 100
--max-pending-accepts 100
--max-active-queries <available CPU parallelism>
--max-queued-queries <4x available CPU parallelism, minimum 16>
--max-active-reads <available CPU parallelism>
--max-queued-reads <4x available CPU parallelism, minimum 16>
--max-active-writes <available CPU parallelism>
--max-queued-writes <4x available CPU parallelism, minimum 16>
--idle-timeout-seconds 300
--shutdown-grace-seconds 10
--query-timeout-ms 30000
--overload-timeout-ms 30000
--write-timeout-ms 30000
--max-result-rows 100000
--max-request-bytes 10485760
--per-connection-memory-limit 16777216
```

## Status

Offline/local database status:

```bash
cargo run --release -p bicdb-cli -- server status ./testdb
```

Live status from a running server:

```sql
SELECT * FROM bicdb_server_connections;
SELECT * FROM bicdb_server_stats;
```

`bicdb_server_connections` exposes connection ID, user, peer address, connected
time, last query time, query counters, failure counters, and transaction state.

`bicdb_server_stats` exposes active connections, total connections, queries
executed, failed queries, uptime, database size, memory estimate, last
checkpoint, writes executed, configured max connections, configured max active
queries, active query count, peak active query count, rejected query count,
configured max queued queries, current and peak queued queries, active/peak
read and write counts, queued/peak queued reads and writes, queue wait
p50/p95/p99, configured max queued writes, current and peak write queue depth,
write wait time, write execution time, rejected write count, timed-out write
count, canceled query count, query timeout count, and last-cancel metadata
(`last_cancel_at`, `last_cancel_connection_id`, `last_cancel_reason`,
`last_cancel_sqlstate`). Large-result counters include `rows_streamed`,
`bytes_streamed`, `cursor_count`, `cursor_memory_bytes`, and
`spilled_to_disk_bytes`.

Use [Large Result Streaming](docs/large-results.md) for large report/export
patterns with pgwire cursor fetches and `COPY TO STDOUT`.

Read and write SQL first enters bounded query admission. If no active slot is
available, the request waits in a bounded read or write queue until
`--overload-timeout-ms` elapses. Full queues return SQLSTATE `53300`; overload
timeouts and cooperative cancellation return SQLSTATE `57014`. The virtual
status tables are served before query admission and do not take the database
read/write lock.

`bicdb_ha_status` exposes the v0 single-writer HA role, read-only state,
readiness, source/applied checkpoint bytes, replication lag bytes, apply error,
promotion time, and last durable checkpoint bytes. Standby/read replica servers
reject writes until promoted.

Eligible buffered DML executes concurrently under a shared database guard, and
ordinary buffered commits (including explicit `COMMIT`) also use a shared guard.
Write admission is bounded; admission itself is not a single-writer mutex.
`--max-queued-writes` limits outstanding admitted write work. Operations that
require exclusive access, including DDL and non-transactional mutation paths,
and fallback execution still take the database write lock. For those paths,
`--write-timeout-ms` bounds the wait for that exclusive lock.

Core commits coordinate record conflicts, unique/index access, WAL ordering, and
MVCC visibility without taking the global commit mutex for ordinary transactions.
`BICDB_GLOBAL_COMMIT_LOCK=1` restores global commit serialization for comparison;
it is off by default. Transactions carrying native application-invariant
validators also take that mutex, and core serializable transactions use exclusive
serializable commit admission. These exceptions do not make all writes serial.
See [Transactions](TRANSACTIONS.md#write-concurrency) for source references and
[identity and pooling](docs/database-identity-and-pooling.md) for the separate
question of sharing connections between application users.

Recommended starting profiles:

| Profile | Settings |
| --- | --- |
| Local dev | `--max-connections 50 --max-active-queries 4 --max-queued-queries 16 --max-active-reads 4 --max-queued-reads 16 --max-active-writes 2 --max-queued-writes 16 --overload-timeout-ms 30000` |
| LAN deployment | `--max-connections 200 --max-active-queries <cpu> --max-queued-queries <4x cpu> --max-active-reads <cpu> --max-queued-reads <4x cpu> --max-active-writes <cpu/2 or cpu> --max-queued-writes <4x cpu> --overload-timeout-ms 15000` |
| 1000 pooled connections | `--max-connections 1000 --max-active-queries <cpu> --max-queued-queries <4x cpu> --max-active-reads <cpu> --max-queued-reads <4x cpu> --max-active-writes <cpu/2> --max-queued-writes <4x cpu> --overload-timeout-ms 5000` |

Interpretation: high queue p95/p99 with no rejections means clients are waiting
but bounded; rising `rejected_queries` means queues are full; rising
`timed_out_queries` means queued or executing work exceeded its deadline; high
DB lock wait/hold metrics point to write contention or long storage work.

## Graceful Shutdown

On SIGINT/SIGTERM, BicDB requests shutdown, stops accepting new connections,
waits up to `--shutdown-grace-seconds` for active connections to drain, flushes
pending writes, updates checkpoint state, closes the shared database runtime,
and writes `server.shutdown`.

## High Availability

BicDB server deployments use a v0 single-writer HA model: one primary accepts
writes, and hot standby/read replica paths are refreshed with
`bicdb ha ship <primary> <standby>`. Promotion is manual with
`bicdb ha promote <standby>` after the old primary is fenced.

Here, single-writer means one writable primary, not one executing transaction.
It does not provide multiple writable primaries or automatic failover. See
[`docs/high-availability.md`](docs/high-availability.md) for the runbook,
health fields, and limitations.

If connections remain active after the grace period, the server logs the active
count and continues shutdown. PostgreSQL `CancelRequest` and `query_timeout`
use cooperative cancellation tokens for cancellable pgwire SQL paths, including
scan filtering, row joins, grouping/projection, vector ordering, ANN search,
COPY finalization, and other long loops where practical.

## Benchmark

```bash
cargo run --release -p bicdb-cli --features bench -- bench server --clients 10 --queries 10000
cargo run --release -p bicdb-cli --features bench -- bench server-cert --profile ci
cargo run --release -p bicdb-cli --features bench -- bench server-cert --profile full --queries 1000
```

The benchmark starts a real local pgwire listener, creates a table, drives
concurrent TCP clients through simple-query protocol, and reports:

- queries/sec
- p50/p95/p99 latency
- connection setup p50/p95/p99 latency
- concurrent `SELECT COUNT(*)`
- concurrent `INSERT`
- peak active connections
- rejected connections
- DB lock wait and hold time for the shared database lock
- write queue depth, write wait time, write execution time, and write
  reject/timeout counters
- cancellation and timeout counters
- server query/failure/write counters
- server memory estimate
- RSS and process thread count on Linux when available
- database size

Server concurrency scenarios are selected with `--scenario`:

- `idle-pooled`
- `read-only`
- `mixed` (default)
- `long-scan`
- `cancel-contention`
- `churn`

Use `--active-query-concurrency` to hold a larger client pool open while only a
subset issues workload queries.

Exports:

```bash
cargo run --release -p bicdb-cli --features bench -- bench server \
  --clients 10 \
  --queries 10000 \
  --json-out target/bicdb-server.json \
  --csv-out target/bicdb-server.csv \
  --markdown-out target/bicdb-server.md
```

The current 100/500/1000-connection baseline and known bottlenecks are recorded
in `docs/server-concurrency.md`.

`bench server-cert` is the release gate for the 1000 pooled-connection claim.
The full profile defaults to 1000 clients, 32 active query workers, and the
five required scenarios: idle pooled, read-only, mixed read/write,
cancel-contention, and connection churn. It fails if RSS exceeds 256 MiB,
thread count exceeds 160, pooled scenarios do not admit all 1000 clients,
connections are rejected, unexpected queries fail or time out, read-only falls
below 100 q/s or exceeds p99 500 ms, mixed read/write falls below 25 q/s or
exceeds p99 5000 ms, cancellation short reads exceed p99 500 ms, or churn
exceeds p99 1500 ms.

The June 20, 2026 full certification run passed on the documented local Linux
host. It held 1000 idle pooled connections for a 30-second soak with
46.85 MiB RSS and 31 threads; read-only completed 1000 queries at 624.25 q/s
with p99 45.553 ms; mixed read/write completed 1000 queries at 200.32 q/s with
p99 1345.015 ms and correct final row count; cancellation kept unrelated short
reads at p99 44.031 ms and recovered the canceled connection; churn completed
1000 open/query/close cycles with p99 1370.741 ms and no stale final data.

## Limitations

- Single-writer only. Native streaming replication replays ordered commits to a
  standby. The consensus core can coordinate commit-frame quorum, but BicDB is
  not distributed SQL or multi-writer.
- SQL support is still BicDB SQL, not full PostgreSQL.
- No full `pg_catalog`.
- Pgwire TLS is server-certificate only; pgwire client certificates are not
  implemented. SCRAM-PLUS channel binding is supported. Native streaming
  replication uses separate TLS/mTLS configuration.
- SCRAM-SHA-256 is opt-in; cleartext auth remains available for local
  compatibility.
- Query cancellation is cooperative, not thread-killing. Most long SQL,
  vector, ANN, COPY finalization, and row-processing loops check cancellation
  at natural boundaries; sort calls are checked immediately before and after
  sorting.
- Transactions remain local single-node transactions.
- No automatic failover supervisor or distributed SQL.

## Replication

Operational replication commands are available under `bicdb replication`.

```bash
bicdb replication status /var/lib/bicdb/primary
bicdb replication stream /var/lib/bicdb/primary --from 0 --limit 1000 > frames.json
bicdb replication apply /var/lib/bicdb/standby frames.json
bicdb replication stream /var/lib/bicdb/primary --from 0 --listen 10.0.0.10:9443 \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id primary-a \
  --allowed-node-id standby-a
bicdb replication follow /var/lib/bicdb/standby --primary 10.0.0.10:9443 \
  --server-name db-a.example.internal \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id standby-a
bicdb replication follow /var/lib/bicdb/standby --primary 10.0.0.10:9443 \
  --server-name db-a.example.internal \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id standby-a \
  --continuous
bicdb replication snapshot create /var/lib/bicdb/primary /tmp/bootstrap.snapshot.json
bicdb replication snapshot restore /tmp/bootstrap.snapshot.json /var/lib/bicdb/standby --force
bicdb replication lag /var/lib/bicdb/standby --source-commit-seq 1000
```

Remote network replication must use TLS/mTLS. Plaintext is only allowed in
explicit localhost development mode by replication config validation. See
`docs/replication-streaming.md` and `docs/replication-security.md`.

## Online operator login management

The optional [login operator API](docs/login-operator-api.md) creates, binds, rotates,
disables, and revokes pgwire logins without stopping queries. It uses a separate
operator credential, not SQL role grants. The guide defines TLS, input limits,
operation logging, shared catalog locking, and existing-session behavior.
