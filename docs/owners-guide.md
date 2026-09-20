# The BicDB Owner's Guide

**A practical field book for operators, students, application authors, and
software agents**

This guide explains BicDB as a system rather than as a list of commands. It
starts with a mental model, follows a write to durable storage, and then builds
outward through SQL, search, applications, Cells, replication, clustering,
security, backup, recovery, and routine operations. It is deliberately useful
to two audiences:

- an IT student can read it in order as a compact database-systems course;
- an operator or agent can jump to the checklists and command recipes.

Examples use the installed `bicdb` executable. During source development,
replace `bicdb` with `cargo run -p bicdb-cli --`. Command flags can evolve, so
confirm the exact interface of the binary being operated with `bicdb help` and
`bicdb <command> --help`.

> **Release status:** BicDB is beta. Treat this guide, the CLI help from the
> deployed binary, and the release notes as a set. Never make an untested beta
> database the sole copy of critical data.

---

## Contents

1. [What BicDB is](#1-what-bicdb-is)
2. [The system in one picture](#2-the-system-in-one-picture)
3. [Storage, transactions, and recovery](#3-storage-transactions-and-recovery)
4. [Ways to run BicDB](#4-ways-to-run-bicdb)
5. [The CLI owner's tour](#5-the-cli-owners-tour)
6. [SQL, collections, indexes, and specialized data](#6-sql-collections-indexes-and-specialized-data)
7. [Server protocols and client access](#7-server-protocols-and-client-access)
8. [Cells and the application runtime](#8-cells-and-the-application-runtime)
9. [Replication, HA, consensus, and distributed clusters](#9-replication-ha-consensus-and-distributed-clusters)
10. [Backup, restore, and disaster recovery](#10-backup-restore-and-disaster-recovery)
11. [Security and hardening](#11-security-and-hardening)
12. [Observability and day-two operations](#12-observability-and-day-two-operations)
13. [Performance and capacity](#13-performance-and-capacity)
14. [Browser, offline, and synchronization](#14-browser-offline-and-synchronization)
15. [Troubleshooting](#15-troubleshooting)
16. [Safe automation for software agents](#16-safe-automation-for-software-agents)
17. [Learning labs](#17-learning-labs)
18. [Production readiness checklists](#18-production-readiness-checklists)
19. [Glossary and documentation map](#19-glossary-and-documentation-map)

---

## 1. What BicDB is

BicDB is a Rust database that can be embedded in a process, exposed through a
PostgreSQL-compatible server, used as a durable Redis-shaped cache, run in a
browser, or deployed with replication and distributed range placement. The
same transactional substrate supports records, SQL, full-text and vector
search, spatial and graph operations, analytics, durable event streams, and a
sandboxed application runtime.

That breadth is easiest to understand through three principles.

### 1.1 One source of truth

Search indexes, vectors, stream state, and source rows belong to the database.
They are not eventually synchronized copies maintained by unrelated products.
Consequently, transaction, authorization, backup, and recovery boundaries can
cover all of them together.

### 1.2 Deployment shape is not data meaning

An application can begin with an embedded database and later expose familiar
network protocols. A PostgreSQL database name, a process, a cluster node, and a
Cell are different boundaries. In particular, creating another pgwire database
does **not** create the hard security isolation of a Cell.

### 1.3 Fail closed, then recover explicitly

BicDB validates checksums, on-disk versions, epochs, replica identities,
quorums, keys, and capability grants before mutation. A refusal is frequently
a safety feature. Do not work around it by copying control files, deleting
markers, disabling TLS, or forcing a stale node online.

### 1.4 When BicDB fits

Good candidates include offline-first software, multi-tenant systems needing
database-enforced row isolation, PostgreSQL-shaped applications that also need
search, local AI systems, durable caches or queues, and edge deployments that
may grow into HA. A tiny application needing only a minimal key/value store may
not benefit from the larger capability surface.

---

## 2. The system in one picture

```text
 PostgreSQL clients   Redis clients   Messaging clients     Rust / WASM apps
        |                  |         AMQP / MQTT / Kafka          |
      pgwire             RESP2       HTTP / gRPC adapters    native APIs
        |                  |                  |                   |
        |                  |          public broker API           |
        +------------------+------------------+-------------------+
                                              |
                        SQL / collections / streams / application runtime
                                              |
                         authorization, RLS, constraints, transactions
                                              |
                   indexes / full-text / trigrams / vectors / spatial
                            graphs / Arrow analytics / OLAP cubes
                                              |
                           WAL, pages/records, checksums, metadata
                                              |
                       local disk / browser OPFS / backup and recovery
                                              |
                       sync / Mesh / replication / HA / distributed ranges

 Signed applications / Cells
        +--> capability-scoped providers --> HTTP, Redis, email, blobs,
                                             embeddings, LLMs, tokenizers,
                                             outbound gRPC + Protobuf

 Host operator --> CLI / optional login operator API / cluster controls
```

This is a capability map, not a promise that every deployment starts every
listener. PostgreSQL/pgwire and RESP2 servers are in this repository. AMQP
0-9-1, MQTT 3.1.1, Kafka, HTTP, and gRPC **broker frontends are separate
integrations** over the public durable broker API; their packaging and enabled
listeners belong to the integration being deployed. Embedded and browser modes
use their applicable subsets of the same engine facilities.

**Protobuf is a message encoding and schema format, not a separate listener.**
The in-tree `bicdb-provider-grpc` provides outbound unary gRPC calls for hosted
applications using signed method paths and Protobuf schemas. That provider is
distinct from a gRPC frontend accepting broker clients. See
[protocol access](#75-messaging-adapters-and-grpcprotobuf) for the boundaries.

### 2.1 Major workspace components

The source tree separates responsibilities into crates. Owners do not need to
memorize them, but the boundaries make debugging and code exploration easier:

| Area | Crate | Responsibility |
| --- | --- | --- |
| Engine | `bicdb-core` | records, transactions, indexes, WAL, backup, replication, distribution |
| Physical pages | `bicdb-page` | paged storage, page WAL, locking, checks |
| SQL | `bicdb-sql` | parser/execution, catalogs, migrations, RLS-facing SQL behavior |
| PostgreSQL endpoint | `bicdb-pgwire` | wire protocol, sessions, authentication, TLS |
| Redis endpoint | `bicdb-resp` | RESP2 durable cache behavior |
| Analytics | `bicdb-analytics` | Arrow/DataFusion query path and sidecars |
| Browser | `bicdb-wasm` | WebAssembly embedding |
| Sync | `bicdb-sync` | local-first synchronization machinery |
| Applications | `bicdb-app-runtime` | signed packages, capabilities, providers, WASM execution |
| Application providers | `bicdb-provider-*`, `bicdb-blob-s3` | operator-bound external services, including unary gRPC/Protobuf and blob storage |
| Broker engine | `bicdb-core` and `bicdb-sql` | durable queues, groups, acknowledgements, retries, dead letters, SQL broker functions; separate protocol adapters consume these APIs |
| Cell family | `bicdb-cell*` | isolated runtime, admission, device, grants, and Cell HA |
| Fleet | `bicdb-fleet` | lifecycle and controller-facing fleet facilities |
| Operator interface | `bicdb-cli` | commands, servers, inspection, maintenance, TUI |

### 2.2 A write from client to disk

For a normal SQL write, the useful conceptual sequence is:

1. A pgwire session authenticates and establishes its database/user context.
2. SQL parsing, name resolution, permissions, RLS, constraints, and types are
   evaluated.
3. The transaction works against a snapshot and accumulates mutations.
4. Index, audit, event, and related state are updated as part of the engine's
   transaction boundary.
5. WAL/durable storage records the commit according to configured sync policy.
6. The client receives success only at the durability boundary promised by
   that configuration.
7. Recovery replays or resolves recorded outcomes after a crash.

In a distributed range, extra steps precede local application: the range
leader prepares an identical checksummed command on a voter majority, obtains
a durable decision quorum and certificate, and only then applies through the
normal local transaction machinery. This is why distributed operation is not
equivalent to mounting one database directory on several machines.

### 2.3 Control plane versus data plane

The **data plane** stores and serves application data. The **control plane**
decides membership, placement, epochs, lifecycle, and admission. An operator's
control-plane authority is intentionally not blanket authority to read every
Cell's data. Maintaining this distinction prevents operational convenience
from silently becoming a universal data backdoor.

---

## 3. Storage, transactions, and recovery

### 3.1 Database directory ownership

A database path is live mutable state, not a folder to casually copy. Give it
one operating-system owner, restrictive permissions, sufficient space, and a
dedicated lifecycle. Do not let two independent writable processes open the
same path. Do not place live state on an unsafe network filesystem. Use BicDB's
backup, replication, relocation, or sync protocols to move data.

At the root, `format.json` declares the on-disk format version, minimum reader
and writer versions, and required feature flags. A newer or unknown format is
rejected before recovery or writes. Legacy migrations are monotonic; downgrade
is restore-based, not an in-place rewrite.

### 3.2 Storage modes

BicDB includes record-oriented and physical paged-storage facilities. The
operator-facing `bicdb store` family inspects, verifies, migrates, and repairs
physical layout metadata. Treat a storage-mode migration like any other major
maintenance event: read the dry-run output, take and verify a backup, stop
writers where required, provide headroom, and run integrity checks afterward.

Never infer a mode by looking for one filename and never hand-edit mode
metadata. Ask the CLI:

```bash
bicdb inspect /srv/bicdb/app
bicdb store --help
```

### 3.3 Transactions and isolation

BicDB supports explicit transactions, snapshot-oriented isolation, concurrent
write execution, and ordinary concurrent commits. SQL isolation levels and
conflict behavior have specific contracts; see [transactions](../TRANSACTIONS.md).
The essential owner rules are familiar:

- keep write transactions short;
- retry documented serialization or leadership conflicts rather than hiding
  them;
- do not hold a transaction open while waiting for human or network input;
- assume a client disconnect can leave the outcome unknown until inspected;
- make externally retried operations idempotent with stable application keys.

Durability and isolation answer different questions. Isolation determines what
concurrent transactions observe; fsync and replication policy determine what
survives a failure. An isolated but buffered commit is not the same guarantee
as a durably synchronized one.

### 3.4 WAL and crash recovery

The write-ahead log records changes before their data representation is
considered durable. On restart, recovery validates WAL and storage, replays
committed work, and rejects corrupt or ambiguous outcomes. Page storage also
tracks transaction outcomes and checkpoint state.

Operational rules:

1. preserve the entire directory after an unexpected failure;
2. record the binary version and command line;
3. run read-only inspection before attempted repair;
4. copy evidence or take a filesystem snapshot before destructive action;
5. prefer documented repair commands to deleting a WAL/status file;
6. restore into a new path when the correct outcome is uncertain.

### 3.5 Checksums and integrity

`verify`, `check`, and `integrity` serve related but distinct purposes. Consult
their help for the deployed release. In a maintenance window, a conservative
sequence is:

```bash
bicdb inspect /srv/bicdb/app
bicdb verify /srv/bicdb/app
bicdb check /srv/bicdb/app --json > check.json
bicdb integrity --help
```

Store the JSON result with the incident or release record. A successful open
is not a substitute for an integrity check, and a verified backup archive is
not proof that the restored application behaves correctly.

### 3.6 Compaction and large values

Compaction reclaims or reorganizes obsolete storage while preserving logical
state. It consumes I/O and temporary capacity; observe latency and free space,
and never kill it merely because progress is quiet. Large values may live in
sidecar attachments. Backup and strict integrity workflows include them and
report missing blobs, checksum failures, and orphan temporary files.

### 3.7 Schema and format migration

Schema migrations are application changes; format migrations are storage
compatibility changes. Both require rollback planning. Use `bicdb migrate` to
plan/apply supported SQL migration workflows, retain migration history, and
test against a production-shaped restored copy. When a format upgrade fails,
retry the same binary first so it can resume its migration journal. If that
fails, restore the pre-upgrade backup into a clean path with a compatible
binary.

---

## 4. Ways to run BicDB

### 4.1 Embedded Rust

Embedded mode has no daemon and minimizes operational surface:

```rust
use bicdb_core::{BicDb, DbConfig};

let mut db = BicDb::open_with_config(
    "./appdb",
    DbConfig::default().with_fsync(true),
)?;
db.create_collection("notes")?;
db.flush()?;
# Ok::<(), bicdb_core::BicDbError>(())
```

The application owns process lifetime, shutdown, keys, backups, and exclusive
path access. Call the clean flush/close path, but design for recovery after an
unclean exit.

### 4.2 Standalone PostgreSQL-compatible server

```bash
bicdb serve /srv/bicdb/app --host 127.0.0.1 --port 5433
psql -h 127.0.0.1 -p 5433 -U bicdb -d bicdb
```

This is the normal networked SQL shape. Bind loopback by default. Shared LAN or
production binding requires authentication, TLS, firewalling, resource limits,
monitoring, and a tested shutdown policy.

### 4.3 Durable RESP cache

```bash
bicdb cache-serve /srv/bicdb/cache --host 127.0.0.1 --port 6379
redis-cli -p 6379 SET greeting hello EX 300
```

Cache keys and TTLs are durable BicDB records. Choose an eviction policy and
memory budget intentionally. “Cache” does not mean “safe to expose without
authentication” or “never needs backup” if it contains irreplaceable state.

### 4.4 Browser/WASM

`@bicdb/client` runs the engine in a Web Worker over OPFS. Web Locks enforce
single ownership. The main-thread interface provides operations such as open,
query, stats, compact, and close. Browser storage quotas, eviction, tab
lifecycle, version skew, and sync conflicts become part of the application's
operating model.

### 4.5 TUI

Launch the terminal UI with:

```bash
bicdb --tui /srv/bicdb/app
```

The TUI is an operational view, not an authorization bypass. Supply encryption
material through the supported key options, prefer environment-backed secrets,
and see `docs/tui.md` for panels and keys.

### 4.6 Application host and Cell runtime

The signed application host runs ABI-v2 packages with declared capabilities.
The Cell runtime goes further: one process represents one fail-closed security
Cell with dedicated identity, keying, storage, application, HA, grant, and
admission ceremonies. Chapter 8 explains why these are separate deployment
choices.

---

## 5. The CLI owner's tour

### 5.1 Discovery before execution

The CLI is intentionally broad. Discover rather than guess:

```bash
bicdb --version
bicdb help
bicdb help backup
bicdb backup create --help
```

Capture `--version` and the relevant `--help` in automated change records.
Never paste production secrets after `--key`; they may appear in shell history
and process listings. Prefer `--key-env` or the documented environment variable.

### 5.2 Command families

The top-level families can be understood by job:

| Job | Commands/families |
| --- | --- |
| Create and inspect | `init`, `inspect`, `export`, `verify`, `check`, `integrity` |
| Maintain storage | `store`, `compact`, `migrate`, `index` |
| Query and analyze | `sql`, `analytics`, `vector`, `spatial`, `graph`, `model`, `memory` |
| Serve clients | `serve`, `serve-pg`, `cache-serve`, `sync-serve`, `server` |
| Identity and security | `user`, `security`, encryption key options |
| Protect data | `backup`, `replication`, `ha` |
| Distribute data | `consensus`, `cluster`, `sync` |
| Run applications | `app`, `cell` |
| Observe and diagnose | `metrics`, `health`, `doctor`, `--tui` |
| Validate a release | `bench`, `compat` |

Some families contain nested subcommands and some options are feature- or
build-dependent. CLI help is authoritative for syntax.

### 5.3 Initialize and inspect

```bash
install -d -m 0700 /srv/bicdb/app
bicdb init /srv/bicdb/app
bicdb inspect /srv/bicdb/app
```

For encryption, use a generated high-entropy secret and a secret manager:

```bash
export BICDB_DB_KEY='value-supplied-by-secret-manager'
bicdb init /srv/bicdb/app --encrypted --key-env BICDB_DB_KEY
bicdb inspect /srv/bicdb/app --key-env BICDB_DB_KEY
```

Do not invent variable names for other commands; use that command's help.

### 5.4 SQL and export

Use `bicdb sql --help` for local SQL execution. For restartable collection
exports, BicDB emits JSON Lines in primary-key order:

```bash
bicdb export /srv/bicdb/app --collection patients \
  --batch-rows 1000 --batch-bytes 67108864 > patients.jsonl
```

Resume strictly after the last durable ID with `--after-id`, or use
`--after-id-hex` for typed/internal IDs that cannot safely be represented in an
OS argument. `--locality` may improve physical read order while preserving
primary-key output and cursor order. Store the output and resume cursor
atomically in automation.

### 5.5 Health, metrics, and doctor

```bash
bicdb health status /srv/bicdb/app
bicdb metrics /srv/bicdb/app
bicdb doctor /srv/bicdb/app --json > doctor.json
```

Readiness asks whether the instance should receive work; liveness asks whether
the process is functioning. Do not promote an instance merely because its TCP
port opens. Doctor bundles can contain topology and operational metadata;
review and redact them before sharing outside the trust boundary.

### 5.6 Maintenance pattern

For any mutating maintenance command:

1. read its exact `--help` on the target version;
2. inspect the database and available capacity;
3. verify a recent backup and preferably complete a restore drill;
4. stop or drain traffic if the command is not explicitly online;
5. run the smallest scoped operation;
6. retain stdout, stderr, exit status, version, and timestamps;
7. run post-operation integrity and application smoke checks;
8. restore service gradually while watching error and latency metrics.

---

## 6. SQL, collections, indexes, and specialized data

### 6.1 Collections and SQL tables

The native API works with named collections and `Record` values. SQL provides
tables, types, constraints, indexes, roles, and PostgreSQL-shaped behavior over
the engine. Choose the interface that fits the application, but remember they
share storage and transaction machinery.

### 6.2 PostgreSQL compatibility

BicDB speaks the PostgreSQL wire protocol and supports a substantial, tested
subset of PostgreSQL semantics. It is not PostgreSQL internals in Rust and does
not promise every extension, system catalog detail, type corner case, or
administrative command. Before migration:

- read `POSTGRES_COMPATIBILITY.md`;
- run the real driver/ORM against a staging BicDB instance;
- inventory extensions, functions, types, catalog queries, and isolation
  assumptions;
- run differential and client-gauntlet tests relevant to the application;
- test dump/restore and rollback, not only happy-path queries.

### 6.3 Constraints, RLS, and tenant context

Constraints protect invariants at the data boundary. Row-level security (RLS)
protects row visibility and mutation using trusted identity, tenant, and role
context. RLS is defense in depth inside a Cell, not a replacement for Cell
isolation. Test positive and negative cases: correct tenant, wrong tenant,
missing context, forged context, privileged maintenance, background jobs, and
newly added tables.

### 6.4 Indexes and planner statistics

Indexes accelerate reads at the cost of write work, storage, and maintenance.
Use `EXPLAIN`-style evidence and representative data rather than creating an
index for every predicate. Keep planner statistics current. Concurrent or
resumable build support has its own lifecycle and status; do not infer success
from a vanished client connection. Verify the published generation and query
results after an interrupted build.

GIN `gin_trgm_ops` indexes accelerate supported `LIKE` / `ILIKE` substring
predicates, with full predicate and RLS rechecks. This is a bounded PostgreSQL
trigram compatibility surface, not the complete `pg_trgm` extension: similarity
operators, GiST, and regex index acceleration are not implied. See
[trigram indexes](trigram-indexes.md) for supported candidate shapes and fallbacks.

### 6.5 Full-text search

Full-text indexes store term dictionaries, compressed postings, positions,
document statistics, and source rows under one database lifecycle. BicDB
supports ranking, phrases, language stemming, weighted fields, filtering, and
optimized top-k retrieval. Packed immutable segments publish atomically, so
readers can remain pinned to the prior generation during a build.

Progressive FTS (`BICDB_FTS_PROGRESSIVE=1`) can publish searchable subsegments
during a long build. That improves time-to-first-query, but operators must
still distinguish “partially searchable” from “complete generation.” Monitor
authoritative build state and retain enough disk for temporary and published
artifacts.

### 6.6 Vector search and models

Records can carry vectors; vector indexes trade recall, build time, memory, and
latency. Record vector dimension and distance metric as schema decisions. Test
recall against an exact baseline, not only throughput. The model registry and
embedding providers are operational dependencies: pin model identity,
dimension, tokenizer, preprocessing, and provider version so a rebuild does
not silently change meaning.

### 6.7 Spatial, graph, and analytics

- Spatial support includes geometry, H3 cells, nearest-neighbor and routing
  facilities. Validate coordinate reference and longitude/latitude order.
- Graph projections are derived operational objects. Define rebuild and
  verification procedures and do not confuse a stale projection with source
  truth.
- Analytics uses Arrow/DataFusion and sidecars. Sidecars can be rebuilt and
  verified; their lifecycle must be included in monitoring and capacity plans.
- Incremental OLAP cubes accelerate aggregates but require the same correctness
  checks as an index.

### 6.8 Streams, queues, and notifications

The durable stream layer supports event logs, consumer groups, acknowledgement
and negative acknowledgement, visibility-timeout retries, dead-letter queues,
retention, redrive, role ACLs, and idempotent publishing. SQL `broker_*`
functions expose the broker over pgwire; transactional publish-on-commit and
LISTEN/NOTIFY delivery connect it to application transactions. Rolled-back
work, including handled-exception and savepoint rollback, must not publish its
discarded events.

AMQP, MQTT, Kafka, HTTP, and gRPC broker frontends are separate adapters over
these APIs. Owners must define retention, maximum event size, retry backoff,
poison-message handling, idempotency, group lag alerts, and redrive authority.
An adapter's acknowledgement and delivery guarantees must be checked against
its implementation; sharing a durable store does not establish full RabbitMQ,
MQTT, or Kafka compatibility. See [Stream Broker](../STREAM_BROKER.md).

---

## 7. Server protocols and client access

### 7.1 PostgreSQL wire server

Use a dedicated service account and directory. A baseline service manager
should enforce:

- explicit binary and absolute database path;
- restrictive `UMask` and filesystem ownership;
- restart limits rather than an infinite crash loop;
- file descriptor and memory limits sized from measurements;
- a graceful stop timeout long enough for shutdown;
- secrets from protected environment files or a secret manager;
- logs directed to the site's collection system.

For remote service, configure TLS and SCRAM-SHA-256 where supported by the
selected command options. Cleartext authentication is not encryption; without
TLS, credentials and data are exposed.

### 7.2 Users and identity

SQL roles and physical pgwire login records are separate. Create and bind
trusted login identities through `bicdb user` or the optional
[online login operator API](login-operator-api.md). Give applications distinct
users, never share an owner credential, and separate migration from runtime
identities. SQL superuser privileges do not authorize the operator API.

A login-bound identity stays immutable for an established connection. Ordinary
session settings cannot switch it to another authenticated principal. For a
shared application pool, an operator can enable
[verified transaction-scoped delegation](transaction-delegation.md): authenticate
the connection as the application, begin a transaction, supply the signed
challenge-bound end-user identity, execute under RLS, then commit or roll back.
Transaction end clears delegation before reuse. Resetting arbitrary SQL settings
is not a substitute for that protocol.

The operator API can create/bind, rotate, disable/re-enable, revoke, and list
logins while queries continue. Its host-only credential policy persists after
activation, even if a later restart omits the API flags. Use host-authorized
management rather than SQL password mirroring in that mode. Existing ordinary
sessions retain their identity until disconnected; disabling a login does not
silently terminate those sessions. Review the operator guide before choosing a
revocation and pool-draining policy.

### 7.3 RESP cache server

RESP2 compatibility lets existing Redis clients access the durable cache.
Test the exact command subset used by the application. Set memory limits and
choose among no-eviction, random all-key eviction, or volatile-TTL behavior
according to the deployed CLI. Avoid interpreting Redis compatibility as a
claim that every Redis module or clustering behavior exists.

### 7.4 Timeouts, cancellation, and large results

Bound connection count, statement time, result size, and application retries.
Cancellation is cooperative across layers; verify that the real driver sends
it and that expensive work stops. Stream large results rather than collecting
them in RAM. Backpressure is a normal control mechanism, not an error to bypass.

### 7.5 Messaging adapters and gRPC/Protobuf

| Access path | Where it is implemented | Owner responsibility |
| --- | --- | --- |
| PostgreSQL clients, SQL broker functions, LISTEN/NOTIFY | In-tree `bicdb-pgwire`, `bicdb-sql`, and broker engine | Configure pgwire authentication, grants, TLS, limits, and queue ACLs. |
| Redis cache clients | In-tree `bicdb-resp` | Validate the supported RESP2 command subset and eviction policy. |
| AMQP 0-9-1, MQTT 3.1.1, Kafka, HTTP/gRPC broker clients | Separate adapters using the public broker API | Deploy the adapter, configure its listener/authentication, and verify protocol-specific semantics. These are not automatic `bicdb serve` endpoints. |
| Hosted application calls to remote gRPC services | In-tree `bicdb-provider-grpc` | Bind endpoints, TLS/credentials, allowed methods, timeouts, and size limits through operator policy; the signed application supplies method/schema contracts. |
| Rust embedding and browser WASM | Native engine APIs and `bicdb-wasm` / browser client | Select the host-appropriate storage and provider configuration. |

The gRPC provider supports unary calls, not an unrestricted streaming gRPC
client or a general Protobuf schema registry. Its implementation and tests are
in [`bicdb-provider-grpc`](../crates/bicdb-provider-grpc/src/lib.rs); the separate
broker contract is documented in [Stream Broker](../STREAM_BROKER.md).

### 7.6 TLS channel binding

SCRAM channel binding ties authentication to the TLS server certificate. The
pgwire CLI exposes `--channel-binding require|prefer|disable`; choose a policy
compatible with the actual client and certificate, and verify it in staging.
Do not disable certificate verification to make a connection succeed. See
[server authentication and TLS](../SERVER_MODE.md) for defaults and failure
behavior, including certificate algorithms without a supported binding digest.

---

## 8. Cells and the application runtime

### 8.1 What a Cell is

A **Cell** is a policy-selected, fail-closed trust boundary for regulated or
otherwise isolated workloads. It is not automatically one customer, one user,
one SQL database, one schema, or one server. The policy chooses the grouping;
the runtime makes that choice concrete.

The foundational invariant is **one runtime, one Cell**. Production isolation
also requires dedicated mounts, uid, network policy, cgroup/resource limits,
keys, and backup/HA scope. Several databases in one general pgwire process
still share a process and failure domain.

### 8.2 The five trust domains

The Cell architecture separates concerns that are often accidentally merged:

1. global control-plane/fleet authority;
2. application release authority;
3. Cell-local runtime and data authority;
4. device replica authority;
5. explicitly granted cross-Cell access.

Operational authority is not data authority. Global code may be distributed
everywhere without receiving global execution rights or universal decryption
keys.

### 8.3 Manifest and startup ceremony

A Cell starts from a verified `CellManifest` that binds identity, expected
artifacts, policy, and required services. The runtime must establish identity
and obtain its key lease before opening protected storage or accepting work.
Missing, expired, mismatched, or untrusted evidence fails before mutation.

Use the `bicdb cell` command family to discover the exact release's operations:

```bash
bicdb cell --help
bicdb cell serve --help
bicdb cell verify --help
bicdb cell rotate-key --help
```

Development file-backed key providers are for development. Production key
leases should be attested, short-lived, auditable, revocable within the stated
online model, and kept out of arguments, manifests, logs, and backups.

### 8.4 Cell-native applications

ABI-v2 applications are signed packages. Installation establishes provenance;
the manifest declares capabilities; the host mediates access to SQL, HTTP,
Redis, email, gRPC, tokenizers, embeddings, LLMs, blobs, routes, secrets, and
other providers. A signature says who authorized bytes—it does not make unsafe
capabilities safe.

Owner workflow:

1. trust only reviewed release keys;
2. inspect package identity, digest, ABI, and requested capabilities;
3. bind the least-powerful providers and egress policy;
4. keep secrets host-side and versioned;
5. canary the package in a non-production Cell;
6. retain installation and execution receipts;
7. prepare a signed rollback package;
8. revoke or replace compromised keys deliberately.

The general host is operated through `bicdb app <path> ...`; inspect
`bicdb app --help` and the application runtime author/operator guides.

### 8.5 Cell keys and rotation

Cell keys encrypt Cell storage and must remain distinct from backup keys,
package-signing keys, cluster TLS keys, and device keys. Rotation is a stateful
ceremony, not a string replacement. Confirm a restorable backup, prevent
concurrent conflicting rotation, retain evidence, validate all replicas, and
do not destroy the old recovery path before the new state is verified.

### 8.6 Cell HA, devices, and grants

- **Cell HA** replicates within the Cell's trust and key boundary. Promotion
  still requires fencing, freshness, and recovery evidence.
- **Device replicas** use hardware-bound identity and explicit authorization.
  Offline revocation cannot be instantaneous; documentation must state the
  real reconnect/revalidation boundary.
- **Cross-Cell grants** are explicit, recipient-encrypted objects. They should
  carry narrow scope, provenance, expiry/revocation semantics, and audit
  evidence rather than creating a hidden global data concentrator.

### 8.7 Regulated-data admission

The Phase 8 implementation provides an exact-build hardened-fleet evidence
verification path, but regulated-data admission requires the complete
threshold-signed bundle and every documented gate. “The binary supports
Cells” is not a compliance certification. Owners remain responsible for host,
network, identity, key custody, logging, incident response, and legal controls.

---

## 9. Replication, HA, consensus, and distributed clusters

These terms describe different layers. Choosing the wrong one is a common
design error.

| Mechanism | Primary purpose | Write shape |
| --- | --- | --- |
| Sync/Mesh | authorized local-first peer exchange and conflicts | multi-origin, conflict-aware |
| Streaming replication | copy ordered commits to a standby | primary to standby |
| HA | health, promotion, fencing, file/replica operations | one active writer |
| Consensus loop | elect/order native commit frames | single elected leader |
| Distributed ranges | place and quorum-replicate partitions | one leader per range; one-range transaction |
| Cell HA | HA constrained to one Cell trust boundary | Cell-scoped |

### 9.1 Streaming replication and HA

Replication improves availability; it is not backup. A deletion or logical
error can replicate perfectly. Configure unique node identity, shared cluster
identity, TLS/mTLS, lag monitoring, and standby write protection. Test the
exact promotion and rewind/reseed path.

A safe failover requires:

1. establish that the old primary is fenced from clients and replicas;
2. choose an eligible, sufficiently fresh standby;
3. verify storage/recovery health and expected cluster identity;
4. promote exactly once;
5. update routing and validate writes/read-after-write;
6. quarantine the old primary;
7. rejoin it only through the documented reseed/reconciliation path.

Never promote two nodes to “see which works.” That manufactures split brain.

### 9.2 Raft-style consensus loop

`bicdb consensus run` maintains persistent term/vote/log state, conducts
elections, sends heartbeats, and replicates `CommitFrame` entries over the
native TLS transport. A majority of voting peers advances commit index.

```bash
bicdb consensus run /srv/bicdb/node-a \
  --listen 10.0.0.10:9444 \
  --cluster-id prod-east --node-id node-a \
  --peer node-a=10.0.0.10:9444 \
  --peer node-b=10.0.0.11:9444 \
  --peer node-c=10.0.0.12:9444 \
  --tls-cert /etc/bicdb/node-a.crt \
  --tls-key /etc/bicdb/node-a.key \
  --tls-ca /etc/bicdb/ca.crt
```

Run it beside `bicdb serve` under a service manager; the server does not yet
embed this supervisor. Writes remain single-leader. Client routing/failover and
production fencing remain operator policy.

### 9.3 Distributed range architecture

Distributed mode partitions the key space into virtual ranges. Durable
metadata records nodes, placement, leaders, epochs, and topology generation.
Virtual range count is independent of current server count. Replicas should be
placed across declared server/rack/zone/region failure domains.

Every mutation is hashed from its final collection and record IDs. A normal
distributed transaction must map to one range. The range's current leader and
epoch fence stale writers. Cross-range atomic writes require an explicit
distributed protocol; they are never silently approximated.

### 9.4 Bootstrap the first member

```bash
bicdb cluster init /srv/bicdb \
  --cluster-id production --node-id server-1 \
  --address 10.0.0.11:9444 --capacity-bytes 4000000000000 \
  --replication-factor 3 --initial-ranges 256 \
  --failure-domain server --failure-domain rack --failure-domain zone \
  --label rack=rack-a --label zone=us-west-2a \
  --cluster-tls-cert /etc/bicdb/server-1.crt \
  --cluster-tls-key /etc/bicdb/server-1.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

Non-loopback clustering requires all mutual-TLS materials. Protect the cluster
CA: pre-membership bootstrap intentionally grants a narrow registration flow
to holders of a valid certificate.

### 9.5 Join a member

Run the preferred self-bootstrap command on the empty new server:

```bash
bicdb cluster join \
  --cluster-id production \
  --seed-node-id server-1 --seed-address 10.0.0.11:9444 \
  --node-id server-2 --address 10.0.0.12:9444 \
  --capacity-bytes 4000000000000 \
  --label rack=rack-b --label zone=us-west-2b \
  --node-root /srv/bicdb \
  --cluster-tls-cert /etc/bicdb/server-2.crt \
  --cluster-tls-key /etc/bicdb/server-2.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

The node registers first as a metadata learner. Only after it catches up and a
joint quorum promotes it can placement use it. Identical retry is safe. Do not
copy `cluster-topology.json`, `bicdb-distribution.json`, private keys, or a live
member directory to “join faster.” Reusing a decommissioned node ID requires a
higher incarnation.

### 9.6 Inspect and route

```bash
bicdb cluster status /srv/bicdb
bicdb cluster status /srv/bicdb --json
bicdb cluster status /srv/bicdb --prometheus
bicdb cluster route /srv/bicdb patients patient-42
```

Route output includes generation, range ID, epoch, and leader. A client or
gateway should cache routes by generation, follow structured redirects, and
retry only idempotent operations. A non-leader currently rejects a write
rather than transparently forwarding it.

### 9.7 Quorum write and recovery model

For each one-range transaction, the leader durably prepares the same
checksummed command on a current voter majority, reaches a provisional decision
quorum, records a quorum certificate, and applies through the local engine.
Followers validate identity, cluster, range, epoch, index, and checksum.
Idempotent retry cannot replace one command with another at the same index.

On startup, certified-but-not-applied commands replay through normal
transactions. A merely provisional decision is not crash-replayed. New leaders
recover a quorum-evidenced prefix before accepting writes; ambiguous or
divergent evidence fails closed rather than guessing. Lag beyond retained
repair history requires a snapshot.

### 9.8 Schema consistency

Range consensus does not automatically replicate arbitrary DDL. Nodes publish
a canonical schema fingerprint. Placement, leadership, repair, and write
admission reject drift. An actually empty node may receive a bounded schema
bootstrap; a node with user rows is not automatically overwritten. Plan
cluster-wide schema rollouts explicitly and wait for the new fingerprint to be
quorum-visible before normal writes resume.

### 9.9 Rebalance, drain, and remove

Use `bicdb cluster` subcommands and the distributed operations guide. General
order:

1. verify metadata quorum and all current replicas;
2. ensure target failure domains and free capacity exist;
3. start a bounded relocation/rebalance;
4. monitor snapshot, catch-up, epoch transition, and cleanup;
5. retry the state machine rather than manually moving files;
6. drain leaders and replicas before removal;
7. remove membership only after placement is healthy;
8. revoke the retired certificate and preserve audit evidence.

### 9.10 Cluster boundaries

Cluster-wide backup/restore is still an operator-coordinated set of node/range
artifacts rather than one global quorum-bound snapshot. Automatic distributed
sharding and multi-writer claims have explicit current boundaries. Read the
distributed operations document for the release before production admission.

---

## 10. Backup, restore, and disaster recovery

### 10.1 Backups are encrypted, authenticated containers

New backups use the streaming `BICBAK03` format. The manifest, per-file hashes,
encryption metadata, recovery metadata, source format, and independently
authenticated data frames detect wrong keys, corruption, truncation,
insertion, and reordering. Compression is per frame when beneficial. Creation
uses a protected online checkpoint, temporary output, flush, and atomic rename.

Incrementals reference the preceding backup ID and manifest hash and include
authenticated deletions. Therefore an incremental is useful only with its
ordered verified chain.

### 10.2 Key separation

Backup encryption keys must be separate from database encryption, Cell,
package-signing, TLS, and device keys. Put keys in a secret manager and inject
them through `--key-env` or `BICDB_BACKUP_KEY`, not command arguments. Back up
the key under a distinct custody process; an encrypted archive without its key
is intentionally unrecoverable.

### 10.3 Create full and incremental backups

```bash
export BICDB_BACKUP_KEY='value-supplied-by-secret-manager'

bicdb backup create /srv/bicdb/app /backup/app-full.bicbackup
bicdb backup create /srv/bicdb/app /backup/app-inc-001.bicbackup \
  --base /backup/app-full.bicbackup
```

Write initially to storage with enough capacity, then upload using a mechanism
that preserves bytes and records a digest. Do not overwrite the only known-good
chain. Keep retention metadata and archive keys recoverable independently of
the production host.

### 10.4 Verify artifacts and chains

```bash
bicdb backup verify /backup/app-full.bicbackup
bicdb backup verify /backup/app-full.bicbackup \
  /backup/app-inc-001.bicbackup --json > chain-verify.json
```

Verify after creation, after transport, periodically at rest, and with the
target upgrade binary. Verification authenticates the archive and chain; only
a restore drill proves that operators, keys, capacity, binary, and application
procedures work together.

### 10.5 Restore safely

Restore into a clean, isolated directory:

```bash
bicdb backup restore /backup/app-full.bicbackup \
  /srv/bicdb/restore-candidate --force
```

Do not point clients at the target until restore finishes, strict integrity
passes, the database opens with the expected key, and application smoke tests
pass. `--force` is destructive; validate the target path and mount before use.

### 10.6 Point-in-time recovery

Timestamp PITR replays durable record-audit events and therefore requires audit
events to have been enabled **before** the backup:

```bash
bicdb backup restore /backup/app-full.bicbackup /srv/bicdb/pitr \
  --force --target-timestamp 1781913600 \
  --pitr-record-batch 1024 \
  --pitr-event-batch 1024 \
  --pitr-event-bytes 8388608
```

The limits bound rows deleted per transaction, events scanned per pass, and
serialized target-event bytes retained per pass. Increase them only after
measurement. This is BicDB record-audit PITR, not a claim of PostgreSQL WAL
compatibility.

### 10.7 Restore drills and evidence

```bash
bicdb backup drill /backup/app-full.bicbackup \
  --target /srv/bicdb/drill \
  --json-out /var/log/bicdb/backup-drill.json
```

A drill restores, runs strict storage/SQL integrity, opens the database, and
records smoke counts, large-value checks, RTO, and available RPO evidence. Add
application-owned checks: authenticate, query a known invariant, validate RLS,
consume a test event, and exercise a critical index. Reports must not contain
keys or protected record payloads.

### 10.8 A practical policy

A starting point for a high-throughput business system is one full backup per
day, incrementals every 15 minutes during business hours, verification of every
artifact and ordered chain, and at least one daily restore drill. This is not a
universal SLO. Derive frequency from acceptable data loss (RPO), and size the
restore process from acceptable downtime (RTO). Measure both on target-like
hardware.

Use a 3-2-1-style posture: multiple copies, different failure media/domains,
and at least one off-site or logically isolated copy. Include immutable or
deletion-resistant retention where ransomware is in the threat model.

### 10.9 Disaster sequence

1. declare the incident and stop unsafe writes;
2. preserve evidence and identify the last known-good point;
3. select a compatible binary, full artifact, incrementals, keys, and target;
4. verify the complete chain before changing production;
5. restore to isolation and run integrity plus application checks;
6. fence the old deployment;
7. switch traffic with an explicit rollback point;
8. monitor and reconcile downstream consumers;
9. take a new full backup after stability;
10. record actual RPO/RTO and improve the runbook.

---

## 11. Security and hardening

### 11.1 Threat model first

List protected data, actors, trust boundaries, accepted downtime, and likely
attack paths. Then select a hardening profile such as local development,
shared LAN, production server, or regulated data. A profile name is a starting
policy, not evidence that the environment satisfies it.

### 11.2 Host baseline

- run as a dedicated unprivileged uid/gid;
- make database, key, certificate, and backup paths separately permissioned;
- use dedicated mounts and prohibit unexpected symlinks;
- patch the OS and pin the BicDB artifact by digest/version;
- restrict outbound as well as inbound network access;
- use cgroup/service-manager CPU, memory, process, and file limits;
- synchronize time and monitor drift;
- encrypt disks where physical media loss matters;
- centralize logs without recording secrets or protected query values.

### 11.3 Network baseline

Bind loopback unless remote access is necessary. Segment client, replication,
cluster, management, and provider traffic. Use TLS for clients and mTLS for
node identity. Rotate certificates before expiry and practice revocation.
Cluster membership pins the committed leaf certificate fingerprint; another
certificate signed by the same CA does not silently become that node.

### 11.4 Authentication and authorization

Use SCRAM rather than cleartext password exchange, least-privilege roles, RLS,
short-lived operational credentials, and separate migration/runtime accounts.
Review grants regularly. Test denial paths. Keep trusted tenant context outside
user-controlled SQL parameters.

### 11.5 Secret classes

Inventory separately:

- database-at-rest keys;
- backup archive keys;
- Cell/key-lease material;
- TLS private keys and CA authority;
- package release signing keys;
- login operator API credentials and transaction-delegation signing keys;
- application provider secrets;
- device and cross-Cell grant keys.

Never reuse one class for another. Define generation, storage, distribution,
rotation, revocation, escrow, destruction, and audit owners for each.

### 11.6 Extensions and applications

WASM sandboxing reduces risk but does not erase it. Capabilities, provider
bindings, host calls, network egress, quotas, package signatures, and update
authority are the real security perimeter. Review application packages as
production code and fail closed on an unknown signer, digest, ABI, capability,
or provider.

### 11.7 Audit and incident handling

Enable the audit streams needed for investigation and PITR before an incident.
Protect them against unauthorized deletion and avoid sensitive payloads where
metadata suffices. On suspected compromise, fence first, preserve immutable
evidence, rotate by credential class, validate replicas/backups for the same
exposure, and re-admit only from known artifacts.

---

## 12. Observability and day-two operations

### 12.1 What to measure

At minimum:

- process liveness, readiness, restarts, CPU, resident memory, descriptors;
- request rate, latency percentiles, error/cancellation rate, connection count;
- disk bytes/free space, IOPS, latency, WAL/checkpoint and compaction progress;
- transaction conflicts, long transactions, slow queries, lock/wait pressure;
- index build state, planner/statistics age, search latency and recall checks;
- stream consumer lag, retries, DLQ size, retention pressure;
- replication lag, role, peer health, certificate expiry;
- cluster metadata role/term/commit index, unavailable or under-replicated
  ranges, relocation failures, capacity/leader skew, schema fingerprints;
- backup age, chain verification, drill age, observed RPO and RTO;
- Cell manifest/key-lease/admission state and package receipt failures.

### 12.2 Logs and slow queries

Use structured logs with timestamps, node/Cell identity, request correlation,
error class, and bounded safe context. Configure slow-query thresholds from an
SLO, not from annoyance. Query text and parameters may contain secrets or
personal data; use redaction and restricted retention.

The application runtime also supports OTLP/HTTP Protobuf and OTLP/gRPC
exporters with bounded non-blocking queues and optional JSONL fan-out. Exporter
endpoints and credentials are operator state; signed application capabilities
control which telemetry operations are available. This is another use of
Protobuf, separate from broker adapters and application gRPC calls. See the
[application ABI observability contract](application-runtime-abi-v2.md).

### 12.3 Alert design

Alert on user impact or impending loss of safety margin: readiness failure,
repeated crash recovery, low space, backup/drill overdue, quorum loss,
replication lag beyond RPO, under-replicated ranges, certificate/key expiry,
schema drift, or admission failure. Avoid paging on every leader election if
the system remains healthy; alert on rate, duration, and impact.

### 12.4 Daily checklist

- review health/readiness and overnight errors;
- confirm free space and growth trend;
- inspect replication, range, and consumer lag;
- confirm newest backup and chain verification;
- check certificate/key-lease expiry horizon;
- review failed logins, denied capabilities, and unusual slow queries.

### 12.5 Weekly/monthly checklist

- run or review a restore drill;
- sample strict integrity and application invariants;
- review capacity and compaction/index maintenance;
- rehearse failover on a non-production or approved environment;
- review users, roles, package keys, device grants, and stale nodes;
- test upgrade/rollback against a restored production-shaped copy;
- reconcile documentation with the actual command lines and service units.

---

## 13. Performance and capacity

### 13.1 Measure the real workload

Benchmark with production-shaped row/value sizes, index count, query mix,
concurrency, durability, dataset larger than RAM, and target storage. A
buffered local benchmark does not predict a durable replicated deployment.
Record hardware, filesystem, kernel, compiler, BicDB version, config, warm/cold
cache state, and percentiles.

### 13.2 Capacity dimensions

Budget separately for live rows, indexes, FTS generations, vectors, analytics
sidecars, WAL/recovery state, event retention, compaction temporary space,
relocation snapshots, backups, and growth during maintenance. Keep an emergency
reserve; a completely full filesystem turns routine recovery into an incident.

### 13.3 Tuning order

1. prove query and schema correctness;
2. locate the bottleneck with metrics/profiles;
3. fix unbounded results, missing selectivity, or poor access patterns;
4. update statistics and add only evidence-backed indexes;
5. tune concurrency and memory within host limits;
6. tune storage/checkpoint/compaction behavior;
7. scale replicas or ranges only after understanding the single-node limit;
8. repeat failure and recovery tests after every durability-related change.

### 13.4 Optional developer benchmarks

Build benchmark and compatibility tooling with
`cargo build --release -p bicdb-cli --features bench`. Normal CLI builds omit
these commands and the `bicdb-bench` dependency. The separate
`bench-comparison-engines` feature also enables external comparison engines.

`bicdb bench` and `bicdb compat` include targeted storage, SQL, vector, graph,
spatial, server, recovery, and compatibility workloads. They are diagnostic
tools and release evidence, not a substitute for application load tests. Use
`bicdb bench --help` to select a bounded test; never run an unreviewed benchmark
against production state.

---

## 14. Browser, offline, and synchronization

### 14.1 Local ownership

In the browser, a Worker owns the OPFS database and Web Locks prevent two live
owners. Handle quota exhaustion, private browsing limitations, browser
eviction, tab termination, and upgrade interruption as normal failure modes.
Close cleanly when possible but rely on crash recovery, not on unload events.

### 14.2 Sync and Mesh

BicDB Mesh uses per-origin version information, authenticated frames,
transitive relay, and explicit conflicts. Network reachability does not imply
authorization. Peers should authenticate, authorize the permitted working set,
bound frame size and replay work, and checkpoint durable progress.

### 14.3 Conflict policy

Do not claim “last writer wins” for regulated truth. Classify fields and
operations:

- commutative operations can merge;
- append-only facts can coexist;
- exclusive state changes may require a lease or server decision;
- conflicting clinical, financial, or authorization facts require review;
- derived data can often be recomputed from preserved source events.

Surface conflict provenance to users and retain the rejected alternatives long
enough for audit and repair.

### 14.4 Sync server

`bicdb sync-serve` exposes server-side synchronization facilities, while
`bicdb sync` performs administrative sync operations. Treat this endpoint like
a data API: TLS, authentication, authorization, rate limits, bounded working
sets, monitoring, and hostile-input testing are required.

---

## 15. Troubleshooting

### 15.1 A disciplined first response

```bash
date -u
bicdb --version
bicdb inspect /srv/bicdb/app
bicdb health status /srv/bicdb/app
bicdb doctor /srv/bicdb/app --json > doctor-incident.json
df -h /srv/bicdb/app
df -i /srv/bicdb/app
```

Also capture service status, recent logs, mount/options, memory pressure, and
the exact client error. Redact secrets before sharing. Do not repeatedly
restart a crash-looping writer; each restart can erase the best timeline and
consume recovery resources.

### 15.2 Database will not open

Check, in order: ownership/permissions, concurrent owner, disk/inodes, correct
key source, binary/on-disk format compatibility, migration journal, WAL or page
integrity, and symlink/mount changes. Preserve a copy before repair. A wrong key
should fail authentication; do not interpret it as permission to reinitialize.

### 15.3 Server is alive but not ready

Look for recovery in progress, unavailable key/manifest, schema fingerprint
not published, metadata learner state, leadership recovery, lost quorum,
under-replicated range, or admission failure. Bypassing readiness merely moves
the failure to clients.

### 15.4 Writes are rejected

Determine whether the error is authorization/RLS, standby protection,
non-leader redirect, stale range epoch, schema drift, quorum loss, one-range
restriction, key lease, resource governance, or recovery-required admission.
Only retry classes documented as retryable, with bounded exponential backoff
and a stable idempotency key.

### 15.5 Replication or cluster is unhealthy

Verify time, DNS/addressing, CA and leaf validity, committed fingerprint, node
ID/incarnation, cluster ID, metadata quorum, route generation/epoch, schema
digest, disk capacity, and lag window. Do not regenerate identity or topology
files on an existing member. A follower beyond retained history needs the
documented snapshot/reseed flow.

### 15.6 Backup verification fails

Stop rotation/deletion. Preserve the artifact and logs. Confirm ordered chain,
key source, complete upload/download, compatible binary, and source format.
Try another independent stored copy, not a hand-edited archive. Escalate any
authentication/hash failure as corruption or wrong provenance.

### 15.7 Slow queries or high memory

Bound result size, find long transactions, inspect the plan/statistics, observe
cache state, check compaction/index builds, and separate engine memory from
client buffering. Reproduce on a restored copy with the same durability and
dataset scale before changing production knobs.

---

## 16. Safe automation for software agents

An agent can operate BicDB safely only if its authority and evidence are
bounded. The following contract is suitable for runbooks and coding agents.

### 16.1 Read before acting

1. identify the exact repository/binary version;
2. read the nearest task documentation and release notes;
3. run `--help` for every command family involved;
4. inspect current health, role, topology, and storage mode;
5. inspect repository status before editing source;
6. state assumptions and stop if identity/path/target is ambiguous.

### 16.2 Classify actions

| Class | Examples | Agent behavior |
| --- | --- | --- |
| Read-only | help, inspect, status, metrics, route, verification | run and retain output |
| Reversible | create new backup, restore to new path, add an index in staging | record rollback and verify result |
| Disruptive | compact, migration, rebalance, package rollout | require maintenance policy and evidence |
| Destructive/security-critical | `--force`, member removal, key rotation/destruction, grant/revocation, production restore | require explicit target and human-approved runbook |

### 16.3 Command construction rules

- use absolute paths and quote them;
- never interpolate untrusted text into a shell command;
- never put secrets in source, output, arguments, or commit messages;
- use `set -euo pipefail` in reviewed shell automation;
- keep stdout/stderr, exit code, UTC start/end, version, and host/node identity;
- parse `--json` output rather than scraping human text;
- use timeouts only when abort semantics are known;
- retry only idempotent operations and only on documented transient errors;
- validate destination mount before any `--force` operation;
- do not “fix” consensus by editing JSON state files.

### 16.4 Evidence loop

Every mutation should produce four records:

1. **before:** health, role, configuration digest, backup/drill status;
2. **intent:** exact command with secrets redacted and expected invariant;
3. **result:** exit code and machine-readable report;
4. **after:** integrity, health, application smoke test, and rollback status.

If the after-state cannot be proven, the task is incomplete even if the
command returned zero.

### 16.5 Repository changes

For source/documentation work, an agent should follow repository instructions,
make the smallest coherent patch, format it, run focused tests, run link or doc
checks, inspect the diff, and commit only intended files. Claims in docs must
distinguish current implementation, configuration-dependent behavior, and
future design.

---

## 17. Learning labs

Run labs only on disposable paths.

### Lab 1: lifecycle and crash thinking

1. Initialize `/tmp/bicdb-lab`.
2. Inspect it and identify format/storage metadata.
3. Start a local server and create a table through `psql`.
4. Stop gracefully, restart, and verify the row.
5. Explain which guarantee came from the transaction and which from fsync/WAL.

### Lab 2: constraints, RLS, and indexing

1. Create two users and a tenant-keyed table.
2. Add constraints and an RLS policy.
3. Test correct, wrong, and missing tenant context.
4. Load representative rows and compare query plans before/after an index.
5. Explain why an application `WHERE tenant_id = ?` is not equivalent to RLS.

### Lab 3: backup and restore

1. Enable the recovery/audit settings needed for PITR.
2. Create a full backup, mutate data, then create an incremental.
3. Verify each artifact and the chain.
4. Restore to a new path and run a drill report.
5. Restore to a timestamp and measure RPO/RTO.
6. Demonstrate that the wrong key and reversed chain fail closed.

### Lab 4: three-node consensus

1. Create three disposable paths and localhost ports.
2. Run a consensus loop for each with the same peers and unique node IDs.
3. Observe status and leader election.
4. Stop the leader, observe a new term/leader, then restart the old node.
5. Explain majority, term, commit index, and why two nodes cannot be promoted
   manually during a partition.

### Lab 5: distributed routing

1. Bootstrap a test topology with virtual ranges and failure-domain labels.
2. Join another empty learner through the supported command.
3. Route several collection/record pairs and record range/epoch/leader.
4. Attempt a stale-epoch write in a test harness and inspect the refresh error.
5. Explain why copying the topology file is not a membership protocol.

### Lab 6: signed application and Cell boundaries

1. Read the ABI-v2 author and operator guides.
2. Inspect a test package's signer, digest, and capability manifest.
3. Bind a minimal provider set and deny unnecessary egress.
4. Run it in a disposable general host, then map the additional requirements
   needed for a one-runtime/one-Cell deployment.
5. Threat-model a malicious package, compromised operator, and lost key lease.

### Lab 7: offline conflict

1. Create two authorized local replicas.
2. Apply independent non-conflicting edits and synchronize.
3. Create a conflict in a non-commutative field.
4. Inspect both versions/provenance and resolve explicitly.
5. Explain why wall-clock last-writer-wins is unsafe for regulated truth.

---

## 18. Production readiness checklists

### 18.1 Before first production traffic

- [ ] Workload and compatibility suite pass on the exact client/ORM versions.
- [ ] Data classification and threat model are approved.
- [ ] Deployment boundary (embedded/server/Cell/cluster) is explicit.
- [ ] Dedicated users, mounts, permissions, resource limits, and network policy exist.
- [ ] TLS/mTLS, SCRAM, RLS, roles, and negative authorization tests pass.
- [ ] Every secret class has custody, rotation, revocation, and recovery owners.
- [ ] Capacity includes index, WAL, compaction, relocation, and backup headroom.
- [ ] Full/incremental backup policy and off-site/immutable retention operate.
- [ ] A restore drill passes on target-like infrastructure within RPO/RTO.
- [ ] Metrics, logs, redaction, alerts, and on-call runbooks are exercised.
- [ ] Upgrade and restore-based rollback pass on production-shaped data.
- [ ] Current beta limitations are accepted by the service owner.

### 18.2 Additional cluster gates

- [ ] Three or more voters span the intended independent failure domains.
- [ ] Node/cluster IDs, incarnations, addresses, and certificate fingerprints are inventoried.
- [ ] Metadata quorum and range quorum loss scenarios are rehearsed.
- [ ] Gateways understand route generation, epochs, redirects, and idempotency.
- [ ] Schema fingerprint rollout procedure is tested.
- [ ] Relocation, drain, removal, replacement, and certificate rotation are rehearsed.
- [ ] Under-replication, unavailable ranges, skew, lag, and expiry are alerted.
- [ ] Cluster backup/restore boundaries and coordination are explicitly accepted.

### 18.3 Additional Cell gates

- [ ] The policy definition of a Cell is documented.
- [ ] One runtime/one Cell plus uid, mount, network, and cgroup isolation is enforced.
- [ ] Manifest, exact build, admission bundle, and threshold signatures pass.
- [ ] Key lease acquisition, expiry, rotation, outage, and recovery are rehearsed.
- [ ] Package signers/capabilities/providers/egress are reviewed and receipted.
- [ ] Cell-scoped HA and backup restore without global data concentration are tested.
- [ ] Device offline/revocation limitations and conflict policy are disclosed.
- [ ] Cross-Cell grants are narrow, recipient-encrypted, expiring, and auditable.

### 18.4 Before every upgrade

- [ ] Read release notes, compatibility, security, and on-disk format changes.
- [ ] Pin and verify the exact artifact.
- [ ] Run compatibility and application tests on a restored copy.
- [ ] Verify a fresh backup chain and complete a restore drill.
- [ ] Check free space and maintenance duration.
- [ ] Stage schema/package changes in the documented order.
- [ ] Define abort criteria, traffic drain, rollback owner, and observation window.
- [ ] After upgrade, run integrity, health, RLS, query, stream, and backup checks.

---

## 19. Glossary and documentation map

### 19.1 Glossary

**ABI** — the host/application binary interface used by signed WASM packages.

**Cell** — one policy-selected, fail-closed runtime and data trust boundary.

**Checkpoint** — a durable point that bounds recovery work and backup capture.

**Commit frame** — authenticated/checksummed native replication commit unit.

**Epoch** — monotonically changing range generation that fences stale owners.

**Failure domain** — server, rack, zone, or region whose replicas may fail together.

**Follower/learner/voter** — consensus roles with differing election and quorum rights.

**Manifest** — signed/verified declaration binding identity, artifacts, and policy.

**MVCC** — multi-version concurrency control; versions support transactional snapshots.

**OPFS** — browser Origin Private File System used by the WASM worker.

**PITR** — point-in-time recovery; BicDB replays enabled record-audit history to a timestamp.

**Range** — virtual partition of distributed keys with its own replicas, leader, and epoch.

**RLS** — row-level security, enforcing row access from trusted session identity/context.

**RPO/RTO** — maximum accepted data loss / time to restore service.

**WAL** — write-ahead log used to make commits recoverable before final data placement.

### 19.2 Where to go deeper

| Subject | Primary document |
| --- | --- |
| Server setup/auth/TLS | [`SERVER_MODE.md`](../SERVER_MODE.md) |
| Transaction semantics | [`TRANSACTIONS.md`](../TRANSACTIONS.md) |
| Format/migration lifecycle | [`on-disk-format.md`](on-disk-format.md) |
| Backup and PITR details | [`backup-recovery.md`](backup-recovery.md) |
| HA runbook | [`high-availability.md`](high-availability.md) |
| Streaming replication | [`replication-streaming.md`](replication-streaming.md) |
| Consensus | [`consensus-clustering.md`](consensus-clustering.md) |
| Distributed operations | [`distributed-cluster-operations.md`](distributed-cluster-operations.md) |
| Cluster schema rollout | [`cluster-schema-rollouts.md`](cluster-schema-rollouts.md) |
| Cell security architecture | [`bicdb-cell-application-architecture.md`](bicdb-cell-application-architecture.md) |
| Cell Phase 8 admission | [`cell-runtime-phase8.md`](cell-runtime-phase8.md) |
| Application authoring | [`application-runtime-author-guide.md`](application-runtime-author-guide.md) |
| Application operations | [`application-runtime-operator-guide.md`](application-runtime-operator-guide.md) |
| RLS | [`row-level-security.md`](row-level-security.md) |
| Shared-pool end-user identities | [`transaction-delegation.md`](transaction-delegation.md) |
| Online login and identity administration | [`login-operator-api.md`](login-operator-api.md) |
| Trigram substring indexes | [`trigram-indexes.md`](trigram-indexes.md) |
| Security baseline | [`security.md`](security.md) |
| Production hardening | [`production-security-hardening.md`](production-security-hardening.md) |
| Observability | [`production-observability.md`](production-observability.md) |
| Resource governance | [`resource-governance.md`](resource-governance.md) |
| Browser client | [`../web/bicdb-client/README.md`](../web/bicdb-client/README.md) |
| Browser sync | [`browser-sync.md`](browser-sync.md) |
| TUI | [`tui.md`](tui.md) |
| Full-text lifecycle | [`full-text-build-lifecycle.md`](full-text-build-lifecycle.md) |
| Index maintenance | [`index-maintenance.md`](index-maintenance.md) |
| Vector indexes | [`../VECTOR_INDEXES.md`](../VECTOR_INDEXES.md) |
| Spatial | [`SPATIAL.md`](SPATIAL.md) |
| Graph projections | [`../GRAPH_PROJECTIONS.md`](../GRAPH_PROJECTIONS.md) |
| Streams/broker | [`../STREAM_BROKER.md`](../STREAM_BROKER.md) |

The most important owner habit is simple: treat every success claim as an
invariant to test. A running process is not necessarily ready; a replica is not
a backup; an authenticated operator is not automatically a data reader; a
signed package is not automatically least-privileged; and a completed command
is not a completed change until the desired after-state has been verified.
