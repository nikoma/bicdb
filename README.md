# BicDB

> **New operator or contributor?** Start with the
> [BicDB Owner's Guide](docs/owners-guide.md), a book-length tour of the
> architecture, storage engine, CLI, server protocols, Cells, clustering,
> security, backup/recovery, and day-two operations.

**One database—from an offline browser tab to a replicated server cluster.**

BicDB is a Rust database for builders who want fewer
moving parts. Embed it like SQLite, connect with PostgreSQL or Redis clients,
run it in a browser over OPFS, or operate it as a server. SQL, full-text and
vector search, spatial and graph operations, analytics, durable streams, sync,
and a sandboxed application runtime all share one transactional engine.

> **Current release: 1.0.438-beta.** BicDB powers
> [Weowobo](https://weowobo.com), a web-search project indexing
> **2.1 billion documents**, and a scientific corpus of **41 million PubMed
> and other articles**. On a retained Common Crawl
> benchmark, its packed full-text index is smaller than Tantivy's output and
> builds the same single-segment artifact 2.4x faster. BicDB is still beta;
> read [Current limitations](#current-limitations) before making it the only
> copy of critical data.

> **License: Apache 2.0 + three exceptions.** See the [BicDB License](LICENSE).
> Commercial applications and application SaaS are permitted. Database-product
> sales, general-purpose database hosting and console white-labeling require
> separate commercial authorization. [Scope and details](#license)

*BicDB officially means “Because I Can Database.”*

## Why try BicDB?

BicDB is useful when your architecture has accumulated a database, search
engine, cache, queue, vector store, sync service, and separate edge database—and
you would rather operate one coherent system.

- **One engine from browser to cluster.** Use the embedded Rust API, WASM +
  OPFS, a standalone server, or the distributed range/replica machinery without
  changing the data model.
- **Search is part of the database.** Native BM25/BM25F full-text search,
  phrases, weighted fields, Snowball stemming, filter pushdown, Block-Max WAND,
  packed immutable index segments, bounded-memory parallel index builds —
  optionally **progressive**, answering queries while the build runs — and
  vector search eliminate a separate search service and its synchronization
  lag.
- **Use the clients you already have.** BicDB directly speaks the PostgreSQL
  wire protocol and RESP2. Its public durable-stream API supports separate
  AMQP 0-9-1, MQTT 3.1.1, Kafka, HTTP, and gRPC adapters without coupling those
  integrations to the engine.
- **Row-level security is a first-class boundary.** PostgreSQL-style RLS,
  roles, grants, trusted tenant context, and fail-closed native collection
  policies keep tenant isolation in the database instead of depending on every
  application query to remember the right filter. RLS is one of BicDB's most
  heavily used production features.
- **Offline-first by construction.** No daemon or network is required. Durable
  sync logs, browser OPFS support, encrypted storage, and explicit conflict
  handling keep applications useful through unreliable connectivity.
- **Durability is a product feature.** Checksummed storage, WAL recovery,
  snapshot transactions, backup chains, point-in-time recovery, restore drills,
  integrity tooling, replication, and HA are built in.
- **More than rows.** SQL/JSON, vectors, full text, location intelligence
  (H3 cells, vector tiles, isochrones, geofencing), incrementally maintained
  OLAP cubes, graph projections, Arrow/DataFusion analytics, event streams,
  queues, application packages, websites, and sandboxed WASM extensions share
  one transactional substrate.
- **Local-first replication.** BicDB Mesh synchronizes authorized peers over
  whatever moves bytes — LAN discovery today; the cloud is just another peer —
  with per-origin version vectors, transitive relay, ed25519-signed frames,
  and surfaced, reviewable conflicts.
- **Evidence over compatibility slogans.** PostgreSQL behavior is
  differential-tested against PostgreSQL 18.4, real drivers run in the client
  gauntlet, and crash/recovery boundaries are exercised directly.

### The practical advantages

| Advantage | What it means for your project |
| --- | --- |
| One transactional source of truth | Rows, search indexes, vectors, streams, graphs, and analytics do not drift across services. |
| Start embedded, grow into a server | Prototype without infrastructure, then keep the same engine and data model as deployment grows. |
| Familiar clients and SQL | Use `psql`, PostgreSQL drivers and ORMs, Redis clients, or the native Rust API instead of adopting a proprietary query language. |
| Tenant isolation at the data boundary | PostgreSQL-style RLS and native collection policies enforce visibility and writes using trusted user, tenant, and role context—even when an application query forgets a tenant filter. |
| Works offline and at the edge | Browser OPFS, local storage, encryption, sync logs, and explicit conflict handling keep applications useful without a reliable network. |
| Search without an external pipeline | Full-text and vector indexes participate in the same backup, security, recovery, and transaction boundaries as source data. |
| Operations are built in | WAL recovery, checksums, backup/PITR, integrity checks, replication, HA, metrics, and restore drills are part of the database. |
| Open extension points | Public controller/provider APIs and capability-scoped WASM packages let you extend BicDB without forking its engine. |
| Licensing | Source-available community terms permit commercial applications and reserve database-product/service uses. |

### Good first projects

- An offline-first desktop, mobile, field, or browser application.
- A multi-tenant SaaS product that needs database-enforced row isolation.
- A PostgreSQL-shaped service that also needs native search or vectors.
- A local-AI application that must keep private data and embeddings nearby.
- A durable cache, event stream, or queue that should survive restarts.
- An edge deployment that may later need replication, HA, or centralized
  operations.

If you only need a tiny key-value store, BicDB may be more database than you
need. If you need several of the capabilities above, the consolidation is the
point.

## One engine, your protocols

| You have | Use | Documentation |
| --- | --- | --- |
| Rust | In-process `BicDb::open()` API | [Embedded quickstart](#embedded-rust) |
| `psql`, an ORM, or a BI tool | PostgreSQL wire protocol with extended queries, SCRAM, and TLS | [Server mode](SERVER_MODE.md) |
| Redis clients | RESP2 cache with durable keys and TTLs | [Redis cache](docs/redis-cache.md) |
| RabbitMQ, MQTT, or Kafka clients | Optional protocol adapters over one durable event log | Build against the public broker API |
| SQL producers and consumers | Consumer groups, DLQs, SQL broker functions, LISTEN/NOTIFY | [Stream broker](STREAM_BROKER.md) |
| A browser | `@bicdb/client` in a Web Worker over OPFS | [Browser client](web/bicdb-client/README.md) |
| A terminal | CLI and Ratatui TUI | [TUI](docs/tui.md) |
| A signed application package | Capability-mediated ABI v2 application runtime and providers | [Author guide](docs/application-runtime-author-guide.md) |
| A fail-closed single security cell | Capability-reduced `bicdb-cell` runtime with Cell-native apps, fleet lifecycle, cell-scoped HA/recovery, hardware-bound device replicas, recipient-encrypted object grants, and exact-build hardened-fleet evidence verification (Phase 8; regulated-data admission requires a complete threshold-signed bundle) | [Cell/admission operator contract](docs/cell-runtime-phase8.md) |
| A custom fleet/controller | Public placement, host lifecycle, HA, sync, package, receipt, and verifier APIs | [Controller APIs](docs/public-controller-apis.md) |
| A WASM extension | Sandboxed functions, routes, websites, and event handlers | [Extensions](docs/extensions.md) |

## Quickstart

### Embedded Rust

```rust
use bicdb_core::{BicDb, CompressionConfig, DbConfig, JsonFilter, Record};
use serde_json::json;

let config = DbConfig::default()
    .with_fsync(true)
    .with_compression(CompressionConfig::zstd(3, 4096));
let mut db = BicDb::open_with_config("./testdb", config)?;
db.create_collection("patients")?;

db.insert(
    "patients",
    Record::new("patient-1")
        .with_vector(vec![0.2, 0.4, 0.8])
        .with_metadata(json!({"clinic": "rural-7"}))
        .with_timestamp(1710000000),
)?;

let filter = JsonFilter::new().eq("clinic", "rural-7");
let matches = db.search_vector("patients", &[0.2, 0.4, 0.8], 10, Some(&filter))?;
db.flush()?;
# Ok::<(), bicdb_core::BicDbError>(())
```

Runnable examples are in
[crates/bicdb-core/examples](crates/bicdb-core/examples/).

### PostgreSQL-compatible server

General pgwire databases share one process and failure domain. Database
separation is not a Cell boundary. For hard Cell isolation, deploy one
`bicdb-cell` runtime per process with dedicated mounts, uid, network policy,
and cgroup resource limits; see the [Cell architecture](docs/bicdb-cell-application-architecture.md).

```bash
cargo run --release -p bicdb-cli -- serve ./testdb --host 127.0.0.1 --port 5433
psql -h 127.0.0.1 -p 5433 -U bicdb -d bicdb
```

The automated client matrix covers `psql`, tokio-postgres, SQLx,
node-postgres, psycopg, SQLAlchemy, libpq, and JDBC. SQL and type behavior are
checked against a PostgreSQL 18.4 oracle. See the generated
[compatibility report](POSTGRES_COMPATIBILITY.md) and
[Nightmare Gauntlet](NIGHTMARE_GAUNTLET.md).

### Native full-text search

```sql
CREATE TABLE articles (
    id BIGINT PRIMARY KEY,
    title TEXT,
    abstract TEXT
);

CREATE INDEX articles_fts ON articles USING GIN (
    to_tsvector('english', coalesce(title, '') || ' ' || coalesce(abstract, ''))
);

SELECT id, title,
       ts_rank(
           to_tsvector('english', coalesce(title, '') || ' ' || coalesce(abstract, '')),
           plainto_tsquery('english', 'gene therapy')
       ) AS rank
FROM articles
WHERE to_tsvector(
          'english',
          coalesce(title, '') || ' ' || coalesce(abstract, '')
      ) @@ plainto_tsquery('english', 'gene therapy')
ORDER BY rank DESC
LIMIT 20;
```

BicDB stores the compact term dictionary, compressed postings, positions,
document statistics, and source rows together. Since 1.0.224 the index lives
in **packed immutable segments** — columnar slim blocks, front-coded term
directories, impact sidecars — instead of millions of keyed rows; on the
retained Common Crawl corpus this made the index 14x smaller and the build
10x faster while reproducing byte-identical results. Index creation is
resumable, bounded-memory, and parallel across term ranges; completed
generations publish atomically while concurrent readers remain pinned to the
previous generation. With `BICDB_FTS_PROGRESSIVE=1` a build publishes
searchable sub-segments as it ingests, so a day-long index answers queries
after its first interval. Read
[the lean-storage campaign](docs/fts-lean-storage-campaign.md) and
[FTS generation format v3](docs/fts-generation-v3.md) for formats, evidence,
and migration details. The
[full-text build lifecycle](docs/full-text-build-lifecycle.md) defines
authoritative status, idempotent crash reconciliation, fleet discovery, and
the operator/API contract for multi-day builds.

### Durable Redis cache

```bash
cargo run --release -p bicdb-cli -- cache-serve ./cachedb --port 6379
redis-cli SET greeting "hello" EX 300
```

Keys and TTLs are BicDB records and survive restarts through the WAL. A
SQL-bound HotView supports cache-through queries.

### Broker integrations

The public broker API provides the durable event log, consumer groups, DLQs,
retry/redrive, schemas, and transactional primitives needed by protocol
adapters. AMQP, MQTT, Kafka, and product-specific broker frontends are separate
integrations that consume those APIs; they are not dependencies of BicDB.

### Browser / WASM

```bash
cargo build --release -p bicdb-wasm --target wasm32-wasip1
```

[`@bicdb/client`](web/bicdb-client/README.md) runs BicDB in a dedicated Web
Worker using an OPFS sync-access-handle pool. Its main-thread API provides
`open`, `query`, `stats`, `compact`, and `close`; Web Locks enforce single
ownership, and the sync server supports per-user working sets.

## Full-text search at web scale

The project operates BicDB search in production for
[Weowobo](https://weowobo.com) at **2.1 billion documents**, alongside a
41-million-article scientific corpus, without Elasticsearch, OpenSearch, or an
external synchronization pipeline. On a retained Common Crawl benchmark (123k
documents, 0.97 GiB of text, frozen query set), the packed index measures
**smaller than Tantivy 0.22's output** while carrying strictly more
recomputable state, builds the single-segment artifact **2.4x faster than
Tantivy reaches the same shape**, and answers the worst extreme-term query in
**3.9 ms** — every step gated on byte-identical results against the previous
format.

The scale-oriented FTS path includes:

- packed immutable segments: columnar bit-packed postings, memory-mapped
  front-coded dictionaries, quantized pruning bounds, and impact sidecars
  elided wherever they are recomputable;
- BM25 and BM25F ranking with weighted fields;
- Block-Max WAND/MaxScore, impact ordering, shallow secondary seeks, and exact
  top-k results;
- phrase and Boolean posting intersections with SIMD-assisted probes;
- filter pushdown into ranked retrieval;
- score-first decoding that never touches position bytes until a candidate
  needs them;
- bounded caches, prefetch, concurrent read sessions, adaptive per-query
  parallelism under a global admission budget, and query budgets with
  cancellation;
- resumable, checksummed, parallel index construction with atomic generation
  promotion — and optional **progressive builds** that serve queries from
  published sub-segments while ingestion continues;
- a bounded transactional tail so fresh inserts, updates, and deletes remain
  visible before a fold.

The search implementation is database-native: backups, snapshots, access
controls, recovery, and application transactions cover the data and its search
structures together.

## Capabilities

### Storage, transactions, and recovery

- Append-only checksummed segment storage plus a first-party page store with
  buffer management, slotted pages, overflow chains, B+ trees, MVCC, WAL,
  fuzzy checkpoints, vacuum, recovery, and integrity instrumentation.
- ACID transactions with snapshot reads, savepoints, per-record conflict
  detection, concurrent commit, and an MVCC visibility watermark
  ([transactions](TRANSACTIONS.md)).
- XChaCha20-Poly1305 storage encryption with Argon2id key derivation, plus
  field-level envelopes and blind indexes for sensitive data
  ([security](docs/security.md)).
- Fail-closed tenant isolation through native collection policies, roles, and
  PostgreSQL-style row-level security. RLS runs in the shared SQL execution
  path for CLI, pgwire simple queries, and prepared statements
  ([security and RLS](docs/security.md#postgresql-rls-catalog-metadata)).
- Full and incremental backup chains, verification, PITR, and executable
  restore drills ([backup and recovery](docs/backup-recovery.md)).
- Large values, governed temporary space, spillable sorting, resource
  admission, metrics, and maintenance tooling.
- Lean storage: autovacuum and SQL `VACUUM`, filesystem hole-punching, a
  storage space report, automatic index folding, WAL-archive pruning, and
  opt-in zstd value compression
  ([lean storage](docs/lean-storage.md)).

### SQL and PostgreSQL compatibility

BicDB implements a broad PostgreSQL-shaped SQL surface: schemas, catalogs,
constraints, indexes, sequences, views, CTEs, window functions, routines,
JSONB/jsonpath, XML/XPath, arrays, composites, domains, ranges, multiranges,
network and geometric types, row-level security, full-text types and operators,
and PostgreSQL wire formats. The
[generated compatibility report](POSTGRES_COMPATIBILITY.md) defines the tested
boundary; it does not claim complete PostgreSQL implementation parity.

### Search, AI, spatial, graph, and analytics

- Exact SIMD-backed cosine, dot-product, and L2 vector search, plus persistent
  HNSW ANN indexes with explicit recall/latency settings
  ([vector indexes](VECTOR_INDEXES.md)).
- First-class semantic, episodic, procedural, preference, fact, goal, task,
  conversation, and observation memory with offline ONNX embeddings.
- Location intelligence: WKB/EWKB and Multi* geometries, DE-9IM predicates
  with geodesic math, R-tree joins and packed Hilbert-ordered spatial
  indexes, H3 cells, Mapbox vector tiles, an FTS-backed geocoder, travel
  times and isochrones, rasters, a geofencing engine, temporal `asof`
  queries, and optional OSM import — fuzz-tested against a PostGIS oracle
  ([spatial](docs/SPATIAL.md), [geo campaign](docs/geo-worldclass.md)).
- Incrementally maintained OLAP cubes: `CREATE CUBE` with declared
  dimensions, hierarchies, and measures — including non-retractable sketch
  measures and identity-keyed quantiles — kept current from the event
  stream rather than rebuilt in batches ([cubes](docs/olap-cubes.md)).
- Graph projections over collections
  ([graph projections](GRAPH_PROJECTIONS.md)).
- Arrow `RecordBatch` export, derived columnar sidecars, and DataFusion SQL.

### Sync, replication, HA, and clustering

- Durable pending-sync operations, portable sync bundles, HTTP synchronization,
  and browser working-set synchronization.
- Streaming replication, read-only standbys, backup-aware HA, and explicit
  promotion workflows.
- BicDB Mesh, local-first peer-to-peer replication: per-origin version
  vectors with delta exchange and transitive relay, duplex sessions over any
  byte pipe, LAN peer discovery with automatic sync, ed25519-signed frames
  with trust-on-first-use pinning, NTP-informed conflict timing evidence,
  causal-first resolution, and SQL-surfaced conflict review
  ([mesh](docs/bicdb-mesh.md)).
- Automatic cluster membership, virtual ranges, replica placement, online
  relocation with epoch fencing, request routing, failure repair, distributed
  SQL/FTS top-k, schema rollout, anti-entropy, and resource governance.
- Raft-style quorum metadata and consensus coordination with mTLS-bound node
  identities.

The distributed implementation has extensive deterministic tests, but its 1,
5, and 20 TB production-hardware certification matrix remains open. See the
[automatic sharding roadmap](docs/automatic-distributed-sharding-todo.md).

### Application runtime and extensions

Signed ABI v2 application packages declare capabilities instead of receiving
ambient host access. The runtime supplies database, HTTP, Redis, email, gRPC,
tokenizer, embeddings, LLM, secrets, schedules, queues, and
operator-controlled blob providers. Sandboxed WASM extensions can add SQL
functions, HTTP routes, versioned websites, and durable database/queue event
handlers.

## Operations and CLI

```text
bicdb app | init | store | inspect | verify | check | integrity | security
      compact | sync | backup | migrate | analytics
      sql | metrics | health | doctor | cluster | serve | sync-serve
      cache-serve | serve-pg | vector | model | memory | index | spatial
      graph | user | server | ha | replication | consensus
```

The CLI includes JSON output for automation and a Ratatui TUI for interactive
operation. Production guidance covers hardening, observability, backups,
restore drills, replication, HA, cluster operations, and incident handling.

Developer `bench` and `compat` commands are optional:

```bash
cargo build --release -p bicdb-cli --features bench
cargo test -p bicdb-cli --features bench --test paged_recovery_cli
```

The default CLI excludes `bicdb-bench` and its benchmark-only dependencies.
Use `--features bench-comparison-engines` to additionally enable the external
`redb`/`fjall` comparison baselines and `bench compare`. Engine, server, storage,
and application commands are available in the default build.

## Testing philosophy

The repository contains more than **2,700 Rust tests**, including byte-level
corruption and crash-boundary recovery tests, kill/restart testing,
multi-threaded transaction stress, raw-wire protocol cases, real-client
gauntlets across five language ecosystems, PostgreSQL 18.4 differential tests,
cross-storage-mode conformance, deterministic cluster failures, and FTS
correctness/performance regressions. Compatibility claims are generated from
checked-in evidence.

## Current limitations

- BicDB is beta software. Rehearse backup/restore and failure procedures before
  making it the only copy of data you cannot recreate.
- PostgreSQL compatibility is broad, not exhaustive. The generated report and
  client matrix are the source of truth; Prisma remains outside the passing
  automated matrix.
- `READ COMMITTED` and snapshot transaction behavior are implemented;
  serializable isolation and general distributed transactions are not.
- The embedded-memory mode rebuilds live data and derived structures into RAM
  at startup. The server-paged path is increasingly disk-resident, but
  end-to-end bounded memory and multi-terabyte production qualification remain
  incomplete.
- Automatic sharding and repair machinery is implemented, but the published
  production-scale saturation, node-loss, rebalance, and restore certification
  runs are not complete.
- Large transactional production certification still requires a fresh 100 GB
  evidence bundle.

## Development

```bash
rustup toolchain install 1.96.0
cargo build --workspace
cargo test --workspace
```

The workspace contains the BicDB engine, protocol/runtime crates, and extension
examples. Product and vertical integrations consume the public APIs from
separate repositories. Rust 1.96 is pinned in `rust-toolchain.toml`.

## Security

Report vulnerabilities privately — see [SECURITY.md](SECURITY.md). Resolved
findings and their remediation are recorded in
[docs/security-audit.md](docs/security-audit.md).

## License

**Apache 2.0 + three exceptions:** [BicDB License 1.0](LICENSE)
(`LicenseRef-BicDB-1.0`), source-available. The Apache text is included verbatim.
These terms are operative now. This is not unmodified Apache-2.0 or an
OSI-approved open-source license.

Commercial applications, unlimited multi-tenant application SaaS, embedded
application storage, consulting and support are permitted. General-purpose
database hosting, commercial database-product sales and white-label removal of
included-console identification require separate written commercial authorization.
Independent applications need no BicDB logo or powered-by badge.

See [LICENSE-SCOPE.md](LICENSE-SCOPE.md), the [guide and 18 scenarios](docs/licensing-faq.md),
and [commercial licensing](docs/commercial-licensing.md). Separable SDKs, clients,
connectors and third-party components retain their own licenses. Earlier Apache
grants remain effective; no new Apache alternative is offered for newly covered
rights. [NOTICE](NOTICE), [third-party notices](THIRD_PARTY_NOTICES.md) and the
[transition record](docs/licensing-transition.md) explain that distinction.
