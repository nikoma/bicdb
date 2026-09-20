# BicDB Roadmap

## v0.1

- [ ] Final greenfield production certification gate (#69): aggregate
      prerequisite evidence is present locally for #42-#48, but the production
      claim remains blocked until a fresh 100GB certification bundle proves
      every published SLO. Product-specific certification moved to the
      integration repository.
- [x] Rust workspace with `bicdb-core`, `bicdb-cli`, and `bicdb-bench`.
- [x] Append-only local segment storage.
- [x] Checksummed frames and trailing-corruption recovery.
- [x] Collections and key-value records.
- [x] JSON metadata, payloads, timestamps.
- [x] Vector embeddings and exact search.
- [x] Time-series scans and wearable benchmark.
- [x] Vectorized query execution module for numeric summaries.
- [x] Durable local sync-log abstraction.
- [x] CLI benchmark commands.
- [x] Stable SIMD-backed vector math for exact search.
- [x] Optional zstd frame compression.
- [x] Optional mmap recovery reads.
- [x] CSV/JSON benchmark exports.
- [x] redb/fjall embedded insert baselines.
- [x] sqlite-vec, LanceDB, and Qdrant baseline slots with explicit skip status.
- [x] Profiling/flamegraph instructions.
- [x] First-class local EventStream with subscribers, queue mode, projections,
      audit events, snapshots, and local event export/import.
- [x] Local SQL layer over BicDB collections using `sqlparser-rs`.
- [x] SQL CLI with table, JSON, CSV, and simple interactive output.
- [x] Minimal PostgreSQL wire server for simple local `psql` queries.
- [x] PostgreSQL 18.4 compatibility scorecard with executable pgwire, SQL,
      catalog, JSONB, transaction, vector, and unsupported-feature checks plus
      JSON/CSV export.
- [x] Practical virtual `pg_catalog` rows for common database, namespace, class,
      attribute, type, index, and constraint introspection.
- [x] Hardened single-node server mode with `serve`/`serve-pg`, shared database
      runtime, concurrent connections, connection IDs, lifecycle logs, local
      password auth, server status tables, request/query/result limits,
      background flush/checkpoint loops, graceful shutdown marker, and server
      benchmark.
- [x] SQL benchmark comparing SQL execution to direct Rust APIs.
- [x] Sync mesh with persisted node identity, event envelopes, `.syncbundle`
      export/import, idempotent import, LWW convergence, and status APIs.
- [x] Repo-owned `bicdb-sync` crate with an app-independent sync coordinator,
      endpoint trait, filesystem endpoint, per-client checkpoints, and offline
      two-client convergence coverage.
- [x] Sync benchmark for export, import, merge, and conflict resolution speed.
- [x] Encrypted full/incremental backup create, verify, and restore.
- [x] Optional encrypted live storage with Argon2id passphrase keys, raw-key
      support, encrypted segment frames, and strict integrity verification.
- [x] Optional encrypted sync bundles.
- [x] Key-rotation foundation with stored key versions and rotation planning.
- [x] Optional `bicdb-analytics` crate with Arrow `RecordBatch` export.
- [x] Rebuildable columnar Arrow sidecars for wearable/time-series metadata.
- [x] DataFusion-backed analytics queries over BicDB collections.
- [x] Analytics CLI for SQL queries, sidecar rebuild, and sidecar verify.
- [x] Analytics benchmark comparing direct Rust, `query_exec`, and DataFusion.
- [x] DataFusion analytics context caches decoded sidecar `RecordBatch` values
      and avoids canonical scans when a readable sidecar exists.
- [x] First-class AI memory subsystem with semantic, episodic, procedural,
      preference, fact, goal, task, conversation, and observation memories.
- [x] Configurable memory recall ranking over similarity, importance, recency,
      and confidence.
- [x] Memory lifecycle operations for reinforce, decay, forget, expire, and
      summarize, integrated with EventStream.
- [x] Conversation replay, agent workspace snapshots, user timelines, and
      domain-neutral agent example.
- [x] Memory benchmark for insert throughput, recall latency, ranking speed,
      timeline generation, and workspace load time.
- [x] Local transaction API with durable transaction IDs, `TxBegin`/`TxWrite`/
      `TxCommit`/`TxAbort` log records, rollback, committed snapshots, and
      recovery that ignores pending/aborted transactions.
- [x] MVCC-style in-memory record versions for committed snapshots.
- [x] SQL `BEGIN`/`COMMIT`/`ROLLBACK` wired to the core transaction log.
- [x] Pgwire transaction-aware `ReadyForQuery` status and prepared statements
      inside SQL transactions.
- [x] Transaction benchmark for single insert transactions, batch transactions,
      rollback throughput, recovery time, and snapshot scan overhead.
- [x] Durable rebuildable secondary index definitions with equality, range,
      timestamp, composite-prefix, and metadata-path lookup support.
- [x] Cost-estimated local SQL planner with `EXPLAIN`, primary-key lookup,
      index-backed filters, indexed timestamp ordering, estimated rows/cost,
      and full-scan fallback.
- [x] Bounded local SQL support for inner joins, basic `GROUP BY` aggregates,
      `IN` subqueries, and scalar subqueries.
- [x] Index benchmark comparing full scans to index scans and reporting build
      time, index size, and write overhead.
- [x] Optional persistent HNSW vector index with cosine, dot, and L2 metrics.
- [x] ANN vector search API, CLI index build/verify/rebuild commands, SQL
      opt-in via `SET bicdb.vector_search = 'ann'`, and recall benchmark export.
- [x] Rebuildable graph projections over collections and event streams.
- [x] Graph traversal APIs for neighbors, edges, paths, and labeled traversals.
- [x] Graph CLI build/rebuild/verify/query commands, SQL graph views, a generic
      graph demo, and graph benchmark.
- [x] Crash-safe compaction checkpoints with live-record rewrites,
      transaction/event/sync log checkpointing, checkpoint metrics, CLI policy
      knobs, and database disk-amplification reporting.
- [x] BicDB Spatial v0.1 vertical slice: point geometry storage, rebuildable
      R-tree spatial indexes, nearest and within-radius APIs/CLI, focused SQL
      spatial predicates, hybrid spatial-filter plus exact vector ordering,
      bounded OSM PBF road-graph import, shortest-path routing, route
      optimization heuristics, spatial audit events through the existing
      EventStream/sync export path, deterministic benchmarks, and integration
      tests for reopen recovery.
- [x] Spatial v0.1 limitations documented without a full PostGIS compatibility
      claim: no map rendering, no SRID/reprojection/geography model, point-only
      nearest/radius lookup, bounded road-graph OSM extraction only, no turn
      restrictions or complete OSM model import, route optimization as a
      heuristic rather than exact TSP, and derived spatial/index/routing state
      recoverable from canonical BicDB records.

## v0.2

- Atomic grouping for data frames plus sync-log/events/sync-state updates.
- Better collection catalog recovery.
- Fuzz tests for frame parsing and recovery.
- Fuzz tests for sync bundle and backup parsing.
- Optional sqlite-vec vector benchmark feature.
- Optional LanceDB vector benchmark feature.
- Qdrant benchmark harness for a configured running service.
- Benchmark matrix automation for insert, vector, and wearable workloads.
- SQL benchmark matrix and regression thresholds.
- Transaction benchmark matrix and regression thresholds.
- Index benchmark matrix and regression thresholds.
- Server benchmark matrix and regression thresholds.
- Transactional DDL and stronger read-your-own-write SQL semantics.
- Transactional grouping for sync-log and event-log side effects.
- Incremental index-key maintenance instead of collection-level index rebuilds
  on each committed mutation batch.
- Filtered ANN search and planner integration for vector queries with metadata
  predicates.
- Incremental HNSW graph maintenance that avoids collection-level rebuilds after
  vector upserts.
- Release-mode ANN benchmark matrix and recall thresholds for 100k, 1M, and
  larger vector collections.
- Persisted custom graph projection registry for application-supplied
  projections. BicDB has no built-in vertical projection.
- Incremental graph sidecar maintenance instead of full projection rebuilds
  after source writes.
- Graph query language expansion beyond the current NEIGHBORS/PATH/TRAVERSE
  command grammar.
- Broader GIS predicates and geometry types beyond the v0.1 point-focused
  storage/query subset.
- Optional SRID and coordinate reprojection support.
- Richer route geometry export, turn restrictions, OSM access-rule handling,
  and additional OSM metadata extraction.
- Persisted or incrementally maintained spatial sidecars that remain rebuildable
  from canonical records.
- Planner improvements for mixed spatial/vector workloads beyond v0.1 exact
  vector reranking of spatial candidates.
- More pgwire client compatibility smoke tests across DBeaver, DataGrip,
  TablePlus, node-postgres, psycopg, SQLAlchemy, Prisma, and tokio-postgres.
- Remaining PostgreSQL catalog tables, storage-specific columns, and
  client-driven virtual `pg_catalog` fixes.
- Client certificates, SCRAM channel binding, broader PostgreSQL auth policy
  coverage, and true running-query cancellation.
- HTTP/cloud sync endpoints and adapters for Google Drive, S3, OneDrive, and a
  hosted home-base service.
- Tauri background-worker integration around `bicdb-sync` for network
  detection, credential storage, status UI, retries, and conflict prompts.
- Backup chain management, pruning, and scheduled verification.
- Full encrypted segment rewrite for key rotation.
- Incremental analytics sidecar maintenance instead of full rebuilds.
- Release-mode analytics benchmark matrix and regression thresholds.
- Parquet sidecar/export option if profiling shows clear benefit.
- ANN-backed memory recall for very large agent memory sets.
- Incremental memory compaction policies for long-running agents.
- Optional memory redaction/export tooling for user-controlled data portability.

## v0.3

- Vector clocks, CRDTs, and pluggable merge policies for sync conflicts.
- Mobile packaging API review for Android/iOS bindings.
- Broader SQL expressions, advanced join planning, recursive CTEs, CTE-fed
  mutations, and richer subquery forms.
- Cost-based join planning, richer statistics, and optimizer rules.
- Repeatable read and serializable transaction validation.
- Disk-backed vector pages.
- Columnar time-series blocks.
- Compression and rollups for wearable streams.

## Later

- WASM UDF sandboxing.
- Cloud sync protocol.
- Conflict handling with last-write-wins, vector clocks, and CRDT-style merge.
- Broader PostgreSQL compatibility after the local SQL engine is stronger.
- Distributed operation inspired by systems like Neon and CockroachDB, without
  pulling consensus complexity into the embedded core prematurely.
