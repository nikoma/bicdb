# BicDB

**One database. Not six services.**

BicDB is a database written in Rust. SQL, a durable cache, queues, full-text
search, vector search, spatial queries and analytics share one engine. Connect
with PostgreSQL and Redis clients, run it as a server, or use it from the
command line against a local data directory.

**In production.** BicDB is production software, not an experiment. It runs a
search cluster with 2.1 billion records, health-record backends and field apps.

This crate installs the `bicdb` command-line program: the database engine,
its servers and its operator tools in one binary.

```sh
cargo install bicdb --locked
bicdb --version
```

`--locked` builds with the dependency versions BicDB was tested with.

## What it does

- **SQL over the PostgreSQL wire protocol.** `bicdb serve` speaks the
  PostgreSQL protocol, so `psql`, libpq, JDBC, tokio-postgres, SQLx,
  node-postgres, psycopg and SQLAlchemy connect unchanged. Transactions,
  schemas, constraints, indexes, views, CTEs, window functions, routines,
  JSONB/jsonpath, arrays, ranges and row-level security are implemented and
  checked against PostgreSQL 18; the
  [compatibility report](https://github.com/nikoma/bicdb/blob/main/POSTGRES_COMPATIBILITY.md)
  defines the tested boundary. It is broad, not complete PostgreSQL parity.
- **Redis-compatible cache.** `bicdb cache-serve` speaks RESP2: strings with
  TTLs, hashes, sorted sets, streams and bounded Lua scripts. Keys are durable
  records that survive restarts.
- **Full-text search.** PostgreSQL-style `to_tsvector` / `tsquery` with GIN
  indexes, BM25 ranking and packed, compressed index segments, stored and
  recovered with the rows they index.
- **Vector search.** Exact cosine, dot-product and L2 search and persistent
  HNSW indexes (`ORDER BY embedding <=> '[...]'`).
- **Geospatial.** WKB/EWKB geometries, DE-9IM predicates, geodesic math,
  R-tree spatial indexes, H3 cells, vector tiles and geofencing.
- **Time travel.** Flashback queries (`AS OF`, `VERSIONS BETWEEN`,
  `FOR SYSTEM_TIME`), `FLASHBACK TABLE` and a recycle bin for dropped tables,
  on tables that enable a flashback archive.
- **OLAP cubes.** `CREATE CUBE` with dimensions, hierarchies and measures,
  maintained incrementally as data changes; Arrow export and DataFusion SQL.
- **Live queries.** `SUBSCRIBE SELECT ...` streams a result and then every
  change to it over any PostgreSQL driver.
- **Message broker.** Durable queues and event streams with consumer groups,
  acknowledgements, retries and dead letters, callable from SQL
  (`broker_publish`, `broker_consume`, transactional publish on commit), plus
  connectors to Redis streams and AMQP.
- **Embedded or server.** Every command can open a data directory directly in
  its own process; `bicdb serve` runs it as a shared server. Rust programs can
  embed the engine through the `bicdb-core` crate.
- **Replication, HA and clustering.** Synchronous streaming replication with
  read-only standbys and promotion, full and incremental backups with
  point-in-time recovery, local-first peer-to-peer sync, and cluster
  membership with range placement and relocation. Production-scale cluster
  certification is not finished; see the repository README.
- **Operations.** Storage encryption, roles and row-level security, integrity
  checks, `bicdb doctor`, metrics, JSON output for automation and a terminal UI.

## Quick start

Store two rows and search them; each command opens the database in a new
process:

```sh
bicdb sql ./demo "
  CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(3));
  INSERT INTO notes VALUES
    (1, 'Search works offline', '[1,0,0]'),
    (2, 'Ship fewer services', '[0,1,0]');
  CREATE INDEX notes_search ON notes USING GIN (to_tsvector('english', body));
"

bicdb sql ./demo --csv "
  SELECT id, body FROM notes
  WHERE to_tsvector('english', body) @@ plainto_tsquery('english', 'offline');
"

bicdb sql ./demo --csv "SELECT id FROM notes ORDER BY embedding <=> '[1,0,0]' LIMIT 1;"
```

Serve the same data to PostgreSQL clients:

```sh
bicdb serve ./demo --host 127.0.0.1 --port 5433
psql -h 127.0.0.1 -p 5433 -U bicdb -d bicdb -c 'SELECT * FROM notes ORDER BY id;'
```

This loopback demo has no authentication configured. Read the
[server guide](https://github.com/nikoma/bicdb/blob/main/SERVER_MODE.md) before
exposing a server. Run `bicdb help` for every command.

## Building

`cargo install` compiles BicDB from source; a first build takes a while. It
needs the Rust toolchain (1.96 or newer) and native build tools. On Ubuntu:
`build-essential`, `pkg-config`, `libssl-dev`, `clang`, `libclang-dev` and
`cmake`.

The `bench` feature adds the developer benchmark and PostgreSQL comparison
commands (`cargo install bicdb --locked --features bench`); the default build
leaves them out.

## Status

BicDB is under active development. Read the
[current limitations](https://github.com/nikoma/bicdb#current-limitations) and
rehearse backup and restore before making it the only copy of data you cannot
recreate.

The `bicdb-*` crates this one depends on are BicDB's internal components. They
are published so that `cargo install bicdb` works; their APIs may change in any
release.

## Links

- Repository and full documentation: <https://github.com/nikoma/bicdb>
- Website: <https://bicdb.com>
- Security reports: <https://github.com/nikoma/bicdb/blob/main/SECURITY.md>

## License

BicDB is free and open source software, licensed under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
This package includes the `LICENSE` and `NOTICE` files.
