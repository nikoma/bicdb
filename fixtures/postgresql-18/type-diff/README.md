# PostgreSQL 18 Type Differential Matrix

These 88 fixtures run identical DDL, input, output, cast, typmod, catalog,
storage, planner, schema-lifecycle, and protocol queries against BicDB and the
pinned `postgres:18.4` oracle. Every registered PostgreSQL family implemented
by BicDB is covered by one or more numbered fixtures. PostgreSQL pseudo-types
are tested in valid and invalid declaration positions. `vector` is excluded
because it is a pgvector/BicDB extension, not a stock PostgreSQL 18 type; its
extension parity is covered separately.

Run the matrix from the repository root:

```bash
CARGO_BUILD_JOBS=10 RUST_TEST_THREADS=1 ./scripts/pg18-type-diff.sh
```

The runner uses clean-state fixture shards of 40, 38, and 10 cases, then
validates and aggregates them into JSON and Markdown reports under
`target/postgresql-18-type-diff/`. It exits nonzero on any difference, missing
fixture, duplicate fixture ID, or failed shard. Publish the compact report with:

```bash
node scripts/generate-pg18-compatibility-report.mjs
```
