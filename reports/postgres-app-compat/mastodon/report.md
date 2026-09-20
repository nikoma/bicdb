# Mastodon Compatibility Report

Source: https://github.com/mastodon/mastodon

Revision: `1b82717b833a7b3d8b0f84653bf1628a8ade34c5`

Revision date: 2026-07-01 06:33:34 +0000

Commit subject: `New Crowdin Translations (automated) (#39674)`

Status: migrations pass against BicDB on benchmark-primary. Runtime smoke still needs Redis
available for Mastodon cache/session services; no database blocker remains in
`db:migrate`.

## Attempts

### Attempt 001

Start: 2026-07-01T07:39:00+00:00

Command:

```sh
docker run --rm --network host \
  -e RAILS_ENV=production \
  -e NODE_ENV=production \
  -e LOCAL_DOMAIN=example.test \
  -e DB_HOST=127.0.0.1 \
  -e DB_PORT=5433 \
  -e DB_USER=bicdb \
  -e DB_NAME=bicdb \
  -e DB_SSLMODE=disable \
  -e PREPARED_STATEMENTS=false \
  appcompat-mastodon:1b82717 \
  bundle exec rails db:migrate
```

Result: blocked before migrations.

Error:

```text
PG::FeatureNotSupported: ERROR: unsupported SQL: unsupported constant expression EXISTS (SELECT * FROM pg_proc WHERE proname = 'timestamp_id')
```

Fix: Added `Expr::Exists` handling to the session expression evaluator and the
row-query no-`FROM` projection evaluator. Added regression test
`select_exists_catalog_subquery_without_from_matches_mastodon_function_probe`.

Replay: attempt 002 completed this blocker.

### Attempt 002

Start: 2026-07-01T07:43:57+00:00

Result: passed the `SELECT EXISTS (...)` function probe, then blocked during
Rails schema loading.

Error:

```text
PG::SyntaxError: ERROR: invalid SQL: sql parser error: Expected: end of statement, found: SCHEMA at Line: 1, Column: 42
```

Exact SQL:

```sql
CREATE EXTENSION IF NOT EXISTS "plpgsql" SCHEMA pg_catalog
```

Fix: Added raw `CREATE EXTENSION` handling for `SCHEMA`, `WITH SCHEMA`,
`VERSION`, and `CASCADE` options, routing into the existing extension metadata
store. The built-in `plpgsql` path remains idempotent with `IF NOT EXISTS`.

Replay: attempt 003 completed this blocker.

### Attempt 003

Start: 2026-07-01T07:47:42+00:00

Result: passed extension setup and progressed into table/index schema loading,
then blocked on a complex unique expression index.

Error:

```text
PG::FeatureNotSupported: ERROR: unsupported SQL: unsupported field expression COALESCE(lower((domain)::TEXT), ''::TEXT)
```

Exact SQL:

```sql
CREATE UNIQUE INDEX "index_accounts_on_username_and_domain_lower"
ON "accounts" (lower((username)::text), COALESCE(lower((domain)::text), ''::text))
```

Fix: Allow unreducible expression indexes, including unique ones, to be stored as
metadata-only indexes while preserving `pg_index.indisunique`,
`pg_index.indkey = '0'`, and `pg_get_indexdef`. Existing reducible expression
indexes still enforce uniqueness.

Semantic gap: uniqueness for unreducible expression indexes is catalog-visible
but not yet enforced.

Replay: attempt 004 completed this blocker.

### Attempt 004

Start: 2026-07-01T07:50:46+00:00

Result: progressed through schema tables/indexes and blocked on materialized
view creation.

Error:

```text
PG::FeatureNotSupported: ERROR: unsupported SQL: MATERIALIZED VIEW is not supported; BicDB SQL views are logical only
```

Fix: Store materialized views using the existing view metadata model with a new
`materialized` flag. `CREATE MATERIALIZED VIEW` now returns the PostgreSQL
command tag and catalog rows expose `pg_class.relkind = 'm'`. Execution remains
logical view execution; physical refresh/storage semantics are not implemented
yet.

Replay: attempt 005 completed this blocker.

### Attempt 005

Start: 2026-07-01T07:54:29+00:00

Result: materialized view creation succeeded, then Rails blocked while adding
indexes to `account_summaries`.

Error:

```text
PG::UndefinedTable: ERROR: collection not found: account_summaries
```

Exact SQL from BicDB log:

```sql
CREATE INDEX "idx_on_account_id_language_sensitive_250461e1eb"
ON "account_summaries" ("account_id", "language", "sensitive")
```

Fix: Added an index list to `ViewSchema`. `CREATE INDEX` on materialized views
now stores a metadata-only index and regular logical views still reject indexing.
Catalog paths now include those indexes in `pg_class`, `pg_index`, `pg_indexes`,
and `pg_get_indexdef`; `pg_class.relhasindex` is set when a materialized view has
metadata indexes.

Local and benchmark-primary validation:

```text
cargo test -p bicdb-sql unsupported_view_behaviors_return_clear_errors
cargo check -p bicdb-sql
cargo build --release -p bicdb-cli
```

Replay: attempt 006 completed this blocker.

### Attempt 006

Start: 2026-07-01T08:00:04+00:00

Command: same `bundle exec rails db:migrate` container command as attempt 001,
against a fresh BicDB data directory on benchmark-primary.

Result: success. The migration command exited `0`.

BicDB SQL failure log: one expected pre-migration application probe failed with
`42P01` for `SELECT FROM "settings" ...` before the schema existed. No
subsequent database failures were logged during migration.

Rails readback:

```text
SELECT COUNT(*) FROM schema_migrations => 594
SELECT COUNT(*) FROM pg_class WHERE relkind = 'm' => 3
SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'account_summaries' => 2
```

Non-database runtime note: Rails emitted `Redis::CannotConnectError` while
reading/writing cache entries because no Redis server was running on benchmark-primary for
this migration command. The migration still completed successfully.
