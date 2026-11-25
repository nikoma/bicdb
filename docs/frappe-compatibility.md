# Frappe Framework compatibility

The [Frappe Framework](https://github.com/frappe/frappe) (the Python web
framework behind ERPNext) runs on BicDB through its stock PostgreSQL support
(`db_type: postgres`, psycopg2, simple query protocol). Nothing in Frappe is
patched. The qualified version is **Frappe v16.60.2** (Python 3.14, Node 24,
frappe-bench 5.31.1); the CI job `.github/workflows/frappe.yml` repeats the
qualification on every pull request that touches the engine.

## What works

- `bench new-site --db-type postgres` against a BicDB cluster with SCRAM
  password authentication: the root login creates the site's role
  `WITH PASSWORD`, its database, grants and ownership, the framework schema is
  restored with `psql`, and every core DocType is installed and migrated
  (about 4 minutes on a debug build).
- The site serves HTTP. `examples/frappe/smoke.py` logs in as Administrator,
  creates, reads, updates, lists (filter, ORDER BY, LIKE), counts and deletes
  ToDo documents, creates a custom DocType through the REST API
  (`CREATE TABLE`), adds a field to it (`ALTER TABLE ADD COLUMN`) and runs CRUD
  on its documents.
- Frappe's own test suite (`bench run-tests`), see the table below.

## Running it

```sh
cargo build -p bicdb --bin bicdb
pip install frappe-bench==5.31.1          # Python 3.14; node 24 + yarn on PATH
BICDB_BIN=target/debug/bicdb REDIS_URL=redis://127.0.0.1:6379 \
  WORK_DIR=/tmp/frappe examples/frappe/verify.sh
```

`verify.sh` starts BicDB (`serve --cluster --require-auth --auth-method
scram-sha-256`, root login `bicdb`), runs `bench init`, `bench new-site`, the
HTTP smoke test and the test slice, and leaves logs and JUnit files in
`WORK_DIR`. Frappe builds `mysqlclient` even for PostgreSQL sites, so the
MariaDB client headers (`libmariadb-dev`) and `pkg-config` are needed, and
`psql` is used to load the framework schema. `bench` refuses to run as root.

To point an existing bench at BicDB by hand:

```sh
bicdb user create bicdb --password secret --path /srv/bicdb --role superuser
bicdb serve /srv/bicdb --cluster --default-database bicdb --port 5433 \
  --require-auth --auth-method scram-sha-256
bench new-site site1.local --db-type postgres --db-host 127.0.0.1 --db-port 5433 \
  --db-root-username bicdb --db-root-password secret
```

Frappe connects the root login to a database named after it, so the cluster's
default database must carry the root login's name (`--default-database bicdb`
for the `bicdb` login).

## Test slice results

Frappe v16.60.2, `bench run-tests --module ...` on a debug build of BicDB
(2026-11-23), next to the same bench against PostgreSQL 17:

| Module | Tests | Passed | Failed | Skipped | PostgreSQL 17 |
|--------|------:|-------:|-------:|--------:|---------------|
| `frappe.tests.test_db` | 87 | 68 | 9 | 10 | 1 failed (`test_estimated_count`, passes on BicDB) |
| `frappe.tests.test_document` | 60 | 59 | 1 | 0 | 2 failed (the same WebView tests) |
| `frappe.tests.test_query_builder` | 53 | 34 | 0 | 19 | same |
| `frappe.tests.test_model_utils` | 3 | 3 | 0 | 0 | same |
| `frappe.tests.test_permissions` | 38 | 38 | 0 | 0 | same |
| `frappe.tests.test_db_query` | 64 | 64 | 0 | 0 | same |
| `frappe.tests.test_naming` | 24 | 24 | 0 | 0 | same |
| `frappe.tests.test_child_table` | 1 | 1 | 0 | 0 | same |
| `frappe.tests.test_defaults` | 7 | 6 | 0 | 1 | same |
| `frappe.tests.test_sequence` | 2 | 2 | 0 | 0 | same |
| `frappe.tests.test_docstatus` | 3 | 3 | 0 | 0 | same |
| **total** | **342** | **302** | **10** | **30** | |

The failures, all listed in `examples/frappe/known-failures.txt`:

- 9 in `TestPostgresSchemaQueryIndependence` (`test_db`): one cause, the
  DROP TABLE limitation below; the class's later tests then run in the
  aborted transaction.
- 1 in `TestDocumentWebView` (`test_document`): "DocType Test WebViewDocType
  not found", the same on PostgreSQL 17 with the same bench.

`verify.sh` fails on any other failure. Skips are Frappe's own (MariaDB-only
tests). `test_db_update` is not in the slice: it fails on PostgreSQL 17 in
this setup too.

## BicDB fixes made for Frappe

Each has a regression test in `crates/bicdb-sql/tests/frappe_compat.rs` or
`crates/bicdb-pgwire/tests/frappe_compat.rs`.

| Area | Before | Now |
|------|--------|-----|
| `information_schema.*.table_catalog` (and the other `*_catalog` columns) | always `bicdb` | the connected database, as `current_database()`; Frappe lists its tables with `WHERE table_catalog = <db>` |
| `substring(text, pattern)` / `substring(text FROM pattern)` | positional only: "substr start must be an integer" | a text pattern is the POSIX regex form (first group or whole match, NULL when none) |
| `ALTER TABLE ... ADD COLUMN ... DEFAULT`, `ALTER COLUMN TYPE`, `SET NOT NULL`, `DROP COLUMN` inside a transaction that already wrote the table | "transaction conflict: t:1 is already locked by transaction N" (its own lock); rows inserted in the transaction kept the old shape (new NOT NULL DEFAULT column read back NULL) | rows the transaction owns are validated and rewritten through it |
| `CREATE INDEX IF NOT EXISTS "standard" ON t2(...)` when `t1` has an index `standard` | "index `standard` already exists" | skipped (index names share the schema namespace) |
| `pg_database_size(name)` | unsupported | bytes stored by the connected database |
| `DROP TABLE` while another connection has an open transaction | "cannot rename while another transaction is pending", and a leftover recycle-bin intent made the next server start fail ("ambiguous pending recycle index") | intent withdrawn; the table is dropped permanently instead of being moved to the recycle bin (unless the dropping transaction wrote it) |
| `BEGIN` / `START TRANSACTION [READ ONLY]` inside a transaction block | "nested transactions are not supported" | warning 25001, block kept, modes applied as `SET TRANSACTION` |
| `SET [LOCAL] statement_timeout` | accepted, never enforced | cancels the statement with 57014 |
| `DECLARE c CURSOR [WITH/WITHOUT HOLD] FOR ...`, `FETCH`, `MOVE`, `CLOSE` (psycopg2 named cursors, `frappe.db.unbuffered_cursor()`) | unsupported statement | forward-only SQL cursors |
| `pg_class.reltuples` of a never-analyzed table | 0 | the storage row-count estimate (`frappe.db.estimate_count`) |
| `col IN (a, b, ...)` on an indexed column | full scan (Frappe's link check `WHERE fieldtype IN ('Link','Table') AND options = ...` ran ~390 ms per call on a debug build, hundreds of times per DocType save) | one index lookup per value, in the planner and in the single-table row locator (~20 ms) |
| `date(x)` | "function date is not supported" | `CAST(x AS date)` |
| `''` as a text primary key (`INSERT INTO "tabSeries" VALUES ('', 1)`) | "INSERT into tabSeries requires name" | stored like any other key (record-id cell U+0000, which no text value can produce) |

## Known gaps

- **DROP TABLE under a busy server.** Moving a dropped table into the recycle
  bin renames its storage, which requires every other transaction to be idle.
  A DROP of a table the dropping transaction has itself written, while another
  connection has a transaction open, therefore fails with SQLSTATE 40001
  (`TestPostgresSchemaQueryIndependence`, 10 tests). Without such writes the
  table is dropped permanently instead of being kept in the recycle bin. A
  real fix needs scoped catalog admission for storage renames, which the
  engine does not qualify yet.
- **`pg_stat_activity`** is not available, so `bench drop-site` (which
  terminates the site's sessions through it before `DROP DATABASE`) fails;
  drop the database directly instead.
- **SET TRANSACTION READ ONLY after writes** is refused by BicDB (PostgreSQL
  allows switching a transaction to read-only at any time). Frappe switches
  before any write, so it is not affected.
- **SQL cursors** materialize the query's rows at DECLARE (bounded by the
  server's result-row limit) and are forward-only; SCROLL and BINARY cursors
  are refused.
- **Speed.** On a debug build `bench new-site` takes about 4 minutes and the
  test slice about 14 minutes (`test_db_query` 5, `test_permissions` 4),
  against under a minute and about two minutes on PostgreSQL 17. CI uses a
  debug build, as the Rails job does, and leaves `test_db_query` out of its
  slice to stay near 20 minutes.
- Unnamed `CREATE INDEX ON t (col)` gets BicDB's `idx_<table>_<n>` name rather
  than PostgreSQL's `<table>_<col>_idx`; Frappe does not depend on it.
- `bench` refuses to run as root; run it as an ordinary user.

## Redis

Frappe needs Redis for its cache, job queue and realtime events. BicDB's RESP
server (`bicdb cache-serve`, `crates/bicdb-resp`) is not enough yet: site
creation stops at the first `CLIENT TRACKING ON REDIRECT <id> NOLOOP` (Frappe's
client-side cache) and `SUBSCRIBE` (its invalidation and realtime channels).
Frappe and its RQ job queue also use lists (`LPUSH`/`BLPOP`/`LRANGE`/`LREM`),
sets (`SADD`/`SMEMBERS`/`SREM`) and `MULTI`/`EXEC` pipelines, which
`bicdb-resp` refuses. The qualification therefore runs with stock Redis.
