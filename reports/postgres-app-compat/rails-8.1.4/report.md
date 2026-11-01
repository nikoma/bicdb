# Rails 8.1.4 / Team Board qualification

This campaign runs the complete [Team Board sample](../../../examples/rails/team_board/README.md)
through Rails' standard PostgreSQL adapter. Application code has no custom
adapter, monkey patches, database-specific query rewrites, mocked workers, or
alternate cache backend. The engine fixes and sample are delivered in the same
source revision as this report; older downloadable binaries predate these fixes.

## Recorded workload

- Ruby 3.4.12; Rails 8.1.4; pg 1.7.0.
- Solid Queue 1.7.0; Solid Cache 1.0.10; Solid Cable 3.0.12.
- One BicDB process with primary, queue, cache and cable logical databases.
- Development configuration, loopback database listener, public demo credentials.
- Qualification containers use `--network none`. PostgreSQL, Redis,
  Elasticsearch and external broker services cannot be reached by the app.
- Application attachments use Active Storage's disk backend; attachment metadata
  is in BicDB. This does not claim that file bytes are stored in the database.

The development binary passed the complete campaign. Retained evidence:
[report](debug-report.json), [Rails tests](debug-tests.txt),
[HTTP and WebSocket checks](debug-http.txt). The report identifies the executable
by SHA-256 rather than relying on a version string shared by older builds.

## Required invariants

`bin/verify` creates an isolated cluster and fails if any step fails:

1. Load the checked-in schemas, repeat `db:prepare`, reverse all three application
   migrations, and reapply them. Repeat seeds without duplicates.
2. Run the standard Rails suite in two test processes: **19 tests, 87 assertions,
   zero failures, errors or skips**. Coverage includes constraints, associations,
   JSONB/arrays, full-text insert/update/delete, optimistic locking, transactions,
   savepoints, attachments, authentication, forms and workspace access boundaries.
3. Roll back a transaction containing a deferred Active Job enqueue and prove the
   job was not published. Persist 24 pending reports/jobs and a serialized binary
   cache value; SIGKILL BicDB and reopen the same cluster before starting workers.
4. Recover all pending work and the exact cache value. Run two actual Solid Queue
   worker processes. Require both to complete work with overlapping start/finish
   intervals, all 24 reports completed once in this scenario, zero failed jobs,
   matching aggregates/cache payloads and at least 24 persisted Cable messages.
5. Exercise real HTTP login with CSRF protection, search, assets and issue
   creation. Subscribe through a real WebSocket, enqueue a report through HTTP,
   and receive the worker's Turbo update through Solid Cable.

The polling harness disables query caching on its verification connections so it
observes committed worker progress rather than a cached initial row count.
Normal Rails application caching and SQL query tags remain enabled as configured.

## Engine defects found and repaired

| Generic SQL behavior | Observed failure | Regression coverage |
| --- | --- | --- |
| Comments in database and extension DDL | Query tags were interpreted as options; a fresh schema load failed | `database_ddl_comments.rs` |
| Correlated/lateral `generate_subscripts` in FROM | Unsupported relation; missing/wrong result-column types | `generate_subscripts_from.rs` |
| Constraint key arrays | `int2[]` catalog values were encoded as zero-based vector text, breaking composite-key introspection | `generate_subscripts_from.rs` |
| Default deparsing and array text | Literal defaults were missing or quoted incorrectly; array defaults failed to round-trip | `catalog_default_and_index_columns.rs` |
| `pg_get_indexdef(oid, column, pretty)` | Returned full DDL instead of the requested column expression | `catalog_default_and_index_columns.rs` |
| Typed `ANY`/`ALL` on `int2vector`/`oidvector` | Primary-key introspection could not evaluate array comparisons | `catalog_default_and_index_columns.rs` |
| Catalog query execution | Four pre-existing SQL-text shortcuts ignored added predicates and omitted defaults; removed in favor of normal execution | `catalog_default_and_index_columns.rs` |
| SQL columns named `metadata` | Omitted/NULL text returned native record metadata; non-NULL text could be returned as JSON | `native_field_sql_null.rs` |
| Updates leaving foreign keys unchanged | Concurrent updates to sibling rows reserved the same parent key and failed with 40001 | `foreign_key_unchanged_updates.rs` |

All listed tests are in `crates/bicdb-sql/tests/`. Production fixes resolve SQL
semantics and types; they do not recognize an application name or fixture schema.
The foreign-key fix still validates changed keys, including changes made by
BEFORE UPDATE triggers, and retains parent-delete enforcement.

The foreign-key concurrency case was independently checked with PostgreSQL 18.4:
[two committed updates](fk-postgresql-oracle.txt), compared with the
[previous BicDB build's 40001](fk-before-fix.txt). Its SQL shape is two transactions
updating different child rows referencing the same parent; neither changes the
reference. The executable regression test preserves that schedule.

## Reproduce

From the repository root:

```sh
docker compose -f examples/rails/team_board/compose.yml up --build
```

For a qualification run with retained evidence and no external network:

```sh
docker build -f examples/rails/team_board/Dockerfile -t bicdb-rails-demo .
mkdir -p rails-evidence
docker run --rm --network none \
  -v "$PWD/rails-evidence:/evidence" \
  bicdb-rails-demo bin/verify --work-dir /evidence/run
```

Use a new `--work-dir` for each run. The script never prepares or rolls back an
existing external database; it owns its server and scratch cluster. The CI
workflow [runs this campaign](../../../.github/workflows/rails.yml) against the
source build and retains logs even on failure.

This is a qualification of the recorded workload, not a certification of every
Rails application or a production deployment. Primary and queue commits are
separate; after-commit enqueueing does not make them atomically commit together.
The restart test crashes BicDB before jobs are claimed. It does not assert
exactly-once job execution across arbitrary worker crashes.
