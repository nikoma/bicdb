# Agent Instructions: Benchmark-Driven PostgreSQL Compatibility

This file captures the current BicDB optimization loop. Follow it when working on
HammerDB, GitLab, Rails/auth-server, pg_dump, or any other PostgreSQL client
compatibility workload.

## Goal

BicDB must pass demanding PostgreSQL workloads because it implements generic
PostgreSQL semantics, planner behavior, storage behavior, and pgwire behavior.
It must not pass because production code recognizes a benchmark, migration tool,
application name, routine name, table shape, or schema fixture.

## Non-Negotiables

- No workload shims in production code.
- No dispatch on routine names, benchmark names, application names, table names,
  aliases, parameter names, or body shapes.
- No hardcoded benchmark, framework, product, auth-server, transaction/routine
  marker, or fixture-specific shortcut in production code.
- If a construct is unsupported, report the exact unsupported generic SQL or
  PL/pgSQL feature, then implement that feature generically.
- Tests may use benchmark-like schemas only as black-box compatibility fixtures.
  Production code must stay neutral.
- Never cut corners to appease a single migration or benchmark case. The next
  real application should benefit from the same fix.

## Current Work Loop

1. Start from a clean `main`, but do not delete or revert untracked local
   database artifacts unless explicitly asked.
2. Run the workload or a minimal reproduction against the current BicDB build.
3. Capture traces and slow logs.
4. Identify the next actual bottleneck or failure from evidence.
5. Reduce it to the missing generic PostgreSQL/BicDB capability.
6. Add focused neutral tests with arbitrary names for procedures, tables,
   aliases, columns, parameters, and local variables.
7. Implement the generic fix in the parser, IR/VM, planner, executor, storage,
   index, constraint, or pgwire layer as appropriate.
8. Run focused tests first.
9. Run broader package tests and guard checks.
10. Commit the verified milestone.
11. Push to `origin/main`.
12. Re-run the workload and repeat from the next failure or bottleneck.

## Benchmark/Trace Commands

Build the CLI before workload runs:

```bash
cargo build -p bicdb-cli
```

Run the PostgreSQL wire server with execution tracing:

```bash
env BICDB_TRACE_EXEC=1 \
    BICDB_TRACE_SCHEMA=1 \
    BICDB_TRACE_PLAN=1 \
    ./target/debug/bicdb serve-pg /path/to/bicdb-hammerdb-proc-ddl-capture \
      --host 0.0.0.0 \
      --port 55432 \
      --allow-remote-no-auth \
      --query-timeout-ms 300000 \
      --write-timeout-ms 300000 \
      --max-active-queries 32 \
      --max-active-reads 32 \
      --max-active-writes 16 \
      --max-result-rows 1000000 \
      --slow-query-log /tmp/bicdb-hammerdb-slow.log \
      --slow-query-threshold-ms 1
```

When trace output needs clean parsing, redirect stdout/stderr to a file:

```bash
env BICDB_TRACE_EXEC=1 BICDB_TRACE_SCHEMA=1 BICDB_TRACE_PLAN=1 \
  ./target/debug/bicdb serve-pg /path/to/bicdb-hammerdb-proc-ddl-capture \
    --host 0.0.0.0 --port 55432 --allow-remote-no-auth \
    --query-timeout-ms 300000 --write-timeout-ms 300000 \
    --max-active-queries 32 --max-active-reads 32 --max-active-writes 16 \
    --max-result-rows 1000000 \
    --slow-query-log /tmp/bicdb-hammerdb-slow.log \
    --slow-query-threshold-ms 1 \
    > /tmp/bicdb-hammerdb-trace.log 2>&1
```

Run HammerDB stored procedures:

```bash
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v /path/to/bicdb-hammerdb-work:/work \
  tpcorg/hammerdb:postgres \
  ./hammerdbcli auto /work/pg-tpcc-run-storedprocs.tcl
```

Run direct probes with local `psql`:

```bash
/Applications/Postgres.app/Contents/Versions/latest/bin/psql \
  'postgres://bicdb:bicdb@127.0.0.1:55432/bicdb?sslmode=disable' \
  -v ON_ERROR_STOP=1 \
  -c "select 1"
```

## How To Read The Evidence

Prioritize traces and counters over intuition:

- `elapsed_ms`: total statement time.
- `schema_loads`, `schema_saves`, `schema_bytes_read`, `schema_bytes_written`:
  catalog and schema churn.
- `rows_materialized`: avoid full materialization for LIMIT, EXISTS, indexed
  lookup, and bounded routine operations.
- `join_intermediate_rows`, `join_candidate_pairs`: detect join expansion.
- `index_lookups`, `full_scans`, `record_id_prefix_scans`: verify the planner is
  using real indexes and primary-key paths.
- `scalar_subqueries`, `scalar_subquery_elapsed_ms`: find repeated nested query
  costs.
- `write_elapsed_ms`, `write_batches`, `write_rows`: separate storage write cost
  from planner/executor overhead.

For slow-log summaries, group by statement kind and then drill into the hottest
generic statement shape. Do not optimize based on the routine name.

## Required Verification Before Commit

Run focused tests that prove the new generic behavior, then at least the
affected package suite:

```bash
cargo test -p bicdb-sql <focused_test_name> -- --nocapture
cargo test -p bicdb-core <focused_test_name> -- --nocapture
cargo test -p bicdb-sql
```

When core storage/index behavior changes, include:

```bash
cargo test -p bicdb-core -p bicdb-sql
```

Always run guard checks before committing:

```bash
cargo fmt --check
git diff --check
rg -n "HammerDB|TPC-C|GitLab|NEWORD|PAYMENT|OSTAT|SLEV|DELIVERY|order_status|new_order|CustomerOrderStatusQuery" \
  crates/bicdb-sql/src crates/bicdb-core/src crates/bicdb-cli/src crates/bicdb-pgwire/src
```

The final `rg` command should produce no production-code matches.

## Git Discipline

Each verified milestone gets its own commit and push:

```bash
git fetch origin main
git add <only intended files>
git diff --staged --stat
git diff --staged | rg -n "password|secret|api_key|token" || true
git commit -m "fix: concise generic behavior summary"
git push origin main
```

If `origin/main` moved, reconcile cleanly with a normal fetch/rebase/merge as
appropriate. Do not force-push shared `main`.

Keep untracked workload artifacts out of commits unless explicitly requested.
Known local artifacts include:

- `testdb-full.bicbackup`
- `testdb/`
- product-owned directories

## Current Optimization Direction

Recent successful generic fixes followed this pattern:

- Use executable primary-key indexes for prefix and extreme aggregate lookups.
- Batch parent-delete foreign-key checks instead of scanning schema/children per
  deleted row.
- Share immutable materialized CTE rows across query contexts instead of cloning
  large CTE payloads.
- Skip unchanged unique-key validation on UPDATE while still validating changed
  unique keys, foreign keys, exclusion constraints, local constraints, RLS, and
  the actual write.

Continue from the next measured bottleneck. The current HammerDB shape still
points at generic data-modifying CTEs, grouped aggregates, row materialization,
and write-path costs. Fix those semantics generally, test them with neutral
names, then rerun the workload.
