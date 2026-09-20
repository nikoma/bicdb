# BicDB PostgreSQL Nightmare Gauntlet

`pg18-nightmare` is BicDB's PostgreSQL 18.4 torture harness. It reuses the
repository's existing Docker oracle from `compose.pg18.yml` through
`scripts/pg18-up.sh`; it does not install another PostgreSQL.

## Run

Quick PR-sized run:

```bash
./scripts/pg18-nightmare.sh
```

Full local run with generated fuzz seeds and client/protocol checks:

```bash
BICDB_NIGHTMARE_MODE=full BICDB_NIGHTMARE_FUZZ_SEEDS=16 ./scripts/pg18-nightmare.sh
```

Direct CLI form:

```bash
cargo run -p bicdb-cli --features bench -- compat nightmare ./tmp/pg18-nightmare \
  --fixtures fixtures/pg18-nightmare \
  --json-out reports/pg18-nightmare/report.json \
  --markdown-out reports/pg18-nightmare/report.md \
  --repro-dir reports/pg18-nightmare/repros
```

## Coverage

The fixture suite covers NULL behavior, joins with NULLs, nested joins,
subqueries, CTEs, aggregates over empty sets, GROUP BY, ORDER BY, LIMIT/OFFSET,
casts, timestamps, JSONB operator gaps, constraints, transactions, savepoints,
sequences, prepared statements, error SQLSTATE parity, weird and quoted
identifiers, Unicode, huge strings, bytea, invalid SQL, COPY gaps, views through
the existing compatibility fixtures, and trigger/function gaps when applicable.

The full run also executes the existing client/protocol checks for
node-postgres, psycopg, SQLAlchemy, tokio-postgres, malformed protocol frames,
concurrent clients, and cancel-request behavior. Prisma remains documented in
`docs/postgresql-client-gauntlet.md` because it is still a known catalog and
prepared-statement gap.

## Reports

Reports are written to `reports/pg18-nightmare` by default:

- `report.json`: complete machine-readable observations for PostgreSQL and BicDB.
- `report.md`: human-readable summary with every fixture status.
- `repros/failing.sql`: all non-intentional failing SQL cases.
- `repros/minimized.sql`: shortest SQL prefix ending at the first observed mismatch.
- `repros/<fixture>.sql` and `repros/<fixture>.min.sql`: per-fixture reproductions.

The gauntlet never hides differences. A fixture without
`expectation: "expected_difference"` must match PostgreSQL exactly. Unsupported
behavior can pass only when the fixture explicitly declares an intentional gap
and includes an `expected_difference` explanation.

## CI

`docs/ci/pg18-nightmare.workflow.yml` contains the GitHub Actions workflow
that should be copied to `.github/workflows/pg18-nightmare.yml` by a maintainer
or automation credential with GitHub `workflow` scope. The intended workflow
runs the quick suite on every pull request, and the full suite on nightly
schedule or manual dispatch. The workflow
uploads the report directory as an artifact even when the suite fails.

## Adding Real-World Cases

Add public PostgreSQL, driver, or ORM edge cases as new JSON fixtures under
`fixtures/pg18-nightmare`. Prefer one behavioral theme per fixture and keep
cleanup SQL complete. If a case is fuzz-discovered, keep the seed in the fixture
ID and let the repro writer provide the minimized SQL prefix.
