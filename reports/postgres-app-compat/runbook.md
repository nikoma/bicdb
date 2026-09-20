# Runbook

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](../../docs/source-distribution.md).

## Dev9 Layout

```sh
mkdir -p /home/benchmark/app-compat/{sources,db,logs,reports}
```

Source checkouts live in `/home/benchmark/app-compat/sources`. BicDB databases live in
`/home/benchmark/app-compat/db/<app>`. Server logs live in
`/home/benchmark/app-compat/logs/<app>-bicdb.log`.

## BicDB Server

Start a fresh unauthenticated local-only pgwire server for each app:

```sh
/home/benchmark/bicdb-head/target/release/bicdb serve /home/benchmark/app-compat/db/<app> \
  --host 127.0.0.1 \
  --port 5433 \
  --max-connections 200 \
  --max-active-queries 64 \
  --max-active-reads 64 \
  --max-active-writes 32 \
  --query-timeout-ms 120000 \
  --write-timeout-ms 120000 \
  --max-request-bytes 104857600 \
  --max-result-rows 1000000 \
  > /home/benchmark/app-compat/logs/<app>-bicdb.log 2>&1 &
```

Use separate database directories per app so a blocked migration can be replayed
from a clean state.

## Required Evidence Per Attempt

- App name and revision.
- BicDB revision.
- Exact command and environment.
- Start and end timestamps.
- Migration phase reached.
- Error text and SQLSTATE when available.
- BicDB server log excerpt around the failure.
- Fix applied, test added, and rerun result.

## Extension Compatibility Rule

Application migrations should not fail merely because PostgreSQL exposes a
feature as an extension while BicDB implements it natively or as compatibility
metadata. When an app requests extensions such as `postgis`, `vector`,
`pg_trgm`, `btree_gin`, `btree_gist`, `citext`, `uuid-ossp`, `pgcrypto`, or
`unaccent`, the expected behavior is:

- Accept idempotent extension DDL such as `CREATE EXTENSION IF NOT EXISTS`.
- Expose enough `pg_extension`, type, operator, function, cast, and index-access
  metadata for framework introspection.
- Implement the app-used functions/operators/types, or record the exact missing
  semantic subset as a compatibility gap.
- Report successes precisely as native/metadata compatibility, not full upstream
  extension parity unless the app exercised that parity.

## Reporting Cadence

After each app attempt, copy the relevant log excerpts into that app's
`report.md` and update the status table in `README.md`.
