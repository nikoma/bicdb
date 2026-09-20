# Nextcloud Compatibility Report

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](../../../docs/source-distribution.md).

Source: https://github.com/nextcloud/server

Revision: `ed56cf1774f18651f536a26b9af30b717afff7c4`

Revision date: 2026-07-01 07:53:00 +0200

Commit subject: `Merge pull request #61544 from nextcloud/carl/nouserexception`

Status: install/migration and focused behavior smokes passed on benchmark-primary. Attempt
010 completed `maintenance:install`; follow-up smokes verified `occ status`,
`occ user:list`, fresh user persistence, and `occ files:scan --all` without
BicDB query failures.

## Environment

- Host: benchmark-primary / `192.0.2.10`
- Source checkout: `/home/benchmark/app-compat/sources/nextcloud`
- BicDB source: `/home/benchmark/bicdb-head`
- BicDB endpoint: `127.0.0.1:5433`
- Container image: `appcompat-nextcloud-dev:ed56cf1`
- App data dir: `/home/benchmark/app-compat/nextcloud-data`

## Current Install Command

```sh
docker run --rm --network host -u root \
  -v /home/benchmark/app-compat/sources/nextcloud:/var/www/html \
  -v /home/benchmark/app-compat/nextcloud-data:/var/www/data \
  -w /var/www/html appcompat-nextcloud-dev:ed56cf1 \
  php occ maintenance:install --verbose \
    --database=pgsql --database-name=bicdb \
    --database-host=127.0.0.1 --database-port=5433 \
    --database-user=bicdb --database-pass= \
    --admin-user admin --admin-pass admin \
    --data-dir=/var/www/data
```

## Attempts

1. Attempt 001: blocked during installer startup on `SET TIME ZONE 'UTC'`.
   BicDB fix: added PostgreSQL `SET TIME ZONE` handling backed by the
   `timezone` session GUC. Regression: `postgres_timezone_set_is_accepted`.

2. Attempt 002: progressed to Doctrine DBAL introspection and blocked on
   no-FROM `quote_ident` usage. BicDB fix: implemented `quote_ident` and
   `quote_literal` in session/catalog function evaluation. Regression:
   `postgres_quote_helpers_work_without_from_for_dbal_introspection`.

3. Attempt 003: blocked on DBAL `insertIfNotExist` probing an ungrouped
   aggregate predicate, `HAVING COUNT(*) = 0`. BicDB fix: added ungrouped
   aggregate `HAVING` truth evaluation. Grouped `HAVING` remains out of scope.

4. Attempt 004: reached database setup privilege statements and blocked on
   PostgreSQL database-level privilege syntax.

5. Attempt 005: fixed database-level `GRANT`/`REVOKE`, including `PUBLIC`, and
   added regression `postgres_database_privileges_are_accepted_for_app_setup`.
   The next blocker was admin account creation: explicit pgwire transactions
   buffered writes until `COMMIT`, so Nextcloud could not read its own inserted
   user row inside the same transaction.

6. Attempt 006: after pgwire explicit transactions were changed to hold a real
   pending BicDB transaction, Nextcloud could read its own writes. The next
   blocker was `CREATE TABLE oc_migrations` inside an explicit transaction
   requiring exclusive database access. BicDB fix: transaction execution now
   retries DDL under the exclusive write path while preserving pending
   transaction state.

7. Attempt 007: ran through core migrations, admin account creation, and many
   shipped app migrations. It reached DAV repair step `Create system address
   book` and blocked on `SELECT lastval()` because `lastval` was unsupported
   without a `FROM` clause.

8. Attempt 008: infrastructure-only failure. Port cleanup missed an existing
   listener, the new BicDB server failed with `Address already in use`, and
   Nextcloud saw connection refused. Not counted as a SQL compatibility blocker.

9. Attempt 009: after adding basic `lastval()` support, DAV repair still failed
   with `lastval is not yet defined in this session`. A tight PDO reproduction
   showed the real cause: Nextcloud/PDO prepares statements with positional
   `?` placeholders and describes `SELECT lastval()` before execution. BicDB
   fixes:

   - pgwire bind-time substitution now handles positional `?` parameters
     outside strings, identifiers, comments, and dollar-quoted bodies.
   - pgwire describe-time query probes now use the connection's session GUCs
     instead of an empty GUC map, so `Describe` for `SELECT lastval()` sees the
     sequence value recorded by the prior insert.

   Validation: a PDO transaction repro now inserts into a `BIGSERIAL` table,
   `SELECT lastval()` returns `1`, and the row is visible inside the transaction.

10. Attempt 010: clean install after the PDO/describe fixes passed.
    `maintenance:install` completed with `Nextcloud was successfully installed`.
    It passed core server migrations, admin account creation, default app
    installation, DAV migrations/task registration, and the previous system
    address book `lastval()` blocker. BicDB reported over 10k queries and
    `failed_queries=0` during the install. The run was slow because Nextcloud
    used one active connection for serial schema introspection/migration work;
    one BicDB worker thread was saturated while the rest were idle.

11. Smoke 011: `occ status` and `occ user:list` passed. Creating `smokeuser`
    exposed a DAV contact behavior gap: bound numeric values can render as
    unary-plus constants such as `+999999999999`, which BicDB rejected as an
    unsupported constant expression. BicDB fix: unary plus is now a no-op in the
    constant, row, slot-row, aggregate, and bound-expression evaluators.
    Regression: `postgres_unary_plus_numeric_constants_match_nextcloud_dav_smoke`.

12. Smoke 012: fresh user creation succeeded and persisted (`smokeuser2` was
    visible in `occ user:list` and `oc_users`). `files:scan --all` exposed
    PostgreSQL semantics for targetless `INSERT ... ON CONFLICT DO NOTHING`:
    Nextcloud's DB lock path conflicts on unique index `lock_key_index`, not the
    primary key. BicDB fix: targetless `DO NOTHING` now filters conflicts across
    all unique arbiters, including unique indexes. Regression:
    `postgres_targetless_on_conflict_do_nothing_ignores_unique_index_conflict`.

13. Smoke 013: after restarting BicDB with the targetless conflict fix, `occ
    status`, `occ user:list`, and `occ files:scan --all` ran without BicDB query
    failures. The scan still reported one filesystem-only error,
    `mkdir(): File exists`, caused by the earlier partial `smokeuser` data
    directory artifact; there were no SQL exceptions and the patched BicDB log
    had no `query.failed` entries.

14. Final regression closure: local SQL compatibility suite passed 250/250, and
    pgwire library tests passed 10/10. Dev9 release build succeeded and the
    canonical HammerDB regression `compat-final-regression-102500` completed
    with 228,351 DSUM NOPM, HammerDB 225,593 NOPM / 574,851 TPM,
    `FAILED_COUNT=0`, all VUsers successful, server `failed_queries=0`, and
    clean tmpfs teardown.

## Behavioral Smoke Plan

- `php occ status`: passed.
- `php occ user:list`: passed.
- `php occ user:add --password-from-env smokeuser2`: passed and persisted.
- `php occ files:scan --all`: passed from the database perspective; one
  pre-existing filesystem artifact remained from the failed smoke and should be
  cleaned before a perfectly clean UI/filesystem acceptance run.
- BicDB query failure check: passed on the patched smoke server.
