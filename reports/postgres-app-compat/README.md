# PostgreSQL App Compatibility Campaign

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](../../docs/source-distribution.md).

Date started: 2026-07-01

Goal: run real application migrations and smoke paths against BicDB's pgwire
PostgreSQL surface, fix blockers as they appear, and keep an auditable record
of every attempt.

## Ground Rules

- Do not disable RLS.
- Prefer real application migration commands over handcrafted SQL snippets.
- When a migration blocks, capture the exact command, app revision, SQLSTATE or
  error text, BicDB log excerpt, and the BicDB fix or open gap.
- Re-run the blocked migration after each fix before moving on.
- Keep heavy app checkouts and container work on benchmark-primary; keep reports in this repo.

## Environment

Primary test host:

- Host: benchmark.example.com / 192.0.2.10
- Work directory: `/home/benchmark/app-compat`
- BicDB source: `/home/benchmark/bicdb-head`
- BicDB server command: `/home/benchmark/bicdb-head/target/release/bicdb serve`
- Default pgwire endpoint: `127.0.0.1:5433`

Local report tree:

- `reports/postgres-app-compat/walknorth-carrier-chat/report.md`
- `reports/postgres-app-compat/mastodon/report.md`
- `reports/postgres-app-compat/odoo/report.md`
- `reports/postgres-app-compat/zulip/report.md`
- `reports/postgres-app-compat/nextcloud/report.md`
- `reports/postgres-app-compat/discourse/report.md`
- `reports/postgres-app-compat/gitlab/report.md`

## Status

| App | Source | Revision | Migration status | Current blocker |
| --- | --- | --- | --- | --- |
| WalkNorth Carrier Chat | local read-only archive | `9aab20b` | 2,873-line schema applies; final and repeated apps/gallery smokes plus health, readiness, auth, hubs, and overlay write/read return HTTP 200 on `7e6d325` | no blocker in exercised routes; full PostgreSQL 18 JSON and unrestricted lateral parity are not claimed |
| Mastodon | https://github.com/mastodon/mastodon | `1b82717b833a7b3d8b0f84653bf1628a8ade34c5` | passed `db:migrate` on benchmark-primary | App runtime smoke still needs Redis process; database compatibility blockers cleared through migration |
| Odoo | https://github.com/odoo/odoo | `2d0219d5998d4ff996053fa6b61939f1542f1d1b` | pending | source cloned |
| Zulip | https://github.com/zulip/zulip | `19ed17c388ea93ac21613b4a742045bd71a02edb` | pending | source cloned |
| Nextcloud | https://github.com/nextcloud/server | `ed56cf1774f18651f536a26b9af30b717afff7c4` | passed install and focused `occ` smokes on benchmark-primary | one filesystem-only scan artifact from earlier failed smoke; no BicDB query failures on patched smoke |
| Discourse | https://github.com/discourse/discourse | `87b4e93c102f6ad35a179a58a05dee0d461920f6` | pending | source cloned |
| GitLab | https://gitlab.com/gitlab-org/gitlab | `f22602e37afb92eb7028b601a922ebde417df6e4` | pending | source cloned; prior SQL coverage located |

## Attempt Log

1. 2026-07-01: Created `/home/benchmark/app-compat` on benchmark-primary and shallow-cloned the
   official source trees for Mastodon, Odoo, Zulip, Nextcloud, Discourse, and
   GitLab. Dev9 has Docker and Docker Compose available; native Ruby/Node/PHP
   stacks are not installed, so app execution should use containers.
2. 2026-07-01: Mastodon attempt 001 reached Rails boot/database task startup
   and blocked on `SELECT EXISTS (SELECT * FROM pg_proc WHERE proname =
   'timestamp_id')`. Added session and row-query `EXISTS` projection support
   with a Mastodon-shaped regression test; benchmark-primary replay pending.
3. 2026-07-01: Mastodon attempt 002 passed the `EXISTS` probe and blocked on
   ActiveRecord schema loading `CREATE EXTENSION IF NOT EXISTS "plpgsql" SCHEMA
   pg_catalog`. Added raw `CREATE EXTENSION` support for `SCHEMA` / `WITH
   SCHEMA` forms plus extension catalog coverage; benchmark-primary replay pending.
4. 2026-07-01: Mastodon attempt 003 passed extension setup and blocked on a
   complex unique expression index using `COALESCE(lower((domain)::text),
   ''::text)`. Added metadata-only fallback for unreducible expression indexes,
   preserving catalog uniqueness metadata; benchmark-primary replay pending.
5. 2026-07-01: Mastodon attempt 004 progressed through schema tables/indexes and
   blocked on `CREATE MATERIALIZED VIEW account_summaries`. Added metadata
   support for materialized views with `pg_class.relkind = 'm'`; benchmark-primary replay
   pending.
6. 2026-07-01: Mastodon attempt 005 replayed materialized-view support and
   blocked on `CREATE INDEX ... ON account_summaries (...)` because indexes were
   still table-schema-only. Added metadata indexes on materialized views and
   included them in `pg_class`, `pg_index`, `pg_indexes`, and `pg_get_indexdef`.
7. 2026-07-01: Mastodon attempt 006 passed `bundle exec rails db:migrate`
   against BicDB on benchmark-primary. Rails readback returned 594 rows in
   `schema_migrations`, 3 materialized views in `pg_class`, and 2 catalog-visible
   indexes on `account_summaries`. The only emitted app error was Redis
   connection refusal during cache access, not a database failure.
8. 2026-07-01: Nextcloud attempts 001-009 found and fixed PostgreSQL
   compatibility gaps in timezone GUCs, quote helper functions, ungrouped
   aggregate `HAVING`, database-level grants/revokes, explicit pgwire
   transaction read-your-writes, DDL inside explicit transactions, `lastval()`,
   PDO positional `?` bind substitution, and describe-time session GUC
   propagation. A PDO transaction reproduction now returns `lastval() = 1` after
   inserting into a `BIGSERIAL` table.
9. 2026-07-01: Nextcloud attempt 010 passed `maintenance:install` against BicDB
   on benchmark-primary. The installer completed with `Nextcloud was successfully installed`
   after core migrations, admin account creation, shipped app setup, DAV system
   address book repair, and theming/background job setup. BicDB reported over
   10k install queries and `failed_queries=0`.
10. 2026-07-01: Nextcloud behavior smokes ran after install. Fixed unary-plus
   numeric constants from DAV contact creation and targetless `ON CONFLICT DO
   NOTHING` across unique indexes from DB file locking. Final patched smoke:
   `occ status`, `occ user:list`, fresh user persistence, and `occ files:scan
   --all` completed without BicDB query failures. `files:scan` still reported a
   filesystem-only `mkdir(): File exists` artifact left by an earlier failed
   smoke.
11. 2026-07-01: Full local SQL regression after compatibility fixes passed:
    `cargo test -p bicdb-sql --test sql` reported 250 passed / 0 failed. Pgwire
    library regression passed: `cargo test -p bicdb-pgwire --lib` reported
    10 passed / 0 failed.
12. 2026-07-01: Final benchmark-primary HammerDB regression against the rebuilt patched
    binary passed validity checks. `compat-final-regression-102500` produced
    228,351 DSUM NOPM; HammerDB reported 225,593 NOPM from 574,851 PostgreSQL
    TPM; `FAILED_COUNT=0`, all VUsers finished successfully, server
    `failed_queries=0`, and tmpfs cleanup ended with `shm_used=0`.
13. 2026-07-10: WalkNorth Carrier Chat replay after `e737f89` retained the
    original JSONB, DML `RETURNING`, and scalar Describe fixes, then exposed and
    fixed the next blockers in order: a `VALUES`-backed role-rank CTE, JSONB
    existence operators in predicates, the gallery's correlated
    `LEFT JOIN LATERAL`, binary JSONB output described as OID 1009 instead of
    3802, and a
    procedure-variable/outer-column name collision. The lateral implementation
    is scoped to the exercised `INNER` and `LEFT` derived-table forms.
14. 2026-07-10: Final replay used GitHub `main` commit `7e6d325` and the
    read-only Carrier archive at `9aab20b` with CLI 2.0.5; the source checkout
    remained untouched. Health, readiness, `/me`, hubs, apps, gallery, and
    overlay write/read all returned HTTP 200. Three repeated apps requests and
    three repeated gallery requests also returned HTTP 200, and overlay readback
    preserved the nested patch marker `7e6d325`. Focused JSON tests passed 4/4
    and 7/7, pgwire library tests 19/19, protocol tests 80/80, and the release
    build passed. The SQL suite reported 275 passed plus one known pre-existing
    PHI test failure reproduced on the prior clean HEAD.
