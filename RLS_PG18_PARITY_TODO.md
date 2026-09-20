# RLS Feature Parity with PostgreSQL 18 — Todo List

Status (2026-07-12): **implemented through P7** except the explicitly deferred
items marked below. Expected behaviors were verified against a stock
`postgres:18.4` oracle; `scripts/rls-pg18-diff.sh` runs a differential parity
battery against it and currently reports **no differences within that battery**.
This is not a claim of literal 100% PostgreSQL parity. The Rust test suite has
a dedicated `rls_*` battery in `crates/bicdb-sql/tests/sql.rs`. See
`docs/row-level-security.md` for the full model and remaining divergences.

Note: PostgreSQL 18 introduces no RLS-specific changes over 15–17; the parity
target is the stable RLS feature set (MERGE support landed in PG 15,
`security_invoker` views in PG 15).

---

## P0 — Security bugs (fix immediately)

- [x] Reject unrecognized `CREATE POLICY` clauses instead of silently
      discarding them. (Superseded: policy DDL now goes through the sqlparser
      AST with full grammar; the raw-string parsers were deleted.)
- [x] Fix stale `POSTGRES_COMPATIBILITY.md` claim that RLS is "not enforced".
- [x] Make regular and local GUC assignments transactional across embedded SQL
      and pgwire: regular `SET` persists on commit and restores on rollback;
      `SET LOCAL` / `set_config(..., true)` restore on commit, rollback, and
      rollback-to-savepoint so pooled RLS identities do not leak. Effective,
      transaction-local, and savepoint-snapshot GUC allocations are included
      in pgwire per-connection memory accounting.

## P1 — Session identity foundation

- [x] Propagate the authenticated pgwire user into per-connection session
      identity (seeded as the `session_authorization` GUC at startup; startup
      parameters cannot override identity keys).
- [x] `current_user` / `session_user` / `current_role` reflect the session
      identity (identifier, keyword-function, and SELECT fast paths).
- [x] `SET ROLE` / `RESET ROLE` / `SET SESSION AUTHORIZATION` /
      `RESET SESSION AUTHORIZATION` with PostgreSQL permission rules
      (membership for SET ROLE; connect-time superuser for SET SESSION
      AUTHORIZATION; `RESET ALL` preserves identity).
- [x] Role membership: `GRANT role TO role` existed; added transitive
      closure resolution honoring `INHERIT`/`NOINHERIT`, used by both policy
      `TO` matching and owner checks. `pg_has_role()` still absent (deferred).
- [x] Role attributes `SUPERUSER` and `BYPASSRLS` wired into enforcement;
      server-level `SecurityContext` bypass kept as embedded escape hatch.
- [x] Per-table ownership (`owner` on the table schema, set at CREATE TABLE;
      `ALTER TABLE ... OWNER TO` supported). Legacy schemas default to the
      bootstrap role.
- [x] Bonus: `DROP ROLE/USER` (refuses when the role owns tables),
      `ALTER ROLE` attribute changes and rename, and pgwire credential-store
      sync for `CREATE/ALTER/DROP ROLE ... PASSWORD` so least-privilege app
      roles can authenticate (pre-hashed SCRAM literals are skipped with a
      warning; extended-protocol DDL is not sniffed).

## P2 — DDL / grammar parity

- [x] `CREATE POLICY` through the main parser: optional `FOR` (defaults ALL),
      `AS PERMISSIVE | RESTRICTIVE`, `TO role, ... | PUBLIC | CURRENT_USER |
      CURRENT_ROLE | SESSION_USER` (roles persisted, serde default `{public}`
      for pre-existing policies), clause validation matching PG error texts.
- [x] Creation-time expression validation: aggregates rejected, column
      references checked (subqueries left to evaluation).
- [ ] Column dependency tracking (`DROP COLUMN` referenced by a policy should
      error) — deferred.
- [x] `ALTER POLICY ... RENAME TO` and `ALTER POLICY ... [TO] [USING] [WITH
      CHECK]`.
- [x] `DROP POLICY ... CASCADE | RESTRICT` (accepted; both are no-ops), PG
      error message parity for missing/duplicate policies.
- [x] `ALTER TABLE ... NO FORCE ROW LEVEL SECURITY`.
- [x] `rls_enabled` / `rls_forced` independent like
      `relrowsecurity`/`relforcerowsecurity` (verified against oracle).
- [ ] SQLSTATE codes (42710/42704/42501/42601) — messages match; wire-level
      SQLSTATE mapping still uses BicDB's generic codes. Deferred.

## P3 — Enforcement semantics

- [x] Restrictive combination: (OR of permissive) AND (AND of restrictive);
      no applicable permissive ⇒ deny. Policies with no expression contribute
      nothing (oracle-verified NULL-qual behavior).
- [x] `TO` role filtering with membership closure.
- [x] Owner bypass unless FORCE; SUPERUSER/BYPASSRLS always bypass. The
      bootstrap `bicdb` role is owner-not-superuser by design (documented
      divergence keeping embedded GUC-driven tenancy enforceable under FORCE).
- [x] `row_security = off` ⇒ error ("query would be affected by row-level
      security policy") when policies would apply; check ordering matches PG
      (owner check before the error).
- [x] `current_user` etc. evaluate per-session inside policy expressions.
- [x] SELECT-policy interplay: UPDATE/DELETE reading existing columns (WHERE,
      SET right-hand sides, RETURNING, FROM/USING) also filter through SELECT
      policies (oracle-verified, including the no-column-reads blanket case).
- [x] `INSERT ... ON CONFLICT DO UPDATE`: conflicting row failing UPDATE
      USING raises the "(USING expression)" error; conflict detection is
      index-level, independent of RLS visibility.
- [x] Named-restrictive-policy violation messages.
- [x] Partitioning: children inherit parent policies/flags — kept
      deliberately (parent-scan enforcement depends on it); divergence
      documented.
- [ ] FK/internal-query RLS caveat parity — deferred (documented).

## P4 — Planner / information-leak hardening

- [x] RLS filters run before user predicates by construction (scan paths
      filter at record materialization; index probes never evaluate user
      functions on hidden rows). `security_barrier` parsed/persisted, no
      separate planner effect needed.
- [x] Views: `WITH (security_invoker = ...)` parsed and persisted; definer
      views decide RLS as the view owner while expressions evaluate with the
      invoker's identity and GUCs (exact PG-18 behavior, oracle-verified);
      view ownership recorded at CREATE VIEW.
- [x] Prepared statements re-evaluate policies per execution (no policy
      decision caching; GUC-change coverage in pgwire tests).

## P5 — Catalog, dump, tooling

- [x] `pg_policy.polroles` real role-OID array (`{0}` for PUBLIC);
      `pg_policies.roles` real role names.
- [x] `pg_policies.permissive` now meaningful.
- [ ] `pg_class.relowner` from real ownership — deferred (cosmetic; requires
      threading owner through every pg_class caller).
- [ ] pg_dump: policy catalogs are query-ready, but a real `pg_dump` run is
      blocked earlier by `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
      (pre-existing, outside RLS scope).
- [x] psql `\d`-family output driven by the populated catalogs.

## P6 — Previously "blocked" items

- [ ] `MERGE` RLS — still blocked: no MERGE statement.
- [x] `COPY` RLS — the original survey was wrong: pgwire supports COPY.
      `COPY ... TO STDOUT` runs through SELECT (policies apply);
      `COPY ... FROM STDIN` enforces INSERT WITH CHECK via
      `copy_insert_rows`. Covered by the identity model.

## P7 — Testing & docs

- [x] Differential harness: `scripts/rls-pg18-diff.sh` +
      `scripts/rls_parity_battery.sql` against the `postgres:18.4` oracle —
      currently **no differences within the battery**. The battery includes
      labeled regular/local commit and rollback, savepoint restoration, and
      post-commit RLS identity-leak probes. (A full port of PG's
      `rowsecurity.sql` regression file remains open; the battery covers the
      core matrix.)
- [x] Rust `rls_*` test battery: restrictive AND, TO-roles + membership,
      owner bypass vs FORCE/NO FORCE + flag independence, BYPASSRLS/SUPERUSER,
      `row_security=off`, SET ROLE identity, policy validation errors,
      ALTER POLICY, security_invoker/definer views, ON CONFLICT USING error,
      SELECT-interplay on UPDATE, least-privilege role end-to-end.
- [x] Live end-to-end verified over pgwire with psql: least-privilege
      `app_rw` role (CREATE ROLE ... PASSWORD → credential sync → SCRAM/
      cleartext login → tenant fencing → blocked escalation).
- [ ] Browser sync e2e (`web/bicdb-client/test/rls-e2e.mjs`) extension for
      restrictive/role-identity variants — not run in this pass.
- [x] Docs: `docs/row-level-security.md` (new), `POSTGRES_COMPATIBILITY.md`
      RLS section rewritten.
