# Row-Level Security

BicDB implements PostgreSQL-compatible row-level security (RLS). Behaviors in
this document were verified against a stock `postgres:18.4` oracle (see
`../compose.pg18.yml` and the sibling `bicdb-pg18-compat` setup); the Rust test
suite (`crates/bicdb-sql/tests/sql.rs`, tests prefixed `rls_`) pins them.

## What is enforced

- `CREATE POLICY name ON table [AS PERMISSIVE | RESTRICTIVE]
  [FOR ALL | SELECT | INSERT | UPDATE | DELETE] [TO role, ...]
  [USING (expr)] [WITH CHECK (expr)]` — `FOR` defaults to `ALL`, `TO` defaults
  to `PUBLIC`. Creation-time validation matches PostgreSQL: `USING` is rejected
  for `INSERT`, `WITH CHECK` for `SELECT`/`DELETE`, aggregates are rejected,
  and column references are checked against the table.
- `ALTER POLICY ... RENAME TO ...` and
  `ALTER POLICY ... [TO roles] [USING (...)] [WITH CHECK (...)]`.
- `DROP POLICY [IF EXISTS] name ON table [CASCADE | RESTRICT]`.
- `ALTER TABLE ... ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY` —
  the enabled and forced flags are independent, mirroring
  `pg_class.relrowsecurity` / `relforcerowsecurity` (`DISABLE` does not clear
  the force flag).
- Visibility = (OR of applicable permissive policies) AND (AND of applicable
  restrictive policies); no applicable permissive policy means default deny.
  A policy with no expression for a given side contributes nothing, like a
  NULL `polqual`.
- `WITH CHECK` is enforced on INSERT and UPDATE (falling back to `USING` when
  absent, PostgreSQL's rule). Failing a named restrictive policy reports
  `new row violates row-level security policy "name" for table "t"`.
- `INSERT ... ON CONFLICT DO UPDATE`: a conflicting existing row that fails the
  UPDATE policy's `USING` raises
  `new row violates row-level security policy (USING expression) ...` instead
  of the silent skip a plain UPDATE performs.
- UPDATE/DELETE statements that read existing column values (in `WHERE`,
  assignment right-hand sides, `RETURNING`, `FROM`/`USING`) additionally apply
  SELECT policies to those reads.
- `COPY table TO STDOUT` applies SELECT policies; `COPY table FROM STDIN`
  applies INSERT `WITH CHECK` policies.

## Identity model

Session identity follows PostgreSQL's GUC model:

- The pgwire server seeds `session_authorization` from the authenticated
  startup user. `current_user` / `session_user` / `current_role` reflect it.
- `SET ROLE` / `RESET ROLE` change `current_user`; permission requires the
  session user to be a member (directly or transitively) of the target role.
- `SET SESSION AUTHORIZATION` is limited to sessions whose connect-time user
  is a superuser role (the embedded bootstrap identity qualifies).
- `RESET ALL` preserves the session identity, like PostgreSQL.
- Custom dotted GUCs (`SET app.tenant = '...'`, `current_setting('app.tenant',
  true)`) work for GUC-driven policies and remain the recommended pattern for
  multi-tenant fencing.
- Regular `SET` assignments made inside a transaction survive a successful
  commit and are restored on rollback. `SET LOCAL` and
  `set_config(name, value, true)` are transaction-local: they are restored on
  commit or rollback, and rollback to a savepoint restores the state captured
  there. A local assignment in autocommit is visible only to its statement and
  does not remain active for the next request.
- Pgwire's per-connection memory limit includes the effective GUC state, the
  transaction-start and local-restore state, and complete GUC snapshots held
  by savepoints. The estimator deliberately charges conservative hash-table
  and string-allocation estimates so client-controlled custom GUCs cannot use
  savepoints to retain unaccounted memory.
- Embedded sessions (`SqlSession::new`) run as the bootstrap role `bicdb`.

Bypass rules, in PostgreSQL's order:

1. A server-level `SecurityContext::bypass_policy` (embedded escape hatch)
   skips RLS entirely.
2. Roles with `SUPERUSER` or `BYPASSRLS` skip RLS.
3. The table owner skips RLS unless the table has
   `FORCE ROW LEVEL SECURITY`. Tables created before ownership tracking are
   owned by the bootstrap role.
4. `SET row_security = off` raises
   `query would be affected by row-level security policy ...` when policies
   would otherwise apply.

## Least-privilege application roles

The recommended deployment pattern (verified end-to-end over pgwire):

```sql
CREATE ROLE app_rw LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'app-secret';
GRANT SELECT, INSERT, UPDATE, DELETE ON invoices TO app_rw;
ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoices FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON invoices
  USING (tenant = current_setting('app.tenant', true))
  WITH CHECK (tenant = current_setting('app.tenant', true));
```

Install request identity locally inside the same transaction as the protected
work so a pooled connection cannot carry it into the next request:

```sql
BEGIN;
SELECT set_config('app.tenant', 'tenant-42', true);
SELECT * FROM invoices;
COMMIT; -- app.tenant is restored before the connection is reused
```

`CREATE ROLE ... PASSWORD` statements executed over pgwire are mirrored into
the server credential store, so `app_rw` can authenticate immediately (both
cleartext and SCRAM-SHA-256). Pre-hashed passwords
(`PASSWORD 'SCRAM-SHA-256$...'`) cannot be imported and log a warning — use a
plaintext literal in the bootstrap migration or `bicdb user create`.

Because `app_rw` does not own the tables, policies bind for it even without
`FORCE`; `FORCE` additionally binds the owner.

## Views

- `CREATE VIEW ... WITH (security_invoker = true | false)` and
  `security_barrier` are parsed and persisted.
- Default (definer) views decide RLS on the underlying tables as the **view
  owner** (owner bypass, superuser/BYPASSRLS, `TO` role matching), while
  policy expressions still evaluate with the invoking session's identity and
  GUCs — this matches PostgreSQL 18 behavior, verified against the oracle.
- `security_invoker = true` applies the querying session's RLS.
- BicDB always evaluates RLS row filters before user predicates, so
  `security_barrier` has no separate planner effect.

## Catalogs

- `pg_policy`: `polcmd`, `polpermissive`, `polroles` (role OIDs, `{0}` for
  PUBLIC), `polqual`, `polwithcheck`, plus the BicDB extension column
  `bicdb_enforced`.
- `pg_policies`: `permissive`, `roles` (role names), `cmd`, `qual`,
  `with_check`.
- `pg_class.relrowsecurity` / `relforcerowsecurity`.
- Policy expressions are stored and displayed in deparsed form (like
  PostgreSQL's `pg_get_expr`), not the original source text.

Stock PostgreSQL 18.4 `pg_dump` can run its read-only repeatable-read snapshot
and inspect these catalogs. Full, schema-only, and data-only archive round trips
are covered by `scripts/pg18-dump-restore.sh`.

## Known divergences from PostgreSQL 18

The transaction-local guarantees above are covered by embedded SQL, pgwire,
and differential probes. They do not imply literal 100% PostgreSQL parity;
the following documented divergences still apply.

- The bootstrap role `bicdb` is the implicit owner of legacy tables and may
  administer roles, but it does **not** get superuser RLS bypass: with
  `FORCE ROW LEVEL SECURITY` policies bind even for it. This keeps the
  embedded, GUC-driven multi-tenant pattern enforceable.
- Table privileges recorded by `GRANT`/`REVOKE` are catalog metadata; they are
  not enforced at query time. RLS is the enforcement layer.
- Partitions inherit the parent's policies and flags so that scans through
  the parent stay fenced; PostgreSQL instead applies only the queried table's
  policies.
- `MERGE` is unsupported at the statement level, so MERGE-RLS semantics are
  not applicable yet.
- Statement texts for policy DDL executed through the extended protocol
  (prepared statements) are not sniffed for credential sync; run role
  bootstrap DDL through the simple protocol.
