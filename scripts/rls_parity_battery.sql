-- RLS parity battery, run identically against BicDB and the postgres:18.4
-- oracle by scripts/rls-pg18-diff.sh. Keep statements deterministic and
-- ordered; avoid anything with server-specific output (version, oids, ...).
DROP TABLE IF EXISTS rls_diff_t CASCADE;
DROP ROLE IF EXISTS rls_diff_alice;
DROP ROLE IF EXISTS rls_diff_bob;
DROP ROLE IF EXISTS rls_diff_auditors;
CREATE ROLE rls_diff_alice LOGIN;
CREATE ROLE rls_diff_bob LOGIN;
CREATE ROLE rls_diff_auditors;
GRANT rls_diff_auditors TO rls_diff_bob;
CREATE TABLE rls_diff_t (id int PRIMARY KEY, tenant text, label text);
GRANT SELECT, INSERT, UPDATE, DELETE ON rls_diff_t TO rls_diff_alice, rls_diff_bob;
INSERT INTO rls_diff_t VALUES (1, 'rls_diff_alice', 'a'), (2, 'rls_diff_bob', 'b');
ALTER TABLE rls_diff_t ENABLE ROW LEVEL SECURITY;

-- Default deny for non-owner without policies.
SET ROLE rls_diff_alice;
SELECT count(*) AS default_deny FROM rls_diff_t;
RESET ROLE;

-- Permissive + restrictive combination.
CREATE POLICY p_all ON rls_diff_t USING (true);
CREATE POLICY p_restrict ON rls_diff_t AS RESTRICTIVE USING (tenant = current_user);
SET ROLE rls_diff_alice;
SELECT id FROM rls_diff_t ORDER BY id;
RESET ROLE;

-- TO-role matching through membership.
DROP POLICY p_restrict ON rls_diff_t;
DROP POLICY p_all ON rls_diff_t;
CREATE POLICY p_own ON rls_diff_t FOR SELECT TO rls_diff_alice USING (tenant = current_user);
CREATE POLICY p_audit ON rls_diff_t FOR SELECT TO rls_diff_auditors USING (true);
SET ROLE rls_diff_alice;
SELECT id FROM rls_diff_t ORDER BY id;
SET ROLE rls_diff_bob;
SELECT count(*) AS audit_sees_all FROM rls_diff_t;
RESET ROLE;

-- WITH CHECK enforcement and named restrictive violations.
CREATE POLICY p_write ON rls_diff_t FOR INSERT WITH CHECK (tenant = current_user);
SET ROLE rls_diff_alice;
INSERT INTO rls_diff_t VALUES (3, 'rls_diff_bob', 'forged');
INSERT INTO rls_diff_t VALUES (4, 'rls_diff_alice', 'ok');
RESET ROLE;

-- Creation-time validation.
CREATE POLICY bad1 ON rls_diff_t FOR INSERT USING (true);
CREATE POLICY bad2 ON rls_diff_t FOR SELECT WITH CHECK (true);
CREATE POLICY bad3 ON rls_diff_t USING (count(*) > 0);
CREATE POLICY bad4 ON rls_diff_t USING (nosuchcol = 1);
CREATE POLICY p_own ON rls_diff_t FOR SELECT USING (true);
DROP POLICY nonexistent ON rls_diff_t;

-- Flag independence.
ALTER TABLE rls_diff_t FORCE ROW LEVEL SECURITY;
ALTER TABLE rls_diff_t DISABLE ROW LEVEL SECURITY;
SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = 'rls_diff_t';
ALTER TABLE rls_diff_t NO FORCE ROW LEVEL SECURITY;
ALTER TABLE rls_diff_t ENABLE ROW LEVEL SECURITY;

-- row_security = off.
SET ROLE rls_diff_alice;
SET row_security = off;
SELECT count(*) FROM rls_diff_t;
RESET row_security;
RESET ROLE;

-- SET ROLE permission checks (checked against the session user).
SET SESSION AUTHORIZATION rls_diff_alice;
SET ROLE rls_diff_bob;
RESET SESSION AUTHORIZATION;

-- pg_policies exposure.
SELECT policyname, permissive, roles, cmd FROM pg_policies
WHERE tablename = 'rls_diff_t' ORDER BY policyname;

-- Transactional custom-GUC state. Keep every observation explicitly labeled
-- so psql output remains useful when the same battery is diffed server-to-server.
SET app.rls_identity = 'regular-base';
BEGIN;
SET app.rls_identity = 'regular-commit';
COMMIT;
SELECT 'guc_regular_set_after_commit' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SET app.rls_identity = 'regular-rollback';
ROLLBACK;
SELECT 'guc_regular_set_after_rollback' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SET LOCAL app.rls_identity = 'set-local-commit';
SELECT 'guc_set_local_during_commit_tx' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;
COMMIT;
SELECT 'guc_set_local_after_commit' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SET LOCAL app.rls_identity = 'set-local-rollback';
SELECT 'guc_set_local_during_rollback_tx' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;
ROLLBACK;
SELECT 'guc_set_local_after_rollback' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SELECT 'guc_set_config_local_during_commit_tx' AS probe,
       set_config('app.rls_identity', 'set-config-commit', true) AS value;
COMMIT;
SELECT 'guc_set_config_local_after_commit' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SELECT 'guc_set_config_local_during_rollback_tx' AS probe,
       set_config('app.rls_identity', 'set-config-rollback', true) AS value;
ROLLBACK;
SELECT 'guc_set_config_local_after_rollback' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

BEGIN;
SET LOCAL app.rls_identity = 'before-savepoint';
SAVEPOINT guc_identity_before_change;
SET app.rls_identity = 'after-savepoint';
SELECT 'guc_savepoint_before_rollback' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;
ROLLBACK TO SAVEPOINT guc_identity_before_change;
SELECT 'guc_savepoint_after_rollback' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;
COMMIT;
SELECT 'guc_savepoint_after_commit' AS probe,
       coalesce(current_setting('app.rls_identity', true), '<unset>') AS value;

-- A transaction-local policy identity must not authorize the next request on
-- the same connection after commit.
DROP POLICY p_own ON rls_diff_t;
DROP POLICY p_audit ON rls_diff_t;
DROP POLICY p_write ON rls_diff_t;
CREATE POLICY p_guc_identity ON rls_diff_t FOR SELECT
  USING (tenant = current_setting('app.rls_identity', true));
RESET app.rls_identity;
SET ROLE rls_diff_alice;
BEGIN;
SELECT set_config('app.rls_identity', 'rls_diff_alice', true);
SELECT 'guc_identity_rows_during_tx' AS probe, count(*) AS visible_rows
FROM rls_diff_t;
COMMIT;
SELECT 'guc_identity_rows_after_commit' AS probe, count(*) AS visible_rows
FROM rls_diff_t;
RESET ROLE;

-- Cleanup.
DROP TABLE rls_diff_t CASCADE;
DROP ROLE rls_diff_alice;
DROP ROLE rls_diff_bob;
DROP ROLE rls_diff_auditors;
