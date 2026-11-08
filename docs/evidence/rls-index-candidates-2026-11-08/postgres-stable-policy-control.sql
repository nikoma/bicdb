\set VERBOSITY verbose
BEGIN;
SET LOCAL enable_seqscan=off;
SET LOCAL jit=off;
CREATE SCHEMA rls_stable_488e4e8345;
CREATE ROLE rls_stable_488e4e8345_reader;
GRANT USAGE ON SCHEMA rls_stable_488e4e8345 TO rls_stable_488e4e8345_reader;
CREATE TABLE rls_stable_488e4e8345.records(id INTEGER,tenant TEXT);
CREATE INDEX tenant_lookup ON rls_stable_488e4e8345.records(tenant,id);
GRANT SELECT ON rls_stable_488e4e8345.records TO rls_stable_488e4e8345_reader;
CREATE FUNCTION rls_stable_488e4e8345.pl_stable() RETURNS TEXT LANGUAGE plpgsql STABLE AS $$ BEGIN RAISE EXCEPTION 'probe_pl_stable_executed'; END $$;
CREATE FUNCTION rls_stable_488e4e8345.sql_definer_stable() RETURNS TEXT LANGUAGE sql STABLE SECURITY DEFINER AS $$ SELECT (1/0)::text $$;
REVOKE ALL ON FUNCTION rls_stable_488e4e8345.pl_stable(),rls_stable_488e4e8345.sql_definer_stable() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION rls_stable_488e4e8345.pl_stable(),rls_stable_488e4e8345.sql_definer_stable() TO rls_stable_488e4e8345_reader;
ALTER TABLE rls_stable_488e4e8345.records ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_stable_488e4e8345.records FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_policy ON rls_stable_488e4e8345.records FOR SELECT TO rls_stable_488e4e8345_reader USING(tenant=(SELECT rls_stable_488e4e8345.pl_stable()));
\echo CASE empty pl_stable explain_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty pl_stable explain_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty pl_stable select_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty pl_stable select_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
DROP POLICY tenant_policy ON rls_stable_488e4e8345.records;
CREATE POLICY tenant_policy ON rls_stable_488e4e8345.records FOR SELECT TO rls_stable_488e4e8345_reader USING(tenant=(SELECT rls_stable_488e4e8345.sql_definer_stable()));
\echo CASE empty sql_definer_stable explain_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty sql_definer_stable explain_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty sql_definer_stable select_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE empty sql_definer_stable select_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
DROP POLICY tenant_policy ON rls_stable_488e4e8345.records;
INSERT INTO rls_stable_488e4e8345.records VALUES(1,'own');
CREATE POLICY tenant_policy ON rls_stable_488e4e8345.records FOR SELECT TO rls_stable_488e4e8345_reader USING(tenant=(SELECT rls_stable_488e4e8345.pl_stable()));
\echo CASE one_row pl_stable explain_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row pl_stable explain_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row pl_stable select_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row pl_stable select_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
DROP POLICY tenant_policy ON rls_stable_488e4e8345.records;
CREATE POLICY tenant_policy ON rls_stable_488e4e8345.records FOR SELECT TO rls_stable_488e4e8345_reader USING(tenant=(SELECT rls_stable_488e4e8345.sql_definer_stable()));
\echo CASE one_row sql_definer_stable explain_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row sql_definer_stable explain_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
EXPLAIN SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row sql_definer_stable select_false
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE false;
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_row sql_definer_stable select_normal
SAVEPOINT probe;
SET LOCAL ROLE rls_stable_488e4e8345_reader;
SELECT count(*) FROM rls_stable_488e4e8345.records WHERE id=1;
ROLLBACK TO SAVEPOINT probe;
DROP POLICY tenant_policy ON rls_stable_488e4e8345.records;
ROLLBACK;
