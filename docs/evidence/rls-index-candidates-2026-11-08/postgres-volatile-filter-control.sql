\set VERBOSITY verbose
BEGIN;
CREATE SCHEMA rls_volatile_5068b38197;
CREATE ROLE rls_volatile_5068b38197_reader;
GRANT USAGE ON SCHEMA rls_volatile_5068b38197 TO rls_volatile_5068b38197_reader;
CREATE TABLE rls_volatile_5068b38197.records(id INTEGER PRIMARY KEY,tenant TEXT,number TEXT,visible BOOLEAN);
CREATE INDEX tenant_number_lookup ON rls_volatile_5068b38197.records(tenant,number);
INSERT INTO rls_volatile_5068b38197.records VALUES(1,'own','N42',false),(2,'other','N42',true);
GRANT SELECT ON rls_volatile_5068b38197.records TO rls_volatile_5068b38197_reader;
CREATE FUNCTION rls_volatile_5068b38197.tenant() RETURNS TEXT LANGUAGE sql STABLE AS $$ SELECT 'own'::text $$;
CREATE FUNCTION rls_volatile_5068b38197.bad_number() RETURNS TEXT LANGUAGE sql VOLATILE SECURITY DEFINER AS $$ SELECT (1/0)::text $$;
REVOKE ALL ON FUNCTION rls_volatile_5068b38197.tenant(),rls_volatile_5068b38197.bad_number() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION rls_volatile_5068b38197.tenant(),rls_volatile_5068b38197.bad_number() TO rls_volatile_5068b38197_reader;
ALTER TABLE rls_volatile_5068b38197.records ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_volatile_5068b38197.records FORCE ROW LEVEL SECURITY;
CREATE POLICY visible_own ON rls_volatile_5068b38197.records FOR SELECT TO rls_volatile_5068b38197_reader USING(tenant=(SELECT rls_volatile_5068b38197.tenant()) AND visible);
\echo CASE all_own_rows_denied implicit
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
SELECT count(*) FROM rls_volatile_5068b38197.records WHERE number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
\echo CASE all_own_rows_denied explicit_tenant
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
SELECT count(*) FROM rls_volatile_5068b38197.records WHERE tenant='own' AND number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
\echo CASE all_own_rows_denied explain
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
EXPLAIN SELECT count(*) FROM rls_volatile_5068b38197.records WHERE number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
INSERT INTO rls_volatile_5068b38197.records VALUES(3,'own','N42',true);
\echo CASE one_own_row_visible implicit
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
SELECT count(*) FROM rls_volatile_5068b38197.records WHERE number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_own_row_visible explicit_tenant
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
SELECT count(*) FROM rls_volatile_5068b38197.records WHERE tenant='own' AND number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
\echo CASE one_own_row_visible explain
SAVEPOINT probe;
SET LOCAL ROLE rls_volatile_5068b38197_reader;
EXPLAIN SELECT count(*) FROM rls_volatile_5068b38197.records WHERE number=rls_volatile_5068b38197.bad_number();
ROLLBACK TO SAVEPOINT probe;
ROLLBACK;
