\set VERBOSITY verbose
BEGIN;
SET LOCAL jit=off;
CREATE SCHEMA rls_unused_keys_20261108;
CREATE ROLE rls_unused_keys_20261108_reader;
GRANT USAGE ON SCHEMA rls_unused_keys_20261108 TO rls_unused_keys_20261108_reader;
CREATE TABLE rls_unused_keys_20261108.orders(id TEXT PRIMARY KEY,org_id TEXT,number TEXT,region TEXT);
CREATE UNIQUE INDEX by_number ON rls_unused_keys_20261108.orders(number);
CREATE INDEX by_tenant_number ON rls_unused_keys_20261108.orders(org_id);
CREATE INDEX by_region_number ON rls_unused_keys_20261108.orders(region);
INSERT INTO rls_unused_keys_20261108.orders SELECT n::text,'own',n::text,'west' FROM generate_series(1,10000) n;
ANALYZE rls_unused_keys_20261108.orders;
CREATE FUNCTION rls_unused_keys_20261108.failing_key() RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT (1/0)::text $$;
GRANT SELECT,UPDATE ON rls_unused_keys_20261108.orders TO rls_unused_keys_20261108_reader;
ALTER TABLE rls_unused_keys_20261108.orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY one_key ON rls_unused_keys_20261108.orders TO rls_unused_keys_20261108_reader USING(org_id=(SELECT rls_unused_keys_20261108.failing_key()));
SET LOCAL ROLE rls_unused_keys_20261108_reader;
\echo CASE single_policy_key_existing_number_index
EXPLAIN SELECT id FROM rls_unused_keys_20261108.orders WHERE number='missing' FOR UPDATE;
SELECT id FROM rls_unused_keys_20261108.orders WHERE number='missing' FOR UPDATE;
RESET ROLE;
DROP POLICY one_key ON rls_unused_keys_20261108.orders;
CREATE POLICY two_keys ON rls_unused_keys_20261108.orders TO rls_unused_keys_20261108_reader USING(org_id=(SELECT current_setting('app.tenant',true)) AND region=(SELECT rls_unused_keys_20261108.failing_key()));
SET LOCAL app.tenant='own';
SET LOCAL ROLE rls_unused_keys_20261108_reader;
\echo CASE multiple_policy_keys_existing_number_index
EXPLAIN SELECT id FROM rls_unused_keys_20261108.orders WHERE number='missing' FOR UPDATE;
SELECT id FROM rls_unused_keys_20261108.orders WHERE number='missing' FOR UPDATE;
ROLLBACK;
