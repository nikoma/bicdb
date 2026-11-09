-- RLS write-path parity battery: INSERT ... ON CONFLICT, RETURNING and the
-- policy-versus-unique-key order. Run identically against BicDB and a stock
-- PostgreSQL server by scripts/rls-pg18-diff.sh:
--   BATTERY=scripts/rls_write_path_battery.sql scripts/rls-pg18-diff.sh
-- Keep statements deterministic: every RETURNING yields at most one row.
DROP TABLE IF EXISTS rls_wp_t CASCADE;
DROP TABLE IF EXISTS rls_wp_r CASCADE;
DROP ROLE IF EXISTS rls_wp_alice;
CREATE ROLE rls_wp_alice LOGIN;
CREATE TABLE rls_wp_t (id int PRIMARY KEY, org_id int, number int, label text, code int UNIQUE);
INSERT INTO rls_wp_t VALUES (1,1,1,'a',101),(2,1,2,'b',102),(15,2,12,'x',115),(16,2,3,'y',116);
ALTER TABLE rls_wp_t ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON rls_wp_t FOR SELECT USING (org_id = (SELECT current_setting('app.org')::int));
CREATE POLICY pu ON rls_wp_t FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY pi ON rls_wp_t FOR INSERT WITH CHECK (true);
CREATE POLICY pd ON rls_wp_t FOR DELETE USING (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON rls_wp_t TO rls_wp_alice;
SET app.org = '1';
SET ROLE rls_wp_alice;

-- ON CONFLICT DO UPDATE against another tenant's row: refused, row untouched.
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO UPDATE SET label = 'conf' RETURNING id, label;
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO UPDATE SET label = 'conf';
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO UPDATE SET label = excluded.label;
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT ON CONSTRAINT rls_wp_t_pkey DO UPDATE SET label = 'conf';
INSERT INTO rls_wp_t VALUES (60,1,1,'ins',115) ON CONFLICT (code) DO UPDATE SET label = 'conf';
BEGIN;
INSERT INTO rls_wp_t VALUES (2,1,2,'b2',102),(15,1,12,'ins',915) ON CONFLICT (id) DO UPDATE SET label = 'conf';
ROLLBACK;
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO UPDATE SET label = 'c' WHERE false;
-- The visible row may be upserted but not moved out of sight.
INSERT INTO rls_wp_t VALUES (1,1,1,'ins',901) ON CONFLICT (id) DO UPDATE SET org_id = 2;
INSERT INTO rls_wp_t VALUES (1,2,1,'ins',901) ON CONFLICT (id) DO UPDATE SET org_id = excluded.org_id RETURNING id;
INSERT INTO rls_wp_t VALUES (1,1,1,'ins',901) ON CONFLICT (id) DO UPDATE SET label = 'ok' RETURNING id, label;

-- ON CONFLICT DO NOTHING.
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO NOTHING;
INSERT INTO rls_wp_t VALUES (15,1,12,'ins',915) ON CONFLICT (id) DO NOTHING RETURNING id;
INSERT INTO rls_wp_t VALUES (15,2,12,'ins',915) ON CONFLICT (id) DO NOTHING;
INSERT INTO rls_wp_t VALUES (15,2,12,'ins',915) ON CONFLICT (id) DO NOTHING RETURNING id;
INSERT INTO rls_wp_t VALUES (70,2,12,'ins',970) ON CONFLICT ON CONSTRAINT rls_wp_t_pkey DO NOTHING;
INSERT INTO rls_wp_t VALUES (3,1,1,'ok',103),(71,2,12,'ins',971) ON CONFLICT (id) DO NOTHING;
INSERT INTO rls_wp_t VALUES (15,2,12,'ins',915) ON CONFLICT DO NOTHING;

-- RETURNING under the SELECT policy.
INSERT INTO rls_wp_t VALUES (30,2,12,'ins',930) RETURNING id;
INSERT INTO rls_wp_t VALUES (31,2,12,'ins',931) RETURNING id + 0;
INSERT INTO rls_wp_t SELECT 32,2,1,'s',932 RETURNING *;
INSERT INTO rls_wp_t VALUES (33,2,12,'ins',933) RETURNING 1;
INSERT INTO rls_wp_t VALUES (34,2,12,'ins',934);
UPDATE rls_wp_t SET label = 'u' WHERE id = 1 RETURNING id, label;
UPDATE rls_wp_t SET label = 'u' WHERE id = 15 RETURNING id, label;
UPDATE rls_wp_t SET org_id = 2 WHERE number = 1 RETURNING id;
UPDATE rls_wp_t SET org_id = 2 WHERE id = 2;
DELETE FROM rls_wp_t WHERE id = 15 RETURNING id;
BEGIN;
DELETE FROM rls_wp_t WHERE id IN (2, 15) RETURNING id;
ROLLBACK;
-- Statements that read no target column apply no SELECT policy.
BEGIN;
UPDATE rls_wp_t SET label = 'q' WHERE (SELECT true);
UPDATE rls_wp_t SET label = (SELECT 'z');
DELETE FROM rls_wp_t;
ROLLBACK;
BEGIN;
UPDATE rls_wp_t SET label = 'nowhere';
DELETE FROM rls_wp_t WHERE id = 15;
UPDATE rls_wp_t SET number = number + 0;
DELETE FROM rls_wp_t RETURNING 1 AS one;
ROLLBACK;

-- Policy check before the unique key it would reveal.
RESET ROLE;
DROP POLICY pi ON rls_wp_t;
CREATE POLICY pi ON rls_wp_t FOR INSERT WITH CHECK (org_id = (SELECT current_setting('app.org')::int));
DROP POLICY pu ON rls_wp_t;
CREATE POLICY pu ON rls_wp_t FOR UPDATE USING (true) WITH CHECK (org_id = (SELECT current_setting('app.org')::int));
SET ROLE rls_wp_alice;
INSERT INTO rls_wp_t VALUES (15,2,1,'ins',915);
INSERT INTO rls_wp_t VALUES (60,2,1,'ins',115);
INSERT INTO rls_wp_t VALUES (15,1,1,'ins',915);
INSERT INTO rls_wp_t VALUES (1,1,1,'d',901),(40,2,1,'p',940);
INSERT INTO rls_wp_t VALUES (40,2,1,'p',940),(1,1,1,'d',901);
INSERT INTO rls_wp_t VALUES (16,2,1,'ins',916) ON CONFLICT (id) DO NOTHING;
UPDATE rls_wp_t SET code = 115, org_id = 2 WHERE id = 1;
UPDATE rls_wp_t SET code = 115 WHERE id = 1;
RESET ROLE;
SELECT id, org_id, number, label, code FROM rls_wp_t ORDER BY id;

-- UPDATE USING and restrictive policy names on the conflict path.
CREATE TABLE rls_wp_r (id int PRIMARY KEY, org_id int, number int);
INSERT INTO rls_wp_r VALUES (1,1,50),(2,1,1),(5,2,1);
ALTER TABLE rls_wp_r ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON rls_wp_r FOR SELECT USING (true);
CREATE POLICY pi ON rls_wp_r FOR INSERT WITH CHECK (true);
CREATE POLICY pu ON rls_wp_r FOR UPDATE USING (number < 10);
CREATE POLICY r ON rls_wp_r AS RESTRICTIVE FOR UPDATE USING (org_id = 1);
CREATE POLICY ri ON rls_wp_r AS RESTRICTIVE FOR INSERT WITH CHECK (number < 100);
GRANT SELECT, INSERT, UPDATE ON rls_wp_r TO rls_wp_alice;
SET ROLE rls_wp_alice;
INSERT INTO rls_wp_r VALUES (1,1,1) ON CONFLICT (id) DO UPDATE SET number = 2;
INSERT INTO rls_wp_r VALUES (5,1,1) ON CONFLICT (id) DO UPDATE SET number = 2;
INSERT INTO rls_wp_r VALUES (2,1,1) ON CONFLICT (id) DO UPDATE SET number = 3 RETURNING id, number;
INSERT INTO rls_wp_r VALUES (2,1,1) ON CONFLICT (id) DO UPDATE SET org_id = 2;
INSERT INTO rls_wp_r VALUES (2,1,500) ON CONFLICT (id) DO UPDATE SET number = 4;
INSERT INTO rls_wp_r VALUES (1,1,500);
RESET ROLE;
SELECT id, org_id, number FROM rls_wp_r ORDER BY id;

-- Cleanup.
RESET app.org;
DROP TABLE rls_wp_t CASCADE;
DROP TABLE rls_wp_r CASCADE;
DROP ROLE rls_wp_alice;
