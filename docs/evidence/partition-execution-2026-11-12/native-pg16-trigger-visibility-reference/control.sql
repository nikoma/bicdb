\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA root_part_visibility_8b890ec8b7e5;
SET LOCAL search_path=root_part_visibility_8b890ec8b7e5;
CREATE TABLE audit(seq BIGSERIAL,op TEXT,phase TEXT,rel TEXT,oldk INT,newk INT,low_n INT,high_n INT,parent_n INT);
CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k);
CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
CREATE FUNCTION audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO audit(op,phase,rel,oldk,newk,low_n,high_n,parent_n)
 VALUES(TG_OP,TG_WHEN,TG_TABLE_NAME,OLD.k,NEW.k,(SELECT COUNT(*) FROM low),(SELECT COUNT(*) FROM high),(SELECT COUNT(*) FROM p));
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END$$;
CREATE FUNCTION veto() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$;
CREATE TRIGGER parent_b BEFORE INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn();
CREATE TRIGGER parent_a AFTER INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn();
SAVEPOINT insert_multi;

DELETE FROM audit;

WITH changed AS (INSERT INTO p VALUES(1,'first-ack'),(11,'second-ack') RETURNING k,payload) SELECT 'insert_multi_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'insert_multi_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'insert_multi_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT insert_multi;
SAVEPOINT move;
INSERT INTO p VALUES(1,'original-ack');
DELETE FROM audit;

WITH changed AS (UPDATE p SET k=11 WHERE k=1 RETURNING k,payload) SELECT 'move_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'move_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'move_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT move;
SAVEPOINT veto_source_update;
INSERT INTO p VALUES(1,'original-ack');
DELETE FROM audit;
CREATE TRIGGER z_veto BEFORE UPDATE ON low FOR EACH ROW EXECUTE FUNCTION veto();
WITH changed AS (UPDATE p SET k=11 WHERE k=1 RETURNING k,payload) SELECT 'veto_source_update_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'veto_source_update_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'veto_source_update_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT veto_source_update;
SAVEPOINT veto_source_delete;
INSERT INTO p VALUES(1,'original-ack');
DELETE FROM audit;
CREATE TRIGGER z_veto BEFORE DELETE ON low FOR EACH ROW EXECUTE FUNCTION veto();
WITH changed AS (UPDATE p SET k=11 WHERE k=1 RETURNING k,payload) SELECT 'veto_source_delete_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'veto_source_delete_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'veto_source_delete_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT veto_source_delete;
SAVEPOINT veto_destination_insert;
INSERT INTO p VALUES(1,'original-ack');
DELETE FROM audit;
CREATE TRIGGER z_veto BEFORE INSERT ON high FOR EACH ROW EXECUTE FUNCTION veto();
WITH changed AS (UPDATE p SET k=11 WHERE k=1 RETURNING k,payload) SELECT 'veto_destination_insert_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'veto_destination_insert_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'veto_destination_insert_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT veto_destination_insert;
SAVEPOINT update_multi;
INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack');
DELETE FROM audit;

WITH changed AS (UPDATE p SET k=k+10 RETURNING k,payload) SELECT 'update_multi_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'update_multi_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'update_multi_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT update_multi;
SAVEPOINT delete_multi;
INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack');
DELETE FROM audit;

WITH changed AS (DELETE FROM p RETURNING k,payload) SELECT 'delete_multi_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT 'delete_multi_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT 'delete_multi_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT delete_multi;
ROLLBACK;
SELECT 'scratch_schema_absent',NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='root_part_visibility_8b890ec8b7e5');
