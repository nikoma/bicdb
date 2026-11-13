\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA partition_sequence_reference;
SET LOCAL search_path=partition_sequence_reference,pg_catalog;
CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k);
CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
INSERT INTO p VALUES(1,'low-ack'),(11,'high-ack');
DO $$DECLARE rows json; BEGIN
 BEGIN
  EXECUTE 'ALTER TABLE p ADD COLUMN sid SERIAL';
  RAISE NOTICE 'serial_add=accepted';
  EXECUTE 'SELECT json_agg(t) FROM (SELECT k,sid FROM p ORDER BY k)t' INTO rows;
  RAISE NOTICE 'serial_rows=%',rows;
  INSERT INTO p(k,payload) VALUES(2,'parent');
  INSERT INTO low(k,payload) VALUES(3,'low');
  INSERT INTO high(k,payload) VALUES(12,'high');
  EXECUTE 'SELECT json_agg(t) FROM (SELECT k,sid FROM p ORDER BY k)t' INTO rows;
  RAISE NOTICE 'serial_after_inserts=%',rows;
 EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'serial_add_or_insert_refused=% %',SQLSTATE,SQLERRM;
 END;
 BEGIN
  EXECUTE 'ALTER TABLE p ADD COLUMN ident BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)';
  RAISE NOTICE 'identity_add=accepted';
  EXECUTE 'SELECT json_agg(t) FROM (SELECT k,ident FROM p ORDER BY k)t' INTO rows;
  RAISE NOTICE 'identity_rows=%',rows;
  INSERT INTO p(k,payload) VALUES(4,'parent-identity');
  RAISE NOTICE 'identity_parent_insert=accepted';
  BEGIN
   INSERT INTO low(k,payload) VALUES(5,'low-identity');
   RAISE NOTICE 'identity_child_insert=accepted';
  EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'identity_child_insert_refused=% %',SQLSTATE,SQLERRM;
  END;
  EXECUTE 'SELECT json_agg(t) FROM (SELECT k,ident FROM p ORDER BY k)t' INTO rows;
  RAISE NOTICE 'identity_after_inserts=%',rows;
 EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'identity_add_or_insert_refused=% %',SQLSTATE,SQLERRM;
 END;
END$$;
SELECT table_name,column_name,column_default,is_identity,identity_generation FROM information_schema.columns WHERE table_schema='partition_sequence_reference' AND column_name IN('sid','ident') ORDER BY table_name,column_name;
CREATE TABLE empty_parent(k INT) PARTITION BY RANGE(k);
ALTER TABLE empty_parent ADD COLUMN ident INT GENERATED ALWAYS AS IDENTITY;
CREATE TABLE empty_low PARTITION OF empty_parent FOR VALUES FROM(0) TO(10);
INSERT INTO empty_parent(k) VALUES(1);
DO $$ BEGIN
 BEGIN
  INSERT INTO empty_low(k) VALUES(2);
  RAISE NOTICE 'empty_parent_identity_child_insert=accepted';
 EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'empty_parent_identity_child_insert_refused=% %',SQLSTATE,SQLERRM;
 END;
END$$;
SELECT table_name,column_name,column_default,is_identity,identity_generation FROM information_schema.columns WHERE table_schema='partition_sequence_reference' AND table_name IN('empty_parent','empty_low') AND column_name='ident' ORDER BY table_name;
SELECT 'empty_identity',k,ident FROM empty_parent ORDER BY k;
ROLLBACK;
