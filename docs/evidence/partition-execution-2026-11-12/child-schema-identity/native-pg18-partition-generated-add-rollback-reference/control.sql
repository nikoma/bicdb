\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA partition_generated_rollback;
SET search_path=partition_generated_rollback,public;
CREATE TABLE p(k int PRIMARY KEY,payload text) PARTITION BY RANGE(k);
CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
INSERT INTO p VALUES(1,'low-ack'),(11,'high-ack');
DO $$BEGIN
  BEGIN
    ALTER TABLE p ADD COLUMN ident INT GENERATED ALWAYS AS IDENTITY (START WITH 1 MAXVALUE 1 NO CYCLE);
    RAISE EXCEPTION 'expected sequence exhaustion';
  EXCEPTION WHEN SQLSTATE '2200H' THEN RAISE NOTICE 'exhaustion_sqlstate=2200H';
  END;
  IF to_regclass('p_ident_seq') IS NOT NULL THEN RAISE EXCEPTION 'failed ADD left sequence'; END IF;
  IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='partition_generated_rollback' AND column_name='ident') THEN RAISE EXCEPTION 'failed ADD left column'; END IF;
  IF (SELECT array_agg(payload ORDER BY k) FROM p) IS DISTINCT FROM ARRAY['low-ack','high-ack'] THEN RAISE EXCEPTION 'ACKs changed'; END IF;
END$$;
CREATE ROLE partition_generated_seq_reader;
ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO partition_generated_seq_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA partition_generated_rollback GRANT USAGE ON SEQUENCES TO partition_generated_seq_reader;
SAVEPOINT s;
ALTER TABLE p ADD COLUMN ident BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5);
DO $$BEGIN
 IF NOT has_sequence_privilege('partition_generated_seq_reader','p_ident_seq','USAGE') THEN RAISE EXCEPTION 'default ACL missing'; END IF;
 IF (SELECT count(DISTINCT ident) FROM p)<>2 THEN RAISE EXCEPTION 'identity backfill missing'; END IF;
END$$;
INSERT INTO p(k,payload) VALUES(2,'transient');
ROLLBACK TO s;
DO $$BEGIN
 IF to_regclass('p_ident_seq') IS NOT NULL THEN RAISE EXCEPTION 'rollback left sequence'; END IF;
 IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='partition_generated_rollback' AND column_name='ident') THEN RAISE EXCEPTION 'rollback left column'; END IF;
 IF (SELECT array_agg(payload ORDER BY k) FROM p) IS DISTINCT FROM ARRAY['low-ack','high-ack'] THEN RAISE EXCEPTION 'rollback changed ACKs'; END IF;
END$$;
ALTER TABLE p ADD COLUMN serial_value BIGSERIAL;
DO $$BEGIN
 IF (SELECT count(DISTINCT serial_value) FROM p)<>2 OR (SELECT min(serial_value) FROM p)<>1 OR (SELECT max(serial_value) FROM p)<>2 THEN RAISE EXCEPTION 'serial backfill missing'; END IF;
END$$;
INSERT INTO p(k,payload) VALUES(2,'routed');
INSERT INTO low(k,payload) VALUES(3,'direct-low');
INSERT INTO high(k,payload) VALUES(12,'direct-high');
DO $$BEGIN
 IF (SELECT array_agg(serial_value ORDER BY k) FROM p WHERE k IN(2,3,12)) IS DISTINCT FROM ARRAY[3,4,5]::bigint[] THEN RAISE EXCEPTION 'shared serial allocation mismatch'; END IF;
END$$;
SELECT 'ALL_PG18_GENERATED_ADD_ROLLBACK_ASSERTIONS_PASSED';
ROLLBACK;
