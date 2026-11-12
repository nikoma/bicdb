\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA partition_identity_reference;
CREATE TABLE partition_identity_reference.p(k INT,payload TEXT,CONSTRAINT parent_primary PRIMARY KEY(k),CONSTRAINT parent_unique UNIQUE(k,payload)) PARTITION BY RANGE(k);
CREATE TABLE partition_identity_reference.low PARTITION OF partition_identity_reference.p FOR VALUES FROM(0) TO(10);
SELECT rel.relname,c.contype,c.conname,idx.relname AS index_name FROM pg_constraint c JOIN pg_class rel ON rel.oid=c.conrelid JOIN pg_namespace ns ON ns.oid=rel.relnamespace LEFT JOIN pg_class idx ON idx.oid=c.conindid WHERE ns.nspname='partition_identity_reference' ORDER BY rel.relname,c.contype,c.conname;
CREATE TABLE partition_identity_reference.implicit_id(id INT,part INT) PARTITION BY RANGE(part);
CREATE TABLE partition_identity_reference.implicit_low PARTITION OF partition_identity_reference.implicit_id FOR VALUES FROM(0) TO(10);
INSERT INTO partition_identity_reference.implicit_id VALUES(1,2),(1,3);
SELECT id,part FROM partition_identity_reference.implicit_id ORDER BY part;
SELECT a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname='partition_identity_reference' AND c.relname='implicit_id' AND a.attname='id';
CREATE TABLE partition_identity_reference.high PARTITION OF partition_identity_reference.p FOR VALUES FROM(10) TO(20);
INSERT INTO partition_identity_reference.p VALUES(1,'original-ack');
CREATE TABLE partition_identity_reference.audit(seq BIGSERIAL,phase TEXT,k INT,rel TEXT,seen INT);
CREATE FUNCTION partition_identity_reference.capture_copy() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO partition_identity_reference.audit(phase,k,rel,seen) VALUES(TG_WHEN,NEW.k,TG_TABLE_NAME,(SELECT COUNT(*) FROM partition_identity_reference.p)); RETURN NEW; END$$;
CREATE TRIGGER copy_b BEFORE INSERT ON partition_identity_reference.p FOR EACH ROW EXECUTE FUNCTION partition_identity_reference.capture_copy();
CREATE TRIGGER copy_a AFTER INSERT ON partition_identity_reference.p FOR EACH ROW EXECUTE FUNCTION partition_identity_reference.capture_copy();
COPY partition_identity_reference.p(k,payload) FROM STDIN;
2	copy-2
12	copy-12
3	copy-3
\.
SELECT phase,k,rel,seen FROM partition_identity_reference.audit ORDER BY seq;
ROLLBACK;
