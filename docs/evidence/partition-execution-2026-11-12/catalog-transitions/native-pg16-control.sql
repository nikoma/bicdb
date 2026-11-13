\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA partition_broad_reference;
SET LOCAL search_path=partition_broad_reference,pg_catalog;
DO $$BEGIN
 BEGIN
  EXECUTE 'CREATE TABLE invalid_hash(id bigint PRIMARY KEY,partition_id bigint NOT NULL) PARTITION BY HASH(partition_id)';
  RAISE EXCEPTION 'accepted PK missing hash partition key';
 EXCEPTION WHEN feature_not_supported THEN RAISE NOTICE 'original hash PK rejected: %',SQLSTATE;
 END;
 BEGIN
  EXECUTE 'CREATE TABLE invalid_range(id bigint PRIMARY KEY,created_at timestamptz NOT NULL) PARTITION BY RANGE(created_at)';
  RAISE EXCEPTION 'accepted PK missing range partition key';
 EXCEPTION WHEN feature_not_supported THEN RAISE NOTICE 'original range PK rejected: %',SQLSTATE;
 END;
END$$;
CREATE TABLE p_ci_build_names(build_id bigint NOT NULL,partition_id bigint NOT NULL,project_id bigint NOT NULL,name text NOT NULL,search_vector tsvector GENERATED ALWAYS AS(to_tsvector('english'::regconfig,COALESCE(name,''::text))) STORED,CONSTRAINT check_1722c96346 CHECK(char_length(name)<=255)) PARTITION BY LIST(partition_id);
CREATE TABLE backup_finding_evidences(created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,original_record_identifier bigint NOT NULL,finding_id bigint NOT NULL,project_id bigint NOT NULL,date date NOT NULL,data jsonb NOT NULL) PARTITION BY RANGE(date);
DO $$BEGIN
 BEGIN
  INSERT INTO p_ci_build_names(build_id,partition_id,project_id,name) VALUES(1,101,5,'build');
  RAISE EXCEPTION 'accepted parent heap without LIST leaf';
 EXCEPTION WHEN check_violation THEN RAISE NOTICE 'original LIST insert rejected: %',SQLSTATE;
 END;
 BEGIN
  INSERT INTO backup_finding_evidences VALUES('2026-06-21 00:00:00+00','2026-06-21 00:00:00+00',42,7,3,'2026-06-21','{"state":"ok"}');
  RAISE EXCEPTION 'accepted parent heap without RANGE leaf';
 EXCEPTION WHEN check_violation THEN RAISE NOTICE 'original RANGE insert rejected: %',SQLSTATE;
 END;
END$$;
CREATE TABLE build_names_101 PARTITION OF p_ci_build_names FOR VALUES IN(101);
INSERT INTO p_ci_build_names(build_id,partition_id,project_id,name) VALUES(1,101,5,'build');
CREATE TABLE finding_evidences_2026 PARTITION OF backup_finding_evidences FOR VALUES FROM('2026-01-01') TO('2027-01-01');
INSERT INTO backup_finding_evidences VALUES('2026-06-21 00:00:00+00','2026-06-21 00:00:00+00',42,7,3,'2026-06-21','{"state":"ok"}');
CREATE TABLE valid_hash(id bigint,partition_id bigint NOT NULL,PRIMARY KEY(id,partition_id)) PARTITION BY HASH(partition_id);
CREATE TABLE hash_leaf(id bigint,partition_id bigint NOT NULL,PRIMARY KEY(id,partition_id));
ALTER TABLE valid_hash ATTACH PARTITION hash_leaf FOR VALUES WITH(MODULUS 32,REMAINDER 0);
CREATE TABLE valid_range(id bigint,created_at timestamptz NOT NULL,PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
SELECT pg_get_partkeydef('valid_range'::regclass);
CREATE TABLE measurements(bucket int,raw text,qty int) PARTITION BY LIST(bucket);
CREATE TABLE measurements_1 PARTITION OF measurements FOR VALUES IN(1);
INSERT INTO measurements_1 VALUES(1,'12',3);
ALTER TABLE measurements ALTER COLUMN raw TYPE int USING raw::int+qty;
SELECT raw,pg_typeof(raw)::text FROM measurements_1;
ALTER TABLE measurements ADD COLUMN digest text;
DO $$BEGIN
 IF (SELECT count(*) FROM p_ci_build_names)<>1 OR (SELECT count(*) FROM backup_finding_evidences)<>1 OR (SELECT raw FROM measurements_1)<>15 OR NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid='measurements_1'::regclass AND attname='digest') THEN RAISE EXCEPTION 'corrected reference failed';END IF;
END$$;
ROLLBACK;
