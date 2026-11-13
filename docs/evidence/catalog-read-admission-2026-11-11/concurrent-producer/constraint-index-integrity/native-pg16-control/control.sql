\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA root_drop_index_control_20261112;
SET LOCAL search_path=root_drop_index_control_20261112;
CREATE TABLE guarded(item_key INT CONSTRAINT keep_pk PRIMARY KEY,code INT CONSTRAINT keep_unique UNIQUE,payload TEXT);
INSERT INTO guarded VALUES(1,10,'acknowledged');
CREATE INDEX free_idx ON guarded(payload);
DO $$ DECLARE command TEXT; BEGIN
 FOREACH command IN ARRAY ARRAY['DROP INDEX keep_pk','DROP INDEX IF EXISTS keep_unique','DROP INDEX keep_pk CASCADE','DROP INDEX keep_unique CASCADE','DROP INDEX free_idx,keep_pk'] LOOP
  BEGIN
   EXECUTE command;
   RAISE EXCEPTION 'unexpectedly accepted %',command;
  EXCEPTION WHEN dependent_objects_still_exist THEN
   RAISE NOTICE '% | 2BP01',command;
  END;
 END LOOP;
 IF to_regclass('free_idx') IS NULL THEN RAISE EXCEPTION 'first index lost during rejected multi-drop'; END IF;
END $$;
SELECT count(*) AS acknowledged FROM guarded WHERE item_key=1 AND code=10 AND payload='acknowledged';
ROLLBACK;
