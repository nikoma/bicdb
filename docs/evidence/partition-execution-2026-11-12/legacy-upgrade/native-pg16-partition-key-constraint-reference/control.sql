\set ON_ERROR_STOP on
BEGIN;CREATE SCHEMA root_key_a76143d393;SET LOCAL search_path=root_key_a76143d393;CREATE TABLE evidence(label TEXT,state TEXT,message TEXT);
DO $$BEGIN BEGIN CREATE TABLE bad_pk(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(payload);INSERT INTO evidence VALUES('primary_key','00000','accepted');EXCEPTION WHEN OTHERS THEN INSERT INTO evidence VALUES('primary_key',SQLSTATE,SQLERRM);END;END$$;
DO $$BEGIN BEGIN CREATE TABLE bad_unique(k INT,payload TEXT,UNIQUE(payload)) PARTITION BY RANGE(k);INSERT INTO evidence VALUES('unique','00000','accepted');EXCEPTION WHEN OTHERS THEN INSERT INTO evidence VALUES('unique',SQLSTATE,SQLERRM);END;END$$;
SELECT jsonb_agg(jsonb_build_object('label',label,'state',state,'message',message) ORDER BY label) FROM evidence;
ROLLBACK;
