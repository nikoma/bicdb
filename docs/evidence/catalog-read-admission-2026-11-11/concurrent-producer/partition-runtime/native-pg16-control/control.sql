\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA root_partition_runtime_20261112;
SET LOCAL search_path=root_partition_runtime_20261112;
CREATE TABLE parent(item_key INT,payload TEXT) PARTITION BY RANGE(item_key);
CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10);
INSERT INTO child VALUES(1,'child-ack');
DO $$ BEGIN
 IF (SELECT array_agg(item_key ORDER BY item_key) FROM parent) IS DISTINCT FROM ARRAY[1] THEN RAISE EXCEPTION 'parent scan omitted child row'; END IF;
END $$;
SELECT 'parent_after_child',item_key,payload FROM parent ORDER BY item_key;
INSERT INTO parent VALUES(2,'parent-ack');
DO $$ BEGIN
 IF (SELECT array_agg(item_key ORDER BY item_key) FROM child) IS DISTINCT FROM ARRAY[1,2] THEN RAISE EXCEPTION 'insert did not route to child'; END IF;
 BEGIN
  INSERT INTO child VALUES(50,'must-refuse');
  RAISE EXCEPTION 'out of bound insert was accepted';
 EXCEPTION WHEN check_violation THEN RAISE NOTICE 'out_of_bound_insert | 23514';
 END;
END $$;
SELECT 'child_after_parent',item_key,payload FROM child ORDER BY item_key;
CREATE TABLE fallback(item_key INT,payload TEXT);
ALTER TABLE parent ATTACH PARTITION fallback DEFAULT;
INSERT INTO parent VALUES(20,'default-ack');
DO $$ BEGIN
 IF (SELECT array_agg(item_key ORDER BY item_key) FROM fallback) IS DISTINCT FROM ARRAY[20] THEN RAISE EXCEPTION 'default insert did not route'; END IF;
 IF (SELECT array_agg(item_key ORDER BY item_key) FROM parent) IS DISTINCT FROM ARRAY[1,2,20] THEN RAISE EXCEPTION 'parent union incorrect'; END IF;
END $$;
SELECT 'default_index',partdefid='fallback'::regclass FROM pg_partitioned_table WHERE partrelid='parent'::regclass;
SELECT 'parent_after_default',item_key,payload FROM parent ORDER BY item_key;
ROLLBACK;
