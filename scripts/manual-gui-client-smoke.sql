DROP TABLE IF EXISTS gui_client_smoke;
CREATE TABLE gui_client_smoke (
  id TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  seen INT NOT NULL
);

INSERT INTO gui_client_smoke (id, label, seen)
VALUES ('g1', 'created from GUI client', 1);

UPDATE gui_client_smoke SET seen = 2 WHERE id = 'g1';

SELECT label, seen
FROM gui_client_smoke
WHERE id = 'g1';

BEGIN;
INSERT INTO gui_client_smoke (id, label, seen)
VALUES ('rolled', 'rollback check', 9);
ROLLBACK;

SELECT COUNT(*) AS rolled_back_rows
FROM gui_client_smoke
WHERE id = 'rolled';

SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'gui_client_smoke'
ORDER BY a.attnum;

SELECT relname
FROM pg_catalog.pg_class
WHERE relname = 'gui_client_smoke';
