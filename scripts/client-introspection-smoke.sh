#!/usr/bin/env bash
set -euo pipefail

database_url="${DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5433/bicdb}"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE IF EXISTS client_catalog_smoke;
CREATE TABLE client_catalog_smoke (id SERIAL PRIMARY KEY, label TEXT NOT NULL);
CREATE INDEX idx_client_catalog_smoke_label ON client_catalog_smoke(label);

SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
WHERE c.relname = 'client_catalog_smoke' AND a.attname = 'label';

SELECT pg_get_expr(d.adbin, d.adrelid)
FROM pg_catalog.pg_attrdef d
JOIN pg_catalog.pg_class c ON d.adrelid = c.oid
WHERE c.relname = 'client_catalog_smoke' AND d.adnum = 1;

SELECT c.relname, am.amname
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_am am ON c.relam = am.oid
WHERE c.relname = 'idx_client_catalog_smoke_label';

SELECT c.relname, pg_table_is_visible(c.oid)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname = 'client_catalog_smoke'
  AND pg_table_is_visible(c.oid)
  AND NOT pg_is_other_temp_schema(n.oid);

SELECT objoid, description FROM pg_catalog.pg_description WHERE objoid = 0;
SELECT inhrelid, inhparent FROM pg_catalog.pg_inherits;
SELECT enumtypid, enumlabel FROM pg_catalog.pg_enum;
SELECT spcname FROM pg_catalog.pg_tablespace WHERE spcname = 'pg_default';
SQL

python3 - "$database_url" <<'PY'
import sys

url = sys.argv[1]
try:
    import psycopg
except Exception:
    print("skip psycopg: module is not installed")
else:
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'client_catalog_smoke'")
            assert cur.fetchone()[0] == "client_catalog_smoke"
    print("psycopg introspection smoke passed")

try:
    import sqlalchemy as sa
except Exception:
    print("skip SQLAlchemy: module is not installed")
else:
    engine = sa.create_engine(url)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'client_catalog_smoke'"
        ).fetchall()
        assert rows and rows[0][0] == "client_catalog_smoke"
    print("SQLAlchemy introspection smoke passed")
PY

node - "$database_url" <<'JS'
const url = process.argv[2];
let pg;
try {
  pg = require("pg");
} catch (_error) {
  console.log("skip node-postgres: module is not installed");
  process.exit(0);
}

(async () => {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  const result = await client.query(
    "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'client_catalog_smoke'"
  );
  if (!result.rows.length || result.rows[0].relname !== "client_catalog_smoke") {
    throw new Error("node-postgres introspection smoke did not find table");
  }
  await client.end();
  console.log("node-postgres introspection smoke passed");
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
JS
