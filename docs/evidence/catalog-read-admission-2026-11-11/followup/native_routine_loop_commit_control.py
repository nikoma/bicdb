"""Native PostgreSQL control for repeated expressions across procedure COMMIT."""
import json
import pathlib
import time
import uuid

import psycopg
from psycopg import sql

output = pathlib.Path('/root/.local/state/bicdb/typed-compare/native-routine-loop-commit-control.json')
assert not output.exists()
options = dict(host='127.0.0.1', port=57940, user='adempiere', dbname='bicdb_native_app_ctrl_20261109', autocommit=True)
schema = 'routine_loop_' + uuid.uuid4().hex[:12]
record = dict(status='running', started_unix=time.time(), protected_application_schema_mutated=False, cases=[])
with psycopg.connect(**options) as connection:
    record['server_version'] = connection.execute('SHOW server_version').fetchone()[0]
    connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
try:
    with psycopg.connect(**options) as connection:
        connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        connection.execute('CREATE TABLE source(id INT PRIMARY KEY,payload TEXT)')
        connection.execute("INSERT INTO source VALUES(1,'kept')")
        connection.execute('CREATE TABLE sink(id INT PRIMARY KEY)')
        for name, condition in [
            ('row_type', "(ROW(i,'kept')::source).payload = 'kept'"),
            ('scalar_subquery', '(SELECT count(*) FROM source WHERE id=1) = 1'),
        ]:
            connection.execute('TRUNCATE sink')
            connection.execute(f'CREATE OR REPLACE PROCEDURE loop_read() LANGUAGE plpgsql AS $$ DECLARE i INT := 0; BEGIN WHILE {condition} AND i < 2 LOOP INSERT INTO sink VALUES(i); COMMIT; i := i + 1; END LOOP; END; $$')
            connection.execute('CALL loop_read()')
            rows = connection.execute('SELECT id FROM sink ORDER BY id').fetchall()
            assert rows == [(0,), (1,)], rows
            record['cases'].append(dict(condition=name, rows=rows, call_completed=True))
finally:
    with psycopg.connect(**options) as connection:
        connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        record['temporary_schema_removed'] = True
record.update(status='passed', finished_unix=time.time())
output.write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record), flush=True)
