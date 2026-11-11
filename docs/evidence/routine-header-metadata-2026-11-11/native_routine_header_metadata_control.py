import json
import pathlib
import time
import uuid

import psycopg
from psycopg import sql

output = pathlib.Path('/root/.local/state/bicdb/typed-compare/native-routine-header-metadata-control.json')
assert not output.exists()
schema = 'routine_headers_' + uuid.uuid4().hex[:12]
connection_options = dict(host='127.0.0.1', port=57940, user='adempiere', dbname='bicdb_native_app_ctrl_20261109', autocommit=True)
record = {'status': 'running', 'started_unix': time.time(), 'protected_application_schema_mutated': False, 'phases': []}

def probe(connection, phase):
    rows = connection.execute("SELECT oid,proname,pg_get_function_result(oid) FROM pg_proc WHERE proname IN ('table_header','set_header') AND pronamespace=%s::regnamespace ORDER BY proname", (schema,)).fetchall()
    assert [(name, declaration) for oid, name, declaration in rows] == [('set_header', 'SETOF integer'), ('table_header', 'TABLE(item typed_row)')], rows
    for oid, name, declaration in rows:
        assert connection.execute('SELECT pg_get_function_result(%s::oid)', (oid,)).fetchone()[0] == declaration
    assert connection.execute('SELECT row_default()').fetchall() == [('kept',)]
    connection.execute('CALL row_default_proc()')
    assert connection.execute('SELECT * FROM table_header()').fetchall() == []
    record['phases'].append({'phase': phase, 'metadata': rows, 'default_value': 'kept', 'procedure_call_passed': True, 'empty_table_function_passed': True})

with psycopg.connect(**connection_options) as connection:
    record['server_version'] = connection.execute('SHOW server_version').fetchone()[0]
    connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
try:
    with psycopg.connect(**connection_options) as connection:
        connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        for statement in [
            'CREATE TABLE typed_row(id INT PRIMARY KEY,payload TEXT)',
            "CREATE FUNCTION row_default(arg TEXT DEFAULT (ROW(1,'kept')::typed_row).payload) RETURNS TEXT LANGUAGE sql AS 'SELECT arg'",
            "CREATE PROCEDURE row_default_proc(arg TEXT DEFAULT (ROW(1,'kept')::typed_row).payload) LANGUAGE sql AS 'SELECT 1'",
            'CREATE FUNCTION table_header() RETURNS TABLE(item typed_row) LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$',
            "CREATE FUNCTION set_header() RETURNS SETOF INTEGER LANGUAGE sql AS 'SELECT 1'",
        ]:
            connection.execute(statement)
        probe(connection, 'before_reconnect')
    with psycopg.connect(**connection_options) as connection:
        connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        probe(connection, 'after_reconnect')
finally:
    with psycopg.connect(**connection_options) as connection:
        connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        record['temporary_schema_removed'] = True
record.update(status='passed', finished_unix=time.time())
output.write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record))
