import json
import pathlib
import time
import uuid

import psycopg
from psycopg import sql

output = pathlib.Path('/root/.local/state/bicdb/typed-compare/native-routine-default-caller-security-control.json')
assert not output.exists()
schema = 'routine_defaults_' + uuid.uuid4().hex[:12]
guest = 'routine_guest_' + uuid.uuid4().hex[:12]
record = {'status': 'running', 'started_unix': time.time(), 'protected_application_schema_mutated': False, 'cases': []}

def save():
    output.write_text(json.dumps(record, indent=2) + '\n')

save()
with psycopg.connect(host='127.0.0.1', port=57940, user='adempiere', dbname='bicdb_native_app_ctrl_20261109', autocommit=True) as connection:
    record['server_version'] = connection.execute('SHOW server_version').fetchone()[0]
    connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    try:
        connection.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(guest)))
        try:
            connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            for statement in [
                "CREATE FUNCTION header_secret() RETURNS TEXT LANGUAGE sql AS 'SELECT ''secret'''",
                'REVOKE ALL ON FUNCTION header_secret() FROM PUBLIC',
                "CREATE FUNCTION header_definer(arg TEXT DEFAULT header_secret()) RETURNS TEXT LANGUAGE sql SECURITY DEFINER AS 'SELECT arg'",
                "CREATE PROCEDURE header_definer_proc(arg TEXT DEFAULT header_secret()) LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'",
                'CREATE FUNCTION header_definer_rows(arg TEXT DEFAULT header_secret()) RETURNS TABLE(item TEXT) LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN; END $$',
            ]:
                connection.execute(statement)
            connection.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(schema), sql.Identifier(guest)))
            for kind, name in [('FUNCTION', 'header_definer'), ('PROCEDURE', 'header_definer_proc'), ('FUNCTION', 'header_definer_rows')]:
                connection.execute(sql.SQL('GRANT EXECUTE ON {} {}(TEXT) TO {}').format(sql.SQL(kind), sql.Identifier(name), sql.Identifier(guest)))
            connection.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(guest)))
            for statement in ['SELECT header_definer()', 'CALL header_definer_proc()', 'SELECT * FROM header_definer_rows()']:
                try:
                    connection.execute(statement)
                except psycopg.Error as error:
                    assert error.sqlstate == '42501', str(error)
                    assert 'header_secret' in str(error), str(error)
                    record['cases'].append({'statement': statement, 'sqlstate': error.sqlstate, 'message': str(error)})
                else:
                    raise AssertionError('secret default unexpectedly executed: ' + statement)
            assert connection.execute("SELECT header_definer('explicit')").fetchall() == [('explicit',)]
            connection.execute("CALL header_definer_proc('explicit')")
            assert connection.execute("SELECT * FROM header_definer_rows('explicit')").fetchall() == []
            record['explicit_argument_calls_passed'] = 3
            connection.execute('RESET ROLE')
            connection.execute(sql.SQL('GRANT EXECUTE ON FUNCTION header_secret() TO {}').format(sql.Identifier(guest)))
            connection.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(guest)))
            assert connection.execute('SELECT header_definer()').fetchall() == [('secret',)]
            connection.execute('CALL header_definer_proc()')
            assert connection.execute('SELECT * FROM header_definer_rows()').fetchall() == []
            record['granted_default_calls_passed'] = 3
        finally:
            connection.execute('RESET ROLE')
            connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
            connection.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(guest)))
            record['temporary_schema_and_role_removed'] = True
    finally:
        if not record.get('temporary_schema_and_role_removed'):
            connection.execute('RESET ROLE')
            connection.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
record.update(status='passed', finished_unix=time.time())
save()
print(json.dumps(record))
