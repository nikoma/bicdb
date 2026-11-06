"""Live stored-source capture against an explicitly supplied empty Oracle owner."""
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import export_reference as target


@unittest.skipUnless(os.environ.get('ORACLE_SOURCE_CAPTURE_CONFIG'),
                     'set ORACLE_SOURCE_CAPTURE_CONFIG for an empty disposable Oracle owner')
class StoredSourceOracleTests(unittest.TestCase):
    def test_compiled_package_type_bodies_and_enabled_disabled_triggers(self):
        import oracledb
        config_path = Path(os.environ['ORACLE_SOURCE_CAPTURE_CONFIG'])
        config = json.loads(config_path.read_text())
        self.assertNotIn(config['user'].upper(), {'SYS', 'SYSTEM', 'PDBADMIN'})
        password_file = Path(config['password_file'])
        if not password_file.is_absolute(): password_file = config_path.parent/password_file
        connection = oracledb.connect(user=config['user'], password=password_file.read_text().strip(),
            host=config['host'], port=config['port'], service_name=config['service_name'])
        self.addCleanup(connection.close)
        connection.call_timeout = 30000
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM user_objects')
            self.assertEqual(cursor.fetchone(), (0,), 'fixture requires an empty disposable Oracle owner')
            prefix = 'BCS_'+uuid.uuid4().hex[:12].upper()
            names = {key: prefix+'_'+key for key in ['T','F','P','TY','PKG','TR_ON','TR_OFF']}
            q = lambda key: target.quote(names[key])
            ddl = [
                ('TABLE', names['T'], 'CREATE TABLE '+q('T')+'(n NUMBER)'),
                ('FUNCTION', names['F'], 'CREATE FUNCTION '+q('F')+' RETURN NUMBER IS BEGIN RETURN 7; END;'),
                ('PROCEDURE', names['P'], 'CREATE PROCEDURE '+q('P')+' IS BEGIN NULL; END;'),
                ('TYPE', names['TY'], 'CREATE TYPE '+q('TY')+' AS OBJECT(n NUMBER, MEMBER FUNCTION get_n RETURN NUMBER)'),
                ('TYPE BODY', names['TY'], 'CREATE TYPE BODY '+q('TY')+' AS MEMBER FUNCTION get_n RETURN NUMBER IS BEGIN RETURN SELF.n; END; END;'),
                ('PACKAGE', names['PKG'], 'CREATE PACKAGE '+q('PKG')+' AS FUNCTION answer RETURN NUMBER; END;'),
                ('PACKAGE BODY', names['PKG'], 'CREATE PACKAGE BODY '+q('PKG')+' AS FUNCTION answer RETURN NUMBER IS BEGIN RETURN 42; END; END;'),
                ('TRIGGER', names['TR_ON'], 'CREATE TRIGGER '+q('TR_ON')+' BEFORE INSERT ON '+q('T')+' FOR EACH ROW BEGIN NULL; END;'),
                ('TRIGGER', names['TR_OFF'], 'CREATE TRIGGER '+q('TR_OFF')+' BEFORE INSERT ON '+q('T')+' FOR EACH ROW BEGIN NULL; END;'),
            ]
            created = []
            try:
                for kind, name, sql in ddl:
                    # Dropping the spec/declaration also drops its body.
                    if kind not in {'TYPE BODY', 'PACKAGE BODY'}: created.append((kind, name))
                    cursor.execute(sql)
                cursor.execute('ALTER TRIGGER '+q('TR_OFF')+' DISABLE')
                cursor.execute("SELECT COUNT(*) FROM user_objects WHERE status<>'VALID'")
                self.assertEqual(cursor.fetchone(), (0,))
                cursor.execute('SELECT '+q('F')+'(),'+q('PKG')+'.answer() FROM dual')
                self.assertEqual(cursor.fetchone(), (7,42))
                with tempfile.TemporaryDirectory(prefix='bicdb-source-capture-') as name:
                    directory = Path(name)/'export'
                    target.export(config_path, directory)
                    manifest = json.loads((directory/'manifest.json').read_text())
                    self.assertEqual([len(manifest[k]) for k in ['routines','types','type_bodies','packages','triggers']], [2,1,1,2,2])
                    states = {item['name']:item['status'] for item in manifest['triggers']}
                    self.assertEqual(states, {names['TR_ON']:'ENABLED',names['TR_OFF']:'DISABLED'})
                    for group in ['routines','types','type_bodies','packages','triggers']:
                        for item in manifest[group]:
                            cursor.execute('SELECT line,text FROM user_source WHERE name=:1 AND type=:2 ORDER BY line',
                                           [item['name'],item['kind']])
                            lines = cursor.fetchall()
                            self.assertEqual([int(line) for line,text in lines], list(range(1,len(lines)+1)))
                            self.assertEqual(item['source_line_count'], len(lines))
                            self.assertEqual(item['ddl'], 'CREATE OR REPLACE '+''.join('\n' if text is None else text for line,text in lines))
            finally:
                errors = []
                for kind, name in reversed(created):
                    try: cursor.execute('DROP '+kind+' '+target.quote(name)+(' PURGE' if kind=='TABLE' else ''))
                    except oracledb.DatabaseError as error:
                        if error.args[0].code not in {942,4043,4080}: errors.append(error.args[0].code)
                self.assertEqual(errors, [], 'owned stored-source fixture cleanup failed')
                cursor.execute('SELECT COUNT(*) FROM user_objects')
                self.assertEqual(cursor.fetchone(), (0,), 'owned stored-source fixture objects remain')


if __name__ == '__main__': unittest.main()
