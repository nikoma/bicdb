"""The export owner comes from Oracle, independent of name folding and default schema."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import export_reference as target


class ExportOwnerIdentityTests(unittest.TestCase):
    def test_snapshot_resolves_owner_before_read_only_and_quotes_schema_reset(self):
        events = []
        class Cursor:
            def execute(self, sql): events.append(sql)
            def fetchone(self): return ('Mi"X', 'OTHER')
        connection = SimpleNamespace(rollback=lambda: events.append('rollback'))
        identity = target.begin_export_snapshot(connection, Cursor())
        self.assertEqual(identity['session_user'], 'Mi"X')
        self.assertEqual(identity['read_schema'], 'Mi"X')
        self.assertEqual(identity['initial_current_schema'], 'OTHER')
        self.assertEqual(events[1:], ['ALTER SESSION SET CURRENT_SCHEMA = "Mi""X"',
                                      'rollback', 'SET TRANSACTION READ ONLY'])

    def test_already_selected_owner_needs_no_schema_change(self):
        events = []
        class Cursor:
            def execute(self, sql): events.append(sql)
            def fetchone(self): return ('SOURCE', 'SOURCE')
        target.begin_export_snapshot(SimpleNamespace(rollback=lambda: events.append('rollback')), Cursor())
        self.assertFalse(any(sql.startswith('ALTER') for sql in events))
        self.assertEqual(events[-2:], ['rollback', 'SET TRANSACTION READ ONLY'])

    def test_missing_invalid_and_privileged_database_identity_is_refused(self):
        for row in [None, 'AB', {'A':'B','C':'D'}, (), ('A',), ('A', None), ('', 'A'), ('A\0B', 'A'),
                    ('SYS','SYS'), ('SYSTEM','OTHER'), ('PDBADMIN','OTHER')]:
            events = []
            class Cursor:
                def execute(self, sql): events.append(sql)
                def fetchone(self): return row
            with self.subTest(row=row), self.assertRaises(ValueError):
                target.begin_export_snapshot(SimpleNamespace(rollback=lambda: events.append('rollback')), Cursor())
            self.assertEqual(len(events), 1, 'invalid identity must stop before reset, rollback or catalog access')

    def test_full_export_uses_exact_owner_and_unshadowed_dictionary(self):
        owner = 'MixedOwner'
        events = []
        class Cursor:
            description = []
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, **kwargs):
                events.append(sql)
                self.rows = []
                if sql.startswith("SELECT SYS_CONTEXT('USERENV','SESSION_USER')"):
                    self.rows = [(owner, 'OTHER')]
                elif sql.startswith('ALTER SESSION') or sql == 'SET TRANSACTION READ ONLY': pass
                elif sql == 'SELECT current_scn FROM SYS.V_$DATABASE': self.rows = [(123,)]
                elif ' FROM SYS.user_objects ' in sql:
                    self.rows = [('TABLE', name, 'VALID') for name in ['T','USER_TABLES']]
                elif ' FROM SYS.user_tables ' in sql: self.rows = [('T',),('USER_TABLES',)]
                elif ' FROM SYS.user_tab_cols ' in sql:
                    self.rows = [(name,'ID',1,'NUMBER',22,10,0,None,None,'N',None,'NO')
                                 for name in ['T','USER_TABLES']]
                elif sql in ['SELECT "ID" FROM "MixedOwner"."T"',
                             'SELECT "ID" FROM "MixedOwner"."USER_TABLES"']:
                    self.description = [SimpleNamespace(name='ID')]
                    self.rows = [(7 if sql.endswith('."T"') else 11,)]
                elif ' FROM SYS.user_' not in sql:
                    raise AssertionError('unqualified or unexpected source read: '+sql)
            def fetchone(self): return self.rows[0]
            def fetchmany(self, size):
                rows, self.rows = self.rows[:size], self.rows[size:]
                return rows
            def __iter__(self): return iter(self.rows)
        class Connection:
            version = 'fixture'
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def cursor(self): return Cursor()
            def rollback(self): events.append('rollback')
        driver = SimpleNamespace(defaults=SimpleNamespace(),connect=lambda **kwargs: Connection())
        with tempfile.TemporaryDirectory() as name:
            root=Path(name);(root/'password').write_text('fixture')
            config=root/'connection.json';config.write_text(json.dumps(dict(user=owner,host='fixture',port=1521,service_name='TEST',password_file='password')))
            with patch.dict(sys.modules,oracledb=driver): target.export(config,root/'export')
            manifest=json.loads((root/'export/manifest.json').read_text())
            catalog=json.loads((root/'export/catalog.json').read_text())
            self.assertEqual(manifest['source_schema'],owner)
            self.assertEqual(catalog['source_schema'],owner)
            self.assertEqual(manifest['source_identity'],catalog['source_identity'])
            self.assertFalse(manifest['cdc_resume_position'])
            self.assertEqual(manifest['observed_scn'],'123')
            rows={item['name']:json.loads((root/'export'/item['file']).read_text()) for item in manifest['tables']}
            self.assertEqual(rows,{'T':[7],'USER_TABLES':[11]})
            start=events.index('SET TRANSACTION READ ONLY')
            self.assertEqual(events[start-1],'rollback')
            self.assertFalse(any('FROM user_' in event for event in events))


if __name__=='__main__': unittest.main()
