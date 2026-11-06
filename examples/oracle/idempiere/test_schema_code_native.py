"""Native stored-code import, dependency ordering, restart, and drift rejection."""
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import install_schema_code
import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native stored-code migration')
class SchemaCodeNative(unittest.TestCase):
    def test_source_code_install_resume_restart_and_drift(self):
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'native fixture cleanup failed'))
        fixture.setUp()
        fixture.conn.execute('CREATE SCHEMA "SOURCE"')
        fixture.conn.execute('CREATE TABLE "SOURCE"."T"("X" NUMBER)')
        fixture.conn.execute('INSERT INTO "SOURCE"."T" VALUES(7)')
        manifest = dict(source_schema='SOURCE', routines=[dict(name='ADD_ONE', kind='FUNCTION',
            ddl='CREATE OR REPLACE FUNCTION ADD_ONE(x NUMBER) RETURN NUMBER AS BEGIN RETURN x+1; END;')],
            views=[dict(name='V_PARENT', query='SELECT X FROM T', read_only='N'),
                   dict(name='V_CHILD', query='SELECT X FROM V_PARENT', read_only='N')],
            columns=[dict(table=name, name='X', position=1) for name in ['V_CHILD', 'V_PARENT']])
        (fixture.root/'manifest.json').write_text(json.dumps(manifest))
        def migrate(phase, resume=False):
            args = SimpleNamespace(snapshot=fixture.root, report=fixture.root/(phase+'.json'),
                                   phase=phase, resume=resume, writers_quiesced=True,
                                   pg_dsn_env='BICDB_CODE_TEST_DSN')
            with patch.dict(os.environ, BICDB_CODE_TEST_DSN=fixture.dsn):
                return install_schema_code.migrate(args)
        self.assertEqual(migrate('routines'), 0)
        self.assertEqual(migrate('views'), 0, (fixture.root/'views.json').read_text())
        fixture.conn.execute('SET search_path TO "SOURCE",public')
        self.assertEqual(fixture.conn.execute('SELECT "SOURCE"."ADD_ONE"(7)').fetchone()[0], 8)
        self.assertEqual(fixture.conn.execute('SELECT "X" FROM "SOURCE"."V_CHILD"').fetchone()[0], 7)
        fixture.restart_server()
        self.assertEqual(migrate('routines', True), 0)
        self.assertEqual(migrate('views', True), 0)
        fixture.conn.execute('CREATE OR REPLACE FUNCTION "SOURCE"."ADD_ONE"(x NUMBER) RETURN NUMBER AS BEGIN RETURN x+2; END;')
        with self.assertRaisesRegex(ValueError, 'changed outside'):
            migrate('routines', True)
        self.assertFalse(json.loads((fixture.root/'routines.json').read_text())['success'])
        fixture.conn.execute('CREATE OR REPLACE VIEW "SOURCE"."V_CHILD"("X") AS SELECT 99 FROM "SOURCE"."T"')
        with self.assertRaisesRegex(ValueError, 'changed outside'):
            migrate('views', True)
        self.assertFalse(json.loads((fixture.root/'views.json').read_text())['success'])


if __name__ == '__main__': unittest.main()
