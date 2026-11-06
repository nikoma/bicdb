"""Exported routine owner planning and atomic native migration controls."""
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import routine_owners as target
import test_foreign_keys as fixtures


class RoutineOwnerPlanTests(unittest.TestCase):
    def test_plan_preserves_source_names_and_rejects_folded_ambiguity(self):
        self.assertEqual(target.declarations(dict(source_schema='Source Q',
            routines=[dict(name='Fn',kind='FUNCTION'),dict(name='Proc',kind='PROCEDURE')])),
            ('Source Q', {'fn':dict(source_name='Fn',kind='FUNCTION'),
                          'proc':dict(source_name='Proc',kind='PROCEDURE')}))
        for items in [[dict(name='F',kind='FUNCTION'),dict(name='f',kind='PROCEDURE')],
                      [dict(name='P',kind='PACKAGE')], [dict(name='',kind='FUNCTION')]]:
            with self.subTest(items=items), self.assertRaises(ValueError):
                target.declarations(dict(source_schema='S',routines=items))

    def test_catalog_refuses_extra_missing_wrong_kind_and_cross_schema_names(self):
        plan = {'f':dict(kind='FUNCTION')}
        row = ['f','f','S','migration',101,'definition',0,'',23,False]
        cursor = unittest.mock.Mock()
        cursor.fetchall.return_value = [tuple(row)]
        good = target.catalog(cursor,'S',plan)
        target.validate_owners(good, {'migration'})
        with self.assertRaises(ValueError):
            target.validate_owners(good, {'mapped'})
        for rows in [[], [tuple(row)]*2, [('f','p',*row[2:])],
                     [('f','f','other',*row[3:])], [('extra',*row[1:])]]:
            cursor.fetchall.return_value = rows
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                target.catalog(cursor,'S',plan)

    def test_writer_refusal_precedes_source_and_database_access(self):
        with self.assertRaisesRegex(ValueError,'writers-quiesced'):
            target.migrate(SimpleNamespace(writers_quiesced=False))


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native routine owner migration')
class RoutineOwnerMigrationNativeTests(unittest.TestCase):
    def fixture(self, profile):
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'routine mapping cleanup failed'))
        f.setUp()
        schema = 'routine_mapping'
        with f.conn.cursor() as c:
            c.execute('CREATE ROLE mapped_owner LOGIN NOSUPERUSER')
            c.execute('CREATE ROLE other_owner LOGIN NOSUPERUSER')
            c.execute('CREATE SCHEMA "routine_mapping"')
            if profile == 'oracle-preview-v1':
                c.execute('CREATE FUNCTION "routine_mapping"."F" RETURN NUMBER IS '
                          'BEGIN RETURN 7; END;', prepare=False)
                c.execute('CREATE PROCEDURE "routine_mapping"."P" IS BEGIN NULL; END;', prepare=False)
            else:
                c.execute('CREATE FUNCTION "routine_mapping"."f"() RETURNS integer LANGUAGE plpgsql '
                          'AS $$ BEGIN RETURN 7; END $$', prepare=False)
                c.execute('CREATE PROCEDURE "routine_mapping"."p"() LANGUAGE plpgsql '
                          'AS $$ BEGIN NULL; END $$', prepare=False)
        (f.snapshot/'manifest.json').write_text(json.dumps(dict(source_schema=schema,
            routines=[dict(name='F',kind='FUNCTION'),dict(name='P',kind='PROCEDURE')])))
        args = SimpleNamespace(snapshot=f.snapshot, report=f.root/'routine-owners.json',
            pg_dsn_env='BICDB_ROUTINE_OWNER_TEST_DSN',target_owner='mapped_owner',profile=profile,
            writers_quiesced=True,check_only=False,resume=False)
        return f,args

    def migrate(self,f,args):
        with patch.dict(os.environ,BICDB_ROUTINE_OWNER_TEST_DSN=f.dsn):
            return target.migrate(args)

    def check_profile(self,profile):
        import psycopg
        f,args = self.fixture(profile)
        result = self.migrate(f,args)
        self.assertTrue(result['ownership_transaction_committed'])
        self.assertTrue(result['routine_owners_verified'])
        self.assertTrue(result['definitions_preserved'])
        self.assertFalse(result['routine_execution_qualified'])
        args.resume=True
        self.assertTrue(self.migrate(f,args)['routine_owners_verified'])
        saved=args.report.read_bytes()
        args.target_owner='other_owner'
        with self.assertRaisesRegex(ValueError,'journal differs'):
            self.migrate(f,args)
        self.assertEqual(args.report.read_bytes(),saved)
        f.restart_server()
        with psycopg.connect(host='127.0.0.1',port=f.port,user='mapped_owner',dbname='bicdb',
                             autocommit=True,options='-c bicdb.sql_profile='+profile) as app, app.cursor() as c:
            c.execute('SELECT "routine_mapping"."f"()')
            self.assertEqual(c.fetchall(),[(7,)])
            c.execute('CALL "routine_mapping"."p"()',prepare=False)
        with psycopg.connect(host='127.0.0.1',port=f.port,user='other_owner',dbname='bicdb',
                             autocommit=True,options='-c bicdb.sql_profile='+profile) as other, other.cursor() as c:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('SELECT "routine_mapping"."f"()')

    def test_postgres_mapping_resume_restart_and_access(self):
        self.check_profile('postgres')

    def test_oracle_mapping_resume_restart_and_access(self):
        self.check_profile('oracle-preview-v1')

    def test_missing_superuser_other_owner_and_extra_routine_are_refused(self):
        f,args = self.fixture('oracle-preview-v1')
        schema,plan = target.declarations(json.loads((f.snapshot/'manifest.json').read_text()))
        with f.conn.cursor() as c:
            before=target.catalog(c,schema,plan)
        for role in ['missing','bicdb']:
            args.target_owner=role
            with self.assertRaisesRegex(ValueError,'non-superuser'):
                self.migrate(f,args)
            self.assertFalse(args.report.exists())
        args.target_owner='mapped_owner'
        with f.conn.cursor() as c:
            c.execute('ALTER FUNCTION "routine_mapping"."f" OWNER TO other_owner',prepare=False)
        with self.assertRaisesRegex(ValueError,'outside the explicit'):
            self.migrate(f,args)
        with f.conn.cursor() as c:
            c.execute('ALTER FUNCTION "routine_mapping"."f" OWNER TO bicdb',prepare=False)
            self.assertEqual(target.catalog(c,schema,plan),before)
            c.execute('CREATE FUNCTION "routine_mapping"."extra" RETURN NUMBER IS BEGIN RETURN 1; END;',prepare=False)
        with self.assertRaisesRegex(ValueError,'extra, colliding'):
            self.migrate(f,args)
        self.assertFalse(args.report.exists())

    def test_later_ddl_failure_rolls_back_the_complete_owner_mapping(self):
        import psycopg
        f,args = self.fixture('postgres')
        schema,plan = target.declarations(json.loads((f.snapshot/'manifest.json').read_text()))
        with f.conn.cursor() as c:
            before=target.catalog(c,schema,plan)
        original=target.quote
        with patch.object(target,'quote',side_effect=lambda name: original('missing_routine') if name=='p' else original(name)):
            with self.assertRaises(psycopg.errors.UndefinedFunction):
                self.migrate(f,args)
        with f.conn.cursor() as c:
            self.assertEqual(target.catalog(c,schema,plan),before)
        report=json.loads(args.report.read_text())
        self.assertFalse(report['success'])
        self.assertFalse(report['ownership_transaction_committed'])


if __name__ == '__main__':
    unittest.main()
