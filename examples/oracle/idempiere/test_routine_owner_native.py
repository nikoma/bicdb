"""Routine owner changes preserve identities, rollback and ordinary-role access."""
import os
import unittest

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native routine ownership checks')
class RoutineOwnerNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        import psycopg
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'routine fixture cleanup failed'))
        f.setUp()
        schema = '"routine_scope"'
        fn = schema+'."mapped_fn"'
        proc = schema+'."mapped_proc"'
        arg = 'numeric' if profile == 'oracle-preview-v1' else 'integer'
        with f.conn.cursor() as c:
            c.execute('CREATE ROLE routine_owner LOGIN NOSUPERUSER')
            c.execute('CREATE ROLE unrelated_owner LOGIN NOSUPERUSER')
            c.execute('CREATE SCHEMA '+schema)
            c.execute('CREATE TABLE '+schema+'."records" ("ID" INTEGER)')
            c.execute('ALTER TABLE '+schema+'."records" OWNER TO routine_owner')
            if profile == 'oracle-preview-v1':
                c.execute('CREATE FUNCTION '+fn+'(p IN NUMBER) RETURN NUMBER IS '
                          'BEGIN RETURN p+1; END;', prepare=False)
                c.execute('CREATE PROCEDURE '+proc+'(p IN NUMBER) IS '
                          'BEGIN INSERT INTO '+schema+'."records" VALUES (p); END;', prepare=False)
            else:
                c.execute('CREATE FUNCTION '+fn+'(p integer) RETURNS integer LANGUAGE plpgsql '
                          'AS $$ BEGIN RETURN p+1; END $$', prepare=False)
                c.execute('CREATE PROCEDURE '+proc+'(p integer) LANGUAGE plpgsql '
                          'AS $$ BEGIN INSERT INTO '+schema+'."records" VALUES (p); END $$', prepare=False)
            def owners():
                c.execute('SELECT p."proname",r."rolname" FROM "pg_catalog"."pg_proc" p '
                          'JOIN "pg_catalog"."pg_namespace" n ON n."oid"=p."pronamespace" '
                          'JOIN "pg_catalog"."pg_roles" r ON r."oid"=p."proowner" '
                          'WHERE n."nspname"=%s ORDER BY p."proname"', ('routine_scope',))
                return c.fetchall()
            initial = [('mapped_fn','bicdb'),('mapped_proc','bicdb')]
            self.assertEqual(owners(), initial)
            for kind,name in [('FUNCTION',fn),('PROCEDURE',proc)]:
                for bad in [name+'(text)', '"wrong_scope".'+name.split('.')[-1]+'('+arg+')']:
                    with self.subTest(kind=kind,bad=bad), self.assertRaises(psycopg.errors.UndefinedFunction):
                        c.execute('ALTER '+kind+' '+bad+' OWNER TO routine_owner', prepare=False)
                    self.assertEqual(owners(), initial)
            with self.assertRaisesRegex(RuntimeError, 'rollback control'):
                with f.conn.transaction():
                    c.execute('ALTER FUNCTION '+fn+'('+arg+') OWNER TO routine_owner', prepare=False)
                    c.execute('ALTER PROCEDURE '+proc+'('+arg+') OWNER TO routine_owner', prepare=False)
                    self.assertEqual(owners(), [('mapped_fn','routine_owner'),('mapped_proc','routine_owner')])
                    raise RuntimeError('rollback control')
            self.assertEqual(owners(), initial)
            for kind,name in [('FUNCTION',fn),('PROCEDURE',proc)]:
                c.execute('ALTER '+kind+' '+name+'('+arg+') OWNER TO routine_owner', prepare=False)
        def connect(role):
            return psycopg.connect(host='127.0.0.1',port=f.port,user=role,dbname='bicdb',
                                   autocommit=True,options='-c bicdb.sql_profile='+profile)
        with connect('unrelated_owner') as other, other.cursor() as c:
            for sql in ['SELECT '+fn+'(7)', 'CALL '+proc+'(7)',
                        'ALTER FUNCTION '+fn+'('+arg+') OWNER TO unrelated_owner',
                        'ALTER PROCEDURE '+proc+'('+arg+') OWNER TO unrelated_owner']:
                with self.subTest(sql=sql), self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    c.execute(sql,prepare=False)
        with connect('routine_owner') as owner, owner.cursor() as c:
            for kind,name in [('FUNCTION',fn),('PROCEDURE',proc)]:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    c.execute('ALTER '+kind+' '+name+'('+arg+') OWNER TO unrelated_owner',prepare=False)
            c.execute('SELECT '+fn+'(7)')
            self.assertEqual(c.fetchone(), (8,))
            c.execute('CALL '+proc+'(7)',prepare=False)
            c.execute('SELECT "ID" FROM '+schema+'."records"')
            self.assertEqual(c.fetchall(), [(7,)])
        f.restart_server()
        with connect('routine_owner') as owner, owner.cursor() as c:
            c.execute('SELECT '+fn+'(9)')
            self.assertEqual(c.fetchone(), (10,))
            c.execute('CALL '+proc+'(9)',prepare=False)
            c.execute('SELECT "ID" FROM '+schema+'."records" ORDER BY "ID"')
            self.assertEqual(c.fetchall(), [(7,),(9,)])

    def test_postgres_routine_ownership_identity_rollback_and_access(self):
        self.check_profile('postgres')

    def test_oracle_routine_ownership_identity_rollback_and_access(self):
        self.check_profile('oracle-preview-v1')


if __name__ == '__main__':
    unittest.main()
