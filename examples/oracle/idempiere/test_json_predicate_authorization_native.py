"""IS JSON constraints run as ordinary writers in both native profiles."""
import os
import unittest

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native JSON predicate checks')
class JsonPredicateAuthorizationNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        import psycopg
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'JSON fixture cleanup failed'))
        f.setUp()
        text_type = 'VARCHAR2(4000)' if profile == 'oracle-preview-v1' else 'TEXT'
        with f.conn.cursor() as cursor:
            cursor.execute('CREATE ROLE json_writer LOGIN NOSUPERUSER')
            cursor.execute('CREATE TABLE "JSON_INPUT" ("ID" INTEGER PRIMARY KEY, '
                           '"PAYLOAD" '+text_type+' CHECK ("PAYLOAD" IS JSON))')
            cursor.execute('ALTER TABLE "JSON_INPUT" OWNER TO json_writer')
        with psycopg.connect(host='127.0.0.1', port=f.port, user='json_writer', dbname='bicdb',
                             autocommit=True, options='-c bicdb.sql_profile='+profile) as writer:
            with writer.cursor() as cursor:
                cursor.execute('INSERT INTO "JSON_INPUT" VALUES (%s,%s)', (1,'{"k":1}'))
                cursor.execute('INSERT INTO "JSON_INPUT" VALUES (%s,%s)', (2,None))
                with self.assertRaises(psycopg.errors.CheckViolation):
                    cursor.execute('INSERT INTO "JSON_INPUT" VALUES (%s,%s)', (3,'{broken'))
                cursor.execute('UPDATE "JSON_INPUT" SET "PAYLOAD"=%s WHERE "ID"=1', ('[1,2]',))
                with self.assertRaises(psycopg.errors.CheckViolation):
                    cursor.execute('UPDATE "JSON_INPUT" SET "PAYLOAD"=%s WHERE "ID"=1', ('{broken',))
                cursor.execute('SELECT "ID","PAYLOAD" FROM "JSON_INPUT" ORDER BY "ID"')
                self.assertEqual(cursor.fetchall(), [(1,'[1,2]'),(2,None)])
                # Allowing the scalar predicate must not grant administrative
                # authority to the authenticated writer or after SET ROLE.
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    cursor.execute('SELECT bicdb_advance_transaction_floor(0)')
        with f.conn.cursor() as cursor:
            cursor.execute('SET ROLE json_writer')
            try:
                cursor.execute('INSERT INTO "JSON_INPUT" VALUES (%s,%s)', (4,'true'))
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    cursor.execute('SELECT bicdb_advance_transaction_floor(0)')
            finally:
                cursor.execute('RESET ROLE')

    def test_postgres_ordinary_writer_json_constraints_and_admin_boundary(self):
        self.check_profile('postgres')

    def test_oracle_ordinary_writer_json_constraints_and_admin_boundary(self):
        self.check_profile('oracle-preview-v1')


if __name__ == '__main__':
    unittest.main()
