"""Host status queries retain their wire meaning independently of SQL profile."""
import os
import unittest

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native host-status acceptance')
class ServerStatusNativeTests(unittest.TestCase):
    def check_profile(self, profile, simple):
        import psycopg
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'status fixture cleanup failed'))
        fixture.setUp()
        factory = psycopg.ClientCursor if simple else psycopg.Cursor
        with factory(fixture.conn) as cursor:
            for relation in ['bicdb_activity', 'bicdb_server_connections']:
                cursor.execute('SELECT * FROM ' + relation, **({} if simple else {'prepare': True}))
                self.assertEqual(len(cursor.description), 9)
                rows = cursor.fetchall()
                self.assertTrue(rows)
                self.assertTrue(any(row[1] == 'bicdb' for row in rows))
            cursor.execute('SELECT * FROM bicdb_server_stats', **({} if simple else {'prepare': True}))
            self.assertGreaterEqual(len(cursor.description), 37)
            self.assertEqual(len(cursor.fetchall()), 1)
            cursor.execute('SELECT sum(xact_commit + xact_rollback) FROM pg_stat_database',
                           **({} if simple else {'prepare': True}))
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(len(rows[0]), 1)
            self.assertGreaterEqual(rows[0][0], 0)

            # Quoted application identifiers keep their ordinary profile/name
            # resolution; only the existing exact host commands are reserved.
            cursor.execute('CREATE TABLE "BICDB_ACTIVITY" ("VALUE" INTEGER)')
            cursor.execute('INSERT INTO "BICDB_ACTIVITY" VALUES (42)')
            cursor.execute('SELECT * FROM "BICDB_ACTIVITY"', **({} if simple else {'prepare': True}))
            self.assertEqual(cursor.fetchall(), [(42,)])

    def test_postgres_simple_status_and_quoted_relation(self):
        self.check_profile('postgres', True)

    def test_postgres_prepared_status_and_quoted_relation(self):
        self.check_profile('postgres', False)

    def test_oracle_simple_status_and_quoted_relation(self):
        self.check_profile('oracle-preview-v1', True)

    def test_oracle_prepared_status_and_quoted_relation(self):
        self.check_profile('oracle-preview-v1', False)


if __name__ == '__main__':
    unittest.main()
