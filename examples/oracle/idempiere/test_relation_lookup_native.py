"""Relation identity and DDL visibility through independent native profiles."""
import os
import unittest

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native relation lookup')
class RelationLookupNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        import psycopg
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'lookup fixture cleanup failed'))
        f.setUp()
        schema = '"Point.Q"'
        table = schema + '."Rows""Q"'
        view = schema + '."View.Q"'
        text_type = 'VARCHAR2(40)' if profile == 'oracle-preview-v1' else 'TEXT'
        with f.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA ' + schema)
            cursor.execute('CREATE TABLE ' + table + '("ID" INTEGER,"PAYLOAD" ' + text_type + ')')
            cursor.execute('INSERT INTO ' + table + ' VALUES (1,%s),(2,%s)', ('a','b'))
            cursor.execute('CREATE VIEW ' + view + ' AS SELECT "ID","PAYLOAD" FROM ' + table)
            for relation in [table,view]:
                cursor.execute('SELECT "ID","PAYLOAD" FROM ' + relation + ' ORDER BY "ID"', prepare=True)
                self.assertEqual(cursor.fetchall(), [(1,'a'),(2,'b')])
            missing = schema + '."CreatedLater"'
            with self.assertRaises(psycopg.errors.UndefinedTable):
                cursor.execute('SELECT * FROM ' + missing)
            cursor.execute('CREATE TABLE ' + missing + '("ID" INTEGER)')
            cursor.execute('INSERT INTO ' + missing + ' VALUES (3)')
            cursor.execute('SELECT * FROM ' + missing)
            self.assertEqual(cursor.fetchall(), [(3,)])
            with self.assertRaises(psycopg.errors.UndefinedTable):
                with f.conn.transaction():
                    cursor.execute('DROP VIEW ' + view)
                    cursor.execute('SELECT * FROM ' + view)
            cursor.execute('SELECT "ID","PAYLOAD" FROM ' + view + ' ORDER BY "ID"', prepare=True)
            self.assertEqual(cursor.fetchall(), [(1,'a'),(2,'b')])
            cursor.execute('CREATE ROLE lookup_reader LOGIN NOSUPERUSER')
        with psycopg.connect(host='127.0.0.1',port=f.port,user='lookup_reader',dbname='bicdb',
                             autocommit=True,options='-c bicdb.sql_profile='+profile) as reader:
            with reader.cursor() as cursor:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    cursor.execute('SELECT * FROM ' + table)
        f.restart_server()
        with f.conn.cursor() as cursor:
            cursor.execute('SELECT "ID","PAYLOAD" FROM ' + view + ' ORDER BY "ID"',prepare=True)
            self.assertEqual(cursor.fetchall(), [(1,'a'),(2,'b')])

    def test_default_postgres_relation_identity_ddl_rollback_and_isolation(self):
        self.check_profile('postgres')

    def test_oracle_relation_identity_ddl_rollback_and_isolation(self):
        self.check_profile('oracle-preview-v1')


if __name__ == '__main__':
    unittest.main()
