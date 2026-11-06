"""Missing savepoints retain Oracle statements and PostgreSQL abort semantics."""
import json
import os
import unittest

import test_foreign_keys as fixtures


class MissingSavepointCases:
    def fixture(self):
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'savepoint fixture cleanup failed'))
        fixture.setUp()
        fixture.conn.execute('CREATE TABLE retained_rows(id INTEGER PRIMARY KEY)')
        return fixture.conn

    def missing(self, connection, command):
        import psycopg
        with self.assertRaises(psycopg.Error) as caught:
            connection.execute(command)
        self.assertEqual(caught.exception.sqlstate, '3B001')
        if self.profile == 'oracle-preview-v1':
            self.assertIn('ORA-01086', caught.exception.diag.message_primary)
            detail = json.loads(caught.exception.diag.message_detail)
            self.assertEqual(detail['oracle_error_stack'][0]['code'], -1086)

    def test_missing_rollback_preserves_rows_and_existing_savepoints(self):
        import psycopg
        connection = self.fixture()
        connection.execute('BEGIN')
        connection.execute('INSERT INTO retained_rows VALUES(1)')
        connection.execute('SAVEPOINT retained')
        connection.execute('INSERT INTO retained_rows VALUES(2)')
        self.missing(connection, 'ROLLBACK TO SAVEPOINT missing')
        if self.profile == 'postgres':
            self.assertEqual(connection.info.transaction_status.name, 'INERROR')
            with self.assertRaises(psycopg.errors.InFailedSqlTransaction):
                connection.execute('INSERT INTO retained_rows VALUES(3)')
        else:
            self.assertEqual(connection.info.transaction_status.name, 'INTRANS')
            self.assertEqual(connection.execute('SELECT id FROM retained_rows ORDER BY id').fetchall(), [(1,), (2,)])
            connection.execute('INSERT INTO retained_rows VALUES(3)')
        connection.execute('ROLLBACK TO SAVEPOINT retained')
        self.assertEqual(connection.execute('SELECT id FROM retained_rows').fetchall(), [(1,)])
        connection.execute('COMMIT')

    def test_expired_savepoint_after_full_rollback_allows_oracle_reuse(self):
        import psycopg
        connection = self.fixture()
        connection.execute('BEGIN')
        connection.execute('SAVEPOINT "JDBC_SAVEPOINT_0"')
        connection.execute('INSERT INTO retained_rows VALUES(1)')
        connection.execute('ROLLBACK')
        connection.execute('BEGIN')
        self.missing(connection, 'ROLLBACK TO SAVEPOINT "JDBC_SAVEPOINT_0"')
        if self.profile == 'postgres':
            with self.assertRaises(psycopg.errors.InFailedSqlTransaction):
                connection.execute('SAVEPOINT next_object')
            connection.execute('ROLLBACK')
        else:
            connection.execute('SAVEPOINT next_object')
            connection.execute('INSERT INTO retained_rows VALUES(2)')
            connection.execute('COMMIT')
            self.assertEqual(connection.execute('SELECT id FROM retained_rows').fetchall(), [(2,)])

    def test_quoted_names_keep_case_spaces_and_escaped_quotes(self):
        connection = self.fixture()
        connection.execute('BEGIN')
        for mark, row in [('"Point"', 1), ('"point"', 2), ('"space name"', 3), ('"quote""name"', 4)]:
            connection.execute('SAVEPOINT ' + mark)
            connection.execute('INSERT INTO retained_rows VALUES(%s)', (row,))
        connection.execute('ROLLBACK TO SAVEPOINT "quote""name"')
        self.assertEqual(connection.execute('SELECT id FROM retained_rows ORDER BY id').fetchall(), [(1,), (2,), (3,)])
        connection.execute('ROLLBACK TO SAVEPOINT "space name"')
        self.assertEqual(connection.execute('SELECT id FROM retained_rows ORDER BY id').fetchall(), [(1,), (2,)])
        connection.execute('ROLLBACK TO SAVEPOINT "point"')
        self.assertEqual(connection.execute('SELECT id FROM retained_rows').fetchall(), [(1,)])
        connection.execute('ROLLBACK TO SAVEPOINT "Point"')
        self.assertEqual(connection.execute('SELECT id FROM retained_rows').fetchall(), [])
        connection.execute('COMMIT')


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresMissingSavepointTests(MissingSavepointCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleMissingSavepointTests(MissingSavepointCases, unittest.TestCase):
    profile = 'oracle-preview-v1'
