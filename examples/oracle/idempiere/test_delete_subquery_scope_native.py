"""DELETE init plans stay within one selection snapshot and preserve correlation."""
import os
import re
import unittest
from unittest.mock import patch

import test_foreign_keys as fixtures


class DeleteSubqueryScopeCases:
    def fixture(self):
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'DELETE fixture cleanup failed'))
        with patch.dict(os.environ, {'BICDB_TRACE_PLAN': '1'}):
            fixture.setUp()
        return fixture

    def test_duplicate_cleanup_has_bounded_scans_and_fresh_statement_data(self):
        fixture = self.fixture()
        connection = fixture.conn
        connection.execute('CREATE TABLE duplicate_rows(id INTEGER PRIMARY KEY,bucket INTEGER,label VARCHAR(10))')
        connection.execute('INSERT INTO duplicate_rows VALUES '+','.join("(%d,%d,'same')" % (i, i//2) for i in range(64)))
        sql = 'DELETE FROM duplicate_rows a WHERE (bucket,label) IN (SELECT bucket,label FROM duplicate_rows b GROUP BY bucket,label HAVING COUNT(*)>1) AND id NOT IN (SELECT MIN(id) FROM duplicate_rows c GROUP BY bucket,label)'
        self.assertEqual(connection.execute(sql).rowcount, 32)
        self.assertEqual(connection.execute('SELECT id FROM duplicate_rows ORDER BY id').fetchall(), [(i,) for i in range(0, 64, 2)])
        traces = [line for line in (fixture.root/'server.log').read_text().splitlines()
                  if line.startswith('bicdb_trace stmt=DELETE ')]
        self.assertTrue(traces)
        metrics = dict(re.findall(r'(\w+)=([^ ]+)', traces[-1]))
        self.assertLessEqual(int(metrics['full_scans']), 3)
        self.assertLessEqual(int(metrics['rows_materialized']), 64*3)
        connection.execute("INSERT INTO duplicate_rows VALUES(65,0,'same')")
        self.assertEqual(connection.execute(sql).rowcount, 1)
        self.assertEqual(connection.execute('SELECT COUNT(*) FROM duplicate_rows WHERE id=65').fetchall(), [(0,)])

    def test_correlated_predicate_rechecks_outer_rows(self):
        connection = self.fixture().conn
        for sql in [
            'CREATE TABLE scoped_rows(id INTEGER PRIMARY KEY,bucket INTEGER)',
            'CREATE TABLE scoped_keys(bucket INTEGER)',
            'INSERT INTO scoped_rows VALUES(1,20),(2,10),(3,20)',
            'INSERT INTO scoped_keys VALUES(10)',
        ]:
            connection.execute(sql)
        self.assertEqual(connection.execute('DELETE FROM scoped_rows a WHERE EXISTS(SELECT 1 FROM scoped_keys k WHERE k.bucket=a.bucket)').rowcount, 1)
        self.assertEqual(connection.execute('SELECT id FROM scoped_rows ORDER BY id').fetchall(), [(1,), (3,)])
        if self.profile == 'postgres':
            connection.execute('INSERT INTO scoped_rows VALUES(4,10)')
            self.assertEqual(connection.execute('DELETE FROM scoped_rows a USING scoped_keys u WHERE a.bucket=u.bucket AND a.id=(SELECT MIN(k.id) FROM scoped_rows k WHERE k.bucket=u.bucket)').rowcount, 1)
            self.assertEqual(connection.execute('SELECT id FROM scoped_rows ORDER BY id').fetchall(), [(1,), (3,)])

    def test_pending_writes_and_rollback_remain_visible(self):
        connection = self.fixture().conn
        connection.execute('CREATE TABLE pending_rows(id INTEGER PRIMARY KEY,bucket INTEGER)')
        connection.execute('INSERT INTO pending_rows VALUES(1,10),(2,10),(3,20)')
        connection.execute('BEGIN')
        connection.execute('INSERT INTO pending_rows VALUES(4,20)')
        connection.execute('SAVEPOINT retained')
        self.assertEqual(connection.execute('DELETE FROM pending_rows a WHERE bucket IN (SELECT bucket FROM pending_rows b GROUP BY bucket HAVING COUNT(*)>1) AND id NOT IN (SELECT MIN(id) FROM pending_rows c GROUP BY bucket)').rowcount, 2)
        self.assertEqual(connection.execute('SELECT id FROM pending_rows ORDER BY id').fetchall(), [(1,), (3,)])
        connection.execute('ROLLBACK TO SAVEPOINT retained')
        self.assertEqual(connection.execute('SELECT id FROM pending_rows ORDER BY id').fetchall(), [(1,), (2,), (3,), (4,)])
        connection.execute('ROLLBACK')
        self.assertEqual(connection.execute('SELECT id FROM pending_rows ORDER BY id').fetchall(), [(1,), (2,), (3,)])


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresDeleteSubqueryScopeTests(DeleteSubqueryScopeCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleDeleteSubqueryScopeTests(DeleteSubqueryScopeCases, unittest.TestCase):
    profile = 'oracle-preview-v1'
