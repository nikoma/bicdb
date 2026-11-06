"""Savepoint rollback and update-then-insert recovery over PostgreSQL transport."""
import os
import unittest

import test_foreign_keys as native


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native savepoint acceptance')
class SavepointRecoveryNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        fixture = native.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(),
                                                'savepoint fixture cleanup failed'))
        fixture.setUp()
        with fixture.conn.cursor() as cursor:
            cursor.execute('CREATE TABLE recovery_parent(id INTEGER PRIMARY KEY, state VARCHAR(20))')
            cursor.execute('CREATE TABLE recovery_child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES recovery_parent(id), state VARCHAR(20))')
            cursor.execute('INSERT INTO recovery_parent VALUES(1, %s)', ('original',))
            cursor.execute('BEGIN')
            cursor.execute('UPDATE recovery_parent SET state=%s WHERE id=1', ('before',))
            cursor.execute('SAVEPOINT workflow_start')
            cursor.execute('INSERT INTO recovery_parent VALUES(2, %s)', ('running',))
            cursor.execute('SAVEPOINT nested_save')
            cursor.execute('INSERT INTO recovery_child VALUES(2, 2, %s)', ('running',))
            cursor.execute('UPDATE recovery_parent SET state=%s WHERE id=1', ('after',))
            cursor.execute('ROLLBACK TO SAVEPOINT workflow_start')
            cursor.execute('SELECT id, state FROM recovery_parent ORDER BY id')
            self.assertEqual(cursor.fetchall(), [(1, 'before')])
            cursor.execute('SELECT COUNT(*) FROM recovery_child')
            self.assertEqual(cursor.fetchone(), (0,))
            # An application may retain objects whose inserts were rolled back.
            # A zero update count must allow reinsertion in the same transaction.
            for table in ['recovery_parent', 'recovery_child']:
                cursor.execute('SAVEPOINT object_save')
                cursor.execute('UPDATE ' + table + ' SET state=%s WHERE id=2', ('failed',))
                self.assertEqual(cursor.rowcount, 0)
                cursor.execute('ROLLBACK TO SAVEPOINT object_save')
                if table == 'recovery_parent':
                    cursor.execute('INSERT INTO recovery_parent VALUES(3, %s)', ('failed',))
                else:
                    cursor.execute('INSERT INTO recovery_child VALUES(3, 3, %s)', ('failed',))
                self.assertEqual(cursor.rowcount, 1)
            cursor.execute('COMMIT')
        fixture.restart_server()
        with fixture.conn.cursor() as cursor:
            cursor.execute('SELECT id, state FROM recovery_parent ORDER BY id')
            self.assertEqual(cursor.fetchall(), [(1, 'before'), (3, 'failed')])
            cursor.execute('SELECT id, parent_id, state FROM recovery_child ORDER BY id')
            self.assertEqual(cursor.fetchall(), [(3, 3, 'failed')])

    def test_oracle_profile(self):
        self.check_profile('oracle-preview-v1')

    def test_postgresql_profile(self):
        self.check_profile('postgres')


if __name__ == '__main__':
    unittest.main()
