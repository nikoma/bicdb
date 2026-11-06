"""Physical heap locks preserve duplicate rows, transactions, joins and security."""
import os
import unittest

import test_foreign_keys as fixtures


class HeapLockCases:
    def fixture(self):
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'heap fixture cleanup failed'))
        f.setUp()
        return f

    def peer(self, f):
        import psycopg
        conn = psycopg.connect(f.dsn, autocommit=True,
                               options='-c bicdb.sql_profile='+self.profile)
        self.addCleanup(conn.close)
        return conn

    def test_identical_heap_rows_lock_independently_and_stay_hidden(self):
        import psycopg
        f = self.fixture()
        a, b, c = f.conn, self.peer(f), self.peer(f)
        a.execute('CREATE TABLE heap_rows(label VARCHAR(30))')
        a.execute("INSERT INTO heap_rows VALUES ('same'),('same'),('other')")
        a.execute('BEGIN')
        cursor = a.execute("SELECT * FROM heap_rows WHERE label='same' LIMIT 1 FOR UPDATE")
        self.assertEqual(cursor.fetchall(), [('same',)])
        self.assertEqual(len(cursor.description), 1)
        b.execute('BEGIN')
        self.assertEqual(b.execute("SELECT label FROM heap_rows WHERE label='same' FOR UPDATE SKIP LOCKED").fetchall(), [('same',)])
        c.execute('BEGIN')
        with self.assertRaises(psycopg.errors.LockNotAvailable):
            c.execute("SELECT label FROM heap_rows WHERE label='same' FOR UPDATE NOWAIT")
        c.execute('ROLLBACK')
        b.execute('ROLLBACK')
        c.execute('BEGIN')
        self.assertEqual(c.execute("SELECT label FROM heap_rows WHERE label='same' FOR UPDATE SKIP LOCKED").fetchall(), [('same',)])
        a.execute('ROLLBACK')
        self.assertEqual(c.execute("SELECT label FROM heap_rows WHERE label='same' FOR UPDATE NOWAIT").fetchall(), [('same',), ('same',)])
        c.execute('ROLLBACK')

    def test_pending_heap_rows_savepoints_and_restart(self):
        f = self.fixture()
        a, b = f.conn, self.peer(f)
        a.execute('CREATE TABLE heap_pending(tag INTEGER,label VARCHAR(30))')
        a.execute("INSERT INTO heap_pending VALUES (1,'old'),(2,'old')")
        a.execute('BEGIN')
        a.execute("INSERT INTO heap_pending VALUES (3,'new')")
        a.execute("UPDATE heap_pending SET label='new' WHERE tag=1")
        self.assertEqual(a.execute("SELECT tag FROM heap_pending WHERE label='new' ORDER BY tag FOR UPDATE NOWAIT").fetchall(), [(1,), (3,)])
        a.execute('ROLLBACK')
        a.execute('BEGIN')
        a.execute('SAVEPOINT heap_mark')
        self.assertEqual(a.execute('SELECT tag FROM heap_pending WHERE tag=1 FOR UPDATE').fetchall(), [(1,)])
        a.execute('ROLLBACK TO SAVEPOINT heap_mark')
        b.execute('BEGIN')
        self.assertEqual(b.execute('SELECT tag FROM heap_pending WHERE tag=1 FOR UPDATE NOWAIT').fetchall(), [(1,)])
        b.execute('ROLLBACK')
        a.execute('COMMIT')
        b.close()
        f.restart_server()
        self.assertEqual(f.conn.execute('SELECT tag FROM heap_pending ORDER BY tag FOR UPDATE').fetchall(), [(1,), (2,)])

    def test_heap_join_locks_only_requested_preserved_input(self):
        import psycopg
        f = self.fixture()
        a, b = f.conn, self.peer(f)
        for table in ('heap_left', 'heap_right'):
            a.execute('CREATE TABLE '+table+'(tag INTEGER,label VARCHAR(30))')
            a.execute("INSERT INTO "+table+" VALUES (1,'same'),(2,'same')")
            a.execute('CREATE INDEX '+table+'_tag ON '+table+'(tag)')
        a.execute('BEGIN')
        self.assertEqual(a.execute('SELECT l.label,r.label FROM heap_left l JOIN heap_right r ON l.tag=r.tag WHERE l.tag=1 FOR UPDATE OF r').fetchall(), [('same', 'same')])
        b.execute('BEGIN')
        self.assertEqual(b.execute('SELECT tag FROM heap_left WHERE tag=1 FOR UPDATE NOWAIT').fetchall(), [(1,)])
        with self.assertRaises(psycopg.errors.LockNotAvailable):
            b.execute('SELECT tag FROM heap_right WHERE tag=1 FOR UPDATE NOWAIT')
        b.execute('ROLLBACK')
        a.execute('ROLLBACK')
        self.assertEqual(a.execute('SELECT l.tag FROM heap_left l LEFT JOIN heap_right r ON l.tag=r.tag WHERE l.tag=1 FOR UPDATE OF l').fetchall(), [(1,)])
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            a.execute('SELECT l.tag FROM heap_left l LEFT JOIN heap_right r ON l.tag=r.tag FOR UPDATE OF r')

    def test_heap_lock_requires_update_and_respects_update_rls(self):
        import psycopg
        f = self.fixture()
        admin, reader, peer = f.conn, self.peer(f), self.peer(f)
        admin.execute('CREATE TABLE heap_secure(tag INTEGER)')
        admin.execute('INSERT INTO heap_secure VALUES (1),(2)')
        admin.execute('CREATE ROLE heap_reader')
        admin.execute('GRANT SELECT ON heap_secure TO heap_reader')
        reader.execute('SET ROLE heap_reader')
        with self.assertRaises(psycopg.errors.InsufficientPrivilege):
            reader.execute('SELECT tag FROM heap_secure WHERE FALSE FOR UPDATE')
        admin.execute('GRANT UPDATE ON heap_secure TO heap_reader')
        admin.execute('ALTER TABLE heap_secure ENABLE ROW LEVEL SECURITY')
        admin.execute('CREATE POLICY heap_select ON heap_secure FOR SELECT USING (tag=1)')
        reader.execute('BEGIN')
        self.assertEqual(reader.execute('SELECT tag FROM heap_secure FOR UPDATE').fetchall(), [])
        peer.execute('BEGIN')
        self.assertEqual(peer.execute('SELECT tag FROM heap_secure WHERE tag=1 FOR UPDATE NOWAIT').fetchall(), [(1,)])
        peer.execute('ROLLBACK')
        reader.execute('ROLLBACK')
        admin.execute('CREATE POLICY heap_update ON heap_secure FOR UPDATE USING (tag=1)')
        reader.execute('BEGIN')
        self.assertEqual(reader.execute('SELECT tag FROM heap_secure FOR UPDATE').fetchall(), [(1,)])
        with self.assertRaises(psycopg.errors.LockNotAvailable):
            peer.execute('SELECT tag FROM heap_secure WHERE tag=1 FOR UPDATE NOWAIT')
        reader.execute('ROLLBACK')


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresHeapLockTests(HeapLockCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleHeapLockTests(HeapLockCases, unittest.TestCase):
    profile = 'oracle-preview-v1'


if __name__ == '__main__':
    unittest.main()
