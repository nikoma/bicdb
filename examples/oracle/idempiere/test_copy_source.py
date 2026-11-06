"""COPY consumes validated source bytes and commits before journal advancement."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import sys
from unittest.mock import patch

import export_reference as export
import import_bicdb as target
import test_foreign_keys as native


class CopySourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root/'rows.jsonl'

    def table(self, data=b'[1]\n', rows=1):
        self.path.write_bytes(data)
        return dict(name='T', columns=['C'], file='rows.jsonl', rows=rows,
                    sha256=hashlib.sha256(data).hexdigest())

    def fixture(self, rewrite=False, final_count=1, initial_count=0, fail_commit=False):
        owner = self
        class Connection:
            def __init__(self): self.transactions = 0;self.committed = False
            @contextmanager
            def transaction(self):
                self.transactions += 1
                yield
                if fail_commit: raise RuntimeError('commit failed')
                self.committed = True
        class Cursor:
            def __init__(self): self.calls = [];self.rows = [];self.counts = iter([initial_count, final_count])
            def execute(self, sql): self.calls.append(sql)
            def fetchone(self): return (next(self.counts),)
            @contextmanager
            def copy(self, sql):
                self.calls.append(sql)
                if rewrite: owner.path.write_bytes(b'[999]\n')
                yield self
            def write_row(self, row): self.rows.append(row)
        return Connection(), Cursor()

    def test_changed_original_file_cannot_replace_the_validated_copy(self):
        table = self.table(b'[{"number":"12345678901234567890123456789012345678"}]\n')
        connection, cursor = self.fixture(rewrite=True)
        target.copy_table(connection, cursor, self.root, 'S"Q', table)
        self.assertEqual(cursor.rows, [[target.decimal.Decimal('12345678901234567890123456789012345678')]])
        self.assertTrue(connection.committed)
        self.assertEqual(cursor.calls[1], 'COPY "S""Q"."T"("C") FROM STDIN')
        self.assertEqual(self.path.read_bytes(), b'[999]\n')

    def test_invalid_rows_counts_and_digest_fail_before_any_target_access(self):
        cases = [(b'{"C":1}\n', 1), (b'[1,2]\n', 1), (b'[{"unknown":"x"}]\n', 1),
                 (b'[1.2]\n', 1), (b'[1]\n', True), (b'[1]\n', 0),
                 (b'[1]\n', 2), (b'not json\n', 1)]
        for data, count in cases:
            table = self.table(data, count)
            connection, cursor = self.fixture()
            with self.subTest(data=data, count=count), self.assertRaises((ValueError, TypeError)):
                target.copy_table(connection, cursor, self.root, 'S', table)
            self.assertEqual(cursor.calls, [])
            self.assertEqual(connection.transactions, 0)
        table = self.table(b'[1]\n');table['sha256'] = '0'*64
        connection, cursor = self.fixture()
        with self.assertRaisesRegex(ValueError, 'digest'): target.copy_table(connection, cursor, self.root, 'S', table)
        self.assertEqual(cursor.calls, [])
        for projection in ['C', [''], [None], ['C', 'C']]:
            table = self.table(b'[1]\n');table['columns'] = projection
            connection, cursor = self.fixture()
            with self.subTest(projection=projection), self.assertRaisesRegex(ValueError, 'projection'):
                target.copy_table(connection, cursor, self.root, 'S', table)
            self.assertEqual(cursor.calls, [])

    def test_nonempty_target_is_not_appended_to(self):
        connection, cursor = self.fixture(initial_count=1)
        with self.assertRaisesRegex(ValueError, 'empty'):
            target.copy_table(connection, cursor, self.root, 'S', self.table(b'[1]\n'))
        self.assertFalse(connection.committed)
        self.assertEqual(cursor.rows, [])

    def test_row_count_or_commit_failure_does_not_report_commit(self):
        for count, fail in [(2, False), (1, True)]:
            connection, cursor = self.fixture(final_count=count, fail_commit=fail)
            with self.subTest(count=count, fail=fail), self.assertRaises((ValueError, RuntimeError)):
                target.copy_table(connection, cursor, self.root, 'S', self.table(b'[1]\n'))
            self.assertFalse(connection.committed)


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native COPY acceptance')
class CopySourceNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        import psycopg
        fixture = native.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'COPY fixture cleanup failed'))
        fixture.setUp()
        rows = [[target.decimal.Decimal('12345678901234567890123456789012345678'), 'é  ', b'\x00\xff'],
                [target.decimal.Decimal('0'), None, b'']]
        raw = b''.join((json.dumps([export.cell(v) for v in row])+'\n').encode() for row in rows)
        (fixture.root/'rows.jsonl').write_bytes(raw)
        table = dict(name='copy_values', columns=['n', 'v', 'b'], rows=2, file='rows.jsonl', sha256=hashlib.sha256(raw).hexdigest())
        def run_phase(item, name):
            manifest = dict(source_schema='copy_source', tables=[item], columns=[], constraints=[])
            (fixture.root/'manifest.json').write_text(json.dumps(manifest))
            report = fixture.root/(name+'.json')
            argv = ['import_bicdb.py', '--snapshot', str(fixture.root), '--phase', 'data',
                    '--report', str(report), '--pg-dsn-env', 'BICDB_COPY_TEST_DSN']
            with patch.object(sys, 'argv', argv), patch.dict(os.environ, BICDB_COPY_TEST_DSN=fixture.dsn):
                target.main()
            return json.loads(report.read_text())
        with fixture.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA "copy_source"')
            cursor.execute('CREATE TABLE "copy_source"."copy_values"("n" NUMERIC(38,0),"v" TEXT,"b" BYTEA)')
            if profile == 'oracle-preview-v1':
                report = run_phase(table, 'data-success')
                self.assertTrue(report['success']);self.assertEqual(report['completed'], ['copy_values'])
            else:
                target.copy_table(fixture.conn, cursor, fixture.root, 'copy_source', table)
            target.verify_values(cursor, fixture.root, 'copy_source', table)
            with self.assertRaisesRegex(ValueError, 'empty'):
                target.copy_table(fixture.conn, cursor, fixture.root, 'copy_source', table)
            cursor.execute('SELECT COUNT(*) FROM "copy_source"."copy_values"')
            self.assertEqual(cursor.fetchone(), (2,))
            cursor.execute('CREATE TABLE "copy_source"."invalid_values"("n" NUMERIC CHECK("n">=0),"v" TEXT,"b" BYTEA)')
            bad = b'[1,"before",null]\n[-1,"bad",null]\n'
            (fixture.root/'rows.jsonl').write_bytes(bad)
            invalid = dict(table, name='invalid_values', sha256=hashlib.sha256(bad).hexdigest())
            with self.assertRaises(psycopg.Error):
                if profile == 'oracle-preview-v1': run_phase(invalid, 'data-failure')
                else: target.copy_table(fixture.conn, cursor, fixture.root, 'copy_source', invalid)
            if profile == 'oracle-preview-v1':
                report = json.loads((fixture.root/'data-failure.json').read_text())
                self.assertFalse(report['success']);self.assertEqual(report['completed'], [])
            cursor.execute('SELECT COUNT(*) FROM "copy_source"."invalid_values"')
            self.assertEqual(cursor.fetchone(), (0,))
            cursor.execute('INSERT INTO "copy_source"."invalid_values" VALUES(2, %s, NULL)', ('recovered',))
            cursor.execute('CREATE TABLE "copy_source"."parents"("n" NUMERIC PRIMARY KEY)')
            cursor.execute('CREATE TABLE "copy_source"."deferred_values"("n" NUMERIC REFERENCES "copy_source"."parents"("n") DEFERRABLE INITIALLY DEFERRED,"v" TEXT,"b" BYTEA)')
            deferred = dict(invalid, name='deferred_values')
            class RecordingCursor:
                def __init__(self): self.counts = []
                def execute(self, sql): return cursor.execute(sql)
                def fetchone(self):
                    row = cursor.fetchone();self.counts.append(row);return row
                def copy(self, sql): return cursor.copy(sql)
            observed = RecordingCursor()
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                target.copy_table(fixture.conn, observed, fixture.root, 'copy_source', deferred)
            # Both rows were visible inside COPY's transaction before COMMIT
            # rejected the deferred reference, rather than an early row failure.
            self.assertEqual(observed.counts, [(0,), (2,)])
            cursor.execute('SELECT COUNT(*) FROM "copy_source"."deferred_values"')
            self.assertEqual(cursor.fetchone(), (0,))
            cursor.execute('INSERT INTO "copy_source"."parents" VALUES(1),(-1)')
            target.copy_table(fixture.conn, cursor, fixture.root, 'copy_source', deferred)
        fixture.restart_server()
        with fixture.conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM "copy_source"."copy_values"')
            self.assertEqual(cursor.fetchone(), (2,))
            cursor.execute('SELECT "n","v" FROM "copy_source"."invalid_values"')
            self.assertEqual(cursor.fetchall(), [(2, 'recovered')])
            cursor.execute('SELECT COUNT(*) FROM "copy_source"."deferred_values"')
            self.assertEqual(cursor.fetchone(), (2,))

    def test_oracle_profile(self): self.check_profile('oracle-preview-v1')
    def test_postgresql_profile(self): self.check_profile('postgres')


if __name__ == '__main__': unittest.main()
