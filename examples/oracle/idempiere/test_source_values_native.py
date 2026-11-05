"""Strict exported-value reconciliation over PostgreSQL transport in both profiles."""
import datetime
import decimal
import hashlib
import json
import os
import sys
import unittest
from unittest.mock import patch

import export_reference as export
import import_bicdb as target
import test_foreign_keys as fixture_module


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native value reconciliation')
class SourceValuesNativeTests(unittest.TestCase):
    def check_profile(self, profile):
        fixture = fixture_module.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'native value fixture cleanup failed'))
        fixture.setUp()
        oracle = profile == 'oracle-preview-v1'
        binary_type = 'BLOB' if oracle else 'BYTEA'
        schema, name = 'Source Q', 'T.Values'
        relation = target.quote(schema) + '.' + target.quote(name)
        columns = ['N', 'V', 'C', 'B', 'D']
        row = [decimal.Decimal('12345678901234567890123456789012345678'), 'é', 'X  ',
               bytes([0, 255, 1, 128]), datetime.datetime(2026, 11, 1, 12, 34, 56, 123456)]
        other = [decimal.Decimal('0'), None if oracle else '', None, b'', None]
        with fixture.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA ' + target.quote(schema))
            cursor.execute('CREATE TABLE ' + relation + '("N" NUMERIC(38,0),"V" VARCHAR(20),"C" CHAR(3),"B" ' + binary_type + ',"D" TIMESTAMP(6))')
            for values in [row, [other[0], '', *other[2:]], row]:
                cursor.execute('INSERT INTO ' + relation + ' VALUES(%s,%s,%s,%s,%s)', values)
            data = b''.join((json.dumps([export.cell(value) for value in values], ensure_ascii=False) + '\n').encode() for values in [other, row, row])
            (fixture.root/'rows.jsonl').write_bytes(data)
            table = dict(name=name, columns=columns, rows=3, file='rows.jsonl', sha256=hashlib.sha256(data).hexdigest())
            with decimal.localcontext() as context:
                context.prec = 2
                digest = target.verify_values(cursor, fixture.root, schema, table)
            self.assertEqual(len(digest), 64)
            if oracle:
                manifest = dict(source_schema=schema, tables=[table], columns=[], constraints=[])
                (fixture.root/'manifest.json').write_text(json.dumps(manifest))
                report_path = fixture.root/'verify.json'
                command = ['import_bicdb.py', '--snapshot', str(fixture.root), '--phase', 'verify',
                           '--report', str(report_path), '--pg-dsn-env', 'BICDB_VALUE_NATIVE_DSN']
                def verify(resume=False):
                    with patch.object(sys, 'argv', command + (['--resume'] if resume else [])), patch.dict(os.environ, BICDB_VALUE_NATIVE_DSN=fixture.dsn):
                        target.main()
                verify();verify(resume=True)
                good_report = report_path.read_bytes()
                incomplete = json.loads(good_report);incomplete['value_digests'] = {}
                report_path.write_text(json.dumps(incomplete))
                with self.assertRaisesRegex(ValueError, 'matching saved digest'):verify(resume=True)
                report_path.write_bytes(good_report)
            cursor.execute('UPDATE ' + relation + ' SET "V"=%s WHERE "N"=0', ('changed',))
            with self.assertRaisesRegex(ValueError, 'target values differ'):
                target.verify_values(cursor, fixture.root, schema, table)
            if oracle:
                with self.assertRaisesRegex(ValueError, 'target values differ'):verify(resume=True)
                self.assertFalse(json.loads(report_path.read_text())['success'])

    def test_default_postgres_value_reconciliation(self):
        self.check_profile('postgres')

    def test_oracle_preview_value_reconciliation(self):
        self.check_profile('oracle-preview-v1')


if __name__ == '__main__':
    unittest.main()
