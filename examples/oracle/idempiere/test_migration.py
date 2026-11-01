"""Value fidelity and migration-policy regression tests; no database required."""
import datetime
import decimal
import tempfile
import unittest
from pathlib import Path

import export_reference as export
import import_bicdb as target


class MigrationTests(unittest.TestCase):
    def test_round_trip_retains_numeric_precision_binary_padding_and_null(self):
        values = [None, '', 'flag ', decimal.Decimal('12345678901234567890123456789012345678'),
                  decimal.Decimal('0.00000000000000000000000000000000000001'),
                  bytes(range(256)), datetime.datetime(2026, 11, 1, 12, 34, 56, 123456,
                                                        datetime.timezone(datetime.timedelta(hours=8)))]
        for value in values:
            self.assertEqual(target.canonical(target.decode(export.cell(value))), target.canonical(value))
        self.assertEqual(target.row_hash(values), target.row_hash([target.decode(export.cell(v)) for v in values]))
        self.assertNotEqual(target.row_hash([None]), target.row_hash(['']))
        self.assertNotEqual(target.row_hash(['flag ']), target.row_hash(['flag']))

    def test_decimal_context_does_not_round_reconciliation(self):
        with decimal.localcontext() as context:
            context.prec = 2
            self.assertEqual(target.canonical(decimal.Decimal('12345678901234567890123456789012345678.000')),
                             {'number': '12345678901234567890123456789012345678'})
        self.assertEqual(target.canonical(decimal.Decimal('-0.000')), {'number': '0'})

    def test_journal_is_replaced_with_valid_json(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'report.json'
            target.save_report(path, {'completed': ['A']})
            target.save_report(path, {'completed': ['A', 'B']})
            self.assertEqual(json.loads(path.read_text())['completed'], ['A', 'B'])
            self.assertFalse(path.with_name('report.json.tmp').exists())

    def test_unknown_value_encodings_and_types_fail(self):
        with self.assertRaises(ValueError): target.decode({'unknown': 'value'})
        with self.assertRaises(ValueError): target.decode({'bytes': '?'})
        with self.assertRaises(TypeError): target.canonical(1.2)

    def test_source_constraint_policy_is_explicit(self):
        constraint = dict(name='C', kind='R', status='ENABLED', deferrable='DEFERRABLE')
        self.assertEqual(target.table_ddl('S', 'T', [], [constraint]), 'CREATE TABLE "S"."T"()')
        with self.assertRaises(ValueError):
            target.table_ddl('S', 'T', [], [dict(constraint, status='DISABLED')])
        with self.assertRaises(ValueError):
            target.table_ddl('S', 'T', [], [dict(constraint, kind='U')])


if __name__ == '__main__': unittest.main()
