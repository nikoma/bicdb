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

    def test_character_migration_preserves_units_without_padding_workarounds(self):
        for kind in ['VARCHAR2','CHAR']:
            for unit, expected in [('B','BYTE'),('C','CHAR')]:
                column=dict(name='V',type=kind,characters=3,character_unit=unit,bytes=3 if unit=='B' else 12,virtual='NO',default=None,nullable='Y')
                self.assertEqual(target.oracle_type(column),kind+'(3 '+expected+')')
                ddl=target.table_ddl('S','T',[column],[])
                self.assertIn(kind+'(3 '+expected+')',ddl)
                self.assertNotIn('octet_length',ddl)
        with self.assertRaisesRegex(ValueError,'provenance'):
            target.oracle_type(dict(type='VARCHAR2',characters=3))

    def test_resume_rejects_lost_character_provenance(self):
        column=dict(name='B',type='VARCHAR2',character_unit='B',characters=3)
        class Cursor:
            def __init__(self,row): self.row=row
            def execute(self,sql,args): self.args=args
            def fetchone(self): return self.row
        cursor=Cursor(('VARCHAR2(3 BYTE)',))
        self.assertEqual(target.verify_character_column_units(cursor,'S','T',[column]),['T.B'])
        self.assertEqual(cursor.args,['"S"."T"','B'])
        for row in [None,(None,),('VARCHAR2(3 CHAR)',)]:
            with self.assertRaisesRegex(ValueError,'provenance'):
                target.verify_character_column_units(Cursor(row),'S','T',[column])

    def test_unknown_value_encodings_and_types_fail(self):
        with self.assertRaises(ValueError): target.decode({'unknown': 'value'})
        with self.assertRaises(ValueError): target.decode({'bytes': '?'})
        with self.assertRaises(TypeError): target.canonical(1.2)

    def test_nullable_unique_policy_checks_completed_target_metadata(self):
        columns = [dict(name='A', nullable='N'), dict(name='B', nullable='Y')]
        constraint = dict(name='UQ', kind='U', columns=['A', 'B'])
        class Cursor:
            def __init__(self, rows):
                self.rows = iter(rows)
                self.calls = []
            def execute(self, sql, values): self.calls.append((sql, values))
            def fetchall(self): return next(self.rows)
        valid = Cursor([[('NO',)], [(True, True, True, False)]])
        self.assertEqual(target.verify_constraint_null_policies(valid, 'S', 'T', columns, [constraint]), ['UQ'])
        self.assertEqual(valid.calls[0][1], ('S', 'T', 'UQ', 'UNIQUE'))
        for bad in [[], [('YES',)], [(None,)]]:
            with self.assertRaisesRegex(ValueError, 'nullable-key semantics'):
                target.verify_constraint_null_policies(Cursor([bad]), 'S', 'T', columns, [constraint])
        with self.assertRaisesRegex(ValueError, 'executable native'):
            target.verify_constraint_null_policies(Cursor([[('NO',)], [(True, True, False, False)]]),
                                                  'S', 'T', columns, [constraint])
        columns[0]['nullable'] = 'Y'
        self.assertEqual(target.verify_constraint_null_policies(
            Cursor([[(None,)], [(True, True, True, True)]]), 'S', 'T', columns, [constraint]), ['UQ'])
        with self.assertRaisesRegex(ValueError, 'executable native'):
            target.verify_constraint_null_policies(Cursor([[(None,)], [(True, True, True, False)]]),
                                                  'S', 'T', columns, [constraint])
        # Ordinary single-column and wholly non-NULL keys need no hybrid policy.
        columns[0]['nullable'] = 'N'
        columns[1]['nullable'] = 'N'
        self.assertEqual(target.nullable_composite_constraints(columns, [constraint]), [])
        self.assertEqual(target.nullable_composite_constraints(columns, [dict(constraint, columns=['A'])]), [])

    def test_source_constraint_policy_is_explicit(self):
        constraint = dict(name='C', kind='R', status='ENABLED', deferrable='DEFERRABLE')
        self.assertEqual(target.table_ddl('S', 'T', [], [constraint]), 'CREATE TABLE "S"."T"()')
        with self.assertRaises(ValueError):
            target.table_ddl('S', 'T', [], [dict(constraint, status='DISABLED')])
        with self.assertRaises(ValueError):
            target.table_ddl('S', 'T', [], [dict(constraint, kind='U')])


if __name__ == '__main__': unittest.main()
