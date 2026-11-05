"""Value fidelity and migration-policy regression tests; no database required."""
import datetime
import decimal
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import export_reference as export
import import_bicdb as target


class SourceValueVerificationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def table(self, rows, columns=None):
        data = b''.join((json.dumps([export.cell(value) for value in row], ensure_ascii=False) + '\n').encode() for row in rows)
        (self.root/'rows.jsonl').write_bytes(data)
        return dict(name='T', columns=columns if columns is not None else ['C'+str(i) for i in range(len(rows[0]) if rows else 1)],
                    rows=len(rows), file='rows.jsonl', sha256=hashlib.sha256(data).hexdigest())

    def cursor(self, rows):
        class Cursor:
            def __init__(self):self.calls = []
            def execute(self, sql):self.calls.append(sql);self.rows = iter(rows)
            def fetchmany(self, count):
                import itertools
                return list(itertools.islice(self.rows, count))
        return Cursor()

    def test_reordered_rows_keep_precision_padding_binary_and_duplicate_multiplicity(self):
        row = [None, 'flag ', decimal.Decimal('12345678901234567890123456789012345678'), bytes(range(256)),
               datetime.datetime(2026, 11, 1, 12, 34, 56, 123456, datetime.timezone(datetime.timedelta(hours=8)))]
        other = [None, 'flag', decimal.Decimal('0.00000000000000000000000000000000000001'), b'', row[-1]]
        table = self.table([row, other, row])
        with decimal.localcontext() as context:
            context.prec = 2
            left = target.verify_values(self.cursor([row, row, other]), self.root, 'S"Q', table)
            right = target.verify_values(self.cursor([other, row, row]), self.root, 'S"Q', table)
        self.assertEqual(left, right);self.assertEqual(len(left), 64)
        with self.assertRaisesRegex(ValueError, 'target values differ'):
            target.verify_values(self.cursor([row, other]), self.root, 'S"Q', table)
        with self.assertRaisesRegex(ValueError, 'target values differ'):
            target.verify_values(self.cursor([row, other, other]), self.root, 'S"Q', table)

    def test_empty_table_and_literal_identifier_projection(self):
        table = self.table([], ['C"D'])
        cursor = self.cursor([])
        self.assertEqual(target.verify_values(cursor, self.root, 'S"Q', table), hashlib.sha256(b'').hexdigest())
        self.assertEqual(cursor.calls, ['SELECT "C""D" FROM "S""Q"."T"'])

    def test_source_row_count_is_required_even_when_target_matches_file(self):
        table = self.table([[1]])
        for count in [0, 2, -1, True, 1.0, '1']:
            cursor = self.cursor([[1]])
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, 'row count'):
                target.verify_values(cursor, self.root, 'S', dict(table, rows=count))
            self.assertEqual(cursor.calls, [])

    def test_source_row_width_and_projection_are_checked_before_target_access(self):
        table = self.table([[1]])
        for columns in [[], ['C', 'C'], ['C', 'D']]:
            cursor = self.cursor([[1]])
            with self.subTest(columns=columns), self.assertRaisesRegex(ValueError, 'projection'):
                target.verify_values(cursor, self.root, 'S', dict(table, columns=columns))
            self.assertEqual(cursor.calls, [])
        data = b'{"C":1}\n';(self.root/'rows.jsonl').write_bytes(data)
        with self.assertRaisesRegex(ValueError, 'row width'):
            target.verify_values(self.cursor([[1]]), self.root, 'S', dict(table, sha256=hashlib.sha256(data).hexdigest()))

    def test_corrupt_file_and_external_path_are_refused_before_target_access(self):
        table = self.table([[1]])
        for changed in [dict(table, sha256='0'*64), dict(table, file='../rows.jsonl')]:
            cursor = self.cursor([[1]])
            with self.subTest(table=changed), self.assertRaises(ValueError):
                target.verify_values(cursor, self.root, 'S', changed)
            self.assertEqual(cursor.calls, [])

    def test_unknown_or_inexact_source_values_are_not_coerced(self):
        table = self.table([[1]])
        for data in [b'[{"unknown":"value"}]\n', b'[1.2]\n']:
            (self.root/'rows.jsonl').write_bytes(data);cursor = self.cursor([[1]])
            with self.subTest(data=data), self.assertRaises((ValueError, TypeError)):
                target.verify_values(cursor, self.root, 'S', dict(table, sha256=hashlib.sha256(data).hexdigest()))
            self.assertEqual(cursor.calls, [])


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
