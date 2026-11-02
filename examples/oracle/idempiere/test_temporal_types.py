"""Source declaration and snapshot boundary checks, without a database."""
import unittest

import temporal_types as target


class TemporalTypesTests(unittest.TestCase):
    def test_declarations_preserve_date_timestamp_and_timezone_distinctions(self):
        for kind, expected in [
            ('DATE', ('DATE', 1114, {0})),
            ('TIMESTAMP', ('TIMESTAMP(6)', 1114, {6, -1})),
            ('TIMESTAMP(0)', ('TIMESTAMP(0)', 1114, {0})),
            ('TIMESTAMP(3)', ('TIMESTAMP(3)', 1114, {3})),
            ('TIMESTAMP(6) WITH TIME ZONE', ('TIMESTAMP(6) WITH TIME ZONE', 1184, {6, -1})),
        ]:
            self.assertEqual(target.declaration({'type': kind}), expected)

    def test_unqualified_precision_and_local_timezone_fail_explicitly(self):
        for kind in ['TIMESTAMP(7)', 'TIMESTAMP(9)', 'TIMESTAMP WITH LOCAL TIME ZONE', 'VARCHAR2']:
            with self.assertRaisesRegex(ValueError, 'unqualified'):
                target.declaration({'type': kind})

    def test_view_columns_are_excluded_from_table_backfill(self):
        manifest = dict(tables=[dict(name='T', columns=['D', 'N'])], columns=[
            dict(table='T', name='D', type='DATE'),
            dict(table='T', name='N', type='NUMBER'),
            dict(table='V', name='D', type='DATE'),
        ])
        self.assertEqual(target.temporal_columns(manifest), [manifest['columns'][0]])

    def test_ambiguous_and_missing_source_columns_fail(self):
        column = dict(table='T', name='D', type='DATE')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            target.temporal_columns(dict(tables=[dict(name='T', columns=['D'])], columns=[column, column]))
        with self.assertRaisesRegex(ValueError, 'absent'):
            target.temporal_columns(dict(tables=[dict(name='T', columns=['OTHER'])], columns=[column]))

    def test_physical_type_check_rejects_ambiguous_or_different_storage(self):
        for columns in [{}, {('T','D'): (1082, 0)}, {('T','D'): (1114, 6)}]:
            with self.assertRaisesRegex(ValueError, 'physical temporal type'):
                target.check_physical_type(columns, 'T', 'D', 1114, {0})
        target.check_physical_type({('T','D'): (1114,0)}, 'T', 'D', 1114, {0})

    def test_catalog_inventory_filters_other_schemas_and_rejects_duplicates(self):
        class Cursor:
            def __init__(self, attributes):
                self.results = iter([[(99,)], [(1,'T')], attributes])
            def execute(self, *args): pass
            def fetchall(self): return next(self.results)
        cursor = Cursor([(1,'D',1114,0), (2,'D',1082,-1)])
        self.assertEqual(target.physical_columns(cursor, 'S'), {('T','D'): (1114,0)})
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            target.physical_columns(Cursor([(1,'D',1114,0), (1,'D',1114,0)]), 'S')
        self.assertEqual(target.physical_columns(
            Cursor([(1,'D',1114,0), (1,'OTHER',23,-1), (1,'OTHER',23,-1)]),
            'S', {('T','D')}), {('T','D'): (1114,0)})

    def test_target_fingerprints_preserve_duplicates_nulls_and_whitespace(self):
        class Cursor:
            def __init__(self, rows): self.rows = rows
            def execute(self, *args, **kwargs): pass
            def __iter__(self): return iter(self.rows)
        table = dict(name='T', columns=['D'])
        fingerprint = lambda rows: target.table_fingerprint(Cursor(rows), 'S', table)
        self.assertEqual(fingerprint([(None,), ('x ',)]), fingerprint([('x ',), (None,)]))
        self.assertNotEqual(fingerprint([('x ',)]), fingerprint([('x',)]))
        self.assertNotEqual(fingerprint([('x',)]), fingerprint([('x',), ('x',)]))

    def test_batched_logical_metadata_preserves_quoted_names(self):
        class Cursor:
            def execute(self, sql, args): self.args = args
            def fetchone(self): return ('DATE', None)
        cursor = Cursor()
        self.assertEqual(target.column_types(cursor, 'A"B', 'T', [{'name':'D'}, {'name':'Quoted.Date'}]),
                         {'D':'DATE', 'Quoted.Date':None})
        self.assertEqual(cursor.args, ['"A""B"."T"','D','"A""B"."T"','Quoted.Date'])


if __name__ == '__main__':
    unittest.main()
