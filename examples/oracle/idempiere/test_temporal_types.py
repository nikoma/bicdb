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
        class Cursor:
            def __init__(self, rows): self.rows = rows
            def execute(self, sql, args): self.args = args
            def fetchall(self): return self.rows
        for rows in [[], [(1082, 0)], [(1114, 6)], [(1114, 0), (1114, 0)]]:
            with self.assertRaisesRegex(ValueError, 'physical temporal type'):
                target.check_physical_type(Cursor(rows), 'S', 'T', 'D', 1114, {0})
        cursor = Cursor([(1114, 0)])
        target.check_physical_type(cursor, 'S', 'T', 'D', 1114, {0})
        self.assertEqual(cursor.args, ('S', 'T', 'D'))


if __name__ == '__main__':
    unittest.main()
