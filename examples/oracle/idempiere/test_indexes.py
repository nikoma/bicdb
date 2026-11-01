"""Index migration policies, including Oracle's composite unique NULL rule."""
import unittest
from unittest.mock import Mock

import indexes


def key(name, expression=None):
    return dict(column=name, expression=expression, direction='ASC', position=1)


def index(keys, **changes):
    result = dict(name='IX', table='T', kind='NORMAL', status='VALID',
                  uniqueness='UNIQUE', columns=[dict(k, position=n) for n, k in enumerate(keys, 1)])
    result.update(changes)
    return result


class IndexPolicies(unittest.TestCase):
    def setUp(self):
        self.columns = {('T', 'A'): dict(nullable='N'), ('T', 'B'): dict(nullable='Y')}

    def test_plan_separates_constraint_and_lob_indexes(self):
        manifest = dict(constraints=[dict(kind='P', index='PK')], indexes=[
            index([key('A')], name='PK'), index([key('A')]),
            index([], name='LOB', kind='LOB')])
        logical, storage = indexes.index_plan(manifest)
        self.assertEqual([i['name'] for i in logical], ['IX'])
        self.assertEqual(storage, ['LOB'])
        manifest['indexes'][1]['status'] = 'UNUSABLE'
        with self.assertRaises(ValueError): indexes.index_plan(manifest)

    def test_resume_normalizes_late_retries_without_accepting_unknown_or_duplicate_indexes(self):
        names = ['EARLY', 'MIDDLE', 'LATE']
        self.assertEqual(indexes.completed_in_source_order(['LATE', 'EARLY'], names), ['EARLY', 'LATE'])
        for completed in [['EARLY', 'EARLY'], ['UNKNOWN'], [None]]:
            with self.assertRaises(ValueError): indexes.completed_in_source_order(completed, names)

    def test_plain_keys_remain_prefix_indexable(self):
        sql = indexes.index_ddl('S', index([key('A')]), self.columns)
        self.assertEqual(sql, 'CREATE UNIQUE INDEX "S"."IX" ON "S"."T"("A" ASC)')

    def test_nullable_composite_rejects_unverified_enforcement(self):
        item = index([key('A'), key('B')])
        with self.assertRaises(ValueError): indexes.index_ddl('S', item, self.columns)
        sql = indexes.index_ddl('S', item, self.columns, composite_nulls=True)
        self.assertTrue(sql.endswith('NULLS NOT DISTINCT'))
        self.assertNotIn(' WHERE ', sql)

    def test_all_nullable_keys_exclude_only_all_null_rows(self):
        item = index([key('B'), key('B', 'UPPER("B")')])
        sql = indexes.index_ddl('S', item, self.columns, composite_nulls=True)
        self.assertIn('NULLS NOT DISTINCT WHERE ("B") IS NOT NULL OR (UPPER("B")) IS NOT NULL', sql)
        nonnull = index([key('B', 'COALESCE("B",0)'), key('A', 'UPPER("A")')])
        self.assertFalse(indexes.needs_composite_null_policy(nonnull, self.columns))
        unknown = index([key('A'), key('B', 'COALESCE("B",\'\')')])
        self.assertTrue(indexes.needs_composite_null_policy(unknown, self.columns))

    def test_catalog_entry_alone_does_not_prove_an_executable_index(self):
        cursor = Mock()
        cursor.fetchall.side_effect = [[('T', 'CREATE INDEX example')], [(False, False)]]
        result = indexes.target_definition(cursor, 'S', 'IX')
        self.assertFalse(result['executable'])
        cursor.fetchall.side_effect = [[('T', 'CREATE INDEX example')], [(True, True)]]
        self.assertTrue(indexes.target_definition(cursor, 'S', 'IX')['executable'])

    def test_index_status_probe_rejects_legacy_placeholder_claims(self):
        from unittest.mock import patch
        cursor = Mock()
        with patch('indexes.target_definition', side_effect=[dict(executable=True), dict(executable=True)]):
            self.assertFalse(indexes.executable_status_support(cursor, 'S'))
        with patch('indexes.target_definition', side_effect=[dict(executable=True), dict(executable=False)]):
            self.assertTrue(indexes.executable_status_support(cursor, 'S'))
        self.assertTrue(cursor.execute.call_args.args[0].startswith('DROP TABLE '))

    def test_capability_requires_duplicate_rejection_and_always_cleans_up(self):
        cursor = Mock()
        self.assertFalse(indexes.composite_null_support(cursor, 'S'))
        self.assertTrue(cursor.execute.call_args.args[0].startswith('DROP TABLE '))
        duplicate = Exception('duplicate')
        duplicate.sqlstate = '23505'
        cursor.reset_mock()
        cursor.execute.side_effect = [None, None, None, duplicate, None]
        self.assertTrue(indexes.composite_null_support(cursor, 'S'))
        self.assertTrue(cursor.execute.call_args.args[0].startswith('DROP TABLE '))


if __name__ == '__main__': unittest.main()
