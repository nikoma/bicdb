"""Stored-code journal safety, dependency retries, and catalog drift checks."""
import unittest
from unittest.mock import Mock, patch

from install_schema_code import install, target_definition


class DatabaseError(Exception):
    sqlstate = '42P01'


def report(**changes):
    result = dict(completed=[], definitions={}, failures=[])
    result.update(changes)
    return result


class InstallationSafety(unittest.TestCase):
    def test_dependencies_retry_only_while_progress_is_made(self):
        items = [dict(name=name, ddl=name) for name in ['CHILD', 'PARENT']]
        state = report()
        created = set()
        cursor = Mock()
        def execute(sql, **kwargs):
            if sql == 'CHILD' and 'PARENT' not in created: raise DatabaseError('missing parent')
            created.add(sql)
        cursor.execute.side_effect = execute
        def definition(cursor, schema, item, phase):
            return dict(definition=item['name']) if item['name'] in created else None
        with patch('install_schema_code.target_definition', side_effect=definition):
            self.assertTrue(install(cursor, 'S', items, 'routines', state, Mock(), DatabaseError))
        self.assertEqual(state['completed'], ['PARENT', 'CHILD'])
        self.assertEqual(state['failures'], [])
        self.assertEqual(cursor.execute.call_count, 3)

    def test_no_progress_returns_failure_without_infinite_retries(self):
        cursor = Mock()
        cursor.execute.side_effect = DatabaseError('missing dependency')
        state = report()
        with patch('install_schema_code.target_definition', return_value=None):
            self.assertFalse(install(cursor, 'S', [dict(name='F', ddl='DDL')], 'routines', state, Mock(), DatabaseError))
        self.assertEqual(cursor.execute.call_count, 1)
        self.assertEqual(state['failures'][0]['sqlstate'], '42P01')

    def test_resume_validates_all_completed_definitions_before_new_ddl(self):
        cursor = Mock()
        items = [dict(name='NEW', ddl='DDL'), dict(name='OLD', ddl='old')]
        state = report(completed=['OLD'], definitions={'OLD': dict(catalog=['old'])})
        with patch('install_schema_code.target_definition', side_effect=[None, dict(catalog=['changed'])]):
            with self.assertRaisesRegex(ValueError, 'changed outside'):
                install(cursor, 'S', items, 'routines', state, Mock(), DatabaseError)
        cursor.execute.assert_not_called()

    def test_unjournaled_definitions_are_never_adopted_or_replaced(self):
        cursor = Mock()
        with patch('install_schema_code.target_definition', return_value=dict(catalog=['existing'])):
            with self.assertRaisesRegex(ValueError, 'unjournaled'):
                install(cursor, 'S', [dict(name='F', ddl='CREATE OR REPLACE')], 'routines', report(), Mock(), DatabaseError)
        cursor.execute.assert_not_called()

    def test_incomplete_or_duplicate_journals_fail_before_target_access(self):
        for state in [report(completed=['F', 'F'], definitions={'F': {}}),
                      report(completed=['UNKNOWN'], definitions={'UNKNOWN': {}}),
                      report(completed=['F'])]:
            cursor = Mock()
            with self.subTest(state=state), self.assertRaises(ValueError):
                install(cursor, 'S', [dict(name='F')], 'routines', state, Mock(), DatabaseError)
            cursor.execute.assert_not_called()

    def test_view_projection_mismatch_is_not_completed(self):
        state = report()
        with patch('install_schema_code.target_definition', side_effect=[None, dict(columns=['WRONG'])]):
            with self.assertRaisesRegex(ValueError, 'labels differ'):
                install(Mock(), 'S', [dict(name='V', ddl='DDL', columns=['RIGHT'])], 'views', state, Mock(), DatabaseError)
        self.assertEqual(state['completed'], [])

    def test_missing_catalog_definition_after_ddl_is_rejected(self):
        with patch('install_schema_code.target_definition', return_value=None):
            with self.assertRaisesRegex(ValueError, 'without a target'):
                install(Mock(), 'S', [dict(name='F', ddl='DDL')], 'routines', report(), Mock(), DatabaseError)

    def test_ambiguous_routines_are_rejected_and_catalog_records_owner(self):
        cursor = Mock()
        row = ('f', 'f', 'body', 0, 23, 10, False, 'S')
        cursor.fetchall.return_value = [row]
        self.assertEqual(target_definition(cursor, 'S', dict(name='F', kind='FUNCTION'), 'routines'),
                         dict(catalog=list(row)))
        cursor.fetchall.return_value = [(*row[:-1], 'OTHER')]
        with self.assertRaisesRegex(ValueError, 'another namespace'):
            target_definition(cursor, 'S', dict(name='F', kind='FUNCTION'), 'routines')
        cursor.fetchall.return_value = [row, row]
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            target_definition(cursor, 'S', dict(name='F', kind='FUNCTION'), 'routines')


if __name__ == '__main__': unittest.main()
