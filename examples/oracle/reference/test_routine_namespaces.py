import unittest

from routine_namespaces import fixture, identifier, run


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self): return self
    def __exit__(self, *args): pass

    def execute(self, sql):
        self.connection.statements.append(sql)
        if sql.startswith(self.connection.fail_prefix):
            raise RuntimeError('private connection information')


class Connection:
    version = 'test'

    def __init__(self, fail_prefix):
        self.fail_prefix = fail_prefix
        self.statements = []

    def cursor(self): return Cursor(self)


class RoutineNamespaceTests(unittest.TestCase):
    def test_identifiers_quote_contents_and_reject_nul(self):
        self.assertEqual(identifier('Quoted.Schema"Name'), '"Quoted.Schema""Name"')
        for value in ('', 'a\x00b', None):
            with self.assertRaises(ValueError): identifier(value)
        with self.assertRaises(ValueError): fixture('owner', 'unsafe;DROP')

    def test_fixture_has_distinct_shadows_and_explicit_oracle_expectations(self):
        plan = fixture('Quoted.Schema', 'BICDB_TEST')
        self.assertEqual(len(plan['cases']), 24)
        self.assertEqual(len({case['id'] for case in plan['cases']}), 24)
        shadows = [obj for obj in plan['setup'] if obj['name'].startswith('"public".')]
        self.assertEqual(len(shadows), 3)
        self.assertTrue(all(obj['backend'] == 1 for obj in shadows))
        self.assertTrue(all('-999' in obj['inserts'][0] for obj in shadows))
        self.assertTrue(all(case['expected'] != [[{'number': '-999'}]] for case in plan['cases']))

    def test_calendar_values_are_independent_and_include_exact_month_boundaries(self):
        from routine_calendar import fixture as calendar_fixture
        plan = calendar_fixture('owner', 'BICDB_TEST')
        self.assertEqual(len(plan['cases']), 36)
        self.assertEqual(len({case['id'] for case in plan['cases']}), 36)
        self.assertEqual({obj['backend'] for obj in plan['setup']}, {0, 1})
        self.assertTrue(all(obj['kind'] == 'FUNCTION' for obj in plan['setup']))
        self.assertTrue(any('1.999999999999999999999' in case['sql'] for case in plan['cases']))
        for prefix in ['bad;DROP', '', 'a']:
            with self.assertRaises(ValueError): calendar_fixture('owner', prefix)

    def test_calendar_suite_rejects_alternate_reference_calendar_before_ddl(self):
        class CalendarCursor(Cursor):
            def fetchone(self): return ('ARABIC HIJRAH',)

        class CalendarConnection(Connection):
            def cursor(self): return CalendarCursor(self)

        source = CalendarConnection('never matched')
        target = Connection('never matched')
        report = run(source, target, 'owner', 'BICDB_TEST', suite='calendar')
        self.assertFalse(report['success'])
        self.assertEqual(report['oracle_nls_calendar'], 'ARABIC HIJRAH')
        self.assertEqual(report['setup_error']['error'], 'ValueError')
        self.assertEqual(target.statements, [])
        self.assertFalse(any(sql.startswith('CREATE ') for sql in source.statements))
        self.assertEqual(report['cleanup_errors'], [])

    def test_week_fixture_asserts_values_and_errors_separately(self):
        from routine_week import fixture as week_fixture
        plan = week_fixture('owner', 'BICDB_TEST')
        self.assertEqual(len(plan['cases']), 102)
        self.assertEqual(len({case['id'] for case in plan['cases']}), 102)
        errors = [case for case in plan['cases'] if 'expected_error' in case]
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(case['expected_error'] == dict(oracle='ORA-01899', bicdb='22023') for case in errors))
        self.assertTrue(all('procedure_null_format' in case['id'] for case in errors))
        self.assertEqual(plan['cases'][-1]['id'], 'recovery_after_errors')

    def test_week_suite_rejects_unqualified_reference_territory_before_ddl(self):
        class TerritoryCursor(Cursor):
            def fetchone(self):
                return ('UNITED KINGDOM' if 'NLS_TERRITORY' in self.connection.statements[-1] else 'GREGORIAN',)
        class TerritoryConnection(Connection):
            def cursor(self): return TerritoryCursor(self)
        source, target = TerritoryConnection('never matched'), Connection('never matched')
        report = run(source, target, 'owner', 'BICDB_TEST', suite='week')
        self.assertFalse(report['success'])
        self.assertEqual(report['oracle_nls_territory'], 'UNITED KINGDOM')
        self.assertFalse(any(sql.startswith('CREATE ') for sql in source.statements))
        self.assertEqual(target.statements, [])

    def test_setup_failure_drops_only_successfully_created_objects_and_redacts_errors(self):
        source = Connection('CREATE TABLE "owner"."BICDB_TEST_A"')
        target = Connection('never matched')
        report = run(source, target, 'owner', 'BICDB_TEST')
        self.assertFalse(report['success'])
        self.assertEqual(sum(sql.startswith('DROP ') for sql in source.statements), 1)
        self.assertEqual(sum(sql.startswith('DROP ') for sql in target.statements), 1)
        self.assertNotIn('private connection information', str(report))

    def test_cleanup_errors_remain_failures(self):
        class FailingCleanupCursor(Cursor):
            def execute(self, sql):
                if sql.startswith('DROP '):
                    raise RuntimeError('private cleanup details')
                super().execute(sql)

        class FailingCleanup(Connection):
            def cursor(self): return FailingCleanupCursor(self)

        source = FailingCleanup('INSERT INTO')
        report = run(source, Connection('never matched'), 'owner', 'BICDB_TEST')
        self.assertFalse(report['success'])
        self.assertEqual(len(report['cleanup_errors']), 1)
        self.assertNotIn('private cleanup details', str(report))

    def test_temporal_fixture_distinguishes_number_interval_and_datetime_results(self):
        from routine_temporal import fixture as temporal_fixture
        plan = temporal_fixture('owner', 'BICDB_TEST')
        cases = {case['id']: case for case in plan['cases']}
        self.assertEqual(len(cases), 29)
        self.assertIn('number', cases['delta_0']['expected'][0][0])
        self.assertIn('interval', cases['stamp_delta_0']['expected'][0][0])
        self.assertIn('timestamp', cases['cast_date_0']['expected'][0][0])
        literals = [obj for obj in plan['setup'] if obj['name'].endswith('_LITERAL"')]
        self.assertTrue(all('() RETURN' not in obj['sql'] for obj in literals))


if __name__ == '__main__':
    unittest.main()
