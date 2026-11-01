import datetime
import decimal
import unittest

from compare import canonical, compare_cases, outcome, read_cases, ROOT


class Cursor:
    def __init__(self, rows=None, error=None):
        self.rows, self.error = rows, error

    def __enter__(self): return self
    def __exit__(self, *args): pass

    def execute(self, sql):
        if self.error: raise self.error

    def fetchmany(self, count): return self.rows[:count]


class Connection:
    def __init__(self, **args): self.args = args
    def cursor(self): return Cursor(**self.args)


class ComparisonTests(unittest.TestCase):
    def test_numbers_preserve_precision_and_ignore_representation_scale(self):
        exact = '12345678901234567890123456789012345678'
        self.assertEqual(canonical(decimal.Decimal(exact)), {'number': exact})
        self.assertEqual(canonical(decimal.Decimal('12.00')), canonical(12))
        self.assertEqual(canonical(decimal.Decimal('-0.000')), canonical(0))
        self.assertNotEqual(canonical('12'), canonical(12))
        self.assertNotEqual(canonical('a '), canonical('a'))
        with self.assertRaises(TypeError): canonical(0.1)

    def test_dates_are_not_silently_equated_with_timestamps(self):
        date = datetime.date(2020, 2, 29)
        self.assertNotEqual(canonical(date), canonical(datetime.datetime(2020, 2, 29)))

    def test_matching_errors_are_failures_and_do_not_leak_messages(self):
        failing = Connection(error=RuntimeError('private connection information'))
        result = compare_cases(failing, failing, [{'id': 'a', 'sql': 'SELECT 1'}])[0]
        self.assertFalse(result['matched'])
        self.assertNotIn('private', str(result))

    def test_row_bound_and_order_are_enforced(self):
        self.assertIn('error', outcome(Connection(rows=[(1,), (2,)]), 'SELECT 1', max_rows=1))
        result = compare_cases(Connection(rows=[(1,), (2,)]), Connection(rows=[(2,), (1,)]),
                               [{'id': 'a', 'sql': 'SELECT 1'}])[0]
        self.assertFalse(result['matched'])

    def test_fixture_is_nonempty_and_has_unique_identifiers(self):
        fixture, digest = read_cases(ROOT / 'cases.json')
        self.assertGreater(len(fixture['cases']), 20)
        self.assertEqual(len(digest), 64)


if __name__ == '__main__': unittest.main()
