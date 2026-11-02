import datetime
import decimal
import unittest

from compare import canonical, compare_cases, outcome, read_cases, ROOT


class Cursor:
    def __init__(self, rows=None, error=None):
        self.rows, self.error = rows, error
        self.description = ('synthetic',) if rows is not None else None

    def __enter__(self): return self
    def __exit__(self, *args): pass

    def execute(self, sql):
        if self.error: raise self.error

    def fetchmany(self, count): return self.rows[:count]


class Connection:
    def __init__(self, **args): self.args = args
    def cursor(self): return Cursor(**self.args)


class ComparisonTests(unittest.TestCase):
    def test_command_success_does_not_fetch_a_nonexistent_result(self):
        self.assertEqual(outcome(Connection(), 'CALL sample()'), {'rows': []})

    def test_numbers_preserve_precision_and_ignore_representation_scale(self):
        exact = '12345678901234567890123456789012345678'
        self.assertEqual(canonical(decimal.Decimal(exact)), {'number': exact})
        self.assertEqual(canonical(decimal.Decimal('12.00')), canonical(12))
        self.assertEqual(canonical(decimal.Decimal('-0.000')), canonical(0))
        self.assertNotEqual(canonical('12'), canonical(12))
        self.assertNotEqual(canonical('a '), canonical('a'))
        with self.assertRaises(TypeError): canonical(0.1)

    def test_oracle_number_infinities_do_not_reinterpret_native_finite_values(self):
        endpoint = decimal.Decimal('1e126')
        self.assertEqual(canonical(endpoint, oracle_number_sentinels=True), {'number': 'Infinity'})
        self.assertEqual(canonical(endpoint.copy_negate(), oracle_number_sentinels=True), {'number': '-Infinity'})
        self.assertEqual(canonical(decimal.Decimal('Infinity')), {'number': 'Infinity'})
        self.assertEqual(canonical(endpoint), {'number': '1' + '0' * 126})
        near = decimal.Decimal('9.9999999999999999999999999999999999999e125')
        self.assertNotEqual(canonical(near, oracle_number_sentinels=True), {'number': 'Infinity'})

    def test_dates_are_not_silently_equated_with_timestamps(self):
        date = datetime.date(2020, 2, 29)
        self.assertNotEqual(canonical(date), canonical(datetime.datetime(2020, 2, 29)))

    def test_day_second_intervals_preserve_microseconds_and_sign(self):
        self.assertEqual(canonical(datetime.timedelta(microseconds=1_000_001)),
                         {'interval': {'days': 0, 'seconds': 1, 'microseconds': 1}})
        self.assertEqual(canonical(datetime.timedelta(microseconds=-1)),
                         {'interval': {'days': -1, 'seconds': 86399, 'microseconds': 999999}})

    def test_matching_errors_are_failures_and_do_not_leak_messages(self):
        failing = Connection(error=RuntimeError('private connection information'))
        result = compare_cases(failing, failing, [{'id': 'a', 'sql': 'SELECT 1'}])[0]
        self.assertFalse(result['matched'])
        self.assertNotIn('private', str(result))

    def test_expected_errors_require_both_exact_backend_codes(self):
        class CodedError(Exception):
            def __init__(self, code):
                self.sqlstate = code
                super().__init__('private details')
        case = dict(id='format', sql='SELECT 1', expected_error=dict(oracle='ORA-01899', bicdb='22023'))
        source = Connection(error=CodedError('ORA-01899'))
        target = Connection(error=CodedError('22023'))
        self.assertTrue(compare_cases(source, target, [case])[0]['matched'])
        self.assertFalse(compare_cases(source, Connection(error=CodedError('0A000')), [case])[0]['matched'])
        self.assertFalse(compare_cases(source, Connection(rows=[(None,)]), [case])[0]['matched'])
        self.assertFalse(compare_cases(Connection(rows=[(None,)]), target, [case])[0]['matched'])
        self.assertNotIn('private', str(compare_cases(source, target, [case])))

    def test_expected_error_contract_rejects_missing_or_empty_codes(self):
        for expected in [dict(oracle='ORA-01899'), dict(oracle=None, bicdb='22023'), dict(oracle='ORA-01899', bicdb='')]:
            with self.assertRaises(ValueError):
                compare_cases(Connection(rows=[]), Connection(rows=[]),
                              [dict(id='bad', sql='SELECT 1', expected_error=expected)])

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
