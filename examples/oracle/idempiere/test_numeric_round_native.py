"""Oracle two-argument numeric ROUND retains its NUMBER result contract."""
import os
import unittest
from decimal import Decimal

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for numeric ROUND checks')
class NumericRoundNativeTests(unittest.TestCase):
    def fixture(self, profile):
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'round fixture cleanup failed'))
        f.setUp()
        return f

    def test_oracle_round_scale_coerces_numeric_operands_and_retains_exact_literals(self):
        f = self.fixture('oracle-preview-v1')
        with f.conn.cursor() as c:
            for expression, expected in [
                ('ROUND(SQRT(2),2)', Decimal('1.41')),
                ('ROUND(15.193,1)', Decimal('15.2')),
                ('ROUND(-15.193,1)', Decimal('-15.2')),
                ('ROUND(15.193,-1)', Decimal('20')),
                ('ROUND(9007199254740993.123,2)', Decimal('9007199254740993.12')),
                ('ROUND(NULL,2)', None),
            ]:
                with self.subTest(expression=expression):
                    c.execute('SELECT '+expression+' FROM DUAL', prepare=True)
                    self.assertEqual(c.fetchone(), (expected,))
                    self.assertEqual(c.description[0].type_code, 1700)
            c.execute('SELECT ROUND(%s,2) FROM DUAL', (2**0.5,), prepare=True)
            self.assertEqual(c.fetchone(), (Decimal('1.41'),))
            self.assertEqual(c.description[0].type_code, 1700)

    def test_postgres_round_keeps_explicit_numeric_coercion_and_float_overload_rejection(self):
        import psycopg
        f = self.fixture('postgres')
        with f.conn.cursor() as c:
            c.execute('SELECT ROUND(CAST(SQRT(2) AS numeric),2)', prepare=True)
            self.assertEqual(c.fetchone(), (Decimal('1.41'),))
            self.assertEqual(c.description[0].type_code, 1700)
            # PostgreSQL does not provide ROUND(double precision, integer).
            # This control asserts rejection, without claiming exact error
            # field parity for the existing unsupported-overload path.
            with self.assertRaises(psycopg.Error):
                c.execute('SELECT ROUND(SQRT(2),2)', prepare=True)


if __name__ == '__main__':
    unittest.main()
