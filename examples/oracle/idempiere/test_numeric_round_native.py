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

    def test_oracle_explicit_numeric_operands_share_precision_policy(self):
        import psycopg
        f = self.fixture('oracle-preview-v1')
        with f.conn.cursor() as c:
            for operand in ['15.193', 'CAST(15.193 AS NUMERIC)',
                            'CAST(15.193 AS NUMBER)', '(CAST(15.193 AS NUMBER))']:
                for precision, expected in [('1.5', Decimal('15.2')),
                                            ('-1.5', Decimal('20'))]:
                    with self.subTest(operand=operand, precision=precision):
                        c.execute('SELECT ROUND('+operand+','+precision+') FROM DUAL', prepare=True)
                        self.assertEqual(c.fetchone(), (expected,))
                        self.assertEqual(c.description[0].type_code, 1700)
                with self.subTest(operand=operand), self.assertRaises(psycopg.errors.NumericValueOutOfRange):
                    c.execute('SELECT ROUND('+operand+',1001) FROM DUAL', prepare=True)

    def test_postgres_explicit_numeric_operands_keep_native_rounding(self):
        f = self.fixture('postgres')
        with f.conn.cursor() as c:
            for precision, expected in [(1, Decimal('15.2')), (-1, Decimal('20')),
                                        (2, Decimal('15.19'))]:
                c.execute('SELECT ROUND(CAST(15.193 AS NUMERIC),%s)', (precision,), prepare=True)
                self.assertEqual(c.fetchone(), (expected,))
                self.assertEqual(c.description[0].type_code, 1700)

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
            for binary, expected in [(2.675, Decimal('2.67')), (1.005, Decimal('1')),
                                     (-2.675, Decimal('-2.67'))]:
                c.execute('SELECT ROUND(%s,2) FROM DUAL', (binary,), prepare=True)
                self.assertEqual(c.fetchone(), (expected,))
                self.assertEqual(c.description[0].type_code, 1700)
            c.execute('SELECT ROUND(2.675,2),ROUND(1.005,2) FROM DUAL')
            self.assertEqual(c.fetchone(), (Decimal('2.68'),Decimal('1.01')))
            c.execute('CREATE FUNCTION "round"(n IN NUMBER,p IN NUMBER) RETURN NUMBER IS '
                      'BEGIN RETURN 77; END;', prepare=False)
            c.execute('SELECT "round"(1,2) FROM DUAL')
            self.assertEqual(c.fetchone(), (77,))
            c.execute('SELECT "round"(%s,%s),ROUND(CAST(15.193 AS NUMERIC),1) FROM DUAL',
                      (1,2), prepare=True)
            self.assertEqual(c.fetchone(), (77,Decimal('15.2')))
            c.execute('SELECT ROUND(SQRT(2),2) FROM DUAL')
            self.assertEqual(c.fetchone(), (Decimal('1.41'),))
            c.execute('DROP FUNCTION "round"', prepare=False)
            c.execute('CREATE SCHEMA "other_round_scope"')
            c.execute('CREATE FUNCTION "other_round_scope"."round"(n IN NUMBER,p IN NUMBER) '
                      'RETURN NUMBER IS BEGIN RETURN 88; END;', prepare=False)
            import psycopg
            with self.assertRaises(psycopg.errors.UndefinedFunction):
                c.execute('SELECT "round"(1,2) FROM DUAL')

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
            c.execute('CREATE FUNCTION "public"."round"(n numeric,p integer) '
                      'RETURNS integer LANGUAGE sql AS $$ SELECT 77 $$', prepare=False)
            c.execute('SELECT "public"."round"(1,2)')
            self.assertEqual(c.fetchone(), (77,))


if __name__ == '__main__':
    unittest.main()
