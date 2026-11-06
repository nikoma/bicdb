"""Observer controls; fake driver results are not Oracle qualification."""
import sys
import types
import unittest
from unittest.mock import patch

import like_escape_contract as contract


class DriverError(Exception):
    pass


class Connection:
    version = 'observer-test-only'

    def __init__(self, failure=None):
        self.failure = failure
        self.statements = []
        self.closed = False
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def rollback(self):
        self.rollbacks += 1

    def cursor(self):
        connection = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def execute(self, sql, binds=None):
                connection.statements.append(sql)
                self.sql = sql
                if sql.startswith('SELECT CASE') and connection.failure:
                    code, full_code = connection.failure
                    connection.failure = None
                    raise DriverError(types.SimpleNamespace(code=code, full_code=full_code))

            def fetchone(self):
                if self.sql.startswith("SELECT LENGTH('§')"):
                    return (1, 2, 'AL32UTF8', 1, 2, 'AL32UTF8')
                return (1,)

            def fetchall(self):
                if 'NLS_DATABASE_PARAMETERS' in self.sql:
                    return [('NLS_CHARACTERSET', 'AL32UTF8'), ('NLS_NCHAR_CHARACTERSET', 'AL16UTF16')]
                if 'NLS_SESSION_PARAMETERS' in self.sql:
                    return [('NLS_COMP', 'BINARY'), ('NLS_SORT', 'BINARY')]
                return [(0,)]

        return Cursor()


class LikeObservationTests(unittest.TestCase):
    def setUp(self):
        driver = types.SimpleNamespace(Error=DriverError, __version__='test-only', is_thin_mode=lambda: True)
        self.patch = patch.dict(sys.modules, {'oracledb': driver})
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_corpora_have_stable_counts_and_unique_ids(self):
        self.assertEqual(len(contract.PLANS), 22)
        self.assertEqual(len(contract.CONTEXT_PLANS), 5)
        national = contract.national_plans()
        self.assertEqual(len(national), 82)
        self.assertEqual(len({row[0] for row in national}), 82)
        self.assertEqual(len(contract.NATIONAL_CONTEXT_PLANS), 2)
        self.assertTrue(all(not binds for _, _, binds in national))
        self.assertTrue(any('😀' in sql and "N'__'" in sql for _, sql, _ in national))

    def test_default_contract_stays_ordinary_and_read_only(self):
        connection = Connection()
        result = contract.observe(lambda: connection)
        self.assertEqual(result['contract'], 'ordinary')
        self.assertEqual(len(result['cases']), 22)
        self.assertEqual(len(result['predicate_contexts']), 5)
        self.assertTrue(result['observation_completed'])
        self.assertFalse(result['implementation_qualified'])
        self.assertFalse(result['application_qualified'])
        self.assertFalse(result['original_application_objects_modified'])
        self.assertEqual(connection.statements[0], 'SET TRANSACTION READ ONLY')
        self.assertTrue(all(sql.startswith(('SET TRANSACTION READ ONLY', 'SELECT ')) for sql in connection.statements))
        self.assertEqual(connection.rollbacks, 1)
        self.assertTrue(connection.closed)

    def test_national_contract_selects_its_complete_corpus(self):
        connection = Connection()
        result = contract.observe(lambda: connection, contract='national')
        self.assertEqual(result['contract'], 'national')
        self.assertEqual(len(result['cases']), 82)
        self.assertEqual(len(result['predicate_contexts']), 2)
        self.assertTrue(result['observation_completed'])
        self.assertFalse(result['implementation_qualified'])
        self.assertTrue(connection.closed)

    def test_cast_contract_is_independent_of_pattern_contract(self):
        connection = Connection()
        result = contract.observe(lambda: connection, contract='national-casts')
        self.assertEqual(len(result['cases']), 24)
        self.assertEqual(len(result['predicate_contexts']), 15)
        self.assertTrue(result['observation_completed'])
        self.assertFalse(result['implementation_qualified'])
        self.assertTrue(connection.closed)

    def test_raw_unit_contract_does_not_claim_direct_text_decoding(self):
        plans = contract.national_cast_unit_contexts()
        self.assertEqual(len(plans), 15)
        self.assertEqual(sum(sql.startswith('SELECT DUMP(') for _, sql, _ in plans), 13)
        self.assertTrue(all(sql.startswith('SELECT CAST(') for _, sql, _ in contract.NATIONAL_CAST_CONTEXT_PLANS[:13]))
        connection = Connection()
        result = contract.observe(lambda: connection, contract='national-cast-units')
        self.assertEqual(result['contract'], 'national-cast-units')
        self.assertEqual(len(result['predicate_contexts']), 15)
        self.assertTrue(result['observation_completed'])
        self.assertFalse(result['implementation_qualified'])

    def test_character_set_mismatch_is_recorded_as_semantic_evidence(self):
        connection = Connection((12704, 'ORA-12704'))
        result = contract.observe(lambda: connection, contract='national')
        self.assertEqual(result['cases'][0]['oracle_error_code'], 12704)
        self.assertEqual(len(result['cases']), 82)
        self.assertTrue(result['observation_completed'])
        self.assertEqual(connection.rollbacks, 1)
        self.assertTrue(connection.closed)

    def test_transport_failure_aborts_and_closes_the_reference(self):
        connection = Connection((12541, 'ORA-12541'))
        with self.assertRaises(DriverError):
            contract.observe(lambda: connection, contract='national')
        self.assertEqual(connection.rollbacks, 1)
        self.assertTrue(connection.closed)

    def test_error_classification_keeps_auth_cancellation_and_driver_failures(self):
        for code in [1424, 1425, 12704, 20000]:
            self.assertTrue(contract.semantic_error(code, f'ORA-{code:05d}'))
        for code in [28, 1013, 1017, 1033, 1034, 1089, 1090, 2396, 3113, 3114, 3135, 12154, 12541]:
            self.assertFalse(contract.semantic_error(code, f'ORA-{code:05d}'))
        self.assertFalse(contract.semantic_error(None, 'DPY-6005'))
        self.assertFalse(contract.semantic_error(1425, 'DPY-1425'))
        self.assertFalse(contract.semantic_error(0, 'ORA-00000'))

    def test_unknown_contract_does_not_open_a_connection(self):
        def refuse():
            self.fail('unknown contract must not connect')
        with self.assertRaises(ValueError):
            contract.observe(refuse, contract='unknown')


if __name__ == '__main__':
    unittest.main()
