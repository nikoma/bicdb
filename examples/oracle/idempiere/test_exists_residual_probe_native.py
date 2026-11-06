"""Correlated candidate filters retain safe residual SQL over the PG transport."""
import os
import re
import unittest
from unittest.mock import patch

import test_foreign_keys as fixtures


class ResidualProbeCases:
    def test_disjunctive_inner_filter_has_bounded_scans(self):
        case = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        case.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(case.doCleanups(), 'residual fixture cleanup failed'))
        with patch.dict(os.environ, {'BICDB_TRACE_PLAN': '1'}):
            case.setUp()
        connection = case.conn
        number = 'INTEGER' if self.profile == 'postgres' else 'NUMBER(10)'
        connection.execute(f'CREATE TABLE residual_requests(id {number} PRIMARY KEY,period {number})')
        connection.execute(f'CREATE TABLE residual_controls(id {number} PRIMARY KEY,period {number},enabled VARCHAR(1),note VARCHAR(8))')
        connection.execute('INSERT INTO residual_requests VALUES '+','.join(f'({i},{i})' for i in range(1,25))+',(25,NULL)')
        controls = []
        for i in range(1,49):
            period = (i+1)//2
            enabled = "'Y'" if period % 2 == 0 else "'N'"
            note = "'present'" if period % 5 == 0 else 'NULL'
            controls.append(f'({i},{period},{enabled},{note})')
        connection.execute('INSERT INTO residual_controls VALUES '+','.join(controls))
        query = "SELECT r.id FROM residual_requests r WHERE NOT EXISTS(SELECT * FROM residual_controls c WHERE c.period=r.period AND (c.enabled='Y' OR c.note IS NOT NULL)) AND r.id NOT IN(SELECT r.id FROM residual_requests r WHERE r.id<0) ORDER BY r.id"
        expected = [(i,) for i in range(1,25) if i % 2 != 0 and i % 5 != 0]+[(25,)]
        self.assertEqual(connection.execute(query).fetchall(), expected)
        traces = [line for line in (case.root/'server.log').read_text().splitlines()
                  if line.startswith('bicdb_trace stmt=SELECT ')]
        self.assertTrue(traces)
        metrics = dict(re.findall(r'(\w+)=([^ ]+)', traces[-1]))
        self.assertLessEqual(int(metrics['full_scans']), 3, f'{self.profile}: full_scans={metrics["full_scans"]}')


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresResidualProbeTests(ResidualProbeCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleResidualProbeTests(ResidualProbeCases, unittest.TestCase):
    profile = 'oracle-preview-v1'


if __name__ == '__main__':
    unittest.main()
