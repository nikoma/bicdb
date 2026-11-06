"""Projection checks distinguish label/type mismatches from execution evidence."""
import os
import unittest
from unittest.mock import Mock, patch

from verify_view_metadata import compare, describe, projection_plan, verify


def manifest():
    return dict(source_schema='S', routines=[], views=[dict(name='V', query='SELECT N FROM T', read_only='N')],
                columns=[dict(table='V', name='N', position=1, type='NUMBER')])


class ProjectionContracts(unittest.TestCase):
    def test_numeric_text_and_approximate_float_are_not_accepted_as_exact_number(self):
        expected = projection_plan(manifest())[0]['columns']
        for oid in [20, 21, 23, 1700]:
            self.assertTrue(compare(expected, [dict(name='N', oid=oid)])['matched'])
        for actual in [[dict(name='N', oid=25)], [dict(name='N', oid=701)],
                       [dict(name='WRONG', oid=1700)], []]:
            self.assertFalse(compare(expected, actual)['matched'])

    def test_unsupported_source_types_are_rejected_before_describe(self):
        source = manifest();source['columns'][0]['type'] = 'UNSUPPORTED'
        with patch('verify_view_metadata.describe') as mocked:
            with self.assertRaises(ValueError): verify(Mock(), source)
            mocked.assert_not_called()

    def test_parse_and_describe_use_bounded_unnamed_statement_without_execute(self):
        connection = Mock()
        connection.pgconn.prepare.return_value.status = 1
        result = connection.pgconn.describe_prepared.return_value
        result.status = 1;result.nfields = 1
        result.fname.return_value = b'N';result.ftype.return_value = 1700;result.fmod.return_value = -1
        self.assertEqual(describe(connection, 'SELECT 1/0'), [dict(name='N', oid=1700, typmod=-1)])
        connection.pgconn.prepare.assert_called_once_with(b'', b'SELECT 1/0')
        connection.pgconn.describe_prepared.assert_called_once_with(b'')
        connection.execute.assert_not_called()
        connection.pgconn.exec_prepared.assert_not_called()

    def test_description_errors_and_mismatches_do_not_claim_execution_or_value_compatibility(self):
        with patch('verify_view_metadata.describe', side_effect=ValueError('missing dependency')):
            report = verify(Mock(), manifest())
        self.assertFalse(report['success'])
        self.assertFalse(report['view_execution_qualified'])
        self.assertFalse(report['view_values_qualified'])
        self.assertFalse(report['precision_scale_width_nullability_qualified'])
        self.assertEqual(report['checks'][0]['error_type'], 'ValueError')


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native Parse/Describe checks')
class ProjectionNative(unittest.TestCase):
    def check_profile(self, profile):
        import psycopg
        import test_foreign_keys as fixtures
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'native metadata fixture cleanup failed'))
        fixture.setUp()
        fixture.conn.execute('CREATE SCHEMA "S"')
        fixture.conn.execute('CREATE TABLE "S"."T"("N" INTEGER)')
        fixture.conn.execute('INSERT INTO "S"."T" VALUES(1)')
        fixture.conn.execute('CREATE VIEW "S"."V"("N") AS SELECT 1/0 FROM "S"."T"')
        report = verify(fixture.conn, manifest())
        self.assertTrue(report['success'], report)
        with self.assertRaises(psycopg.Error) as error:
            fixture.conn.execute('SELECT * FROM "S"."V"').fetchall()
        self.assertEqual(error.exception.sqlstate, '22012')
        fixture.conn.execute('CREATE OR REPLACE VIEW "S"."V"("N") AS SELECT CAST(1 AS VARCHAR(10)) FROM "S"."T"')
        report = verify(fixture.conn, manifest())
        self.assertFalse(report['success'])
        self.assertTrue(report['checks'][0]['labels_matched'])
        self.assertFalse(report['checks'][0]['type_families_matched'])

    def test_default_postgres_metadata_without_row_execution(self): self.check_profile('postgres')

    def test_oracle_preview_metadata_without_row_execution(self): self.check_profile('oracle-preview-v1')


if __name__ == '__main__': unittest.main()
