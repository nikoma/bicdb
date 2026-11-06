"""Explicit NULL escape is unknown; source empty-string rules stay independent."""
import os
import unittest

import test_foreign_keys as fixtures


class NullEscapeCases:
    def fixture(self):
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'null-escape fixture cleanup failed'))
        fixture.setUp()
        return fixture.conn

    def test_null_escape_has_three_valued_constant_and_row_predicates(self):
        connection = self.fixture()
        query = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE NULL THEN 1 WHEN 'abc' NOT LIKE 'abc' ESCAPE NULL THEN 0 ELSE 2 END"
        self.assertEqual(connection.execute(query).fetchall(), [(2,)])
        connection.execute('CREATE TABLE null_escape_rows(label VARCHAR(30))')
        connection.execute("INSERT INTO null_escape_rows VALUES('abc'),('other')")
        self.assertEqual(connection.execute('SELECT COUNT(*) FROM null_escape_rows WHERE label LIKE label ESCAPE NULL').fetchall(), [(0,)])
        self.assertEqual(connection.execute('SELECT COUNT(*) FROM null_escape_rows WHERE label NOT LIKE label ESCAPE NULL').fetchall(), [(0,)])

    def test_bound_null_and_literal_empty_escape_keep_profile_rules(self):
        connection = self.fixture()
        query = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE %s THEN 1 ELSE 2 END"
        self.assertEqual(connection.execute(query, (None,)).fetchall(), [(2,)])
        self.assertEqual(connection.execute(query, ('|',)).fetchall(), [(1,)])
        empty = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE '' THEN 1 ELSE 2 END"
        self.assertEqual(connection.execute(empty).fetchall(), [(1 if self.profile == 'postgres' else 2,)])


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresNullEscapeTests(NullEscapeCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleNullEscapeTests(NullEscapeCases, unittest.TestCase):
    profile = 'oracle-preview-v1'
