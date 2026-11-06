"""Independent PostgreSQL strictness and source-observed Oracle escape errors."""
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

    def assert_invalid_oracle_escape(self, connection, query, params=None):
        with self.assertRaises(Exception) as caught:
            connection.execute(query, params) if params is not None else connection.execute(query)
        self.assertEqual(caught.exception.sqlstate, '22025')
        self.assertIn('ORA-01425', str(caught.exception))

    def test_null_escape_has_profile_specific_constant_and_row_predicates(self):
        connection = self.fixture()
        query = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE NULL THEN 1 WHEN 'abc' NOT LIKE 'abc' ESCAPE NULL THEN 0 ELSE 2 END"
        if self.profile == 'postgres':
            self.assertEqual(connection.execute(query).fetchall(), [(2,)])
        else:
            self.assert_invalid_oracle_escape(connection, query)
        connection.execute('CREATE TABLE null_escape_rows(label VARCHAR(30))')
        connection.execute("INSERT INTO null_escape_rows VALUES('abc'),('other')")
        for operator in ['LIKE', 'NOT LIKE']:
            query = 'SELECT COUNT(*) FROM null_escape_rows WHERE label '+operator+' label ESCAPE NULL'
            if self.profile == 'postgres':
                self.assertEqual(connection.execute(query).fetchall(), [(0,)])
            else:
                self.assert_invalid_oracle_escape(connection, query)

    def test_bound_null_and_literal_empty_escape_keep_profile_rules(self):
        connection = self.fixture()
        query = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE %s THEN 1 ELSE 2 END"
        if self.profile == 'postgres':
            self.assertEqual(connection.execute(query, (None,)).fetchall(), [(2,)])
        else:
            self.assert_invalid_oracle_escape(connection, query, (None,))
            self.assert_invalid_oracle_escape(connection, query, ('',))
            self.assert_invalid_oracle_escape(connection, query, ('xx',))
        self.assertEqual(connection.execute(query, ('|',)).fetchall(), [(1,)])
        unicode_escape = "SELECT CASE WHEN 'x%tail' LIKE %s ESCAPE %s THEN 1 ELSE 0 END"
        if self.profile == 'postgres':
            self.assertEqual(connection.execute(unicode_escape, ('x§%%', '§')).fetchall(), [(1,)])
        else:
            self.assert_invalid_oracle_escape(connection, unicode_escape, ('x§%%', '§'))
        empty = "SELECT CASE WHEN 'abc' LIKE 'abc' ESCAPE '' THEN 1 ELSE 2 END"
        if self.profile == 'postgres':
            self.assertEqual(connection.execute(empty).fetchall(), [(1,)])
        else:
            self.assert_invalid_oracle_escape(connection, empty)
            for predicate in ["NULL LIKE 'abc' ESCAPE NULL", "NULL LIKE 'abc' ESCAPE 'xx'", "'abc' LIKE NULL ESCAPE 'xx'", "'abc' LIKE 'abc' ESCAPE 'xx'"]:
                self.assert_invalid_oracle_escape(connection, 'SELECT 1 WHERE '+predicate)
            connection.execute('CREATE TABLE empty_escape_input(label VARCHAR(30))')
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM empty_escape_input WHERE label LIKE 'abc' ESCAPE NULL").fetchall(), [(0,)])
            for pattern in ['abc|', 'abc|x']:
                with self.assertRaises(Exception) as caught:
                    connection.execute("SELECT 1 WHERE 'abc' LIKE %s ESCAPE '|'", (pattern,))
                self.assertEqual(caught.exception.sqlstate, '22025')
                self.assertIn('ORA-01424', str(caught.exception))
            for source, pattern, escape in [('a_b', 'a%_b', '%'), ('a%b', 'a_%b', '_')]:
                self.assertEqual(connection.execute("SELECT CASE WHEN %s LIKE %s ESCAPE %s THEN 1 ELSE 0 END", (source, pattern, escape)).fetchall(), [(1,)])


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresNullEscapeTests(NullEscapeCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleNullEscapeTests(NullEscapeCases, unittest.TestCase):
    profile = 'oracle-preview-v1'
