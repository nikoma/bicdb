"""Explicit LIKE ESCAPE grammar and identifier folding in independent profiles."""
import os
import unittest

import test_foreign_keys as fixtures


class LikeEscapeCases:
    def fixture(self):
        fixture = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = self.profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'pattern fixture cleanup failed'))
        fixture.setUp()
        fixture.conn.execute('CREATE TABLE pattern_rows(escape VARCHAR(30),label VARCHAR(30))')
        fixture.conn.execute("INSERT INTO pattern_rows VALUES('I|_%','I_name'),('T|_%','Text'),('x§%%','x%tail')")
        return fixture.conn

    def test_explicit_escapes_bind_parameters_and_nonreserved_identifiers(self):
        connection = self.fixture()
        cases = [
            ("SELECT label FROM pattern_rows WHERE label LIKE 'I|_%' ESCAPE '|'", ('I_name',)),
            ("SELECT label FROM pattern_rows WHERE label NOT LIKE 'I|_%' ESCAPE '|' AND label NOT LIKE 'x§%%' ESCAPE '§'", ('Text',)),
            ("SELECT label FROM pattern_rows WHERE label LIKE escape ESCAPE '|'", ('I_name',)),
            ("SELECT label FROM pattern_rows t WHERE t.label LIKE t.escape ESCAPE '|'", ('I_name',)),
            ("SELECT label FROM pattern_rows WHERE label LIKE (SUBSTR('I',1,1)||'|_%') ESCAPE '|'", ('I_name',)),
            ("SELECT escape FROM pattern_rows WHERE label='I_name'", ('I|_%',)),
            ("SELECT label FROM pattern_rows WHERE label LIKE 'x§%%' ESCAPE '§'", ('x%tail',)),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql):
                self.assertEqual(connection.execute(sql).fetchall(), [expected])
        self.assertEqual(connection.execute("SELECT label FROM pattern_rows WHERE label LIKE %s ESCAPE '|'", ('I|_%',)).fetchall(), [('I_name',)])

    def test_nested_patterns_comments_and_literals_preserve_grammar(self):
        connection = self.fixture()
        self.assertEqual(connection.execute("SELECT label FROM pattern_rows WHERE label LIKE (CASE WHEN 'a_b' LIKE 'a|_b' ESCAPE '|' THEN 'I|_%' ELSE 'none' END) /* ESCAPE is syntax here */ ESCAPE '|' AND 'ESCAPE'='ESCAPE'").fetchall(), [('I_name',)])
        self.assertEqual(connection.execute("SELECT label FROM pattern_rows WHERE label LIKE NULL ESCAPE '|'").fetchall(), [])
        self.assertEqual(connection.execute("SELECT label FROM pattern_rows WHERE label NOT LIKE NULL ESCAPE '|'").fetchall(), [])


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class PostgresLikeEscapeTests(LikeEscapeCases, unittest.TestCase):
    profile = 'postgres'


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class OracleLikeEscapeTests(LikeEscapeCases, unittest.TestCase):
    profile = 'oracle-preview-v1'


if __name__ == '__main__':
    unittest.main()
