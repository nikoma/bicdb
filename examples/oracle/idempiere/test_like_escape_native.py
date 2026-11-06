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
        # Source 23.26.3 AL32UTF8/BINARY rejects the multibyte explicit
        # escape. PostgreSQL accepts one Unicode character independently.
        cases = [
            ("SELECT label FROM pattern_rows WHERE label LIKE 'I|_%' ESCAPE '|'", ('I_name',), None),
            ("SELECT label FROM pattern_rows WHERE label NOT LIKE 'I|_%' ESCAPE '|' AND label NOT LIKE 'x§%%' ESCAPE '§'", ('Text',), 1425),
            ("SELECT label FROM pattern_rows WHERE label LIKE escape ESCAPE '|'", ('I_name',), None),
            ("SELECT label FROM pattern_rows t WHERE t.label LIKE t.escape ESCAPE '|'", ('I_name',), None),
            ("SELECT label FROM pattern_rows WHERE label LIKE (SUBSTR('I',1,1)||'|_%') ESCAPE '|'", ('I_name',), None),
            ("SELECT escape FROM pattern_rows WHERE label='I_name'", ('I|_%',), None),
            ("SELECT label FROM pattern_rows WHERE label LIKE 'x§%%' ESCAPE '§'", ('x%tail',), 1425),
        ]
        for sql, expected, oracle_error in cases:
            with self.subTest(sql=sql):
                if self.profile == 'oracle-preview-v1' and oracle_error is not None:
                    import psycopg
                    with self.assertRaises(psycopg.errors.InvalidEscapeSequence) as caught:
                        connection.execute(sql)
                    self.assertIn('ORA-%05d' % oracle_error, str(caught.exception))
                else:
                    self.assertEqual(connection.execute(sql).fetchall(), [expected])
        self.assertEqual(connection.execute("SELECT label FROM pattern_rows WHERE label LIKE %s ESCAPE '|'", ('I|_%',)).fetchall(), [('I_name',)])

    def test_default_escape_keeps_postgres_and_oracle_rules_independent(self):
        connection = self.fixture()
        query = "SELECT CASE WHEN %s LIKE %s THEN 1 ELSE 0 END"
        self.assertEqual(connection.execute(query, ('a_b', r'a\_b')).fetchall(),
                         [(1 if self.profile == 'postgres' else 0,)])
        explicit = r"SELECT CASE WHEN 'a_b' LIKE 'a\_b' ESCAPE '\' THEN 1 ELSE 0 END"
        self.assertEqual(connection.execute(explicit).fetchall(), [(1,)])
        self.assertEqual(connection.execute("SELECT CASE WHEN %s NOT LIKE %s THEN 1 ELSE 0 END",
                                            ('a_b', r'a\_b')).fetchall(),
                         [(0 if self.profile == 'postgres' else 1,)])

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
