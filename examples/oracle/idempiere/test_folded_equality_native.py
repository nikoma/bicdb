"""Exact Oracle text predicates can use case-converted index prefixes safely."""
import os
import unittest

import test_foreign_keys as fixtures


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER')
class FoldedEqualityNativeTests(unittest.TestCase):
    def test_oracle_exact_varchar_predicate_retains_residual_and_pending_rows(self):
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = 'oracle-preview-v1'
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'index fixture cleanup failed'))
        f.setUp()
        with f.conn.cursor() as c:
            c.execute('CREATE TABLE folded_labels(id NUMBER PRIMARY KEY, tenant NUMBER, label VARCHAR2(40 CHAR))')
            c.execute("INSERT INTO folded_labels VALUES (1,7,'Straße'),(2,7,'STRASSE'),(3,8,'Straße'),(4,7,NULL),(5,7,'Mixed'),(6,7,'mixed')")
            query = "SELECT id FROM folded_labels n WHERE n.label=%s AND tenant=7 ORDER BY id"
            for function in ('UPPER', 'LOWER'):
                c.execute('CREATE INDEX folded_lookup ON folded_labels('+function+'(label),tenant)')
                for label, expected in [('Straße', [(1,)]), ('STRASSE', [(2,)]),
                                        ('Mixed', [(5,)]), ('mixed', [(6,)]), ('absent', [])]:
                    with self.subTest(function=function, label=label):
                        c.execute(query, (label,), prepare=True)
                        self.assertEqual(c.fetchall(), expected)
                c.execute('BEGIN')
                c.execute("UPDATE folded_labels SET label='Straße' WHERE id=2")
                c.execute("INSERT INTO folded_labels VALUES (7,7,'Straße')")
                c.execute(query, ('Straße',), prepare=True)
                self.assertEqual(c.fetchall(), [(1,), (2,), (7,)])
                c.execute('ROLLBACK')
                c.execute(query, ('Straße',), prepare=True)
                self.assertEqual(c.fetchall(), [(1,)])
                c.execute('DROP INDEX folded_lookup')


if __name__ == '__main__':
    unittest.main()
