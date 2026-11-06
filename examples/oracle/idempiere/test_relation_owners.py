"""Explicit relation-owner migration and independent native profile controls."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import relation_owners as target
import test_foreign_keys as fixtures


class RelationOwnerPlanTests(unittest.TestCase):
    def test_quoted_identifiers_and_source_order_are_retained(self):
        schema, plan = target.declarations(dict(source_schema='Own"er',
            tables=[dict(name='T.Q')], views=[dict(name='V Q')]))
        self.assertEqual(schema, 'Own"er')
        self.assertEqual(plan, [dict(name='T.Q', kind='TABLE'), dict(name='V Q', kind='VIEW')])

    def test_duplicate_or_invalid_source_names_are_refused(self):
        for tables, views in [([dict(name='T')]*2, []),
                              ([dict(name='T')], [dict(name='T')]),
                              ([dict(name='')], []), ([dict(name='X\x00')], [])]:
            with self.subTest(tables=tables, views=views), self.assertRaises(ValueError):
                target.declarations(dict(source_schema='S', tables=tables, views=views))

    def test_catalog_requires_exact_kinds_inventory_and_allowed_owners(self):
        plan = [dict(name='T', kind='TABLE')]
        good = dict(schema_owner='migration', relations={'T': dict(kind='TABLE', owner='mapped')})
        target.validate_catalog(good, plan, {'migration', 'mapped'})
        for actual in [dict(schema_owner='other', relations=good['relations']),
                       dict(schema_owner='migration', relations={}),
                       dict(schema_owner='migration', relations={'T': dict(kind='VIEW', owner='mapped')}),
                       dict(schema_owner='migration', relations={'T': dict(kind='TABLE', owner='other')})]:
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                target.validate_catalog(actual, plan, {'migration', 'mapped'})

    def test_active_writer_refusal_precedes_manifest_and_target_access(self):
        with self.assertRaisesRegex(ValueError, 'writers-quiesced'):
            target.migrate(SimpleNamespace(writers_quiesced=False))


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native relation-owner migration')
class RelationOwnerNativeTests(unittest.TestCase):
    def fixture(self, profile='oracle-preview-v1'):
        f = fixtures.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'owner fixture cleanup failed'))
        f.setUp()
        schema = 'Own"er Q'
        q = target.quote
        with f.conn.cursor() as c:
            c.execute('CREATE ROLE app_owner LOGIN NOSUPERUSER')
            c.execute('CREATE ROLE other_owner LOGIN NOSUPERUSER')
            c.execute('CREATE SCHEMA ' + q(schema))
            c.execute('CREATE TABLE ' + q(schema) + '."T.Q" ("ID" INTEGER)')
            c.execute('INSERT INTO ' + q(schema) + '."T.Q" VALUES (1),(1),(NULL)')
            c.execute('CREATE VIEW ' + q(schema) + '."V Q" AS SELECT "ID" FROM ' + q(schema) + '."T.Q"')
            c.execute('CREATE TABLE unrelated (id INTEGER)')
            c.execute('INSERT INTO unrelated VALUES (99)')
            c.execute('CREATE VIEW "V Q" AS SELECT id FROM unrelated')
        (f.root/'manifest.json').write_text(json.dumps(dict(source_schema=schema,
            tables=[dict(name='T.Q')], views=[dict(name='V Q')])))
        args = SimpleNamespace(snapshot=f.root, report=f.root/'owners.json',
            pg_dsn_env='BICDB_OWNER_TEST_DSN', target_owner='app_owner',
            writers_quiesced=True, resume=False, check_only=False, profile=profile)
        return f, args, schema

    def migrate(self, f, args):
        with patch.dict(os.environ, BICDB_OWNER_TEST_DSN=f.dsn):
            return target.migrate(args)

    def check_profile(self, profile):
        import psycopg
        f, args, schema = self.fixture(profile)
        with f.conn.cursor() as c:
            before = target.catalog(c, schema)
        args.check_only = True
        report = self.migrate(f, args)
        self.assertTrue(report['success'])
        self.assertFalse(report['ownership_transaction_committed'])
        with f.conn.cursor() as c:
            self.assertEqual(target.catalog(c, schema), before)
        args.report = f.root/'applied-owners.json';args.check_only = False
        report = self.migrate(f, args)
        self.assertTrue(report['success'] and report['relation_owners_verified'])
        self.assertTrue(report['ownership_transaction_committed'])
        self.assertFalse(report['routine_ownership_qualified'])
        self.assertFalse(report['application_qualified'])
        f.restart_server()
        args.resume = True
        self.assertTrue(self.migrate(f, args)['relation_owners_verified'])
        with psycopg.connect(host='127.0.0.1', port=f.port, user='app_owner', dbname='bicdb',
                             autocommit=True, options='-c bicdb.sql_profile=' + profile) as app:
            with app.cursor() as c:
                for name in ['T.Q', 'V Q']:
                    c.execute('SELECT "ID" FROM ' + target.quote(schema) + '.' + target.quote(name))
                    self.assertCountEqual(c.fetchall(), [(1,), (1,), (None,)])
                c.execute('UPDATE ' + target.quote(schema) + '."T.Q" SET "ID"=2 WHERE "ID"=1')
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    c.execute('SELECT * FROM unrelated')
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    c.execute('ALTER VIEW ' + target.quote(schema) + '."V Q" OWNER TO other_owner')
        with f.conn.cursor() as c:
            c.execute('SELECT "viewowner" FROM "pg_catalog"."pg_views" '
                      'WHERE "schemaname"=%s AND "viewname"=%s', ('public', 'V Q'))
            self.assertEqual(c.fetchall(), [('bicdb',)])
            c.execute('CREATE TABLE word_columns (role INTEGER,owner INTEGER,login INTEGER)')
            c.execute('INSERT INTO word_columns VALUES (1,2,3)')
            c.execute('SELECT role,owner,login FROM word_columns')
            self.assertEqual(c.fetchall(), [(1,2,3)])

    def test_default_postgres_mapping_survives_restart_and_preserves_isolation(self):
        self.check_profile('postgres')

    def test_oracle_mapping_survives_restart_and_preserves_isolation(self):
        self.check_profile('oracle-preview-v1')

    def test_missing_or_superuser_target_is_refused_before_any_ownership_change(self):
        f, args, schema = self.fixture()
        with f.conn.cursor() as c:
            before = target.catalog(c, schema)
        for owner in ['missing_owner', 'bicdb']:
            args.target_owner = owner
            with self.subTest(owner=owner), self.assertRaisesRegex(ValueError, 'non-superuser'):
                self.migrate(f, args)
            self.assertFalse(args.report.exists())
            with f.conn.cursor() as c:
                self.assertEqual(target.catalog(c, schema), before)

    def test_other_owner_or_unexported_relation_is_refused_before_any_change(self):
        f, args, schema = self.fixture()
        with f.conn.cursor() as c:
            c.execute('ALTER TABLE ' + target.quote(schema) + '."T.Q" OWNER TO other_owner')
            before = target.catalog(c, schema)
        with self.assertRaisesRegex(ValueError, 'owner outside'):
            self.migrate(f, args)
        self.assertFalse(args.report.exists())
        with f.conn.cursor() as c:
            self.assertEqual(target.catalog(c, schema), before)
            c.execute('ALTER TABLE ' + target.quote(schema) + '."T.Q" OWNER TO bicdb')
            c.execute('CREATE TABLE ' + target.quote(schema) + '.extra (id INTEGER)')
            before = target.catalog(c, schema)
        with self.assertRaisesRegex(ValueError, 'relation inventory'):
            self.migrate(f, args)
        with f.conn.cursor() as c:
            self.assertEqual(target.catalog(c, schema), before)

    def test_resume_identity_refusal_preserves_existing_report(self):
        f, args, schema = self.fixture()
        self.migrate(f, args)
        saved = args.report.read_bytes()
        args.resume = True;args.target_owner = 'other_owner'
        with self.assertRaisesRegex(ValueError, 'journal differs'):
            self.migrate(f, args)
        self.assertEqual(args.report.read_bytes(), saved)
        args.target_owner = 'app_owner';args.check_only = True
        with self.assertRaisesRegex(ValueError, 'journal differs'):
            self.migrate(f, args)
        self.assertEqual(args.report.read_bytes(), saved)

    def test_later_ddl_failure_rolls_back_schema_and_earlier_table_owner_changes(self):
        import psycopg
        f, args, schema = self.fixture()
        with f.conn.cursor() as c:
            before = target.catalog(c, schema)
        original = psycopg.Cursor.execute
        def execute(cursor, query, *a, **kw):
            if isinstance(query, str) and query.startswith('ALTER VIEW '):
                raise RuntimeError('injected later ownership DDL failure')
            return original(cursor, query, *a, **kw)
        with patch.object(psycopg.Cursor, 'execute', execute):
            with self.assertRaisesRegex(RuntimeError, 'later ownership DDL failure'):
                self.migrate(f, args)
        with f.conn.cursor() as c:
            self.assertEqual(target.catalog(c, schema), before)
        report = json.loads(args.report.read_text())
        self.assertFalse(report['success'])
        self.assertFalse(report['ownership_transaction_committed'])
        self.assertFalse(report['relation_owners_verified'])


if __name__ == '__main__':
    unittest.main()
