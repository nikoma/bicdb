"""Planning safety and real-server acceptance for read-only orphan inventory."""
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import foreign_key_orphans as target
import test_foreign_keys as migration_fixture


class OrphanInventoryPlanTests(unittest.TestCase):
    def test_count_query_quotes_identifiers_and_excludes_every_null_component(self):
        item = dict(table='C"T', parent_table='P.T', columns=['X"Y', 'Z'], parent_columns=['A', 'B'])
        sql = target.orphan_count_sql('S"Q', item)
        self.assertEqual(sql, 'SELECT COUNT(*) FROM "S""Q"."C""T" c WHERE c."X""Y" IS NOT NULL AND c."Z" IS NOT NULL AND NOT EXISTS(SELECT 1 FROM "S""Q"."P.T" p WHERE p."A"=c."X""Y" AND p."B"=c."Z")')
        for columns, parent in [([], []), (['A'], []), (['A'], ['B', 'C'])]:
            with self.subTest(columns=columns, parent=parent), self.assertRaises(ValueError):
                target.orphan_count_sql('S', dict(item, columns=columns, parent_columns=parent))

    def test_incomplete_changed_or_unverified_migration_is_refused(self):
        plan = target.declarations(migration_fixture.manifest())
        identity = dict(format=1, phase='foreign-keys', profile='oracle-preview-v1', source_schema='S', manifest_sha256='manifest', target_sha256='endpoint')
        fingerprint = dict(rows=1, multiset_sha256='digest')
        previous = dict(identity, completed=plan, source_foreign_keys=len(plan), success=True, migration_completed=True, check_only=False, capability_verified=True,
                        table_fingerprints={name: dict(before=fingerprint, after=fingerprint) for name in ['C', 'P', 'Q', 'U']})
        self.assertEqual(target.require_completed_migration(previous, identity, plan, ['C', 'P', 'Q', 'U']), previous['table_fingerprints'])
        for field, value in [('completed', plan[:1]), ('target_sha256', 'other'), ('manifest_sha256', 'other'),
                             ('success', False), ('migration_completed', False), ('check_only', True),
                             ('capability_verified', False), ('source_foreign_keys', 0), ('table_fingerprints', {})]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                target.require_completed_migration(dict(previous, **{field: value}), identity, plan, ['C', 'P', 'Q', 'U'])
        changed = copy.deepcopy(previous);changed['table_fingerprints']['C']['after'] = dict(rows=2)
        with self.assertRaisesRegex(ValueError, 'unchanged target rows'):
            target.require_completed_migration(changed, identity, plan, ['C', 'P', 'Q', 'U'])

    def test_quiescence_and_separate_output_required_before_source_access(self):
        with self.assertRaisesRegex(ValueError, 'writers-quiesced'):
            target.inventory(SimpleNamespace(writers_quiesced=False))
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            for output in [root/'manifest.json', root/'migration.json']:
                with self.subTest(output=output), self.assertRaisesRegex(ValueError, 'overwrite'):
                    target.inventory(SimpleNamespace(writers_quiesced=True, snapshot=root, migration_report=root/'migration.json', report=output))

    def test_invalid_count_results_are_never_treated_as_zero(self):
        class Cursor:
            def execute(self, sql):self.sql = sql
            def fetchall(self):return self.rows
        cursor = Cursor();item = dict(table='C', parent_table='P', columns=['X'], parent_columns=['Y'])
        for rows in [[], [(None,)], [(True,)], [(-1,)], [(1.5,)], [(0,), (0,)], [(1, 2)]]:
            cursor.rows = rows
            with self.subTest(rows=rows), self.assertRaises(ValueError):target.count_orphans(cursor, 'S', item)
        cursor.rows = [(2,)];self.assertEqual(target.count_orphans(cursor, 'S', item), 2)
        self.assertTrue(cursor.sql.startswith('SELECT COUNT(*)'))

    def test_new_attempt_replaces_stale_success_before_connection_access(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name);source = root/'manifest.json';journal = root/'migration.json';output = root/'report.json'
            source.write_text(json.dumps(migration_fixture.manifest()))
            journal.write_text(json.dumps(dict(success=True)))
            output.write_text(json.dumps(dict(success=True, inventory_completed=True)))
            args = SimpleNamespace(writers_quiesced=True, snapshot=root, migration_report=journal,
                                   report=output, pg_dsn_env='BICDB_FK_TEST_DSN')
            driver = SimpleNamespace(connect=Mock(side_effect=RuntimeError('injected connection failure')))
            with patch.dict(sys.modules, psycopg=driver), patch.dict(os.environ, BICDB_FK_TEST_DSN='fixture'):
                with self.assertRaisesRegex(RuntimeError, 'injected connection failure'):target.inventory(args)
            report = json.loads(output.read_text())
            self.assertFalse(report['success']);self.assertFalse(report['inventory_completed'])
            self.assertEqual(report['completed'], [])


    def resume_fixture(self):
        plan=target.declarations(migration_fixture.manifest('S'))
        identity=dict(format=1,phase='foreign-key-orphans',profile='oracle-preview-v1',
                      source_schema='S',manifest_sha256='source',migration_report_sha256='migration',target_sha256='endpoint')
        value=dict(rows=1,multiset_sha256='0'*64)
        baseline={name:dict(before=value,after=value) for name in ['C','P','Q','U']}
        prior=dict(identity,writers_quiesced_asserted=True,source_foreign_keys=len(plan),
                   completed=[dict(declaration=item,orphan_rows=1 if i==0 else 0) for i,item in enumerate(plan)],
                   table_fingerprints=copy.deepcopy(baseline))
        return prior,identity,plan,baseline

    def test_resume_accepts_exact_full_partial_and_pre_count_journals(self):
        prior,identity,plan,baseline=self.resume_fixture()
        self.assertEqual(target.validate_inventory_journal(prior,identity,plan,baseline),prior['completed'])
        prior['completed']=prior['completed'][:1]
        for value in prior['table_fingerprints'].values():value.pop('after')
        self.assertEqual(len(target.validate_inventory_journal(prior,identity,plan,baseline)),1)
        prior['completed']=[];prior['table_fingerprints']={}
        self.assertEqual(target.validate_inventory_journal(prior,identity,plan,baseline),[])

    def test_resume_refuses_changed_identity_counts_and_order(self):
        original,identity,plan,baseline=self.resume_fixture()
        for field in identity:
            prior=copy.deepcopy(original);prior[field]='changed'
            with self.subTest(field=field),self.assertRaises(ValueError):target.validate_inventory_journal(prior,identity,plan,baseline)
        for change in ['boolean_format','missing_count','boolean_count','float_count','negative_count','validated_count',
                       'reordered','duplicate','extra','changed_declaration','boolean_declaration','missing_quiescence','wrong_total']:
            prior=copy.deepcopy(original);entries=prior['completed']
            if change=='boolean_format':prior['format']=True
            elif change=='missing_count':del entries[0]['orphan_rows']
            elif change=='boolean_count':entries[0]['orphan_rows']=True
            elif change=='float_count':entries[0]['orphan_rows']=1.0
            elif change=='negative_count':entries[0]['orphan_rows']=-1
            elif change=='validated_count':entries[1]['orphan_rows']=1
            elif change=='reordered':entries.reverse()
            elif change=='duplicate':entries[1]=copy.deepcopy(entries[0])
            elif change=='extra':entries.append(copy.deepcopy(entries[-1]))
            elif change=='changed_declaration':entries[0]['declaration']['name']='OTHER'
            elif change=='boolean_declaration':entries[0]['declaration']['deferrable']=1
            elif change=='missing_quiescence':prior['writers_quiesced_asserted']=False
            else:prior['source_foreign_keys']=len(plan)-1
            with self.subTest(change=change),self.assertRaises(ValueError):target.validate_inventory_journal(prior,identity,plan,baseline)

    def test_resume_requires_the_original_complete_row_baseline(self):
        original,identity,plan,baseline=self.resume_fixture()
        for change in ['missing','extra','before','after','boolean_rows']:
            prior=copy.deepcopy(original);fingerprints=prior['table_fingerprints']
            if change=='missing':del fingerprints['C']
            elif change=='extra':fingerprints['OTHER']=copy.deepcopy(fingerprints['C'])
            elif change=='before':fingerprints['C']['before']['rows']=2
            elif change=='after':fingerprints['C']['after']['rows']=2
            else:fingerprints['C']['before']['rows']=True
            with self.subTest(change=change),self.assertRaises(ValueError):target.validate_inventory_journal(prior,identity,plan,baseline)


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native inventory acceptance')
class OrphanInventoryIntegrationTests(unittest.TestCase):
    def setUp(self):
        # Reuse the migration fixture's native process lifecycle and schema setup.
        # Its test methods are not invoked by this composition.
        self.fixture = migration_fixture.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        self.addCleanup(self.cleanup_fixture)
        self.fixture.setUp()
        self.manifest, self.migration_args = self.fixture.setup_schema()
        self.migration_args.check_only = False;self.migration_args.writers_quiesced = True
        self.fixture.migrate(self.migration_args)
        self.args = SimpleNamespace(snapshot=self.migration_args.snapshot, migration_report=self.migration_args.report,
                                    report=self.fixture.root/'orphans.json', pg_dsn_env='BICDB_FK_TEST_DSN', writers_quiesced=True)

    def cleanup_fixture(self):
        self.assertTrue(self.fixture.doCleanups(), 'native fixture cleanup failed')

    def inventory(self):
        with patch.dict(os.environ, BICDB_FK_TEST_DSN=self.fixture.dsn):return target.inventory(self.args)

    def test_legacy_orphans_are_reported_without_repair_or_source_qualification(self):
        journal_before = self.args.migration_report.read_bytes()
        report = self.inventory()
        self.assertEqual([item['orphan_rows'] for item in report['completed']], [1, 0, 0])
        self.assertTrue(report['success']);self.assertTrue(report['inventory_completed'])
        self.assertTrue(report['validated_relationships_clean']);self.assertFalse(report['all_relationships_clean'])
        for key in ['writer_lease_acquired', 'data_compared_to_source', 'orphan_inventory_qualified', 'application_qualified']:
            self.assertFalse(report[key])
        self.assertTrue(all(item['before'] == item['after'] for item in report['table_fingerprints'].values()))
        self.assertEqual(self.args.migration_report.read_bytes(), journal_before)
        self.assertEqual(report['migration_report_sha256'], hashlib.sha256(journal_before).hexdigest())
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('SELECT "PID" FROM ' + target.quote(self.manifest['source_schema']) + '."C"')
            self.assertEqual(cursor.fetchall(), [(999,)])

    def test_changed_rows_are_refused_before_orphan_counts(self):
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('UPDATE ' + target.quote(self.manifest['source_schema']) + '."C" SET "NOTE"=7')
        with self.assertRaisesRegex(ValueError, 'rows changed since'):self.inventory()
        report = json.loads(self.args.report.read_text())
        self.assertFalse(report['inventory_completed']);self.assertEqual(report['completed'], [])

    def test_removed_relationship_is_refused(self):
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('ALTER TABLE ' + target.quote(self.manifest['source_schema']) + '."C" DROP CONSTRAINT "C""FK"')
        with self.assertRaisesRegex(ValueError, 'FK catalog differs'):self.inventory()

    def test_disabled_enforcement_is_refused(self):
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('ALTER TABLE ' + target.quote(self.manifest['source_schema']) + '."C" DISABLE TRIGGER ALL')
        with self.assertRaisesRegex(ValueError, 'disabled, missing or unverifiable'):self.inventory()

    def test_changed_projection_is_refused(self):
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('ALTER TABLE ' + target.quote(self.manifest['source_schema']) + '."C" ADD COLUMN "EXTRA" INT')
        with self.assertRaisesRegex(ValueError, 'projection differs'):self.inventory()

    def test_partial_failure_retains_incomplete_report(self):
        with patch.object(target, 'count_orphans', side_effect=[1, RuntimeError('injected count failure')]):
            with self.assertRaisesRegex(RuntimeError, 'injected count failure'):self.inventory()
        report = json.loads(self.args.report.read_text())
        self.assertEqual(len(report['completed']), 1)
        self.assertFalse(report['inventory_completed']);self.assertFalse(report['success'])

    def test_restart_resume_recounts_only_the_pending_relationships(self):
        with patch.object(target, 'count_orphans', side_effect=[1, RuntimeError('interrupted inventory')]):
            with self.assertRaisesRegex(RuntimeError, 'interrupted inventory'):self.inventory()
        partial=json.loads(self.args.report.read_text())
        self.assertEqual(len(partial['completed']),1)
        migration_before=self.args.migration_report.read_bytes()
        self.fixture.restart_server();self.args.resume=True
        with patch.object(target,'count_orphans',wraps=target.count_orphans) as counts:
            result=self.inventory()
        self.assertEqual(counts.call_count,2,'resume must preserve the verified count prefix')
        self.assertEqual(result['resumed_from_relationships'],1)
        self.assertEqual([entry['orphan_rows'] for entry in result['completed']],[1,0,0])
        self.assertTrue(result['success']);self.assertTrue(result['inventory_completed'])
        from compare_orphan_inventories import inventory_counts
        digest=hashlib.sha256((self.args.snapshot/'manifest.json').read_bytes()).hexdigest()
        counts=inventory_counts(result,self.manifest,digest,target.declarations(self.manifest))
        self.assertEqual(sorted(counts.values()),[0,0,1])
        self.assertEqual(self.args.migration_report.read_bytes(),migration_before)
        self.assertTrue(all(value['before']==value['after'] for value in result['table_fingerprints'].values()))

    def test_resume_identity_mismatch_preserves_the_supplied_journal(self):
        report=self.inventory();report['target_sha256']='0'*64
        self.args.report.write_text(json.dumps(report));before=self.args.report.read_bytes();self.args.resume=True
        with patch.object(target,'count_orphans') as counts:
            with self.assertRaisesRegex(ValueError,'journal does not match'):self.inventory()
        counts.assert_not_called();self.assertEqual(self.args.report.read_bytes(),before)

    def test_complete_resume_rechecks_rows_before_reusing_counts(self):
        self.inventory();self.args.resume=True
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('UPDATE '+target.quote(self.manifest['source_schema'])+'."C" SET "NOTE"=7')
        with patch.object(target,'count_orphans') as counts:
            with self.assertRaisesRegex(ValueError,'rows changed since'):self.inventory()
        counts.assert_not_called();report=json.loads(self.args.report.read_text())
        self.assertFalse(report['success']);self.assertFalse(report['inventory_completed'])

    def test_complete_resume_rechecks_relationships_before_reusing_counts(self):
        self.inventory();self.args.resume=True
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('ALTER TABLE '+target.quote(self.manifest['source_schema'])+'."C" DROP CONSTRAINT "C""FK"')
        with patch.object(target,'count_orphans') as counts:
            with self.assertRaisesRegex(ValueError,'FK catalog differs'):self.inventory()
        counts.assert_not_called();self.assertFalse(json.loads(self.args.report.read_text())['success'])

    def test_cli_resumes_an_interrupted_inventory(self):
        import subprocess
        with patch.object(target,'count_orphans',side_effect=[1,RuntimeError('interrupted CLI input')]):
            with self.assertRaisesRegex(RuntimeError,'interrupted CLI input'):self.inventory()
        command=[sys.executable,str(Path(target.__file__)),
                 '--snapshot',str(self.args.snapshot),'--migration-report',str(self.args.migration_report),
                 '--report',str(self.args.report),'--pg-dsn-env',self.args.pg_dsn_env,'--writers-quiesced','--resume']
        result=subprocess.run(command,env=dict(os.environ,BICDB_FK_TEST_DSN=self.fixture.dsn),
                              text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=30)
        self.assertEqual(result.returncode,0,result.stdout)
        report=json.loads(self.args.report.read_text());self.assertTrue(report['success'])
        self.assertEqual(report['resumed_from_relationships'],1)
        self.assertEqual([entry['orphan_rows'] for entry in report['completed']],[1,0,0])

    def test_validated_orphan_anomaly_prevents_success(self):
        with patch.object(target, 'count_orphans', side_effect=[1, 1, 0]):
            with self.assertRaisesRegex(ValueError, 'validated source relationship'):self.inventory()
        report = json.loads(self.args.report.read_text())
        self.assertTrue(report['inventory_completed'])
        self.assertFalse(report['validated_relationships_clean']);self.assertFalse(report['success'])

    def test_clean_inventory_after_separate_explicit_repair_and_migration(self):
        with self.fixture.conn.cursor() as cursor:
            cursor.execute('UPDATE ' + target.quote(self.manifest['source_schema']) + '."C" SET "PID"=1')
        # Explicitly rerun migration after the separate data change, rather than
        # letting the inventory reset a baseline or perform its own repair.
        self.fixture.migrate(self.migration_args)
        report = self.inventory()
        self.assertTrue(report['all_relationships_clean']);self.assertTrue(report['success'])
        self.assertEqual([item['orphan_rows'] for item in report['completed']], [0, 0, 0])


if __name__ == '__main__':
    unittest.main()
