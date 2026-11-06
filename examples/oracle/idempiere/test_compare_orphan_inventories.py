"""Report comparison must retain full relationship coverage and fail closed."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import compare_orphan_inventories as target
from test_foreign_keys import manifest


class OrphanComparisonTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.manifest = manifest()
        raw = json.dumps(self.manifest).encode()
        (self.root/'manifest.json').write_bytes(raw)
        self.plan = target.declarations(self.manifest)
        common = dict(format=1, manifest_sha256=hashlib.sha256(raw).hexdigest(),
                      target_sha256='a'*64,
                      source_schema=self.manifest['source_schema'], success=True,
                      inventory_completed=True, validated_relationships_clean=True,
                      source_foreign_keys=len(self.plan),
                      completed=[dict(declaration=item, orphan_rows=0) for item in self.plan])
        self.reference = copy.deepcopy(dict(common, phase='oracle-export-reference',
                                           reference_rows_equal_export=True, cleanup_completed=True,
                                           data_replay_only=True, cleanup_errors=[], verified_tables=[
                                               dict(table=t['name'], rows=t['rows'], sha256='b'*64)
                                               for t in self.manifest['tables']]))
        fingerprint = dict(rows=1, multiset_sha256='0'*64)
        self.inventory = copy.deepcopy(dict(common, phase='foreign-key-orphans', profile='oracle-preview-v1',
                                           migration_report_sha256='c'*64,
                                           writers_quiesced_asserted=True, table_fingerprints={
                                               name: dict(before=fingerprint, after=fingerprint)
                                               for name in ['P', 'C', 'Q', 'U']}))
        self.args = SimpleNamespace(snapshot=self.root, reference_report=self.root/'reference.json',
                                    target_report=self.root/'target.json', report=self.root/'comparison.json')

    def compare(self):
        self.args.reference_report.write_text(json.dumps(self.reference))
        self.args.target_report.write_text(json.dumps(self.inventory))
        return target.compare(self.args)

    def test_equal_counts_include_legacy_orphans_without_claiming_data_equality(self):
        self.reference['completed'][0]['orphan_rows'] = 2
        self.inventory['completed'][0]['orphan_rows'] = 2
        self.inventory['completed'].reverse()
        report = self.compare()
        self.assertTrue(report['success'])
        self.assertEqual(report['compared_relationships'], 3)
        for key in ['data_compared_to_source', 'live_source_compared', 'application_qualified']:
            self.assertFalse(report[key])

    def test_count_difference_retains_completed_failed_comparison(self):
        self.inventory['completed'][0]['orphan_rows'] = 1
        with self.assertRaisesRegex(ValueError, 'counts differ'):
            self.compare()
        report = json.loads(self.args.report.read_text())
        self.assertTrue(report['comparison_completed'])
        self.assertFalse(report['success'])
        self.assertEqual(len(report['mismatches']), 1)

    def test_changed_missing_duplicate_or_invalid_entries_are_refused(self):
        original = copy.deepcopy(self.inventory)
        for change in ['missing', 'duplicate', 'declaration', 'boolean', 'negative', 'float', 'validated']:
            self.inventory = copy.deepcopy(original)
            entries = self.inventory['completed']
            if change == 'missing': entries.pop()
            elif change == 'duplicate': entries[1] = copy.deepcopy(entries[0])
            elif change == 'declaration': entries[0]['declaration']['deferrable'] = 1
            elif change == 'validated': entries[1]['orphan_rows'] = 1
            else: entries[0]['orphan_rows'] = {'boolean': True, 'negative': -1, 'float': 0.0}[change]
            with self.subTest(change=change), self.assertRaises(ValueError): self.compare()
            self.assertFalse(json.loads(self.args.report.read_text())['success'])

    def test_partial_stale_or_changed_identity_reports_are_refused(self):
        for report in [self.reference, self.inventory]:
            for key, value in [('success', False), ('inventory_completed', False),
                               ('manifest_sha256', 'other'), ('source_foreign_keys', 2),
                               ('source_schema', 'OTHER')]:
                old = report[key];report[key] = value
                with self.subTest(key=key), self.assertRaises(ValueError): self.compare()
                report[key] = old
        self.reference['cleanup_completed'] = False
        with self.assertRaises(ValueError): self.compare()

    def test_changed_fingerprints_and_input_overwrite_are_refused(self):
        self.inventory['table_fingerprints']['C']['after'] = dict(rows=2)
        with self.assertRaisesRegex(ValueError, 'fingerprints'): self.compare()
        for path in [self.root/'manifest.json', self.args.reference_report, self.args.target_report]:
            self.args.report = path
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'overwrite'): self.compare()

    def test_input_failure_invalidates_old_success(self):
        self.compare()
        self.args.reference_report.unlink()
        with self.assertRaises(FileNotFoundError): target.compare(self.args)
        result = json.loads(self.args.report.read_text())
        self.assertFalse(result['success'])
        self.assertFalse(result['comparison_completed'])

    def test_reference_requires_complete_value_verification_and_cleanup(self):
        original = copy.deepcopy(self.reference)
        for change in ['missing_table', 'duplicate_table', 'wrong_rows', 'missing_digest', 'cleanup_error']:
            self.reference = copy.deepcopy(original)
            entries = self.reference['verified_tables']
            if change == 'missing_table': entries.pop()
            elif change == 'duplicate_table': entries[1] = copy.deepcopy(entries[0])
            elif change == 'wrong_rows': entries[0]['rows'] += 1
            elif change == 'missing_digest': del entries[0]['sha256']
            else: self.reference['cleanup_errors'] = ['failed']
            with self.subTest(change=change), self.assertRaises(ValueError): self.compare()

    def test_empty_or_invalid_target_fingerprints_are_refused(self):
        for value in [{}, dict(rows=True, multiset_sha256='0'*64), dict(rows=1, multiset_sha256='invalid')]:
            self.inventory['table_fingerprints']['C'] = dict(before=value, after=value)
            with self.subTest(value=value), self.assertRaises(ValueError): self.compare()


if __name__ == '__main__':
    unittest.main()
