"""Evidence-validator regressions, not database or Azure qualification tests."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('verify-azure-long-growth-failure-evidence.py')
SPEC = importlib.util.spec_from_file_location('evidence_verifier', SCRIPT)
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)
EVIDENCE = SCRIPT.parents[1] / 'docs/evidence/azure-long-growth-failure-2026-11-11'


class EvidenceVerifierTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / 'evidence'
        shutil.copytree(EVIDENCE, self.directory)

    def rewrite(self, name, change):
        path = self.directory / name
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value) + '\n')
        manifest_path = self.directory / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['files'][name] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest) + '\n')

    def test_retained_failed_trial_is_valid_evidence(self):
        result = VERIFIER.verify(self.directory)
        self.assertFalse(result['native_trial_passed'])
        self.assertFalse(result['production_qualified'])
        self.assertEqual(result['known_acknowledgments'], 5027)

    def test_corrupted_compressed_observations_are_rejected(self):
        with (self.directory / 'growth-4-to-5-resources.json.gz').open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
            VERIFIER.verify(self.directory)

    def test_rehashed_completion_claim_is_rejected(self):
        self.rewrite('native-report.json', lambda value: value.update(complete=True))
        with self.assertRaisesRegex(ValueError, 'promoted'):
            VERIFIER.verify(self.directory)

    def test_rehashed_ack_omission_is_rejected(self):
        self.rewrite('growth-4-to-5-ledger.json', lambda value: value['keys'].pop())
        with self.assertRaisesRegex(ValueError, 'ACK ledger count'):
            VERIFIER.verify(self.directory)

    def test_rehashed_production_qualification_is_rejected(self):
        self.rewrite('claims.json', lambda value: value.update(production_qualified=True))
        with self.assertRaisesRegex(ValueError, 'unsupported qualification'):
            VERIFIER.verify(self.directory)

    def test_omitted_manifest_entry_cannot_hide_changed_claims(self):
        manifest_path = self.directory / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        del manifest['files']['claims.json']
        manifest_path.write_text(json.dumps(manifest) + '\n')
        (self.directory / 'claims.json').write_text('{}\n')
        with self.assertRaisesRegex(ValueError, 'cover every retained artifact'):
            VERIFIER.verify(self.directory)

    def test_rehashed_diagnostic_failure_cannot_be_promoted(self):
        def promote(value):
            response = value[0]['response']
            result = json.loads(response['stdout'])
            result.update(unverified=0, unverified_key_samples=[], verification_passed=True)
            response.update(code=0, stdout=json.dumps(result))
        self.rewrite('diagnosis-ledger-checks.json', promote)
        with self.assertRaisesRegex(ValueError, 'baseline diagnostic failure missing'):
            VERIFIER.verify(self.directory)

    def test_cleanup_observations_cannot_be_promoted_to_availability_evidence(self):
        self.rewrite('raw-classification-validity.json', lambda value: value.update(scope_valid_for_engine_availability=True))
        with self.assertRaisesRegex(ValueError, 'cleanup-confounded classification promoted'):
            VERIFIER.verify(self.directory)


if __name__ == '__main__':
    unittest.main()
