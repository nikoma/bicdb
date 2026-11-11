"""Portable evidence checks, not engine recovery or Azure qualification."""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('cold_verifier', HERE / 'verify-azure-cold-catalog-evidence.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
EVIDENCE = HERE.parent / 'docs/evidence/azure-cold-catalog-reconciliation-2026-11-11'
LEDGERS = HERE.parent / 'docs/evidence/azure-long-growth-failure-2026-11-11'

class ColdEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.copy = Path(self.temp.name) / 'evidence'
        shutil.copytree(EVIDENCE, self.copy)

    def rehash(self, name):
        manifest_path = self.copy / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['files'][name] = hashlib.sha256((self.copy / name).read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest))

    def test_portable_check_does_not_read_private_archives(self):
        result = CHECK.verify(self.copy, LEDGERS)
        self.assertTrue(result['verified'])
        self.assertFalse(result['private_archives_read'])
        self.assertFalse(result['actual_row_visibility_verified'])

    def test_digest_change_is_rejected(self):
        with (self.copy / 'per-ack-catalog-observations.json.gz').open('ab') as out:
            out.write(b'changed')
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            CHECK.verify(self.copy, LEDGERS)

    def test_rehashed_wrong_expected_payload_is_rejected(self):
        path = self.copy / 'per-ack-catalog-observations.json.gz'
        rows = json.loads(gzip.decompress(path.read_bytes()))
        rows[0]['expected_metadata_sha256'] = '0' * 64
        path.write_bytes(gzip.compress(json.dumps(rows).encode(), mtime=0))
        summary_path = self.copy / 'summary.json'
        summary = json.loads(summary_path.read_text())
        summary['per_ack_observations_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        summary_path.write_text(json.dumps(summary))
        self.rehash(path.name)
        self.rehash(summary_path.name)
        with self.assertRaisesRegex(ValueError, 'expected payload hash mismatch'):
            CHECK.verify(self.copy, LEDGERS)

    def test_rehashed_row_visibility_claim_is_rejected(self):
        path = self.copy / 'summary.json'
        summary = json.loads(path.read_text())
        summary['row_visibility_verified'] = True
        path.write_text(json.dumps(summary))
        self.rehash(path.name)
        with self.assertRaisesRegex(ValueError, 'unsupported qualification'):
            CHECK.verify(self.copy, LEDGERS)

if __name__ == '__main__':
    unittest.main()
