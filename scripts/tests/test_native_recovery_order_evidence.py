import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'verify-native-recovery-order-evidence.py'
spec = importlib.util.spec_from_file_location('recovery_order_evidence', SCRIPT)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

class RecoveryEvidenceTamperTests(unittest.TestCase):
    def rewrite(self, change):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'evidence'
            shutil.copytree(verifier.DEFAULT, root)
            path = root / 'findings.json'
            value = json.loads(path.read_text())
            change(value)
            path.write_text(json.dumps(value))
            manifest = json.loads((root / 'MANIFEST.json').read_text())
            for entry in manifest['files']:
                raw = (root / entry['path']).read_bytes()
                entry.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            (root / 'MANIFEST.json').write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                verifier.verify(root)
    def test_authentic_scopes_verify(self):
        self.assertEqual(verifier.verify()['cohorts'], 4)
    def test_rehashed_large_failure_cannot_become_success(self):
        self.rewrite(lambda v: v['cohorts'][3].update(outcome='PASS', exit_code=0))
    def test_rehashed_unreached_gap_cannot_be_claimed(self):
        self.rewrite(lambda v: v['cohorts'][3].update(later_compacted_gap_exercised=True))
    def test_rehashed_obligations_cannot_be_truncated(self):
        self.rewrite(lambda v: v['cohorts'][3].update(raw_entry_counts={'applied':1023,'quorum_committed':1}))
    def test_rehashed_component_result_cannot_be_qualified(self):
        self.rewrite(lambda v: v.update(physical_qualification=True, production_qualified=True))

if __name__ == '__main__':
    unittest.main()
