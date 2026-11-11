import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1]/'verify-native-nonzero-target-evidence.py'
spec = importlib.util.spec_from_file_location('native_nonzero_evidence', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
EVIDENCE = SCRIPT.parents[1]/'docs/evidence/native-nonzero-target-install-2026-11-11'

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)/'evidence'
        shutil.copytree(EVIDENCE,self.root)
    def tearDown(self):
        self.temp.cleanup()
    def rehash(self, name):
        path = self.root/'manifest.json'
        manifest = json.loads(path.read_text())
        manifest['files'][name] = hashlib.sha256((self.root/name).read_bytes()).hexdigest()
        path.write_text(json.dumps(manifest))
    def summary(self, modify):
        path = self.root/'summary.json'
        value = json.loads(path.read_text()); modify(value)
        path.write_text(json.dumps(value)); self.rehash('summary.json')
    def test_real_bundle_replays_34_local_tests(self):
        self.assertEqual(module.verify(self.root)['distinct_tests'],34)
    def test_rehashed_physical_qualification_upgrade_is_rejected(self):
        self.summary(lambda value: value.update(physical_scale_certified=True))
        with self.assertRaisesRegex(ValueError,'maturity upgrade'):
            module.verify(self.root)
    def test_rehashed_retained_failure_erasure_is_rejected(self):
        self.summary(lambda value: value.update(retained_failed_attempts=[]))
        with self.assertRaisesRegex(ValueError,'failures erased'):
            module.verify(self.root)
    def test_cached_test_output_cannot_replace_actual_compilation(self):
        path = self.root/'attempt-5-image-install.log.gz'
        text = gzip.decompress(path.read_bytes()).decode()
        text = '\n'.join(line for line in text.splitlines() if 'Compiling bicdb-core ' not in line)
        path.write_bytes(gzip.compress(text.encode(),mtime=0));self.rehash(path.name)
        with self.assertRaisesRegex(ValueError,'compilation missing'):
            module.verify(self.root)

if __name__ == '__main__':
    unittest.main()
