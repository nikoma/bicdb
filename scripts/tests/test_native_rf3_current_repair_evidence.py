import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'verify-native-rf3-current-repair-evidence.py'
spec = importlib.util.spec_from_file_location('rf3_current_repair_evidence', SCRIPT)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class RetainedEvidence(unittest.TestCase):
    def mutate(self, path, value):
        target = self.root / path
        target.write_text(json.dumps(value, indent=2) + '\n')
        self.rehash(path)

    def rehash(self, path):
        manifest = json.loads((self.root / 'manifest.json').read_text())
        manifest[path] = hashlib.sha256((self.root / path).read_bytes()).hexdigest()
        (self.root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'evidence'
        shutil.copytree(verifier.DEFAULT, self.root)
        self.addCleanup(self.temp.cleanup)

    def test_actual_retained_bundle_passes_without_mutation(self):
        before = {str(p.relative_to(self.root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(verifier.verify(self.root)['distinct_tests'], 10)
        after = {str(p.relative_to(self.root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before, after)

    def test_rehashed_physical_qualification_claim_is_rejected(self):
        summary = json.loads((self.root / 'summary.json').read_text())
        summary['qualification']['physically_tested'] = True
        self.mutate('summary.json', summary)
        with self.assertRaises(AssertionError):
            verifier.verify(self.root)

    def test_rehashed_failure_erasure_is_rejected(self):
        path = 'runs/attempt-2-normal-repair-result.json'
        result = json.loads((self.root / path).read_text())
        result['exit_code'] = 0
        self.mutate(path, result)
        with self.assertRaises(AssertionError):
            verifier.verify(self.root)

    def test_rehashed_cached_output_cannot_replace_real_compile(self):
        path = 'runs/final-normal-repair.stderr'
        target = self.root / path
        target.write_text('\n'.join(line for line in target.read_text().splitlines() if 'Compiling bicdb-core ' not in line) + '\n')
        self.rehash(path)
        with self.assertRaises(AssertionError):
            verifier.verify(self.root)


if __name__ == '__main__':
    unittest.main()
