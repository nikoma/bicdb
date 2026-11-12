#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
ROOT = Path(__file__).resolve().parent


class SemanticTamper(unittest.TestCase):
    def reject(self, filename, mutate):
        with tempfile.TemporaryDirectory(prefix='postjoin-evidence-tamper-') as temp:
            target = Path(temp) / 'bundle'
            shutil.copytree(ROOT, target)
            path = target / filename
            value = json.loads(path.read_text())
            mutate(value)
            path.write_text(json.dumps(value) + '\n')
            manifest = target / 'manifest.json'
            data = json.loads(manifest.read_text())
            for item in data['files']:
                if item['path'] == filename:
                    body = path.read_bytes()
                    item.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
            manifest.write_text(json.dumps(data) + '\n')
            result = subprocess.run(['python3', str(target / 'verify.py')], capture_output=True)
            self.assertNotEqual(result.returncode, 0)

    def test_not_healthy_continuity(self):
        self.reject('receipt.json', lambda value: value['postjoin'].update(classification='HEALTHY'))

    def test_no_full_db_custody(self):
        self.reject('receipt.json', lambda value: value['artifact'].update(full_database_or_wal_or_range_catalogue_retained=True))

    def test_exact_eight_envelopes(self):
        self.reject('observations/postjoin-typed-responses.json', lambda value: value['typed_responses'].pop())

    def test_actual_unchanged_data_assignments(self):
        def change(value):
            next(iter(value['after']['ranges'].values()))['epoch'] += 1
        self.reject('observations/actual-topology-before-after.json', change)


if __name__ == '__main__':
    unittest.main()
