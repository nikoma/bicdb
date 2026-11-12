#!/usr/bin/env python3
"""Four semantic tamper controls with outer byte manifests honestly refreshed."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


class EvidenceTamperTests(unittest.TestCase):
    def reject(self, filename, mutate):
        with tempfile.TemporaryDirectory(prefix='receiver-evidence-tamper-') as temp:
            target = Path(temp) / 'bundle'
            shutil.copytree(ROOT, target)
            path = target / filename
            if filename.endswith('.json'):
                value = json.loads(path.read_text())
                mutate(value)
                path.write_text(json.dumps(value) + '\n')
            else:
                path.write_text(mutate(path.read_text()))
            manifest = target / 'manifest.json'
            value = json.loads(manifest.read_text())
            for item in value['files']:
                if item['path'] == filename:
                    data = path.read_bytes()
                    item.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            manifest.write_text(json.dumps(value) + '\n')
            result = subprocess.run(['python3', str(target / 'verify.py')],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertNotEqual(result.returncode, 0)

    def test_cohort_cannot_be_relabelled_green(self):
        self.reject('receipt.json', lambda value: value.update(status='CLI_COHORT_GREEN'))

    def test_missing_full_database_cannot_be_claimed(self):
        self.reject('receipt.json', lambda value: value['custody'].update(
            full_database_or_page_wal_retained=True))

    def test_timeout_cannot_be_claimed_healthy(self):
        self.reject('receipt.json', lambda value: value['ledger'].update(
            receiver_after_fifth='HEALTHY_CONTINUITY'))

    def test_stdout_ledger_cannot_be_spliced(self):
        self.reject('observations/ledger-from-cli-stdout.jsonl',
                    lambda value: value.replace('external-json-null', 'invented-null-key'))


if __name__ == '__main__':
    unittest.main()
