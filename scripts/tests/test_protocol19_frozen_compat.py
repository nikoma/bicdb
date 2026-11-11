import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / 'docs/evidence/protocol19-frozen-compat-2026-11-11'
spec = importlib.util.spec_from_file_location('protocol_compat_verifier', ROOT / 'scripts/verify-protocol19-frozen-compat.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def rehash(root):
    save(root / 'manifest.json', {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in root.rglob('*') if p.is_file() and p.name != 'manifest.json'})


class ProtocolCompatibilityEvidenceTests(unittest.TestCase):
    def rejected(self, mutate):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'evidence'
            shutil.copytree(FIXTURE, root)
            mutate(root)
            rehash(root)  # Exercise semantic guards, not merely stale hashes.
            with self.assertRaises(ValueError):
                verifier.verify(root)

    def test_both_retained_cases_keep_partial_observation_distinct(self):
        result = verifier.verify(FIXTURE)
        self.assertEqual(result['actual_field_drop_witnesses'], 1)
        self.assertEqual(result['changed_state_cuts'], 1)
        self.assertFalse(result['tls_or_physical_qualification'])

    def test_non_ack_cannot_be_promoted_to_a_field_drop_witness(self):
        def mutate(root):
            p = root / 'reproducible-options/report.json'
            report = json.loads(p.read_text())
            report['v19_append_ack_and_domain_drop'] = 'PASS'
            save(p, report)
            p = root / 'summary.json'
            summary = json.loads(p.read_text())
            summary['case_expectations']['reproducible-options']['domain_drop'] = 'PASS'
            save(p, summary)
        self.rejected(mutate)

    def test_churn_cannot_be_reported_as_byte_stability(self):
        def mutate(root):
            p = root / 'reproducible-options/report.json'
            report = json.loads(p.read_text())
            report['metadata_bytes_identical_across_v20'] = True
            report['concurrent_metadata_change_observed'] = False
            save(p, report)
        self.rejected(mutate)

    def test_qualification_upgrade_is_refused(self):
        def mutate(root):
            p = root / 'summary.json'
            summary = json.loads(p.read_text())
            summary['physical_failure_or_production_qualification'] = True
            save(p, summary)
        self.rejected(mutate)

    def test_raw_frame_must_keep_its_declared_length(self):
        self.rejected(lambda root: (root / 'initial/02-v20-append.response.bin').write_bytes(b'\x00\x00\x00\x01{}'))

    def test_changed_paired_body_is_refused_even_with_updated_hashes(self):
        def mutate(root):
            p = root / 'initial/calls.json'
            calls = json.loads(p.read_text())
            row = calls[2]
            row['request']['request']['MetadataAppend']['request']['leader_commit'] += 1
            body = json.dumps(row['request'], separators=(',', ':')).encode()
            wire = struct.pack('>I', len(body)) + body
            (root / 'initial/03-v19-same-append.request.bin').write_bytes(wire)
            row['request_sha256'] = hashlib.sha256(wire).hexdigest()
            save(p, calls)
        self.rejected(mutate)

    def test_runner_refuses_an_unpinned_hash_before_starting_a_server(self):
        run = subprocess.run([sys.executable, str(ROOT / 'scripts/experiments/protocol19-frozen-compat.py'),
                              '--expected-sha256', '0' * 64], capture_output=True, text=True, timeout=5)
        self.assertEqual(run.returncode, 2)
        self.assertIn('only the pinned', run.stderr)


if __name__ == '__main__':
    unittest.main()
