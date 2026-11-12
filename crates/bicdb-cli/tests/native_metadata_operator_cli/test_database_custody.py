#!/usr/bin/env python3
"""Real small filesystem/copy checks; no BicDB engine or authority fixtures."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
import database_custody as custody


class FilesystemChecks(unittest.TestCase):
    def setUp(self):
        # A full workspace must not make the copier's positive test relax its
        # real free-space guard. These tiny (<256KiB) helper tests can use the
        # already mounted shared-memory tempdir; no mount/build/cache changes.
        parent = None
        if shutil.disk_usage(tempfile.gettempdir()).free < 2 * 1024 * 1024:
            parent = '/dev/shm'
        self.temp = tempfile.TemporaryDirectory(prefix='bicdb-custody-check-', dir=parent)
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.fixture = self.base / 'fixture'
        self.db = self.fixture / 'db0'
        self.db.mkdir(parents=True)
        # Actual short child, actual wait, never a fake production process/FSM.
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        status = child.wait()
        self.assertEqual(status, 0)
        self.request = {'fixture_root': str(self.fixture), 'owned_database_roots': [str(self.db)],
                        'destination_root': str(self.base / 'artifact'),
                        'actual_children_waited': [{'pid': child.pid, 'wait_ok': True}],
                        'prior_test_exits': [], 'actual_spawn_records': [{'pid': child.pid}]}
        self.caps = custody.Caps(max_bytes=1024, max_seconds=5, min_free_bytes=0, max_entries=32)

    def capture(self, caps=None):
        with contextlib.redirect_stdout(io.StringIO()):
            return custody.capture(self.request, caps or self.caps)

    def partial(self, caps=None):
        path, manifest = self.capture(caps)
        self.assertEqual(manifest['status'], 'PARTIAL')
        self.assertFalse(manifest['complete'])
        self.assertFalse(manifest['payload_published'])
        self.assertFalse((path / 'database-snapshot').exists())
        self.assertTrue(Path(manifest['quarantine_path']).is_dir())
        self.assertFalse(Path(manifest['quarantine_path']).is_relative_to(path.parent))
        return path, manifest

    def test_complete_nested_copy_hashes_modes_original_unchanged_and_tls_outside_excluded(self):
        (self.fixture / 'n0.key').write_text('not part of any owned database root')
        (self.db / 'nested').mkdir()
        (self.db / 'nested' / 'page.bin').write_bytes(b'actual small file\x00\xff')
        (self.db / 'empty').mkdir()
        before = (self.db / 'nested' / 'page.bin').stat()
        path, manifest = self.capture()
        self.assertTrue(manifest['complete'])
        target = path / 'database-snapshot/db0/nested/page.bin'
        self.assertEqual(target.read_bytes(), (self.db / 'nested/page.bin').read_bytes())
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        row = next(r for r in manifest['entries'] if r['path'] == 'db0/nested/page.bin')
        self.assertEqual(row['copied_sha256'], hashlib.sha256(target.read_bytes()).hexdigest())
        self.assertTrue(row['durable_copy'])
        self.assertEqual(custody.metadata(before), custody.metadata((self.db / 'nested/page.bin').stat()))
        self.assertFalse(any(p.name == 'n0.key' for p in path.rglob('*')))

    def test_actual_helper_process_protocol_and_complete_manifest(self):
        (self.db / 'page').write_bytes(b'actual helper subprocess copy')
        request = self.base / 'request.json'
        request.write_text(json.dumps(self.request))
        environment = dict(os.environ, BICDB_TEST_CLI_DB_MAX_BYTES='1024',
                           BICDB_TEST_CLI_DB_MAX_SECONDS='5',
                           BICDB_TEST_CLI_DB_MIN_FREE_BYTES=str(16 * 1024 * 1024),
                           BICDB_TEST_CLI_DB_MAX_ENTRIES='32')
        result = subprocess.run([sys.executable, str(Path(custody.__file__)),
                                 '--request', str(request)], env=environment,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(result.stdout.split('NATIVE_CLI_DATABASE_FAILURE_CUSTODY ', 1)[1])
        manifest_bytes = (Path(record['path']) / 'manifest.json').read_bytes()
        self.assertEqual(record['manifest_sha256'], hashlib.sha256(manifest_bytes).hexdigest())
        self.assertTrue(record['complete'])
        self.assertTrue(json.loads(manifest_bytes)['payload_published'])

    def test_source_symlink_refused(self):
        outside = self.base / 'outside'
        outside.write_bytes(b'must not copy')
        (self.db / 'link').symlink_to(outside)
        _, manifest = self.partial()
        self.assertIn('symlink', manifest['reason'])

    def test_hardlink_refused(self):
        outside = self.base / 'outside'
        outside.write_bytes(b'must not copy')
        os.link(outside, self.db / 'linked')
        _, manifest = self.partial()
        self.assertIn('hardlinked', manifest['reason'])

    def test_tls_name_refused(self):
        (self.db / 'n0.key').write_text('test marker only')
        _, manifest = self.partial()
        self.assertIn('TLS', manifest['reason'])

    def test_embedded_private_key_marker_refused_even_across_chunks(self):
        (self.db / 'blob.bin').write_bytes(b'x' * (65536 - 5) + b'PRIVATE KEY-----')
        caps = custody.Caps(max_bytes=128 * 1024, max_seconds=5, min_free_bytes=0)
        path, manifest = self.partial(caps)
        self.assertIn('private-key', manifest['reason'])
        self.assertFalse(any(p.name == 'blob.bin' for p in path.rglob('*')))
        self.assertFalse((Path(manifest['quarantine_path']) / 'db0/blob.bin').exists())

    def test_byte_cap_refuses_actual_oversized_file(self):
        (self.db / 'page').write_bytes(b'four')
        _, manifest = self.partial(custody.Caps(max_bytes=3, max_seconds=5, min_free_bytes=0))
        self.assertIn('byte cap', manifest['reason'])

    def test_expired_deadline_refuses_before_copy(self):
        _, manifest = self.partial(custody.Caps(max_seconds=0, min_free_bytes=0))
        self.assertIn('time cap', manifest['reason'])

    def test_real_free_space_floor_refuses(self):
        free = shutil.disk_usage(self.base).free
        _, manifest = self.partial(custody.Caps(min_free_bytes=free + 1024 * 1024))
        self.assertIn('free-disk floor', manifest['reason'])

    def test_unwaited_real_child_refuses_before_destination(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'])
        try:
            self.request['actual_spawn_records'].append({'pid': child.pid})
            with self.assertRaisesRegex(ValueError, 'not actually waited'):
                self.capture()
            self.assertFalse((self.base / 'artifact').exists())
        finally:
            child.kill()
            child.wait()

    def test_destination_inside_fixture_refuses_before_copy(self):
        self.request['destination_root'] = str(self.fixture / 'unsafe-output')
        with self.assertRaisesRegex(ValueError, 'overlaps'):
            self.capture()
        self.assertFalse((self.fixture / 'unsafe-output').exists())

    def test_entry_cap_counts_empty_owned_roots(self):
        other = self.fixture / 'db1'
        other.mkdir()
        self.request['owned_database_roots'].append(str(other))
        _, manifest = self.partial(custody.Caps(max_entries=1, min_free_bytes=0))
        self.assertIn('entry cap', manifest['reason'])


if __name__ == '__main__':
    unittest.main()
