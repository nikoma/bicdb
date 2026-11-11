"""Exercise artifact/claim substitution and conservative placement refusal."""
import copy
import importlib.util
import json
import shutil
import tarfile
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'capability_manifest.py'
spec = importlib.util.spec_from_file_location('capability_manifest', SCRIPT)
cap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cap)


class CapabilityManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'capabilities').mkdir()
        (self.root / 'crates/bicdb-cli').mkdir(parents=True)
        (self.root / 'crates/bicdb-cli/Cargo.toml').write_text('[package]\nversion="1.2.3-beta"\n')
        (self.root / 'engine.rs').write_text('// test source\n')
        claim = {'status': 'supported', 'summary': 'fixture only', 'source_paths': ['engine.rs'], 'test_paths': []}
        self.catalog = {'format': 'bicdb.capability-catalog/v1', 'modes': {
            mode: {'rebalance': copy.deepcopy(claim)} for mode in cap.MODES}}
        self.catalog['modes']['distributed_ranges']['multi_range_sql'] = {
            **copy.deepcopy(claim), 'status': 'unsupported'}
        (self.root / 'capabilities/catalog.json').write_text(json.dumps(self.catalog))
        self.binary = self.root / 'bicdb'
        self.binary.write_text('#!/bin/sh\nprintf "bicdb 1.2.3-beta\\n"\n')
        self.binary.chmod(0o700)
        self.git('init', '-q')
        self.git('add', '.')
        self.git('-c', 'user.name=Test', '-c', 'user.email=test@example.org', 'commit', '-qm', 'fixture')
        self.revision = self.git('rev-parse', 'HEAD').strip()

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], text=True)

    def manifest(self):
        return cap.build_manifest(self.root, self.binary, self.revision)

    def test_exact_bytes_and_source_identity_without_fabricated_tests(self):
        result = self.manifest()
        self.assertEqual(result['artifact']['binary_sha256'], cap.sha256(self.binary))
        self.assertEqual(result['artifact']['source_revision'], self.revision)
        for claims in result['modes'].values():
            for claim in claims.values():
                for level in cap.LEVELS[1:]:
                    self.assertEqual(claim[level], {'status': 'not_established', 'evidence': []})
        cap.validate_manifest(result)

    def test_qualified_requirement_refused_even_when_implemented(self):
        result = cap.admission(self.manifest(), 'distributed_ranges', ['rebalance'], 'qualified')
        self.assertFalse(result['allowed'])

    def test_unsupported_or_missing_capability_refused(self):
        for requirement in ('multi_range_sql', 'unknown'):
            self.assertFalse(cap.admission(self.manifest(), 'distributed_ranges', [requirement], 'implemented')['allowed'])

    def test_implemented_preflight_only(self):
        result = cap.admission(self.manifest(), 'distributed_ranges', ['rebalance'], 'implemented')
        self.assertTrue(result['allowed'])
        self.assertIn('runtime_policy_still_required', result['scope'])

    def test_conditional_implementation_cannot_silently_pass(self):
        result = self.manifest()
        claim = result['modes']['distributed_ranges']['rebalance']
        claim['implemented']['status'] = 'conditional'
        claim['conditions'] = ['needs configuration verification']
        self.assertFalse(cap.admission(result, 'distributed_ranges', ['rebalance'], 'implemented')['allowed'])

    def test_asserted_qualification_upgrade_rejected(self):
        result = self.manifest()
        result['modes']['distributed_ranges']['rebalance']['qualified'] = {'status': 'supported', 'evidence': [{'kind': 'invented'}]}
        with self.assertRaises(ValueError):
            cap.validate_manifest(result)

    def test_wrong_revision_rejected(self):
        with self.assertRaises(ValueError):
            cap.build_manifest(self.root, self.binary, '0' * 40)

    def test_dirty_source_rejected(self):
        (self.root / 'engine.rs').write_text('// changed\n')
        with self.assertRaises(subprocess.CalledProcessError):
            self.manifest()

    def test_wrong_binary_version_rejected(self):
        self.binary.write_text('#!/bin/sh\necho "bicdb 0.0.0"\n')
        self.git('add', 'bicdb')
        self.git('-c', 'user.name=Test', '-c', 'user.email=test@example.org', 'commit', '-qm', 'wrong version')
        self.revision = self.git('rev-parse', 'HEAD').strip()
        with self.assertRaises(ValueError):
            self.manifest()

    def test_symlink_binary_rejected(self):
        link = self.root / 'link'
        link.symlink_to(self.binary)
        with self.assertRaises(ValueError):
            cap.build_manifest(self.root, link, self.revision)

    def test_evidence_path_escape_rejected(self):
        self.catalog['modes']['distributed_ranges']['rebalance']['source_paths'] = ['../outside']
        with self.assertRaises(ValueError):
            cap.validate_catalog(self.catalog, self.root)

    def test_untracked_source_cannot_be_release_evidence(self):
        (self.root / 'untracked.rs').write_text('// outside committed revision')
        with self.assertRaises(ValueError):
            cap.committed_evidence(self.root, self.revision, 'untracked.rs')

    def test_parent_symlink_cannot_supply_source_evidence(self):
        with tempfile.TemporaryDirectory() as outside:
            (Path(outside) / 'source.rs').write_text('// external')
            (self.root / 'escape').symlink_to(outside, target_is_directory=True)
            claim = self.catalog['modes']['distributed_ranges']['rebalance']
            claim['source_paths'] = ['escape/source.rs']
            with self.assertRaises(ValueError):
                cap.validate_catalog(self.catalog, self.root)

    def test_cli_package_does_not_claim_separate_application_host(self):
        root = SCRIPT.parent.parent
        catalog = json.loads((root / 'capabilities/catalog.json').read_text())
        self.assertEqual(catalog['modes']['application_fleet']['worker_replacement']['status'], 'unsupported')

    def test_real_catalog_source_paths_exist(self):
        root = SCRIPT.parent.parent
        cap.validate_catalog(json.loads((root / 'capabilities/catalog.json').read_text()), root)

    def test_empty_requirements_cannot_pass(self):
        with self.assertRaises(ValueError):
            cap.admission(self.manifest(), 'distributed_ranges', [], 'qualified')

    @unittest.skipUnless(shutil.which('cc') and shutil.which('strip'), 'native fixture compiler/strip required')
    def test_packager_binds_manifest_to_actual_stripped_archive_binary(self):
        scripts = self.root / 'scripts'
        scripts.mkdir()
        for name in ('package-cli-release.py', 'capability_manifest.py'):
            shutil.copy2(SCRIPT.parent / name, scripts / name)
        for name in ('LICENSE', 'LICENSE-SCOPE.md', 'NOTICE', 'THIRD_PARTY_NOTICES.md'):
            (self.root / name).write_text('Fixture only\n')
        for name in ('LICENSES', 'licensing'):
            (self.root / name).mkdir()
            (self.root / name / 'fixture.txt').write_text('Fixture only\n')
        (self.root / 'Cargo.lock').write_text('[[package]]\nname="bicdb-cli"\nversion="1.2.3-beta"\n')
        self.git('add', '.')
        self.git('-c', 'user.name=Test', '-c', 'user.email=test@example.org', 'commit', '-qm', 'packaging fixture')
        self.revision = self.git('rev-parse', 'HEAD').strip()
        with tempfile.TemporaryDirectory() as temporary:
            temporary = Path(temporary)
            source = temporary / 'fixture.c'
            source.write_text('#include <stdio.h>\nint main(void) { puts("bicdb 1.2.3-beta"); return 0; }\n')
            binary = temporary / 'fixture'
            subprocess.run(['cc', '-g', str(source), '-o', str(binary)], check=True, capture_output=True)
            metadata = temporary / 'metadata.json'
            metadata.write_text(json.dumps({'packages': [{'id': 'fixture', 'name': 'bicdb-cli',
                'source': None, 'manifest_path': str(self.root / 'crates/bicdb-cli/Cargo.toml')}],
                'resolve': {'nodes': [{'id': 'fixture', 'dependencies': []}]}}))
            output = temporary / 'release'
            subprocess.run(['python3', str(scripts / 'package-cli-release.py'), '--binary', str(binary),
                            '--metadata', str(metadata), '--extra-notices', str(temporary),
                            '--revision', self.revision, '--output', str(output)], check=True, capture_output=True)
            with tarfile.open(next(output.glob('*.tar.gz'))) as archive:
                prefix = 'bicdb-1.2.3-beta-linux-x86_64/'
                manifest = json.load(archive.extractfile(prefix + 'CAPABILITIES.json'))
                build = json.load(archive.extractfile(prefix + 'BUILD.json'))
                actual = archive.extractfile(prefix + 'bicdb').read()
            digest = cap.hashlib.sha256(actual).hexdigest()
            self.assertEqual(digest, manifest['artifact']['binary_sha256'])
            self.assertEqual(digest, build['binary_sha256'])
            self.assertNotEqual(digest, cap.sha256(binary))
            self.assertFalse(cap.admission(manifest, 'distributed_ranges', ['multi_range_sql'], 'qualified')['allowed'])

    def test_cli_refuses_binary_substitution_and_manifest_tamper(self):
        result = self.manifest()
        manifest = self.root / 'CAPABILITIES.json'
        manifest.write_text(json.dumps(result))
        args = ['python3', str(SCRIPT), 'check', '--manifest', str(manifest), '--binary', str(self.binary),
                '--revision', self.revision, '--expected-sha256', cap.sha256(manifest),
                '--mode', 'distributed_ranges', '--require', 'rebalance', '--level', 'implemented']
        self.assertEqual(subprocess.run(args, capture_output=True).returncode, 0)
        self.binary.write_text('different bytes')
        changed = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(changed.returncode, 2)
        self.assertIn('binary digest mismatch', changed.stderr)
        manifest.write_text('{}')
        changed = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(changed.returncode, 2)
        self.assertIn('trusted digest', changed.stderr)


if __name__ == '__main__':
    unittest.main()
