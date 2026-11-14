"""Exercise live version synchronization without rewriting historical evidence."""
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'bump-version.py'


class BumpVersionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='bicdb-version-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        self.put('crates/bicdb-core/Cargo.toml', '''[package]
name = "bicdb-core"
version = "1.0.1-beta"
[dependencies]
bicdb-page = { path = "../bicdb-page", version = "^1.0.1-beta" }
''')
        self.put('crates/bicdb-page/Cargo.toml', '''[package]
name = "bicdb-page"
version = "1.0.1-beta"
''')
        self.put('examples/consumer/Cargo.toml', '''[package]
name = "consumer"
version = "0.1.0"
[dependencies]
bicdb-core = { path = "../../crates/bicdb-core", version = "=1.0.1-beta" }
''')
        self.put('Cargo.lock', '''version = 4
[[package]]
name = "bicdb-core"
version = "1.0.1-beta"
[[package]]
name = "bicdb-page"
version = "1.0.1-beta"
''')
        self.put('POSTGRES_COMPATIBILITY.md',
                 'Current source version: **BicDB 1.0.1-beta**\n')
        # Both an old requirement and a snapshot matching the live version
        # must stay byte-exact. Check and mutation used to visit both.
        self.put('docs/evidence/old/Cargo.toml', '''[package]
name = "bicdb-core"
version = "1.0.0-beta"
[dependencies]
bicdb-page = { version = "1.0.0-beta" }
''')
        self.put('docs/evidence/current/Cargo.toml',
                 (self.root / 'crates/bicdb-core/Cargo.toml').read_text())
        self.put('docs/evidence/current/Cargo.lock',
                 (self.root / 'Cargo.lock').read_text())
        self.put('docs/releases/1.0.1-beta.md',
                 'Download https://example.test/1.0.1-beta/bicdb\n')
        subprocess.run(['git', 'add', '.'], cwd=self.root, check=True)

    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    def invoke(self, *args):
        return subprocess.run(['python3', str(SCRIPT), '--repo', str(self.root),
                               *args], text=True, capture_output=True)

    def hashes(self):
        return {str(p.relative_to(self.root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.root.rglob('*') if p.is_file() and '.git' not in p.parts}

    def test_live_bump_preserves_all_historical_bytes(self):
        before = self.hashes()
        self.assertEqual(self.invoke('--check').returncode, 0)
        self.assertEqual(before, self.hashes())
        result = self.invoke('1.0.2-beta')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.invoke('--check').returncode, 0)
        after = self.hashes()
        for name in before:
            if name.startswith(('docs/evidence/', 'docs/releases/')):
                self.assertEqual(before[name], after[name], name)
            else:
                self.assertNotEqual(before[name], after[name], name)
        self.assertIn('version = "=1.0.2-beta"',
                      (self.root / 'examples/consumer/Cargo.toml').read_text())

    def test_live_dependency_mismatch_refuses_without_partial_writes(self):
        path = self.root / 'examples/consumer/Cargo.toml'
        path.write_text(path.read_text().replace('=1.0.1-beta', '=1.0.0-beta'))
        before = self.hashes()
        result = self.invoke('1.0.2-beta')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('mixed internal dependency version', result.stderr)
        self.assertEqual(before, self.hashes())

    def test_live_lock_mismatch_refuses_without_partial_writes(self):
        path = self.root / 'Cargo.lock'
        path.write_text(path.read_text().replace('1.0.1-beta', '1.0.0-beta'))
        before = self.hashes()
        result = self.invoke('1.0.2-beta')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('mixed lockfile version', result.stderr)
        self.assertEqual(before, self.hashes())


if __name__ == '__main__':
    unittest.main()
