"""The private home contains exact original resources, without source mutation."""
from pathlib import Path
import tempfile
import unittest

from prepare_runtime_resources import FEATURE, inventory, prepare


class RuntimeResourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.upstream = self.root / 'upstream'
        self.data = self.upstream / FEATURE / 'data' / 'import'
        self.data.mkdir(parents=True)
        (self.data / 'original.csv').write_bytes(b'account,name\n1,original\n')

    def test_exact_resource_copy_uses_private_home_and_retains_binary_files(self):
        (self.data / 'binary.bin').write_bytes(bytes([0, 128, 255]))
        original = inventory(self.upstream)
        home = self.root / 'home'
        report = prepare(self.upstream, home)
        self.assertEqual(original, inventory(home))
        self.assertEqual(original, inventory(self.upstream))
        self.assertEqual(report['file_count'], 2)
        self.assertFalse((home / 'plugins').exists())
        (home / FEATURE / 'data/import/original.csv').write_text('local')
        self.assertEqual(original, inventory(self.upstream))

    def test_existing_destination_is_preserved(self):
        home = self.root / 'home'
        home.mkdir()
        marker = home / 'keep'
        marker.write_text('retained')
        with self.assertRaises(ValueError):
            prepare(self.upstream, home)
        self.assertEqual(marker.read_text(), 'retained')

    def test_related_paths_are_rejected_before_copy(self):
        for destination in [self.upstream, self.upstream / 'home', self.root]:
            with self.subTest(destination=destination), self.assertRaises(ValueError):
                prepare(self.upstream, destination)

    def test_symlink_resource_cannot_redirect_copy(self):
        (self.data / 'link').symlink_to(self.data / 'original.csv')
        home = self.root / 'home'
        with self.assertRaises(ValueError):
            prepare(self.upstream, home)
        self.assertFalse(home.exists())

    def test_missing_feature_data_is_an_explicit_prerequisite(self):
        with self.assertRaises(ValueError):
            prepare(self.root / 'missing', self.root / 'home')


if __name__ == '__main__':
    unittest.main()
