"""Private-launch arguments must preserve shared and existing installations."""
from pathlib import Path
import tempfile
import unittest

from runtime_installation import launcher_arguments


class RuntimeInstallationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.product = self.root/'product'
        (self.product/'plugins').mkdir(parents=True)
        self.config = self.root/'runtime/configuration'
        self.config.mkdir(parents=True)
        (self.config/'config.ini').write_text('osgi.startLevel=1\n')
        self.installation = self.root/'runtime/installation'

    def test_explicit_private_installation_preserves_product_and_configuration(self):
        old = (self.config/'config.ini').read_bytes()
        self.assertEqual(launcher_arguments(self.product, self.config, self.installation),
                         ['-install', str(self.installation), '-configuration', str(self.config)])
        self.assertEqual(list(self.installation.iterdir()), [])
        self.assertEqual(list(self.product.iterdir()), [self.product/'plugins'])
        self.assertEqual((self.config/'config.ini').read_bytes(), old)
        # Preparing arguments twice before startup is safe while still empty.
        launcher_arguments(self.product, self.config, self.installation)

    def test_shared_product_and_overlapping_directories_are_refused(self):
        for config, installation in [(self.config, self.product/'private'),
                                     (self.config, self.config),
                                     (self.config, self.config/'install'),
                                     (self.config, self.config.parent)]:
            with self.subTest(installation=installation), self.assertRaises(ValueError):
                launcher_arguments(self.product, config, installation)
        shared = self.product/'configuration'
        shared.mkdir()
        (shared/'config.ini').write_text('original')
        with self.assertRaisesRegex(ValueError, 'outside'):
            launcher_arguments(self.product, shared, self.installation)
        self.assertEqual((shared/'config.ini').read_text(), 'original')

    def test_symlink_to_shared_product_is_not_a_private_installation(self):
        alias = self.root/'alias'
        alias.symlink_to(self.product, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'outside'):
            launcher_arguments(self.product, self.config, alias/'new')
        self.assertFalse((self.product/'new').exists())

    def test_existing_installation_is_not_modified(self):
        self.installation.mkdir()
        marker = self.installation/'keep'
        marker.write_text('preserve')
        with self.assertRaisesRegex(ValueError, 'empty'):
            launcher_arguments(self.product, self.config, self.installation)
        self.assertEqual(marker.read_text(), 'preserve')

    def test_missing_product_or_configuration_is_refused(self):
        for product, config in [(self.root/'missing', self.config),
                                (self.product, self.root/'missing')]:
            with self.subTest(product=product, config=config), self.assertRaises(ValueError):
                launcher_arguments(product, config, self.installation)
        self.assertFalse(self.installation.exists())


if __name__ == '__main__':
    unittest.main()
