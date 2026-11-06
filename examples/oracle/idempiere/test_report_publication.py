"""Migration report publication preserves prior results and unrelated files."""
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

import import_bicdb as target


class ReportPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='bicdb-report-publication-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.report = self.root/'progress.json'
        self.old = {'success': False, 'completed': ['first']}
        self.report.write_text(json.dumps(self.old)+'\n')

    def test_successful_replacement_publishes_complete_json(self):
        for count in range(3):
            expected = {'success': count == 2, 'completed': list(range(count))}
            target.save_report(self.report, expected)
            self.assertEqual(json.loads(self.report.read_text()), expected)
            self.assertTrue(self.report.read_bytes().endswith(b'\n'))
            self.assertEqual(set(self.root.iterdir()), {self.report})

    def test_existing_fixed_staging_name_is_not_consumed(self):
        sibling = self.root/'progress.json.tmp'
        content = b'not owned by this report writer\n'
        sibling.write_bytes(content)
        target.save_report(self.report, {'success': True})
        self.assertTrue(sibling.exists(), 'unrelated sibling must be retained')
        self.assertEqual(sibling.read_bytes(), content)
        self.assertEqual(json.loads(self.report.read_text()), {'success': True})

    @unittest.skipUnless(os.name == 'posix', 'POSIX symlink and permission contract')
    def test_fixed_staging_symlink_cannot_overwrite_another_input(self):
        source = self.root/'source.json'
        original = b'{"source": "retained"}\n'
        source.write_bytes(original)
        sibling = self.root/'progress.json.tmp'
        sibling.symlink_to(source)
        target.save_report(self.report, {'success': True})
        self.assertEqual(source.read_bytes(), original)
        self.assertTrue(sibling.is_symlink())
        self.assertEqual(sibling.resolve(), source)

    def test_serialization_failure_preserves_old_report_and_removes_partial_staging(self):
        before = self.report.read_bytes()
        with self.assertRaises(TypeError):
            target.save_report(self.report, {'completed': ['partial'], 'invalid': object()})
        self.assertEqual(self.report.read_bytes(), before)
        self.assertEqual(set(self.root.iterdir()), {self.report})

    def test_replace_failure_preserves_old_report_and_removes_complete_staging(self):
        before = self.report.read_bytes()
        with patch.object(Path, 'replace', side_effect=OSError('injected publication failure')):
            with self.assertRaisesRegex(OSError, 'publication failure'):
                target.save_report(self.report, {'success': True})
        self.assertEqual(self.report.read_bytes(), before)
        self.assertEqual(set(self.root.iterdir()), {self.report})

    @unittest.skipUnless(os.name == 'posix', 'POSIX file permission contract')
    def test_new_journal_is_private_even_under_permissive_umask(self):
        self.report.unlink()
        previous = os.umask(0)
        try:
            target.save_report(self.report, {'target': {'host': 'private-reference'}})
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(self.report.stat().st_mode), 0o600)


if __name__ == '__main__':
    unittest.main()
