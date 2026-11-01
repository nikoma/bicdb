import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('hr_prepare', Path(__file__).with_name('prepare.py'))
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class SamplePreparation(unittest.TestCase):
    def test_pinned_sample_matches_generated_migration(self):
        import json
        self.assertEqual(prepare.prepare(), json.loads((prepare.ROOT/'migration.json').read_text()))

    def test_client_commands_and_slashes_inside_strings_remain_data(self):
        source = "REM client comment\nSET ECHO OFF\nCREATE TABLE t(x VARCHAR2(30));\nBEGIN\nINSERT INTO t VALUES ('semi;\n/\nREM data');\nEND;\n/\nSELECT 'O''Brien; kept' FROM DUAL;"
        result = list(prepare.statements(source))
        self.assertEqual(len(result), 3)
        self.assertIn('/\nREM data', result[1])
        self.assertIn("O''Brien; kept", result[2])

    def test_incomplete_blocks_and_quotes_fail(self):
        with self.assertRaises(ValueError): list(prepare.statements('BEGIN\nNULL;\n'))
        with self.assertRaises(ValueError): list(prepare.statements("SELECT 'unterminated"))


if __name__ == '__main__': unittest.main()
