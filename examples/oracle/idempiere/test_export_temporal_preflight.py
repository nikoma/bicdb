"""Refuse temporal declarations the source codec cannot faithfully represent."""
import unittest
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import export_reference as target


class ExportTemporalPreflightTests(unittest.TestCase):
    def test_supported_precision_and_offsets_keep_existing_projection(self):
        for precision in ['', *(f'({i})' for i in range(7))]:
            for suffix in ['', ' WITH TIME ZONE']:
                kind = 'TIMESTAMP' + precision + suffix
                column = dict(name='Q"T', type=kind)
                target.require_temporal_encoding(column)
                expected = 'TO_CHAR("Q""T", \'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM\') AS "Q""T"' if suffix else '"Q""T"'
                self.assertEqual(target.value_projection([column]), expected)

    def test_submicrosecond_and_local_zone_declarations_fail_before_projection(self):
        for kind in ['TIMESTAMP(7)', 'TIMESTAMP(8)', 'TIMESTAMP(9)',
                     'TIMESTAMP(9) WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE',
                     'TIMESTAMP(6) WITH LOCAL TIME ZONE', 'TIMESTAMP(-1)', 'TIMESTAMP(10)']:
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'export codec contract'):
                target.value_projection([dict(name='T', type=kind)])

    def test_ordinary_date_and_non_temporal_columns_are_unchanged(self):
        columns = [dict(name='D', type='DATE'), dict(name='N', type='NUMBER'), dict(name='B', type='BLOB')]
        self.assertEqual(target.value_projection(columns), '"D","N","B"')

    def check_metadata_preflight(self, base_kind, view_kind=None):
        class MetadataContinued(Exception):pass
        class Cursor:
            def __enter__(self):return self
            def __exit__(self, *args):pass
            def execute(self, sql):
                if sql == 'SET TRANSACTION READ ONLY':self.rows = []
                elif sql.startswith('SELECT current_scn'):self.rows = [(1,)]
                elif sql.startswith('SELECT object_type'):self.rows = [('TABLE', 'T', 'VALID')]
                elif sql.startswith('SELECT table_name FROM'):self.rows = [('T',)]
                elif sql.startswith('SELECT table_name,column_name'):
                    self.rows = [('T', 'STAMP', 1, base_kind, 13, None, None, None, None, 'Y', None, 'NO')]
                    if view_kind:self.rows.append(('V', 'STAMP', 1, view_kind, 13, None, None, None, None, 'Y', None, 'NO'))
                else:raise MetadataContinued('base temporal preflight passed')
            def fetchone(self):return self.rows[0]
            def __iter__(self):return iter(self.rows)
        class Connection:
            def __enter__(self):return self
            def __exit__(self, *args):pass
            def cursor(self):return Cursor()
        driver = SimpleNamespace(defaults=SimpleNamespace(), connect=lambda **kwargs: Connection())
        with tempfile.TemporaryDirectory() as name:
            root = Path(name);password = root/'password';password.write_text('fixture')
            config = root/'config.json';config.write_text(json.dumps(dict(user='SOURCE', host='fixture', port=1521, service_name='TEST', password_file=str(password))))
            with patch.dict(sys.modules, oracledb=driver):
                if base_kind == 'TIMESTAMP(9)':
                    with self.assertRaisesRegex(ValueError, 'export codec contract'):target.export(config, root/'export')
                else:
                    with self.assertRaisesRegex(MetadataContinued, 'preflight passed'):target.export(config, root/'export')
            self.assertEqual(list((root/'export').iterdir()), [])

    def test_unsupported_base_metadata_stops_before_any_catalog_or_row_file(self):
        self.check_metadata_preflight('TIMESTAMP(9)')

    def test_view_only_precision_does_not_block_base_table_export(self):
        self.check_metadata_preflight('TIMESTAMP(6)', 'TIMESTAMP(9)')


if __name__ == '__main__':unittest.main()
