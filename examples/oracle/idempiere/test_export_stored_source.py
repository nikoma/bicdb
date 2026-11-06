"""Stored-source export preserves bodies and refuses incomplete dictionary reads."""
import copy
import decimal
import hashlib
import json
import re
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import export_reference as target


class StoredSourceTests(unittest.TestCase):
    def setUp(self):
        names = ['F', 'P', 'TY', 'TY', 'PKG', 'PKG', 'TR']
        self.objects = [dict(kind=kind, name=name, status='VALID')
                        for kind, name in zip(target.STORED_SOURCE_KINDS, names)]
        self.rows = [(obj['name'], obj['kind'], decimal.Decimal(1), obj['kind']+' '+obj['name']+'\n')
                     for obj in self.objects]
        self.statuses = [('TR', 'DISABLED')]

    def cursor(self):
        owner = self
        class Cursor:
            def execute(self, sql):
                if 'FROM user_source' in sql:
                    kinds = re.findall(r"'([^']+)'", sql)
                    self.rows = [row for row in owner.rows if row[1] in kinds]
                elif 'FROM user_triggers' in sql: self.rows = owner.statuses
                else: raise AssertionError(sql)
            def __iter__(self): return iter(self.rows)
        return Cursor()

    def test_all_categories_remain_distinct_and_trigger_disable_state_is_preserved(self):
        source = target.stored_source(self.cursor(), self.objects)
        self.assertEqual([len(source[k]) for k in ['routines','types','type_bodies','packages','triggers']], [2,1,1,2,1])
        self.assertEqual(source['triggers'][0]['status'], 'DISABLED')
        for group in ['routines', 'types', 'type_bodies', 'packages', 'triggers']:
            for obj in source[group]:
                self.assertEqual(obj['ddl'], 'CREATE OR REPLACE '+obj['kind']+' '+obj['name']+'\n')
                self.assertEqual(obj['ddl_sha256'], hashlib.sha256(obj['ddl'].encode()).hexdigest())
        self.assertTrue(source['stored_source_encoding']['object_coverage_verified'])
        self.assertFalse(source['stored_source_encoding']['execution_qualified'])

    def test_missing_duplicate_gapped_reordered_or_unknown_source_is_refused(self):
        original = copy.deepcopy(self.rows)
        for change in ['missing', 'duplicate', 'gap', 'float', 'boolean', 'unknown', 'bad_text']:
            self.rows = copy.deepcopy(original)
            if change == 'missing': self.rows.pop()
            elif change == 'duplicate': self.rows.append(self.rows[0])
            else:
                row = list(self.rows[0])
                if change == 'gap': row[2] = 2
                elif change == 'float': row[2] = 1.0
                elif change == 'boolean': row[2] = True
                elif change == 'unknown': row[0] = 'MISSING'
                else: row[3] = b'bytes'
                self.rows[0] = tuple(row)
            with self.subTest(change=change), self.assertRaises(ValueError):
                target.stored_source(self.cursor(), self.objects)
        self.rows = original
        with self.assertRaisesRegex(ValueError, 'duplicate stored object'):
            target.stored_source(self.cursor(), self.objects+self.objects[:1])

    def test_blank_lines_and_wrapped_source_are_retained_without_unwrapping(self):
        self.rows = [('P', 'PACKAGE', 1, 'PACKAGE P wrapped\n'), ('P', 'PACKAGE', 2, None),
                     ('P', 'PACKAGE', 3, 'opaque_payload\n')]
        self.statuses = []
        source = target.stored_source(self.cursor(), [dict(kind='PACKAGE', name='P')])
        self.assertEqual(source['packages'][0]['ddl'], 'CREATE OR REPLACE PACKAGE P wrapped\n\nopaque_payload\n')
        self.assertEqual(source['packages'][0]['source_line_count'], 3)
        self.assertFalse(source['stored_source_encoding']['wrapped_source_unwrapped'])
        self.rows[1], self.rows[2] = self.rows[2], self.rows[1]
        with self.assertRaisesRegex(ValueError, 'line numbers'):
            target.stored_source(self.cursor(), [dict(kind='PACKAGE', name='P')])

    def test_missing_extra_duplicate_or_unknown_trigger_state_is_refused(self):
        for statuses in [[], [('OTHER','ENABLED')], [('TR','DISABLED')]*2, [('TR','UNKNOWN')]]:
            self.statuses = statuses
            with self.subTest(statuses=statuses), self.assertRaises(ValueError):
                target.stored_source(self.cursor(), self.objects)

    def test_bodies_require_declarations_and_nonfinite_line_numbers_are_refused(self):
        for kind in ['TYPE', 'PACKAGE']:
            objects = [obj for obj in self.objects if obj['kind'] != kind]
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'no catalog declaration'):
                target.stored_source(self.cursor(), objects)
        for line in [decimal.Decimal('NaN'), decimal.Decimal('sNaN'), decimal.Decimal('Infinity')]:
            self.rows[0] = (*self.rows[0][:2], line, self.rows[0][3])
            with self.subTest(line=line), self.assertRaisesRegex(ValueError, 'line numbers'):
                target.stored_source(self.cursor(), self.objects)

    def test_empty_definitions_are_not_certified_as_captured(self):
        self.statuses = []
        for text in [None, '', ' \n']:
            self.rows = [('P', 'PACKAGE', 1, text)]
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, 'definition is empty'):
                target.stored_source(self.cursor(), [dict(kind='PACKAGE', name='P')])

    def check_export(self, missing=False):
        owner = self
        class Cursor:
            description = []
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, **kwargs):
                self.rows = []
                if sql.startswith('SELECT current_scn'): self.rows = [(1,)]
                elif sql.startswith('SELECT object_type'): self.rows = [(o['kind'],o['name'],o['status']) for o in owner.objects]
                elif 'FROM user_source' in sql:
                    kinds = re.findall(r"'([^']+)'", sql)
                    rows = owner.rows[:-1] if missing else owner.rows
                    self.rows = [row for row in rows if row[1] in kinds]
                elif 'FROM user_triggers' in sql: self.rows = owner.statuses
            def fetchone(self): return self.rows[0]
            def __iter__(self): return iter(self.rows)
        class Connection:
            version = 'fixture'
            call_timeout = None
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def cursor(self): return Cursor()
        driver = SimpleNamespace(defaults=SimpleNamespace(), connect=lambda **kwargs: Connection())
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root/'password').write_text('fixture')
            config = root/'connection.json'
            config.write_text(json.dumps(dict(user='SOURCE',host='fixture',port=1521,service_name='TEST',password_file='password')))
            with patch.dict(sys.modules, oracledb=driver):
                if missing:
                    with self.assertRaisesRegex(ValueError, 'missing catalog definitions'): target.export(config, root/'export')
                    self.assertEqual(list((root/'export').iterdir()), [])
                    return
                summary = target.export(config, root/'export')
            catalog = json.loads((root/'export/catalog.json').read_text())
            manifest = json.loads((root/'export/manifest.json').read_text())
            for key in ['packages', 'type_bodies', 'triggers', 'stored_source_encoding']:
                self.assertEqual(catalog[key], manifest[key])
            self.assertEqual(summary['package_definitions'], 2)
            self.assertEqual(summary['type_bodies'], 1)
            self.assertEqual(summary['triggers'], 1)

    def test_export_publishes_complete_source_groups_to_catalog_and_manifest(self): self.check_export()

    def test_incomplete_source_prevents_catalog_or_manifest_publication(self): self.check_export(missing=True)


if __name__ == '__main__': unittest.main()
