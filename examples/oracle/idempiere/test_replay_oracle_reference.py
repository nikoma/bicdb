"""Reference replay planning, cleanup fencing and optional real Oracle checks."""
import datetime
import decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import export_reference as export
import replay_oracle_reference as target
import test_foreign_keys as fixture


def column(table, name, kind='NUMBER', **extra):
    return dict(table=table, name=name, type=kind, precision=None, scale=None,
                bytes=20, characters=20, character_unit='B', virtual='NO', **extra)


def snapshot(root, extra=False):
    manifest = fixture.manifest('SOURCE_OWNER')
    rows = dict(P=[[1, 10]], C=[[1, 999, 0]], Q=[], U=[])
    manifest['columns'] = [column(table['name'], name) for table in manifest['tables'] for name in table['columns']]
    if extra:
        names = ['N', 'V', 'C', 'L', 'B', 'R', 'D', 'T']
        manifest['tables'].append(dict(name='PAYLOAD', columns=names))
        columns = [column('PAYLOAD', 'N'), column('PAYLOAD', 'V', 'VARCHAR2'), column('PAYLOAD', 'C', 'CHAR'),
                   column('PAYLOAD', 'L', 'CLOB'), column('PAYLOAD', 'B', 'BLOB'), column('PAYLOAD', 'R', 'RAW'),
                   column('PAYLOAD', 'D', 'DATE'), column('PAYLOAD', 'T', 'TIMESTAMP(6)')]
        columns[0].update(precision=38, scale=0);columns[2].update(bytes=3, characters=3)
        manifest['columns'].extend(columns)
        rows['PAYLOAD'] = [[decimal.Decimal('12345678901234567890123456789012345678'), 'hello', 'é ',
                            'é'*9001, bytes([0, 255]), bytes([1, 128]), datetime.datetime(2026, 11, 1, 12, 34, 56),
                            datetime.datetime(2026, 11, 1, 12, 34, 56, 123456)]]
    root.mkdir()
    for index, table in enumerate(manifest['tables']):
        data = b''.join((json.dumps([export.cell(value) for value in row], ensure_ascii=False)+'\n').encode() for row in rows[table['name']])
        table.update(file=str(index)+'.jsonl', sha256=hashlib.sha256(data).hexdigest(), rows=len(rows[table['name']]))
        (root/table['file']).write_bytes(data)
    (root/'manifest.json').write_text(json.dumps(manifest))
    return manifest


class OracleReplayPlanTests(unittest.TestCase):
    def test_reference_ddl_uses_oracle_raw_and_source_character_units(self):
        columns = [column('T', 'Q"R', 'RAW'), column('T', 'B', 'CHAR'), column('T', 'C', 'VARCHAR2')]
        columns[0]['bytes'] = 16;columns[1].update(characters=3, character_unit='B');columns[2].update(characters=3, character_unit='C')
        sql = target.create_table_sql('TEMP"Q', columns)
        self.assertIn('"Q""R" RAW(16)', sql);self.assertIn('CHAR(3 BYTE)', sql);self.assertIn('VARCHAR2(3 CHAR)', sql)
        self.assertIn('ON COMMIT PRESERVE ROWS', sql);self.assertNotIn('BYTEA', sql)
        columns[0]['bytes'] = '16'
        self.assertEqual(target.reference_type(columns[0]), 'RAW(16)')
        columns[0]['bytes'] = -1
        with self.assertRaises(ValueError):target.reference_type(columns[0])

    def test_projection_virtual_columns_and_unsupported_types_are_refused(self):
        with tempfile.TemporaryDirectory() as name:
            manifest = snapshot(Path(name)/'source')
            self.assertEqual(list(target.replay_columns(manifest)), ['P', 'C', 'Q', 'U'])
            manifest['columns'][0]['virtual'] = 'YES'
            with self.assertRaisesRegex(ValueError, 'virtual columns'):target.replay_columns(manifest)
            manifest['columns'][0].update(virtual='NO', type='XMLTYPE')
            with self.assertRaises(ValueError):target.replay_columns(manifest)
            manifest['columns'][0]['type'] = 'NUMBER';manifest['tables'][0]['columns'].reverse()
            with self.assertRaisesRegex(ValueError, 'projection'):target.replay_columns(manifest)

    def test_namespace_is_deterministic_and_rejects_arbitrary_names(self):
        manifest = fixture.manifest()
        self.assertEqual(target.table_mapping(manifest, '012345ABCDEF')['P'], 'BIC_FKR_012345ABCDEF_0000')
        for nonce in ['OTHER', '012345ABCDEF;DROP', '012345abcdef']:
            with self.subTest(nonce=nonce), self.assertRaises(ValueError):target.table_mapping(manifest, nonce)

    def test_cleanup_attempts_all_owned_names_and_reports_real_errors(self):
        class Error(Exception):pass
        class Cursor:
            def __init__(self):self.calls = []
            def execute(self, sql):
                self.calls.append(sql)
                if 'MISSING' in sql:raise Error(SimpleNamespace(code=942))
                if 'DENIED' in sql:raise Error(SimpleNamespace(code=1031))
        cursor = Cursor();errors = target.cleanup_tables(cursor, dict(A='FIRST', B='DENIED', C='MISSING'))
        self.assertEqual(len(cursor.calls), 3);self.assertEqual([item['oracle_code'] for item in errors], [1031])

    def test_reference_scope_required_before_input_access(self):
        with self.assertRaisesRegex(ValueError, 'isolated-reference-schema'):
            target.replay(SimpleNamespace(isolated_reference_schema=False))

    def test_cleanup_journal_cannot_redirect_drops_or_target_another_endpoint(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name);manifest = snapshot(root/'source');password = root/'password';password.write_text('fixture')
            cfg = dict(user='REF', host='localhost', port=1521, service_name='TEST', password_file=str(password))
            config = root/'config.json';config.write_text(json.dumps(cfg));report = root/'report.json'
            args = SimpleNamespace(snapshot=root/'source', oracle_config=config, report=report, isolated_reference_schema=True, cleanup_only=True)
            identity = dict(phase='oracle-export-reference', format=1, nonce='012345ABCDEF',
                            manifest_sha256=hashlib.sha256((root/'source/manifest.json').read_bytes()).hexdigest(),
                            target_sha256=hashlib.sha256(json.dumps([cfg[key] for key in ['host','port','service_name','user']]).encode()).hexdigest(),
                            table_map=target.table_mapping(manifest, '012345ABCDEF'))
            for changed in [dict(identity, target_sha256='different'), dict(identity, table_map=dict(P='AD_USER'))]:
                report.write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, 'cleanup journal'):target.replay(args)


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_REFERENCE_CONFIG'), 'set BICDB_ORACLE_REFERENCE_CONFIG for isolated Oracle replay')
class OracleReplayIntegrationTests(unittest.TestCase):
    def check_replay(self, extra):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name);manifest = snapshot(root/'source', extra)
            args = SimpleNamespace(snapshot=root/'source', oracle_config=Path(os.environ['BICDB_ORACLE_REFERENCE_CONFIG']),
                                   report=root/'report.json', isolated_reference_schema=True, cleanup_only=False)
            report = target.replay(args)
            self.assertTrue(report['success']);self.assertTrue(report['reference_rows_equal_export'])
            self.assertTrue(report['cleanup_completed']);self.assertEqual(report['cleanup_errors'], [])
            self.assertEqual([item['orphan_rows'] for item in report['completed']], [1, 0, 0])
            self.assertEqual(len(report['verified_tables']), len(manifest['tables']))
            for flag in ['live_source_compared', 'target_inventory_compared', 'application_qualified']:self.assertFalse(report[flag])
            args.cleanup_only = True
            self.assertTrue(target.replay(args)['cleanup_completed'])
            incomplete = json.loads(args.report.read_text());incomplete.update(success=False, inventory_completed=False)
            args.report.write_text(json.dumps(incomplete))
            cleaned = target.replay(args)
            self.assertTrue(cleaned['cleanup_completed']);self.assertFalse(cleaned['success'])

    def test_saved_orphans_are_replayed_and_removed(self):self.check_replay(False)

    def test_binary_lobs_character_units_and_precise_values_round_trip(self):self.check_replay(True)


if __name__ == '__main__':unittest.main()
