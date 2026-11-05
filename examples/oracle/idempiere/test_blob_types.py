"""Migration safety and optional real-server BLOB reconciliation checks."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import blob_types as target


def manifest():
    return dict(source_schema='A"B', tables=[dict(name='T', columns=['ID','B','N'], rows=3)],
                columns=[dict(table='T',name='B',type='BLOB',virtual='NO')])


class BlobTypesTests(unittest.TestCase):
    def test_source_declarations_exclude_views_and_native_binary_columns(self):
        m=manifest();m['columns'] += [dict(table='V',name='B',type='BLOB'),dict(table='T',name='N',type='RAW')]
        self.assertEqual(target.blob_columns(m),[m['columns'][0]])

    def test_ambiguous_or_virtual_source_declarations_are_rejected(self):
        for change in ['table','column','projection','missing','virtual']:
            m=manifest()
            if change=='table':m['tables']*=2
            elif change=='column':m['columns']*=2
            elif change=='projection':m['tables'][0]['columns']+=['B']
            elif change=='missing':m['tables'][0]['columns']=['ID']
            else:m['columns'][0]['virtual']='YES'
            with self.subTest(change=change),self.assertRaises(ValueError):target.blob_columns(m)

    def test_journal_rejects_changed_identity_duplicates_and_out_of_order_entries(self):
        columns=manifest()['columns']+[dict(table='OTHER',name='C')]
        identity=dict(phase='blob-types',target_sha256='target',manifest_sha256='source')
        rows=[dict(table='T',column='B',declaration='BLOB'),dict(table='OTHER',column='C',declaration='BLOB')]
        self.assertEqual(target.validate_journal(dict(identity,completed=rows),identity,columns),{('T','B'),('OTHER','C')})
        for bad in [dict(identity,target_sha256='other',completed=[]),dict(identity,completed=rows[::-1]),
                    dict(identity,completed=rows[:1]*2),dict(identity,completed=[dict(rows[0],declaration='BYTEA')])]:
            with self.subTest(bad=bad),self.assertRaises(ValueError):target.validate_journal(bad,identity,columns)

    def test_apply_refuses_writers_before_opening_a_connection(self):
        with self.assertRaisesRegex(ValueError,'writers-quiesced'):
            target.migrate(SimpleNamespace(check_only=False,writers_quiesced=False))

    def test_missing_timezone_status_fails_before_catalogs_or_row_decoding(self):
        from unittest.mock import MagicMock
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'manifest.json').write_text(json.dumps(manifest()))
            args=SimpleNamespace(snapshot=root,pg_dsn_env='BICDB_BLOB_TEST_DSN',report=root/'report.json',resume=False,check_only=True,writers_quiesced=False)
            connection=MagicMock()
            connection.info.parameter_status.side_effect=lambda name:'oracle-preview-v1' if name=='bicdb_sql_profile' else None
            fake=SimpleNamespace(connect=MagicMock(return_value=connection))
            connection.__enter__.return_value=connection
            with patch.dict('sys.modules', psycopg=fake),patch.dict(os.environ,BICDB_BLOB_TEST_DSN='not-a-secret'):
                with self.assertRaisesRegex(ValueError,'must report TimeZone'):target.migrate(args)
            connection.cursor.assert_not_called()
            self.assertFalse(json.loads(args.report.read_text())['success'])

    def test_capability_probe_checks_persisted_bytes_and_cleans_up_on_failure(self):
        class Cursor:
            def __init__(self):self.sql=[]
            def execute(self,sql,**kwargs):self.sql.append(sql)
            def fetchall(self):return [(b'\xab',)]
        c=Cursor()
        with self.assertRaisesRegex(ValueError,'persistent BLOB'):target.probe_support(c,'A"B')
        self.assertIn('"A""B".',c.sql[0]);self.assertTrue(c.sql[-2].startswith('DELETE FROM'))
        self.assertTrue(c.sql[-1].startswith('DROP TABLE'))


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),'set BICDB_ORACLE_SERVER for real-server migration tests')
class BlobMigrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.temp=tempfile.TemporaryDirectory(prefix='bicdb-blob-migration-test-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root=Path(cls.temp.name)
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));cls.port=sock.getsockname()[1]
        cls.log=(cls.root/'server.log').open('wb')
        cls.addClassCleanup(cls.log.close)
        cls.child=subprocess.Popen([os.environ['BICDB_ORACLE_SERVER'],str(cls.root/'db'),str(cls.port),'oracle-preview-v1','8','4'],stdout=cls.log,stderr=subprocess.STDOUT)
        def stop_server():
            if cls.child.poll() is None:
                cls.child.send_signal(signal.SIGINT)
                try:cls.child.wait(timeout=20)
                except subprocess.TimeoutExpired:cls.child.kill();cls.child.wait()
            if cls.child.returncode != 0:
                raise RuntimeError('migration fixture server exited with '+str(cls.child.returncode))
        cls.addClassCleanup(stop_server)
        cls.dsn=f'host=127.0.0.1 port={cls.port} user=bicdb dbname=bicdb'
        for _ in range(600):
            if cls.child.poll() is not None:raise RuntimeError('test server exited: '+(cls.root/'server.log').read_text())
            try:
                cls.conn=psycopg.connect(cls.dsn,autocommit=True,options='-c bicdb.sql_profile=oracle-preview-v1',connect_timeout=1)
                cls.addClassCleanup(cls.conn.close)
                break
            except psycopg.OperationalError:time.sleep(.1)
        else:raise RuntimeError('test server startup timed out')

    def test_preflight_apply_resume_and_changed_rows_refusal(self):
        self.assertEqual(self.conn.info.parameter_status('TimeZone'), 'UTC')
        schema='MIG_'+str(time.time_ns())+'"Q'
        q=target.quote
        relation=q(schema)+'."T"'
        with self.conn.cursor() as c:
            c.execute('CREATE SCHEMA '+q(schema))
            c.execute('CREATE TABLE '+relation+'("ID" INTEGER PRIMARY KEY,"B" BYTEA,"N" BYTEA,"TZ" TIMESTAMP WITH TIME ZONE)')
            c.execute('INSERT INTO '+relation+'("ID","B","N") VALUES(1,HEXTORAW(\'00FF\'),HEXTORAW(\'AB\')),(2,NULL,HEXTORAW(\'AB\')),(3,HEXTORAW(\'\'),NULL)')
            c.execute('UPDATE '+relation+" SET \"TZ\"=CAST('2024-01-01 00:00:00+00' AS TIMESTAMP WITH TIME ZONE) WHERE \"ID\"=1")
        snap=self.root/'snapshot';snap.mkdir();m=manifest();m['source_schema']=schema;m['tables'][0]['columns'].append('TZ')
        (snap/'manifest.json').write_text(json.dumps(m))
        args=SimpleNamespace(snapshot=snap,pg_dsn_env='BICDB_BLOB_TEST_DSN',report=self.root/'migration.json',resume=False,check_only=True,writers_quiesced=False)
        with patch.dict(os.environ, BICDB_BLOB_TEST_DSN=self.dsn):
            target.migrate(args)
            report=json.loads(args.report.read_text());self.assertTrue(report['success']);self.assertFalse(report['conversion_capability_verified']);self.assertEqual(report['completed'],[])
            with self.conn.cursor() as c:self.assertIsNone(target.column_types(c,schema,'T',m['columns'])['B'])
            args.check_only=False;args.writers_quiesced=True;target.migrate(args)
            report=json.loads(args.report.read_text());self.assertTrue(report['success']);self.assertTrue(report['conversion_capability_verified']);self.assertEqual(len(report['completed']),1)
            self.assertEqual(report['table_fingerprints']['T']['before'],report['table_fingerprints']['T']['after'])
            args.resume=True;target.migrate(args)
            saved=args.report.read_bytes();original_manifest=(snap/'manifest.json').read_bytes()
            (snap/'manifest.json').write_text(json.dumps(dict(m,source_schema='DIFFERENT')))
            with self.assertRaisesRegex(ValueError,'journal does not match'):target.migrate(args)
            self.assertEqual(args.report.read_bytes(),saved)
            (snap/'manifest.json').write_bytes(original_manifest)
            with self.conn.cursor() as c:
                c.execute('SELECT bicdb_oracle_column_type(%s,%s)',(relation,'N'));self.assertIsNone(c.fetchone()[0])
                c.execute('UPDATE '+relation+' SET "N"=HEXTORAW(\'CD\') WHERE "ID"=1')
            with self.assertRaisesRegex(ValueError,'rows changed'):target.migrate(args)
            self.assertFalse(json.loads(args.report.read_text())['success'])
            with self.conn.cursor() as c:
                c.execute('DECLARE d BLOB; BEGIN SELECT "B" INTO d FROM '+relation+' WHERE "ID"=1 FOR UPDATE; DBMS_LOB.APPEND(d,HEXTORAW(\'12\')); END;')
                c.execute('SELECT "B","N" FROM '+relation+' WHERE "ID"=1');self.assertEqual(c.fetchone(),(bytes.fromhex('00FF12'),bytes.fromhex('CD')))
                c.execute('DELETE FROM '+relation);c.execute('DROP TABLE '+relation)
                # Class cleanup removes the entire native database, including on failure.


if __name__=='__main__':unittest.main()
