"""Real free-extent inventory, access control and dictionary name resolution."""
import os
import subprocess
import unittest

import test_foreign_keys as fixtures

NATIVE = '"pg_catalog"."bicdb_oracle_free_space"'
LABELS = ['TABLESPACE_NAME', 'FILE_ID', 'BLOCK_ID', 'BYTES', 'BLOCKS', 'RELATIVE_FNO']


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),
                     'set BICDB_ORACLE_SERVER for native storage catalog checks')
class StorageCatalogNativeTests(unittest.TestCase):
    def fixture(self, profile):
        class PagedFixture(fixtures.ForeignKeyMigrationIntegrationTests):
            def start_server(self):
                self.child = subprocess.Popen(
                    [os.environ['BICDB_ORACLE_SERVER'], str(self.root/'db'), str(self.port),
                     self.profile, '8', '4', 'server_paged'],
                    stdout=self.log, stderr=subprocess.STDOUT)
        f = PagedFixture(methodName='runTest')
        f.profile = profile
        self.addCleanup(lambda: self.assertTrue(f.doCleanups(), 'storage fixture cleanup failed'))
        f.setUp()
        return f

    def connect(self, f, role):
        import psycopg
        return psycopg.connect(host='127.0.0.1', port=f.port, user=role, dbname='bicdb',
                               autocommit=True, options='-c bicdb.sql_profile='+f.profile)

    def check_access(self, profile):
        import psycopg
        f = self.fixture(profile)
        with f.conn.cursor() as admin:
            for role in ['monitor_reader', 'monitor_group', 'unrelated_reader']:
                admin.execute('CREATE ROLE '+role+' LOGIN NOSUPERUSER')
            admin.execute('CREATE TABLE "private_records" ("ID" INTEGER)')
            admin.execute('INSERT INTO "private_records" VALUES (7)')
            admin.execute('SET ROLE monitor_reader', prepare=False)
            try:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    admin.execute('SELECT * FROM '+NATIVE)
            finally:
                admin.execute('RESET ROLE', prepare=False)
        with self.connect(f, 'monitor_reader') as reader, reader.cursor() as c:
            for sql in ['SELECT * FROM '+NATIVE,
                        'SELECT * FROM '+NATIVE+' WHERE 1=0',
                        'SELECT count(*) FROM '+NATIVE,
                        'SELECT a.* FROM '+NATIVE+' a JOIN '+NATIVE+' b ON a."BLOCK_ID"=b."BLOCK_ID"']:
                with self.subTest(sql=sql), self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    c.execute(sql, prepare=False)
            # The relation is a view, not a callable catalog function.
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                c.execute('SELECT * FROM '+NATIVE+'()', prepare=False)
            # A monitor cannot self-grant visibility of the storage inventory.
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('GRANT SELECT ON '+NATIVE+' TO monitor_reader', prepare=False)
        with f.conn.cursor() as admin:
            admin.execute('GRANT SELECT ON '+NATIVE+' TO monitor_group', prepare=False)
            admin.execute('GRANT monitor_group TO monitor_reader', prepare=False)
        with self.connect(f, 'monitor_reader') as reader, reader.cursor() as c:
            for prepare in [False, True]:
                c.execute('SELECT * FROM '+NATIVE+' WHERE 1=0', prepare=prepare)
                self.assertEqual([d.name for d in c.description], LABELS)
                self.assertEqual([d.type_code for d in c.description], [1043]+[1700]*5)
                self.assertEqual(c.fetchall(), [])
                c.execute('SELECT count(*) FROM '+NATIVE, prepare=prepare)
                self.assertGreaterEqual(c.fetchone()[0], 0)
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('SELECT * FROM "private_records"')
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('VACUUM', prepare=False)
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('GRANT SELECT ON '+NATIVE+' TO unrelated_reader', prepare=False)
        with f.conn.cursor() as admin:
            admin.execute('REVOKE SELECT ON '+NATIVE+' FROM monitor_group', prepare=False)
        with self.connect(f, 'monitor_reader') as reader, reader.cursor() as c:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('SELECT * FROM '+NATIVE)
        f.restart_server()
        with self.connect(f, 'monitor_reader') as reader, reader.cursor() as c:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute('SELECT * FROM '+NATIVE)

    def test_postgres_monitoring_grants_describe_and_denials(self):
        self.check_access('postgres')

    def test_oracle_monitoring_grants_describe_and_denials(self):
        self.check_access('oracle-preview-v1')

    def test_oracle_dictionary_resolution_respects_local_tables_views_and_ctes(self):
        f = self.fixture('oracle-preview-v1')
        with f.conn.cursor() as c:
            for sql in ['SELECT * FROM DBA_FREE_SPACE', 'SELECT * FROM SYS.DBA_FREE_SPACE',
                        'SELECT DBA_FREE_SPACE.BLOCKS FROM DBA_FREE_SPACE WHERE 1=0']:
                c.execute(sql)
                self.assertTrue(c.description)
                c.fetchall()
            c.execute('WITH DBA_FREE_SPACE AS (SELECT 17 AS VALUE FROM DUAL) '
                      'SELECT VALUE FROM DBA_FREE_SPACE')
            self.assertEqual(c.fetchall(), [(17,)])
            c.execute('CREATE TABLE DBA_FREE_SPACE (VALUE INTEGER)')
            c.execute('INSERT INTO DBA_FREE_SPACE VALUES (23)')
            c.execute('SELECT VALUE FROM DBA_FREE_SPACE')
            self.assertEqual(c.fetchall(), [(23,)])
            c.execute('SELECT * FROM SYS.DBA_FREE_SPACE WHERE 1=0')
            self.assertEqual([d.name for d in c.description], LABELS)
            c.execute('DROP TABLE DBA_FREE_SPACE')
            c.execute('CREATE VIEW DBA_FREE_SPACE AS SELECT 29 AS VALUE FROM DUAL')
            c.execute('SELECT VALUE FROM DBA_FREE_SPACE')
            self.assertEqual(c.fetchall(), [(29,)])
            c.execute('CREATE TABLE "dba_free_space" ("value" INTEGER)')
            c.execute('INSERT INTO "dba_free_space" VALUES (31)')
            c.execute('SELECT "value" FROM "dba_free_space"')
            self.assertEqual(c.fetchall(), [(31,)])

    def test_postgres_unqualified_names_remain_ordinary_relations(self):
        f = self.fixture('postgres')
        with f.conn.cursor() as c:
            for name in ['dba_free_space', 'bicdb_oracle_free_space']:
                c.execute('CREATE TABLE '+name+' (value INTEGER)')
                c.execute('INSERT INTO '+name+' VALUES (37)')
                c.execute('SELECT value FROM '+name)
                self.assertEqual(c.fetchall(), [(37,)])
            c.execute('SELECT * FROM '+NATIVE+' WHERE 1=0')
            self.assertEqual([d.name for d in c.description], LABELS)
            for operand in ['"AMOUNT"', 'a."AMOUNT"']:
                c.execute('WITH amounts("AMOUNT") AS '
                          '(SELECT CAST(3.25 AS numeric) UNION ALL SELECT CAST(6.75 AS numeric)) '
                          'SELECT SUM('+operand+')/2 FROM amounts a', prepare=True)
                self.assertEqual(c.description[0].type_code, 1700)
                self.assertEqual(c.fetchone(), (5,))

    def check_inventory(self, profile):
        import psycopg
        f = self.fixture(profile)
        with f.conn.cursor() as c:
            c.execute('CREATE TABLE churn (id INTEGER PRIMARY KEY, body TEXT)')
            for start in range(0, 400, 50):
                values = ','.join("(%d,'%s')" % (i, 'x'*2000) for i in range(start,start+50))
                c.execute('INSERT INTO churn VALUES '+values)
            c.execute('DELETE FROM churn WHERE id<380')
        f.restart_server()
        with f.conn.cursor() as c:
            c.execute('VACUUM')
            c.execute('SELECT * FROM '+NATIVE+' ORDER BY "BLOCK_ID"')
            runs = c.fetchall()
            self.assertTrue(runs, 'churn and vacuum must produce reusable pages')
            end = None
            for tablespace, file_id, start, size, blocks, relative in runs:
                self.assertEqual((tablespace,file_id,relative), ('BICDB_PAGED',1,1))
                self.assertGreater(blocks, 0)
                self.assertEqual(size, blocks*8192)
                if end is not None:
                    self.assertGreater(start, end, 'adjacent runs should have coalesced')
                end = start+blocks
            c.execute('SELECT bicdb_space_report()')
            import json
            report = c.fetchone()[0]
            if isinstance(report, str):
                report = json.loads(report)
            self.assertEqual(sum(row[4] for row in runs), report['paged']['free_pages'])
            # Declared numeric columns must retain decimal arithmetic inside
            # aggregates, rather than merely advertise numeric wire OIDs.
            from decimal import Decimal, ROUND_HALF_UP
            c.execute('SELECT SUM("BYTES")/1024/1024, '
                      'ROUND(MAX("BYTES")/SUM("BYTES"),2)*100, '
                      'SUM("BLOCKS"), MAX("BLOCKS") FROM '+NATIVE)
            free_mb, ratio, blocks, largest = c.fetchone()
            total_bytes = sum(row[3] for row in runs)
            self.assertEqual(free_mb, total_bytes/Decimal(1024)/1024)
            self.assertEqual(ratio, (max(row[3] for row in runs)/total_bytes).quantize(
                Decimal('.01'), rounding=ROUND_HALF_UP)*100)
            self.assertEqual((blocks, largest),
                             (sum(row[4] for row in runs),max(row[4] for row in runs)))
            c.execute("SET bicdb.storage_inventory_max_read_bytes='1'")
            with self.assertRaises(psycopg.errors.ProgramLimitExceeded):
                c.execute('SELECT * FROM '+NATIVE)
            c.execute('RESET bicdb.storage_inventory_max_read_bytes')
            c.execute('SELECT count(*) FROM '+NATIVE)
            self.assertGreater(c.fetchone()[0], 0)
            values = ','.join("(%d,'%s')" % (i, 'y'*2000) for i in range(500,550))
            c.execute('INSERT INTO churn VALUES '+values)
            c.execute('SELECT COALESCE(SUM("BLOCKS"),0) FROM '+NATIVE)
            self.assertLess(c.fetchone()[0], sum(row[4] for row in runs),
                            'new records must reuse allocator free pages')
        f.restart_server()
        with f.conn.cursor() as c:
            # Checkpoint/restart can truncate free tail pages and reuse holes
            # for metadata. A truthful post-restart inventory may be empty.
            c.execute('SELECT * FROM '+NATIVE)
            runs = c.fetchall()
            c.execute('SELECT bicdb_space_report()')
            report = c.fetchone()[0]
            if isinstance(report, str):
                report = json.loads(report)
            self.assertEqual(sum(row[4] for row in runs), report['paged']['free_pages'])
            c.execute('SELECT count(*) FROM churn')
            self.assertEqual(c.fetchone(), (70,))

    def test_postgres_inventory_reports_free_runs_and_refuses_partial_results(self):
        self.check_inventory('postgres')

    def test_oracle_inventory_reports_free_runs_and_refuses_partial_results(self):
        self.check_inventory('oracle-preview-v1')


if __name__ == '__main__':
    unittest.main()
