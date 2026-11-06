"""Native SQL BLOB read scalars and an independent PostgreSQL namespace control."""
import hashlib
import json
import os
import unittest

import export_reference as export
import import_bicdb as target
import test_foreign_keys as native


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'), 'set BICDB_ORACLE_SERVER for native SQL BLOB reads')
class SqlBlobReadNativeTests(unittest.TestCase):
    def fixture(self, profile):
        fixture = native.ForeignKeyMigrationIntegrationTests(methodName='runTest')
        fixture.profile = profile
        self.addCleanup(lambda: self.assertTrue(fixture.doCleanups(), 'SQL BLOB fixture cleanup failed'))
        fixture.setUp()
        return fixture

    def test_oracle_copy_metrics_prepared_metadata_and_restart(self):
        import psycopg
        fixture = self.fixture('oracle-preview-v1')
        rows = [[1,b'\x00\xff'],[2,b''],[3,None]]
        raw = b''.join((json.dumps([export.cell(v) for v in row])+'\n').encode() for row in rows)
        (fixture.root/'rows.jsonl').write_bytes(raw)
        table = dict(name='PAYLOADS', columns=['N','B'],rows=3,file='rows.jsonl',sha256=hashlib.sha256(raw).hexdigest())
        query = 'SELECT "N",SYS.DBMS_LOB.GETLENGTH(lob_loc=>"B"),DBMS_LOB.ISTEMPORARY("B"),"B" IS NULL FROM "LOBREAD"."PAYLOADS" ORDER BY "N"'
        expected = [(1,2,0,False),(2,0,0,False),(3,None,None,True)]
        with fixture.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA "LOBREAD"')
            cursor.execute('CREATE TABLE "LOBREAD"."PAYLOADS"("N" NUMBER,"B" BLOB)')
            target.copy_table(fixture.conn,cursor,fixture.root,'LOBREAD',table)
            target.verify_values(cursor,fixture.root,'LOBREAD',table)
            cursor.execute(query,prepare=True)
            self.assertEqual(cursor.fetchall(),expected)
            self.assertEqual([column.type_code for column in cursor.description][1:3],[20,20])
            cursor.execute('SELECT DBMS_LOB.GETLENGTH("B") FROM "LOBREAD"."PAYLOADS" WHERE 1=0',prepare=True)
            self.assertEqual(cursor.fetchall(),[])
            self.assertEqual(cursor.description[0].type_code,20)
            cursor.execute('CREATE TABLE "LOBREAD"."RAW_VALUES"("R" BYTEA)')
            cursor.execute('INSERT INTO "LOBREAD"."RAW_VALUES" VALUES(NULL)')
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                cursor.execute('SELECT DBMS_LOB.GETLENGTH("R") FROM "LOBREAD"."RAW_VALUES"')
        fixture.restart_server()
        with fixture.conn.cursor() as cursor:
            target.verify_values(cursor,fixture.root,'LOBREAD',table)
            cursor.execute(query,prepare=True)
            self.assertEqual(cursor.fetchall(),expected)

    def test_postgres_qualified_function_retains_its_ordinary_meaning(self):
        fixture = self.fixture('postgres')
        with fixture.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA "DBMS_LOB"')
            cursor.execute('CREATE FUNCTION "DBMS_LOB"."GETLENGTH"(b BYTEA) RETURNS INTEGER LANGUAGE SQL AS $$SELECT octet_length(b)+5$$')
            cursor.execute('SELECT "DBMS_LOB"."GETLENGTH"(%s)',(b'\xab',),prepare=True)
            self.assertEqual(cursor.fetchall(),[(6,)])
            cursor.execute('SELECT "DBMS_LOB"."GETLENGTH"(%s)',(b'',),prepare=True)
            self.assertEqual(cursor.fetchall(),[(5,)])
            cursor.execute('SELECT "DBMS_LOB"."GETLENGTH"(NULL::BYTEA)',prepare=True)
            self.assertEqual(cursor.fetchall(),[(None,)])
            cursor.execute('CREATE FUNCTION "DBMS_LOB"."TEXT_LENGTH"(b TEXT) RETURNS INTEGER LANGUAGE SQL AS $$SELECT octet_length(b)+5$$')
            cursor.execute('SELECT "DBMS_LOB"."TEXT_LENGTH"(%s)',(r'\xab',),prepare=True)
            self.assertEqual(cursor.fetchall(),[(9,)])
            cursor.execute('CREATE FUNCTION "DBMS_LOB"."BYTE_LENGTH_ROWS"(b BYTEA) RETURNS TABLE(n INTEGER) LANGUAGE SQL AS $$SELECT octet_length(b)+5$$')
            cursor.execute('SELECT n FROM "DBMS_LOB"."BYTE_LENGTH_ROWS"(%s)',(b'\xab',),prepare=True)
            self.assertEqual(cursor.fetchall(),[(6,)])
            cursor.execute('SELECT n FROM "DBMS_LOB"."BYTE_LENGTH_ROWS"(NULL::BYTEA)',prepare=True)
            self.assertEqual(cursor.fetchall(),[(None,)])
            cursor.execute('CREATE TABLE typed_rows(b TEXT)')
            cursor.execute('INSERT INTO typed_rows VALUES(%s)',(r'\xab',))
            cursor.execute('CREATE FUNCTION "DBMS_LOB"."COLUMN_LENGTH"(b BYTEA) RETURNS INTEGER LANGUAGE SQL AS $$SELECT octet_length(b)+5 FROM typed_rows$$')
            cursor.execute('SELECT "DBMS_LOB"."COLUMN_LENGTH"(%s)',(b'\xab',),prepare=True)
            self.assertEqual(cursor.fetchall(),[(9,)])


if __name__ == '__main__': unittest.main()
