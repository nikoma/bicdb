"""Export exact owned rows despite a quoted owner, shadow name and changed schema."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import export_reference as target


@unittest.skipUnless(os.environ.get('ORACLE_EXPORT_IDENTITY_CONFIG'),
                     'set ORACLE_EXPORT_IDENTITY_CONFIG for an empty quoted Oracle owner')
class ExportOwnerIdentityOracleTests(unittest.TestCase):
    def test_quoted_owner_and_catalog_shadow_survive_default_schema_change(self):
        import oracledb
        config_path=Path(os.environ['ORACLE_EXPORT_IDENTITY_CONFIG'])
        config=json.loads(config_path.read_text());password=Path(config['password_file'])
        if not password.is_absolute():password=config_path.parent/password
        real_connect=oracledb.connect
        connection=real_connect(user=config['user'],password=password.read_text().strip(),
                                host=config['host'],port=config['port'],service_name=config['service_name'])
        self.addCleanup(connection.close);connection.call_timeout=30000
        with connection.cursor() as cursor:
            cursor.execute('SELECT USER FROM SYS.DUAL');owner=cursor.fetchone()[0]
            self.assertNotIn(owner.upper(),{'SYS','SYSTEM','PDBADMIN'})
            self.assertNotEqual(owner,owner.upper(),'fixture needs an explicitly quoted mixed-case owner')
            cursor.execute('SELECT COUNT(*) FROM SYS.USER_OBJECTS')
            self.assertEqual(cursor.fetchone(),(0,),'fixture requires an empty disposable owner')
            names=['BICDB_OWNER_DATA','USER_TABLES'];created=[]
            table=lambda name:target.quote(owner)+'.'+target.quote(name)
            try:
                for name,payload in zip(names,['owner-data','shadow-data']):
                    cursor.execute('CREATE TABLE '+table(name)+'(id NUMBER(10),payload VARCHAR2(30))')
                    created.append(name)
                    cursor.execute('INSERT INTO '+table(name)+' VALUES(7,:1)',[payload])
                connection.commit()
                deadline=time.monotonic()+10
                while True:
                    connection.rollback()
                    try:
                        cursor.execute('SET TRANSACTION READ ONLY')
                        for name in names:
                            cursor.execute('SELECT COUNT(*) FROM '+table(name));self.assertEqual(cursor.fetchone(),(1,))
                        break
                    except oracledb.DatabaseError as error:
                        if error.args[0].code!=1466 or time.monotonic()>=deadline:raise
                        time.sleep(.1)
                    finally:connection.rollback()
                opened=[]
                def changed_default_schema(**kwargs):
                    con=real_connect(**kwargs)
                    try:
                        with con.cursor() as control:
                            control.execute('ALTER SESSION SET CURRENT_SCHEMA=SYS')
                            control.execute("SELECT SYS_CONTEXT('USERENV','SESSION_USER'),SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM SYS.DUAL")
                            self.assertEqual(control.fetchone(),(owner,'SYS'))
                    except BaseException:
                        con.close();raise
                    opened.append(con);return con
                try:
                    with tempfile.TemporaryDirectory(prefix='bicdb-export-identity-') as tmp:
                        directory=Path(tmp)/'export'
                        with patch.object(oracledb,'connect',side_effect=changed_default_schema):
                            summary=target.export(config_path,directory)
                        manifest=json.loads((directory/'manifest.json').read_text())
                        self.assertEqual(summary['tables'],2);self.assertEqual(summary['rows'],2)
                        self.assertEqual(manifest['source_schema'],owner)
                        self.assertEqual(manifest['source_identity']['initial_current_schema'],'SYS')
                        self.assertEqual(manifest['source_identity']['read_schema'],owner)
                        self.assertFalse(manifest['cdc_resume_position'])
                        self.assertEqual({t['name'] for t in manifest['tables']},set(names))
                        for item in manifest['tables']:
                            self.assertEqual(item['columns'],['ID','PAYLOAD'])
                            values=[json.loads(line) for line in (directory/item['file']).read_text().splitlines()]
                            self.assertEqual(len(values),1)
                            self.assertEqual(values[0][1],'owner-data' if item['name']==names[0] else 'shadow-data')
                finally:
                    for con in opened:
                        try:con.close()
                        except oracledb.Error:pass
            finally:
                connection.rollback()
                errors=[]
                for name in reversed(created):
                    try:cursor.execute('DROP TABLE '+table(name)+' PURGE')
                    except oracledb.Error as error:errors.append(error.args[0].code)
                self.assertEqual(errors,[],'owned export fixture cleanup failed')
                cursor.execute('SELECT COUNT(*) FROM SYS.USER_OBJECTS');self.assertEqual(cursor.fetchone(),(0,))


if __name__=='__main__':unittest.main()
