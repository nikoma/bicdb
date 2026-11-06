"""Source planning, journal safety and optional native migration acceptance."""
import copy
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

import foreign_keys as target


def manifest(schema='A"B'):
    tables = [dict(name=name, columns=columns, rows=rows) for name, columns, rows in [
        ('P', ['ID', 'ALT'], 1), ('C', ['ID', 'PID', 'NOTE'], 1),
        ('Q', ['ID', 'PID'], 0), ('U', ['ID', 'CODE'], 0)]]
    constraints = []
    def key(table, name, kind, columns, **extra):
        return dict(table=table, name=name, kind=kind, columns=columns,
                    status='ENABLED', validated='VALIDATED',
                    deferrable='NOT DEFERRABLE', deferred='IMMEDIATE', **extra)
    for table in tables:
        constraints.append(key(table['name'], table['name'] + '_PK', 'P', ['ID']))
    constraints.append(key('P', 'P_ALT_U', 'U', ['ALT']))
    for table, name, column, parent, action, validated, deferred in [
        ('C', 'C"FK', 'PID', 'P_PK', 'CASCADE', 'NOT VALIDATED', True),
        ('Q', 'Q_FK', 'PID', 'P_PK', 'SET NULL', 'VALIDATED', True),
        ('U', 'U_FK', 'CODE', 'P_ALT_U', 'NO ACTION', 'VALIDATED', False)]:
        c=key(table, name, 'R', [column], referenced_owner=schema,
              referenced_constraint=parent, delete_rule=action)
        c.update(validated=validated, deferrable='DEFERRABLE' if deferred else 'NOT DEFERRABLE',
                 deferred='DEFERRED' if deferred else 'IMMEDIATE')
        constraints.append(c)
    return dict(source_schema=schema, tables=tables, constraints=constraints)


class ForeignKeyPlanTests(unittest.TestCase):
    def test_source_states_parent_keys_and_quoted_names_are_retained(self):
        plan=target.declarations(manifest())
        self.assertEqual(len(plan),3)
        self.assertEqual(plan[2]['parent_columns'],['ALT'])
        self.assertEqual(plan[2]['parent_kind'],'U')
        ddl=target.ddl('A"B',plan[0])
        self.assertIn('ALTER TABLE "A""B"."C" ADD CONSTRAINT "C""FK"',ddl)
        self.assertIn('ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID',ddl)
        self.assertNotIn('NOT VALID',target.ddl('A"B',plan[1]))
        self.assertIn('NOT DEFERRABLE',target.ddl('A"B',plan[2]))

    def test_unsupported_or_ambiguous_declarations_fail_before_connections(self):
        cases=[('status','DISABLED'),('validated','OTHER'),('deferrable','OTHER'),
               ('deferred','OTHER'),('referenced_owner','EXTERNAL'),
               ('referenced_constraint','MISSING'),('delete_rule','SET DEFAULT'),
               ('columns',['MISSING']),('columns',['PID','PID'])]
        for field,value in cases:
            m=manifest();m['constraints'][-3][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):target.declarations(m)
        for change in ['table','constraint','parent_state','parent_timing','parent_validation','arity']:
            m=manifest()
            if change=='table':m['tables']*=2
            elif change=='constraint':m['constraints'].append(copy.deepcopy(m['constraints'][0]))
            elif change=='parent_state':m['constraints'][0]['status']='DISABLED'
            elif change=='parent_timing':m['constraints'][0]['deferrable']='DEFERRABLE'
            elif change=='parent_validation':m['constraints'][0]['validated']='NOT VALIDATED'
            else:m['constraints'][0]['columns']=['ID','ALT']
            with self.subTest(change=change),self.assertRaises(ValueError):target.declarations(m)

    def test_resume_requires_identity_and_an_exact_completed_prefix(self):
        plan=target.declarations(manifest());identity=dict(phase='foreign-keys',target_sha256='target',manifest_sha256='source')
        self.assertEqual(target.validate_journal(dict(identity,completed=plan[:2]),identity,plan),plan[:2])
        for previous in [dict(identity,target_sha256='other',completed=[]),dict(identity,completed=plan[::-1]),
                         dict(identity,completed=plan[:1]*2),dict(identity,completed=[dict(plan[0],validated=True)]),
                         dict(identity,completed=None)]:
            with self.subTest(previous=previous),self.assertRaises(ValueError):target.validate_journal(previous,identity,plan)

    def test_failed_capability_probe_rolls_back_and_attempts_every_owned_drop(self):
        class Cursor:
            def __init__(self):self.sql=[]
            def execute(self,sql,*args,**kwargs):
                self.sql.append(sql)
                if sql.startswith('DROP TABLE') and sql.endswith('_C"'):
                    raise RuntimeError('injected child cleanup failure')
            def fetchone(self):return (999,)
        cursor=Cursor()
        with self.assertRaisesRegex(RuntimeError,'cleanup failure'):target.probe_support(cursor,'A"B')
        drops=[sql for sql in cursor.sql if sql.startswith('DROP TABLE')]
        self.assertEqual(len(drops),2);self.assertTrue(drops[-1].endswith('_P"'))
        self.assertIn('ROLLBACK',cursor.sql)

    def test_apply_refuses_active_writers_before_manifest_or_connection_access(self):
        with self.assertRaisesRegex(ValueError,'writers-quiesced'):
            target.migrate(SimpleNamespace(check_only=False,writers_quiesced=False))


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),'set BICDB_ORACLE_SERVER for native migration acceptance')
class ForeignKeyMigrationIntegrationTests(unittest.TestCase):
    profile = 'oracle-preview-v1'

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='bicdb-fk-migration-test-');self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1]
        self.log=(self.root/'server.log').open('wb');self.addCleanup(self.log.close)
        self.start_server();self.addCleanup(self.stop_server);self.connect_server()

    def start_server(self):
        self.child=subprocess.Popen([os.environ['BICDB_ORACLE_SERVER'],str(self.root/'db'),str(self.port),self.profile,'8','4'],stdout=self.log,stderr=subprocess.STDOUT)

    def stop_server(self):
        if self.child.poll() is None:
            self.child.send_signal(signal.SIGINT)
            try:self.child.wait(timeout=15)
            except subprocess.TimeoutExpired:self.child.kill();self.child.wait()
        if self.child.returncode!=0:raise RuntimeError('fixture server did not stop cleanly')

    def connect_server(self):
        import psycopg
        self.dsn=f'host=127.0.0.1 port={self.port} user=bicdb dbname=bicdb'
        for _ in range(300):
            if self.child.poll() is not None:raise RuntimeError('fixture startup failed: '+(self.root/'server.log').read_text())
            try:
                self.conn=psycopg.connect(self.dsn,autocommit=True,options='-c bicdb.sql_profile='+self.profile,connect_timeout=1)
                self.addCleanup(self.conn.close);return
            except psycopg.OperationalError as error:
                if error.sqlstate is not None:raise
                time.sleep(.1)
        raise RuntimeError('fixture startup timed out')

    def restart_server(self):
        self.conn.close();self.stop_server();self.start_server();self.connect_server()

    def setup_schema(self):
        schema='FK_'+str(time.time_ns())+'"Q';m=manifest(schema);q=target.quote
        with self.conn.cursor() as c:
            c.execute('CREATE SCHEMA '+q(schema))
            for table,body in [('P','"ID" INTEGER CONSTRAINT "P_PK" PRIMARY KEY,"ALT" INTEGER CONSTRAINT "P_ALT_U" UNIQUE'),
                               ('C','"ID" INTEGER CONSTRAINT "C_PK" PRIMARY KEY,"PID" INTEGER,"NOTE" INTEGER'),
                               ('Q','"ID" INTEGER CONSTRAINT "Q_PK" PRIMARY KEY,"PID" INTEGER'),
                               ('U','"ID" INTEGER CONSTRAINT "U_PK" PRIMARY KEY,"CODE" INTEGER')]:
                c.execute('CREATE TABLE '+q(schema)+'.'+q(table)+'('+body+')')
            c.execute('INSERT INTO '+q(schema)+'."P" VALUES(1,10)')
            c.execute('INSERT INTO '+q(schema)+'."C" VALUES(1,999,0)')
        root=self.root/schema;root.mkdir();(root/'manifest.json').write_text(json.dumps(m))
        args=SimpleNamespace(snapshot=root,report=root/'report.json',pg_dsn_env='BICDB_FK_TEST_DSN',resume=False,check_only=True,writers_quiesced=False)
        return m,args

    def migrate(self,args):
        with patch.dict(os.environ,BICDB_FK_TEST_DSN=self.dsn):return target.migrate(args)

    def test_preflight_apply_restart_resume_and_enforcement_states(self):
        m,args=self.setup_schema();schema=m['source_schema'];q=target.quote
        r=self.migrate(args);self.assertTrue(r['success']);self.assertFalse(r['migration_completed']);self.assertFalse(r['capability_verified']);self.assertEqual(r['completed'],[])
        with self.conn.cursor() as c:
            state=target.catalog(c,schema);self.assertFalse(any(row['kind']=='f' for row in state['constraints'].values()))
        args.check_only=False;args.writers_quiesced=True;r=self.migrate(args)
        self.assertTrue(r['migration_completed']);self.assertTrue(r['capability_verified']);self.assertEqual(len(r['completed']),3)
        self.assertTrue(all(v['before']==v['after'] for v in r['table_fingerprints'].values()))
        self.restart_server();args.resume=True;self.migrate(args)
        with self.conn.cursor() as c:
            c.execute('BEGIN')
            c.execute('INSERT INTO '+q(schema)+'."Q" VALUES(1,2)')
            c.execute('INSERT INTO '+q(schema)+'."P" VALUES(2,20)')
            c.execute('INSERT INTO '+q(schema)+'."C" VALUES(2,2,0)')
            c.execute('INSERT INTO '+q(schema)+'."U" VALUES(1,10)')
            c.execute('SAVEPOINT before_bad_unique_reference')
            import psycopg
            with self.assertRaises(psycopg.Error) as caught:
                c.execute('INSERT INTO '+q(schema)+'."U" VALUES(2,999)')
            self.assertEqual(caught.exception.sqlstate,'23503')
            c.execute('ROLLBACK TO SAVEPOINT before_bad_unique_reference')
            c.execute('SET CONSTRAINTS ALL IMMEDIATE')
            c.execute('DELETE FROM '+q(schema)+'."P" WHERE "ID"=2')
            c.execute('SELECT "PID" FROM '+q(schema)+'."Q"');self.assertEqual(c.fetchone(),(None,))
            c.execute('SELECT COUNT(*) FROM '+q(schema)+'."C" WHERE "ID"=2');self.assertEqual(c.fetchone(),(0,))
            c.execute('ROLLBACK')
            c.execute('ALTER TABLE '+q(schema)+'."C" DISABLE TRIGGER ALL')
        with self.assertRaisesRegex(ValueError,'referential checks'):self.migrate(args)
        with self.conn.cursor() as c:c.execute('ALTER TABLE '+q(schema)+'."C" ENABLE TRIGGER ALL')
        self.migrate(args)
        saved=args.report.read_bytes();original=(args.snapshot/'manifest.json').read_bytes()
        (args.snapshot/'manifest.json').write_text(json.dumps(dict(m,changed=True)))
        with self.assertRaisesRegex(ValueError,'journal does not match'):self.migrate(args)
        self.assertEqual(args.report.read_bytes(),saved);(args.snapshot/'manifest.json').write_bytes(original)
        with self.conn.cursor() as c:c.execute('UPDATE '+q(schema)+'."C" SET "NOTE"=1')
        with self.assertRaisesRegex(ValueError,'rows changed'):self.migrate(args)
        self.assertFalse(json.loads(args.report.read_text())['success'])

    def test_matching_unjournaled_constraints_are_adopted_but_changed_completed_ones_refused(self):
        m,args=self.setup_schema();plan=target.declarations(m)
        with self.conn.cursor() as c:c.execute(target.ddl(m['source_schema'],plan[0]))
        args.check_only=False;args.writers_quiesced=True;r=self.migrate(args);self.assertEqual(r['completed'],plan)
        r['completed']=r['completed'][:1];args.report.write_text(json.dumps(r));args.resume=True
        self.assertEqual(self.migrate(args)['completed'],plan)
        with self.conn.cursor() as c:c.execute('ALTER TABLE '+target.quote(m['source_schema'])+'."C" DROP CONSTRAINT '+target.quote(plan[0]['name']))
        with self.assertRaisesRegex(ValueError,'completed target foreign key'):self.migrate(args)

    def test_resume_uses_whole_catalog_validation_and_reads_back_pending_installs(self):
        import psycopg
        m,args=self.setup_schema();plan=target.declarations(m)
        args.check_only=False;args.writers_quiesced=True;self.migrate(args);args.resume=True
        statements=[];real_connect=psycopg.connect
        class Cursor:
            def __init__(self,real):self.real=real
            def __getattr__(self,name):return getattr(self.real,name)
            def __enter__(self):self.real.__enter__();return self
            def __exit__(self,*args):return self.real.__exit__(*args)
            def __iter__(self):return iter(self.real)
            def execute(self,sql,*args,**kwargs):
                statements.append(sql);return self.real.execute(sql,*args,**kwargs)
        class Connection:
            def __init__(self,real):self.real=real
            def __getattr__(self,name):return getattr(self.real,name)
            def __enter__(self):self.real.__enter__();return self
            def __exit__(self,*args):return self.real.__exit__(*args)
            def cursor(self,*args,**kwargs):return Cursor(self.real.cursor(*args,**kwargs))
        def run():
            statements.clear()
            with patch.object(psycopg,'connect',side_effect=lambda *a,**kw:Connection(real_connect(*a,**kw))):
                return self.migrate(args)
        result=run();self.assertEqual(result['completed'],plan)
        scans=[q for q in statements if q.startswith(target.CONSTRAINT_SELECT)]
        self.assertEqual(len(scans),1,'completed prefixes must use the complete catalog inventory')
        self.assertIn('WHERE "connamespace"=',scans[0])
        # Simulate an interrupted journal with one pending key missing and one
        # installed but unjournaled. Both still require individual readback.
        result['completed']=result['completed'][:1];args.report.write_text(json.dumps(result))
        with self.conn.cursor() as c:
            c.execute('ALTER TABLE '+target.quote(m['source_schema'])+'.'+target.quote(plan[1]['table'])+' DROP CONSTRAINT '+target.quote(plan[1]['name']))
        result=run();self.assertEqual(result['completed'],plan)
        point_reads=[q for q in statements if q.startswith(target.CONSTRAINT_SELECT) and 'WHERE "conrelid"=' in q]
        self.assertEqual(len(point_reads),3,'missing keys need pre/post readback; unjournaled existing keys need verification')
        self.assertIn(target.ddl(m['source_schema'],plan[1]),statements)
        self.assertTrue(all(value['before']==value['after'] for value in result['table_fingerprints'].values()))
        # A completed definition changed before resume must still stop the run.
        with self.conn.cursor() as c:
            c.execute('ALTER TABLE '+target.quote(m['source_schema'])+'.'+target.quote(plan[0]['table'])+' DROP CONSTRAINT '+target.quote(plan[0]['name']))
        with self.assertRaisesRegex(ValueError,'completed target foreign key'):run()
        self.assertFalse(any(q.startswith('ALTER TABLE') for q in statements))

    def test_mismatched_existing_relationship_is_refused_without_replacement(self):
        m,args=self.setup_schema();item=target.declarations(m)[0]
        with self.conn.cursor() as c:c.execute(target.ddl(m['source_schema'],dict(item,delete_rule='NO ACTION')))
        with self.assertRaisesRegex(ValueError,'differs from source'):self.migrate(args)
        with self.conn.cursor() as c:
            state=target.catalog(c,m['source_schema']);self.assertEqual(state['constraints'][(item['table'],item['name'])]['delete_rule'],'NO ACTION')

    def test_composite_unique_parent_and_partial_null_reference_are_preserved(self):
        m,args=self.setup_schema();schema=m['source_schema'];q=target.quote
        parent=copy.deepcopy(m['constraints'][4]);parent.update(name='P_BOTH_U',columns=['ID','ALT']);m['constraints'].append(parent)
        child=copy.deepcopy(m['constraints'][0]);child.update(table='T',name='T_PK');m['constraints'].append(child)
        fk=copy.deepcopy(next(c for c in m['constraints'] if c['kind']=='R'));fk.update(table='T',name='T_FK',columns=['PID','CODE'],referenced_constraint='P_BOTH_U',validated='VALIDATED',deferrable='DEFERRABLE',deferred='DEFERRED');m['constraints'].append(fk)
        m['tables'].append(dict(name='T',columns=['ID','PID','CODE'],rows=1))
        with self.conn.cursor() as c:
            c.execute('ALTER TABLE '+q(schema)+'."P" ADD CONSTRAINT "P_BOTH_U" UNIQUE("ID","ALT")')
            c.execute('CREATE TABLE '+q(schema)+'."T"("ID" INTEGER CONSTRAINT "T_PK" PRIMARY KEY,"PID" INTEGER,"CODE" INTEGER)')
            c.execute('INSERT INTO '+q(schema)+'."T" VALUES(1,999,NULL)')
        (args.snapshot/'manifest.json').write_text(json.dumps(m));args.check_only=False;args.writers_quiesced=True
        self.assertEqual(len(self.migrate(args)['completed']),4)
        with self.conn.cursor() as c:
            c.execute('SELECT "PID","CODE" FROM '+q(schema)+'."T"');self.assertEqual(c.fetchone(),(999,None))

    def test_child_without_explicit_primary_key_has_correct_catalog_column_positions(self):
        m,args=self.setup_schema();schema=m['source_schema'];q=target.quote
        fk=copy.deepcopy(next(c for c in m['constraints'] if c['kind']=='R'));fk.update(table='N',name='N_FK',validated='VALIDATED');m['constraints'].append(fk)
        m['tables'].append(dict(name='N',columns=['PID','NOTE'],rows=0))
        with self.conn.cursor() as c:c.execute('CREATE TABLE '+q(schema)+'."N"("PID" INTEGER,"NOTE" INTEGER)')
        (args.snapshot/'manifest.json').write_text(json.dumps(m));args.check_only=False;args.writers_quiesced=True
        self.assertEqual(len(self.migrate(args)['completed']),4)

    def test_attribute_inventory_excludes_other_namespaces(self):
        m,args=self.setup_schema();other='OTHER_'+str(time.time_ns());q=target.quote
        with self.conn.cursor() as c:
            c.execute('CREATE SCHEMA '+q(other))
            c.execute('CREATE TABLE '+q(other)+'."UNRELATED"("PRIVATE_COLUMN" INTEGER)')
            state=target.catalog(c,m['source_schema'])
        self.assertEqual(set(state['columns']),{'P','C','Q','U'})
        self.assertFalse(any(name=='PRIVATE_COLUMN' for name in state['attributes'].values()))
        self.assertTrue(self.migrate(args)['success'])

    def test_changed_target_projection_is_refused_before_probe_or_installation(self):
        m,args=self.setup_schema()
        with self.conn.cursor() as c:c.execute('ALTER TABLE '+target.quote(m['source_schema'])+'."C" ADD COLUMN "EXTRA" INTEGER')
        with self.assertRaisesRegex(ValueError,'projection differs'):self.migrate(args)
        with self.conn.cursor() as c:
            state=target.catalog(c,m['source_schema']);self.assertFalse(any(row['kind']=='f' for row in state['constraints'].values()))

    def test_validated_source_relationship_rejects_an_existing_orphan_without_repair(self):
        m,args=self.setup_schema();args.check_only=False;args.writers_quiesced=True
        with self.conn.cursor() as c:c.execute('INSERT INTO '+target.quote(m['source_schema'])+'."Q" VALUES(1,999)')
        import psycopg
        with self.assertRaises(psycopg.Error) as caught:self.migrate(args)
        self.assertEqual(caught.exception.sqlstate,'23503')
        r=json.loads(args.report.read_text());self.assertFalse(r['success']);self.assertEqual(len(r['completed']),1)
        with self.conn.cursor() as c:
            c.execute('SELECT "PID" FROM '+target.quote(m['source_schema'])+'."Q"');self.assertEqual(c.fetchone(),(999,))


if __name__=='__main__':unittest.main()
