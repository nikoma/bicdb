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

    def test_apply_refuses_active_writers_before_manifest_or_connection_access(self):
        with self.assertRaisesRegex(ValueError,'writers-quiesced'):
            target.migrate(SimpleNamespace(check_only=False,writers_quiesced=False))


@unittest.skipUnless(os.environ.get('BICDB_ORACLE_SERVER'),'set BICDB_ORACLE_SERVER for native migration acceptance')
class ForeignKeyMigrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='bicdb-fk-migration-test-');cls.addClassCleanup(cls.temp.cleanup)
        cls.root=Path(cls.temp.name)
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));cls.port=sock.getsockname()[1]
        cls.log=(cls.root/'server.log').open('wb');cls.addClassCleanup(cls.log.close)
        cls.start_server();cls.addClassCleanup(cls.stop_server);cls.connect_server()

    @classmethod
    def start_server(cls):
        cls.child=subprocess.Popen([os.environ['BICDB_ORACLE_SERVER'],str(cls.root/'db'),str(cls.port),'oracle-preview-v1','8','4'],stdout=cls.log,stderr=subprocess.STDOUT)

    @classmethod
    def stop_server(cls):
        if cls.child.poll() is None:
            cls.child.send_signal(signal.SIGINT)
            try:cls.child.wait(timeout=15)
            except subprocess.TimeoutExpired:cls.child.kill();cls.child.wait()
        if cls.child.returncode!=0:raise RuntimeError('fixture server did not stop cleanly')

    @classmethod
    def connect_server(cls):
        import psycopg
        cls.dsn=f'host=127.0.0.1 port={cls.port} user=bicdb dbname=bicdb'
        for _ in range(300):
            if cls.child.poll() is not None:raise RuntimeError('fixture startup failed: '+(cls.root/'server.log').read_text())
            try:
                cls.conn=psycopg.connect(cls.dsn,autocommit=True,options='-c bicdb.sql_profile=oracle-preview-v1',connect_timeout=1)
                cls.addClassCleanup(cls.conn.close);return
            except psycopg.OperationalError as error:
                if error.sqlstate is not None:raise
                time.sleep(.1)
        raise RuntimeError('fixture startup timed out')

    @classmethod
    def restart_server(cls):
        cls.conn.close();cls.stop_server();cls.start_server();cls.connect_server()

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

    def test_mismatched_existing_relationship_is_refused_without_replacement(self):
        m,args=self.setup_schema();item=target.declarations(m)[0]
        with self.conn.cursor() as c:c.execute(target.ddl(m['source_schema'],dict(item,delete_rule='NO ACTION')))
        with self.assertRaisesRegex(ValueError,'differs from source'):self.migrate(args)
        with self.conn.cursor() as c:
            state=target.catalog(c,m['source_schema']);self.assertEqual(state['constraints'][(item['table'],item['name'])]['delete_rule'],'NO ACTION')

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
