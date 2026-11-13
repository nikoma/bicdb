import pathlib,subprocess,uuid,time,json
root=pathlib.Path('/root/.local/state/bicdb/typed-compare/native-pg16-partition-trigger-lifecycle-reference');root.mkdir(exist_ok=True);schema='root_part_lifecycle_'+uuid.uuid4().hex[:12]
sql=f'''\\set ON_ERROR_STOP on
BEGIN;CREATE SCHEMA {schema};SET LOCAL search_path={schema};
CREATE TABLE p(k INT PRIMARY KEY) PARTITION BY RANGE(k);
CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
INSERT INTO p VALUES(1);
CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW;END$$;
CREATE TABLE controls(label TEXT,state TEXT,message TEXT);
DO $$BEGIN BEGIN CREATE CONSTRAINT TRIGGER c AFTER INSERT ON p DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f(); INSERT INTO controls VALUES('constraint_parent','00000','accepted');EXCEPTION WHEN OTHERS THEN INSERT INTO controls VALUES('constraint_parent',SQLSTATE,SQLERRM);END;END$$;
CREATE CONSTRAINT TRIGGER child_c AFTER INSERT ON low DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f();
CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f();
DO $$BEGIN BEGIN DROP FUNCTION f() RESTRICT; INSERT INTO controls VALUES('function_restrict','00000','accepted');EXCEPTION WHEN OTHERS THEN INSERT INTO controls VALUES('function_restrict',SQLSTATE,SQLERRM);END;END$$;
DO $$BEGIN BEGIN DROP FUNCTION f(),missing_function(); INSERT INTO controls VALUES('multi_missing_function','00000','accepted');EXCEPTION WHEN OTHERS THEN INSERT INTO controls VALUES('multi_missing_function',SQLSTATE,SQLERRM);END;END$$;
SELECT 'controls',jsonb_agg(jsonb_build_object('label',label,'state',state,'message',message) ORDER BY label) FROM controls;
SELECT 'before_cascade',jsonb_agg(jsonb_build_object('relation',c.relname,'trigger',t.tgname,'parent',t.tgparentid<>0) ORDER BY c.relname,t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='{schema}'::regnamespace AND NOT t.tgisinternal;
SAVEPOINT cascade_undo;
DROP FUNCTION f() CASCADE;
SELECT 'cascade_trigger_count',COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='{schema}'::regnamespace AND NOT t.tgisinternal;
ROLLBACK TO cascade_undo;
SELECT 'after_rollback',jsonb_agg(jsonb_build_object('relation',c.relname,'trigger',t.tgname,'parent',t.tgparentid<>0) ORDER BY c.relname,t.tgname) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace='{schema}'::regnamespace AND NOT t.tgisinternal;
SELECT 'ack',k FROM p;
ROLLBACK;
SELECT 'scratch_schema_absent',NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='{schema}');
''';(root/'control.sql').write_text(sql)
image=subprocess.check_output(['docker','inspect','--format','{{.Image}}','bicdb-pgprov-ref'],text=True).strip();name='root-part-trigger-lifecycle-'+uuid.uuid4().hex[:10];started=time.time()
try:
 subprocess.run(['docker','run','--detach','--name',name,'--network','none','--memory','512m','--memory-swap','512m','--cpus','1','--tmpfs','/var/lib/postgresql/data:rw,size=256m','-e','POSTGRES_HOST_AUTH_METHOD=trust',image],check=True,stdout=subprocess.DEVNULL)
 for _ in range(60):
  if subprocess.run(['docker','exec',name,'pg_isready','-U','postgres'],capture_output=True).returncode==0:break
  time.sleep(.5)
 else:raise RuntimeError('owned ephemeral PG did not start')
 result=subprocess.run(['docker','exec','-i',name,'psql','-X','-A','-t','-F','|','-U','postgres'],input=sql,text=True,capture_output=True);(root/'stdout').write_text(result.stdout);(root/'stderr').write_text(result.stderr);assert result.returncode==0,result.stderr
 data={}
 for line in result.stdout.splitlines():
  if '|' not in line:continue
  label,value=line.split('|',1)
  if label in ['controls','before_cascade','after_rollback']:data[label]=json.loads(value)
  elif label in ['ack','cascade_trigger_count']:data[label]=int(value)
  elif label=='scratch_schema_absent':assert value=='t'
 assert data['before_cascade']==data['after_rollback'] and data['cascade_trigger_count']==0 and data['ack']==1,data
 version=subprocess.check_output(['docker','exec',name,'psql','-X','-A','-t','-U','postgres','-c','SELECT version()'],text=True).strip();receipt={'status':'native_pg_control_passed','version':version,'image_id':image,'started_unix':started,'finished_unix':time.time(),'scope':'ActualPG16 constrainttriggerparentacceptance, leafacceptance, DROPfunctiontriggerdependency/cascade/rollback; temporaryisolatedtmpfs event/catalogsemantics only. Existingdatauntouched; no durabilityclaim.'};(root/'result.json').write_text(json.dumps(receipt,indent=2)+'\n');(root/'catalog.json').write_text(json.dumps(data,indent=2)+'\n');print(json.dumps(data))
finally:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
