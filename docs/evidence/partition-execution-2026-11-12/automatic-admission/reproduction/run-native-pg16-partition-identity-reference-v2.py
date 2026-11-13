import subprocess,pathlib,time,json,uuid
out=pathlib.Path('/root/.local/state/bicdb/typed-compare/native-pg16-partition-identity-reference-v2');out.mkdir(exist_ok=False)
name='bicdb-partition-identity-'+uuid.uuid4().hex[:10]
image=subprocess.check_output(['docker','image','inspect','postgres:16','--format','{{.Id}}'],text=True).strip()
sql="""\\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA partition_identity_reference;
CREATE TABLE partition_identity_reference.p(k INT,payload TEXT,CONSTRAINT parent_primary PRIMARY KEY(k),CONSTRAINT parent_unique UNIQUE(k,payload)) PARTITION BY RANGE(k);
CREATE TABLE partition_identity_reference.low PARTITION OF partition_identity_reference.p FOR VALUES FROM(0) TO(10);
SELECT rel.relname,c.contype,c.conname,idx.relname AS index_name FROM pg_constraint c JOIN pg_class rel ON rel.oid=c.conrelid JOIN pg_namespace ns ON ns.oid=rel.relnamespace LEFT JOIN pg_class idx ON idx.oid=c.conindid WHERE ns.nspname='partition_identity_reference' ORDER BY rel.relname,c.contype,c.conname;
CREATE TABLE partition_identity_reference.implicit_id(id INT,part INT) PARTITION BY RANGE(part);
CREATE TABLE partition_identity_reference.implicit_low PARTITION OF partition_identity_reference.implicit_id FOR VALUES FROM(0) TO(10);
INSERT INTO partition_identity_reference.implicit_id VALUES(1,2),(1,3);
SELECT id,part FROM partition_identity_reference.implicit_id ORDER BY part;
SELECT a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname='partition_identity_reference' AND c.relname='implicit_id' AND a.attname='id';
ROLLBACK;
"""
(out/'control.sql').write_text(sql)
started=time.time();record={'status':'running','container':name,'image':image,'sql':'control.sql','started_unix':started}
try:
 subprocess.run(['docker','run','--detach','--name',name,'--network','none','--memory','384m','--memory-swap','384m','--cpus','1','--tmpfs','/var/lib/postgresql/data:rw,size=256m','--tmpfs','/var/run/postgresql:rw,size=16m','--tmpfs','/tmp:rw,size=16m','-e','POSTGRES_HOST_AUTH_METHOD=trust','postgres:16','-c','shared_buffers=16MB'],check=True,capture_output=True)
 for _ in range(200):
  ready=subprocess.run(['docker','exec','--user','postgres',name,'pg_isready'],capture_output=True)
  logs=subprocess.run(['docker','logs',name],text=True,capture_output=True)
  if ready.returncode==0 and 'PostgreSQL init process complete; ready for start up.' in logs.stdout:break
  time.sleep(.1)
 else:raise RuntimeError('owned PostgreSQL reference did not become ready')
 version=subprocess.check_output(['docker','exec','--user','postgres',name,'psql','-XAt','-c','SELECT version()'],text=True)
 result=subprocess.run(['docker','exec','-i','--user','postgres',name,'psql','-XAt','--csv'],input=sql,text=True,capture_output=True)
 (out/'stdout').write_text(result.stdout);(out/'stderr').write_text(result.stderr)
 record.update(status='reference_passed' if result.returncode==0 else 'reference_failed',exit=result.returncode,version=version.strip(),finished_unix=time.time(),scope='Actual PG16 owned384MiB/no-network/tmpfs reference; rolledback scratch schema. PK/UNIQUE child constraint+index identities, and visible id without authored PK remains nullable/nonunique in partitioned parent. No application modifications.')
except BaseException as error:
 record.update(status='reference_failed',error=repr(error),finished_unix=time.time())
 raise
finally:
 logs=subprocess.run(['docker','logs',name],text=True,capture_output=True);(out/'container.stdout').write_text(logs.stdout);(out/'container.stderr').write_text(logs.stderr)
 removed=subprocess.run(['docker','rm','--force',name],text=True,capture_output=True);record.update(container_removed=removed.returncode==0,cleanup_stdout=removed.stdout,cleanup_stderr=removed.stderr);(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record));assert record.get('status')=='reference_passed',record
