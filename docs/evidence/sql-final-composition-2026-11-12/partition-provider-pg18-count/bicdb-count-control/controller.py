import pathlib,json,subprocess,socket,time,signal,os,hashlib,resource
import psycopg
resource.setrlimit(resource.RLIMIT_CORE,(0,0));r=pathlib.Path('/root/bicdb-durability-s3');out=r/'partition-provider-count-82a-v2';out.mkdir(exist_ok=False);binary=r/'qualification-artifacts-debug/82a8311156e2293a92e7cf9c1ffd70d1df92d57a/oracle_server';assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()=='9d66171db1afa6a5d3bf5257c7918bf3c59b44395812d56c74a33c6ddc762f7c'
s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close();env={k:v for k,v in os.environ.items() if k not in ['BICDB_PAGE_WAL_UNLOCKED_SYNC','BICDB_PAGE_DELTAS']};env.update(BICDB_COMMIT_DURABILITY='all-logs',BICDB_SYNC_OUTBOX='off');p=subprocess.Popen([str(binary),str(out/'db'),str(port),'postgres','8','4','server_paged'],env=env,stdout=(out/'server.log').open('xb'),stderr=subprocess.STDOUT,start_new_session=True)
record={'source':'82a8311156e2293a92e7cf9c1ffd70d1df92d57a','status':'running','scope':'Freshownedstore; exactproviderparentconstraintCOUNT; no originalbaseline/testbytesmodified, no applicationacceptanceclaim. V1failedharness strippedSQLquotes; itsownedserverstopped, failurepreserved.','pid':p.pid,'queries':[]}
try:
 deadline=time.monotonic()+30
 while True:
  assert p.poll() is None
  try:c=psycopg.connect(host='127.0.0.1',port=port,user='postgres',dbname='postgres',autocommit=True,connect_timeout=2);break
  except psycopg.OperationalError:
   assert time.monotonic()<deadline;time.sleep(.1)
 with c:
  for q in ['CREATE SCHEMA adempiere','SET search_path TO adempiere, public','CREATE TABLE parent(k INT NOT NULL,v TEXT NOT NULL) PARTITION BY LIST(k)',"SELECT current_setting('server_version'),bicdb_version()","SELECT conname,contype::text FROM pg_constraint WHERE conrelid='parent'::regclass ORDER BY conname","SELECT COUNT(*) FROM pg_constraint c WHERE conrelid=LOWER('parent')::regclass","SELECT COUNT(*) FROM pg_constraint c WHERE conrelid=LOWER('parent')::regclass AND contype<>'n'"]:
   cur=c.execute(q);rows=cur.fetchall() if cur.description else [];record['queries'].append({'sql':q,'rows':rows})
 record['status']='observed'
except BaseException as error:record.update(status='probe_failed',error=repr(error));raise
finally:
 if p.poll() is None:p.send_signal(signal.SIGINT)
 try:p.wait(timeout=15)
 except subprocess.TimeoutExpired:p.kill();p.wait()
 record['owned_server_stopped']=p.poll() is not None;(out/'result.json').write_text(json.dumps(record,indent=2,default=str)+'\n')
print(json.dumps(record,default=str))
