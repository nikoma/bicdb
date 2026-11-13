import pathlib,subprocess,json,hashlib,os,socket,time,signal
root=pathlib.Path('/root/bicdb-durability-s3/partition-old-store-fixtures-482-20261112');root.mkdir(exist_ok=False)
art=pathlib.Path('/root/bicdb-durability-s3/qualification-artifacts/f43583da4c34209e810c5e6a8f5925831b2949a1');build=json.loads((art/'manifest.json').read_text());binary=pathlib.Path(build['cli_binary']);server=pathlib.Path(build['canonical_binary'])
assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==build['cli_binary_sha256']
assert hashlib.file_digest(server.open('rb'),'sha256').hexdigest()==build['canonical_binary_sha256']
env=dict(os.environ,BICDB_COMMIT_DURABILITY='all-logs',BICDB_SYNC_OUTBOX='off',MALLOC_ARENA_MAX='2')
for key in ['BICDB_CONFIG','BICDB_COMMIT_SYNC_OUTSIDE_APPEND_LOCK','BICDB_PAGE_DELTA_REDO','BICDB_STORAGE_MODE']:
 env.pop(key,None)
record={'status':'running','source':build['source'],'source_version':build['source_version'],'binary':str(binary),'binary_sha256':build['cli_binary_sha256'],'started_unix':time.time(),'cases':[]}
def save():
 temp=root/'result.json.tmp';temp.write_text(json.dumps(record,indent=2)+'\n');os.replace(temp,root/'result.json')
def run(case,sql,acks=None):
 idx=len(case['commands']);result=subprocess.run([str(binary),'sql',str(pathlib.Path(case['directory'])/'db'),sql,'--json'],env=env,text=True,capture_output=True)
 item={'sql':sql,'exit':result.returncode,'stdout':result.stdout,'stderr':result.stderr};case['commands'].append(item);save()
 if result.returncode:raise RuntimeError(str(item))
 if acks:
  for row in acks:
   entry={'acknowledged_unix':time.time(),'source':build['source'],'binary_sha256':build['cli_binary_sha256'],'mode':case['mode'],'case':case['name'],'command_index':idx,**row}
   with (root/'ack-ledger.jsonl').open('a') as f:f.write(json.dumps(entry)+'\n');f.flush();os.fsync(f.fileno())
 return result.stdout
for mode in ['embedded_memory','server_paged']:
 for variant in ['parent_heap','bad_child_bound','independent_child_policy']:
  base=root/(mode+'-'+variant);base.mkdir();case={'mode':mode,'name':variant,'directory':str(base),'commands':[],'status':'running'};record['cases'].append(case);save()
  if mode=='server_paged':
   sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
   with (base/'initializer.log').open('wb') as output:
    child=subprocess.Popen([str(server),str(base/'db'),str(port),'postgres','4','1',mode],env=env,stdout=output,stderr=subprocess.STDOUT)
    try:
     for _ in range(200):
      if child.poll() is not None:raise RuntimeError((base/'initializer.log').read_text())
      try:
       with socket.create_connection(('127.0.0.1',port),timeout=.1):break
      except OSError:time.sleep(.05)
     else:raise RuntimeError('owned initializer did not become ready')
    finally:
     if child.poll() is None:child.send_signal(signal.SIGINT)
     try:child.wait(timeout=20)
     except subprocess.TimeoutExpired:child.kill();child.wait();raise
    assert child.returncode==0,child.returncode
  for sql in ["CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)","CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)","CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)"]:run(case,sql)
  if variant=='parent_heap':
   for table in ['p','low','high']:run(case,f'CREATE INDEX {table}_payload_idx ON {table}(payload)')
   run(case,"INSERT INTO p VALUES(1,'parent-low-ack'),(11,'parent-high-ack')",[{'relation':'p','k':1,'payload':'parent-low-ack'},{'relation':'p','k':11,'payload':'parent-high-ack'}])
   run(case,"INSERT INTO low VALUES(2,'child-low-ack')",[{'relation':'low','k':2,'payload':'child-low-ack'}]);run(case,"INSERT INTO high VALUES(12,'child-high-ack')",[{'relation':'high','k':12,'payload':'child-high-ack'}])
  elif variant=='bad_child_bound':run(case,"INSERT INTO low VALUES(11,'out-of-bound-child-ack')",[{'relation':'low','k':11,'payload':'out-of-bound-child-ack'}])
  else:
   run(case,"INSERT INTO low VALUES(2,'low-policy-ack')",[{'relation':'low','k':2,'payload':'low-policy-ack'}]);run(case,"INSERT INTO high VALUES(11,'high-policy-ack')",[{'relation':'high','k':11,'payload':'high-policy-ack'}])
   for sql in ['CREATE ROLE legacy_reader','GRANT SELECT ON p,low,high TO legacy_reader','ALTER TABLE p ENABLE ROW LEVEL SECURITY','ALTER TABLE p FORCE ROW LEVEL SECURITY','CREATE POLICY parent_guard ON p FOR SELECT USING(k<10)','ALTER TABLE high ENABLE ROW LEVEL SECURITY','CREATE POLICY authored_child ON high FOR SELECT USING(k=11)']:run(case,sql)
   for table in ['p','low','high']:run(case,f'SET ROLE legacy_reader; SELECT k,payload FROM {table} ORDER BY k')
  for table in ['p','low','high']:run(case,f'SELECT k,payload FROM {table} ORDER BY k')
  # Both a second old-binary open and SQL catalog observations use genuine old
  # readers. No rows/catalog/generation files are manufactured or rewritten.
  run(case,'SELECT tablename,policyname,qual FROM pg_policies ORDER BY tablename,policyname')
  files={str(path.relative_to(base/'db')):{'bytes':path.stat().st_size,'sha256':hashlib.file_digest(path.open('rb'),'sha256').hexdigest()} for path in sorted((base/'db').rglob('*')) if path.is_file() and not path.is_symlink()}
  case.update(status='old_binary_fixture_closed',files=files,format=json.loads((base/'db/format.json').read_text()));save()
record.update(status='old_binary_fixtures_created',finished_unix=time.time(),scope='Six genuine oldmerged482CLI-created SQLpartitionstores; bothmodes; 14independentACKs expected total. Originalclosedstores immutable inputs to future copies; no fabricatedcatalog/index generations. No candidatequalification yet.');save();print(json.dumps({'status':record['status'],'root':str(root),'cases':len(record['cases']),'acknowledged_rows':sum(1 for _ in (root/'ack-ledger.jsonl').open())}))
