import hashlib,importlib.util,json,os,shutil,subprocess,time,sys
from pathlib import Path
P=Path('/tmp/bicdb-azure-run');E=Path(sys.argv[2]);S=Path('/tmp/bicdb-azure-integration/scripts/platform')
os.environ['PATH']=str(P/'bin')+':'+os.environ['PATH']
spec=importlib.util.spec_from_file_location('scale',S/'scale-cluster.py');scale=importlib.util.module_from_spec(spec);spec.loader.exec_module(scale)
r=json.loads(Path(sys.argv[1]).read_text());inventory=json.loads((P/'inventory.private.json').read_text());request=json.loads((P/'run-request.private.json').read_text());D=E/'diagnostic-databases.private';D.mkdir(mode=0o700,exist_ok=False);receipt=[]
if shutil.disk_usage(E).free<8*1024**3:raise RuntimeError('less than8GiB controller export headroom')
for i,h in enumerate(inventory['hosts'][:5]):
 host=scale.Host(h,i,r['run'],request['binary'],False,storage_mode='server_paged',buffer_pool_bytes=268435456)
 record={'node':i}
 try:
  process_record=host.action('run',argv=['test','-f',host.root+'/process.json'])
  if process_record['code']==0:
   sample=host.action('sample')
   if sample.get('running'):
    host.action('signal',signal='SIGINT');time.sleep(4);host.action('signal',signal='SIGKILL')
   deadline=time.monotonic()+20
   while host.action('sample').get('running'):
    if time.monotonic()>=deadline:raise RuntimeError('owned engine remains running; refusing archive')
    time.sleep(0.25)
  size=host.action('run',argv=['du','-sb',host.db])
  if size['code']:record['status']='no_database_directory';receipt.append(record);continue
  record['database_bytes']=int(size['stdout'].split()[0])
  if record['database_bytes']>1024**3:raise RuntimeError('database export exceeds1GiB pernode limit')
  listed=host.action('run',argv=['python3','-c','import json,pathlib,sys; r=pathlib.Path(sys.argv[1]); print(json.dumps([p.name for p in r.iterdir() if p.is_file() and (p.name.endswith("-ledger.json") or p.name in ("server.log","process.json"))]))',host.root])
  if listed['code']:raise RuntimeError('could not enumerate run ledgers')
  members=['db',*json.loads(listed['stdout'])]
  record['included_run_artifacts']=members
  archive=host.root+'/diagnostic-db-post-recovery.tar.gz'
  result=host.action('run',argv=['tar','-C',host.root,'-czf',archive,*members],timeout=180)
  if result['code']:raise RuntimeError('archive command failed')
  remote_hash=host.action('run',argv=['sha256sum',archive])
  if remote_hash['code']:raise RuntimeError('remote archive hash failed')
  remote_digest=remote_hash['stdout'].split()[0]
  destination=D/f'node-{i}.tar.gz'
  subprocess.run(['scp','-q','-oBatchMode=yes','-oStrictHostKeyChecking=yes',host.spec['ssh']+':'+archive,str(destination)],check=True,timeout=180)
  digest=hashlib.sha256()
  with destination.open('rb') as stream:
   for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
  if digest.hexdigest()!=remote_digest:raise RuntimeError('remote/controller archive digest mismatch')
  record.update(status='exported',archive_bytes=destination.stat().st_size,sha256=digest.hexdigest(),remote_sha256=remote_digest,owned_engine_stopped_before_archive=True)
 except Exception as error:record.update(status='export_failed',error=str(error))
 receipt.append(record)
(D/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'exported':sum(x['status']=='exported' for x in receipt),'failed':sum(x['status']=='export_failed' for x in receipt)}))
