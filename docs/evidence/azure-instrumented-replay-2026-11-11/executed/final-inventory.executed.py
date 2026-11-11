#!/usr/bin/env python3
"""Read-only all-member diagnostic files; TLS member keys never copied."""
import concurrent.futures,hashlib,importlib.util,json,os,pathlib,subprocess,time
P=pathlib.Path; BASE=P('/tmp/bicdb-azure-repro/experiment'); ATTEMPT=json.loads((BASE/'light-current-run.json').read_text());OUT=P(ATTEMPT['output'])/'poststop-inventory';OUT.mkdir(mode=0o700)
spec=importlib.util.spec_from_file_location('owned',BASE/'host-tools.executed.py');H=importlib.util.module_from_spec(spec);spec.loader.exec_module(H)
RUN=ATTEMPT['data_run']; inv=json.loads(P('/tmp/bicdb-azure-repro/inventory.private.json').read_text());hosts=[H.Host(s,i,RUN,P('/tmp/bicdb-azure-artifact-463-evidence/bicdb-1.0.463-beta-d49a391b'),False,'server_paged',268435456)for i,s in enumerate(inv['hosts'])]
FILES=['server.log','process.json','db/bicdb-distribution.json','db/format.json','db/cluster-range-write-log.json','db/cluster-topology.json','db/cluster-metadata-consensus.json']
INSPECT="""import pathlib,json,sys,time
r=pathlib.Path(sys.argv[1]);names=json.loads(sys.argv[2]);out={'unix_ns':time.time_ns(),'db_exists':(r/'db').exists(),'files':{},'database_top_level':[]}
for n in names:
 p=r/n
 try:s=p.stat();out['files'][n]={'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}
 except FileNotFoundError:out['files'][n]={'absent':True}
if (r/'db').exists():out['database_top_level']=[{'name':p.name,'directory':p.is_dir(),'bytes':p.stat().st_size}for p in sorted((r/'db').iterdir())]
print(json.dumps(out))"""
def save(p,v):
 with p.open('w')as f:json.dump(v,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def node(h):
 dest=OUT/f'n{h.index}';dest.mkdir(mode=0o700);r={'node':h.index,'started_unix_ns':time.time_ns(),'copies':[]}
 try:
  raw=h.action('run',argv=['python3','-c',INSPECT,h.root,json.dumps(FILES)],timeout=15);r['before']=json.loads(raw['stdout'])
  try:r['owned_process']=h.action('sample',timeout=5)
  except Exception as error:r['owned_process_error']=str(error)
  if r['before']['db_exists']:
   try:r['status']=json.loads(h.command('cluster','status',h.db,'--json',timeout=15))
   except Exception as error:r['status_error']=str(error)
  else:r['status_classification']='not_joined_database_absent'
  for name in FILES:
   state=r['before']['files'][name]
   if state.get('absent'):continue
   if state['bytes']>32*1024**2:r['copies'].append({'path':name,'skipped':'32MiB_file_bound'});continue
   local=dest/name.replace('/','__');start=time.time_ns()
   p=subprocess.run(['scp','-q','-oBatchMode=yes','-oStrictHostKeyChecking=yes',h.spec['ssh']+':'+h.root+'/'+name,str(local)],capture_output=True,text=True,timeout=30)
   row={'path':name,'copy_started_unix_ns':start,'copy_finished_unix_ns':time.time_ns(),'code':p.returncode,'stderr':p.stderr}
   if p.returncode==0:row.update(bytes=local.stat().st_size,sha256=hashlib.sha256(local.read_bytes()).hexdigest())
   r['copies'].append(row)
  raw=h.action('run',argv=['python3','-c',INSPECT,h.root,json.dumps(FILES)],timeout=15);r['after']=json.loads(raw['stdout'])
 except BaseException as error:r['error']=repr(error)
 r['finished_unix_ns']=time.time_ns();save(dest/'capture.json',r);return {'node':h.index,'db_exists':r.get('before',{}).get('db_exists'),'copied_files':len(r['copies']),'error':r.get('error')}
save(OUT/'provenance.json',{'helper_sha256':hashlib.sha256(P(__file__).read_bytes()).hexdigest(),'started_unix_ns':time.time_ns(),'classification':'read-only_post-owned-stop_files_not_new_engine_availability_observations','member_private_keys_exported':False,'files':FILES})
with concurrent.futures.ThreadPoolExecutor(max_workers=2)as pool:result=list(pool.map(node,hosts))
save(OUT/'result.json',{'finished_unix_ns':time.time_ns(),'nodes':result});print(json.dumps(result))
