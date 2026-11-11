#!/usr/bin/env python3
"""Owned, no-fault instrumented current-runtime Azure diagnostic orchestration."""
import concurrent.futures, hashlib, importlib.util, json, os, pathlib, shlex, subprocess, sys, threading, time, uuid
P=pathlib.Path
BASE=P('/tmp/bicdb-azure-repro/experiment')
SPEC=importlib.util.spec_from_file_location('owned_host_tools',BASE/'host-tools.executed.py'); H=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(H)
CLIENT=P('/tmp/bicdb-instrumented-client-ready-v3')
BINARY=P('/tmp/bicdb-azure-artifact-463-evidence/bicdb-1.0.463-beta-d49a391b')
WORKER=CLIENT/'wire-worker'
ENGINE_SHA='840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d'
WORKER_SHA='b8cc2607220e32f705a23fd6b7d4f41c22298cd1b0ec92cc8e83847783ef8d2c'
SOURCE='d49a391b3c6157f90f29e9f2f5214143c60f9f4b'
attempt=uuid.uuid4().hex
run='8186dd18b83f4195b1319a85a1ee3498'
OLD=BASE/run
OUT=BASE/('light-'+attempt); OUT.mkdir(mode=0o700)
lock=threading.Lock(); active=[]; stop=threading.Event(); health={}; all_traffic=[OLD/'baseline-4/trace.jsonl',OLD/'cloud-smoke/trace.jsonl']; trace_namespaces={all_traffic[0]:'scale-83da6383fbae4ba38d8d15f621d03cbb',all_traffic[1]:'trace-smoke-'+run}; reconciliation_attempted_inputs=set()
START=time.time(); HARD_STOP=1794394500 # 2026-11-11 10:55 UTC, checked below
HARD_STOP=__import__('datetime').datetime.fromisoformat('2026-11-11T10:55:00+00:00').timestamp()
def emit(kind,**kw):
 r={'kind':kind,'controller_unix_ns':time.time_ns(),'controller_monotonic_ns':time.monotonic_ns(),**kw}
 with lock:
  with (OUT/'operations.jsonl').open('a') as f: f.write(json.dumps(r)+'\n');f.flush();os.fsync(f.fileno())
 print(kind, {k:v for k,v in kw.items() if k not in ('nodes','status','details')},flush=True)
def save(name,value):
 p=OUT/name;p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w') as f:json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def digest(path):return hashlib.sha256(P(path).read_bytes()).hexdigest()
assert digest(BINARY)==ENGINE_SHA and digest(WORKER)==WORKER_SHA
inventory=json.loads(P('/tmp/bicdb-azure-repro/inventory.private.json').read_text());H.validate_inventory(inventory['hosts'],5)
hosts=[H.Host(s,i,run,BINARY,False,'server_paged',268435456) for i,s in enumerate(inventory['hosts'])]
save('run.json',{'run':run,'attempt_id':attempt,'profile':'separate4workerlightdiagnostic','prior_attempt':str(OLD),'hosts':[{'index':h.index,'root':h.root,'db':h.db,'address':h.address,'ssh':h.spec['ssh']}for h in hosts], 'engine_sha256':ENGINE_SHA,'worker_sha256':WORKER_SHA,'engine_source':SOURCE,'version':'1.0.463-beta','client_manifest_sha256':digest(CLIENT/'manifest.json'),'runner_sha256':digest(__file__),'host_tools_sha256':digest(BASE/'host-tools.executed.py'),'no_fault':True,'classification':'instrumented_diagnostic_not_capacity_or_strong_read_qualification'})
(BASE/'light-current-run.json').write_text(json.dumps({'data_run':run,'attempt_id':attempt,'output':str(OUT)}))
PROBE=r'''import pathlib,json,os,platform,ssl,subprocess,sys,time
root=pathlib.Path(sys.argv[1]); files=['db/format.json','db/cluster-range-write-log.json','db/cluster-topology.json','db/cluster-metadata-consensus.json','db/transactions.log','db/paged/store.wal','db/paged/store.pages']; r={'unix_ns':time.time_ns(),'files':{}}
for name in files:
 p=root/name
 try:r['files'][name]={'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns}
 except FileNotFoundError:r['files'][name]={'absent':True}
r['host']={'kernel':platform.release(),'cpu_model':[x for x in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')][:1],'vcpus':os.cpu_count(),'os_release':pathlib.Path('/etc/os-release').read_text(),'python':sys.version,'openssl':ssl.OPENSSL_VERSION,'meminfo':pathlib.Path('/proc/meminfo').read_text(),'proc_stat':pathlib.Path('/proc/stat').read_text(),'diskstats':pathlib.Path('/proc/diskstats').read_text(),'pressure':{n:(pathlib.Path('/proc/pressure')/n).read_text() for n in ['cpu','io','memory']}}
r['mount']=subprocess.run(['findmnt','-J','-T',str(root)],capture_output=True,text=True).stdout;r['statvfs']={'free_bytes':os.statvfs(root).f_bavail*os.statvfs(root).f_frsize}
try:
 proc=json.loads((root/'process.json').read_text()); pid=proc['pid']; stat=pathlib.Path(f'/proc/{pid}/stat').read_text().split();cmd=pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
 if stat[21]!=proc['start'] or str(root/'db').encode() not in cmd:raise RuntimeError('owner mismatch')
 r['process_environment']={x.split('=',1)[0]:x.split('=',1)[1]for x in pathlib.Path(f'/proc/{pid}/environ').read_bytes().decode().split('\0')if x.split('=',1)[0]in['BICDB_STORAGE_MODE','BICDB_PAGED_BUFFER_POOL_BYTES']}
except FileNotFoundError:r['process_absent']=True
print(json.dumps(r))'''
def sample(h):
 begin=time.time_ns();r={'node':h.index,'started_unix_ns':begin}
 try:r['status']=json.loads(h.command('cluster','status',h.db,'--json',timeout=15));r['process']=h.action('sample');r['diagnostics']=h.action('run',argv=['python3','-c',PROBE,h.root],timeout=15)
 except Exception as e:r['error']=str(e)
 r['finished_unix_ns']=time.time_ns();return r
def snapshot(label):
 current=list(active)
 with concurrent.futures.ThreadPoolExecutor(max_workers=3)as pool: rows=list(pool.map(sample,current))
 path=f'snapshots/{label}-{time.time_ns()}.json';save(path,{'collection_finished_unix_ns':time.time_ns(),'nodes':rows})
 for row in rows:health[row['node']]=row
 return rows
def observer():
 while not stop.is_set():
  try:snapshot('periodic')
  except Exception as e:emit('status_observer_error',error=str(e))
  stop.wait(10)
def ready_row(row,count):
 if 'status'not in row or not row.get('process',{}).get('running'):return False
 s=row['status'];m=s['metrics'];t=s['topology']
 return H.capacity_ready(s,count) and m.get('nodes_live')==count and all(len([p for p in r['replicas']if p['role']=='voter'])==3 for r in t['ranges'].values())
def wait_ready(count,timeout=300):
 deadline=time.monotonic()+timeout;stable=None
 while time.monotonic()<deadline and time.time()<HARD_STOP:
  rows=snapshot('health-'+str(count));ok=len(rows)==count and all(ready_row(r,count)for r in rows)
  if ok:
   if stable is None:stable=time.monotonic();emit('health_stable_started',count=count)
   if time.monotonic()-stable>=30:
    formats=H.persisted_storage(active);H.validate_persisted_storage(formats,'server_paged');save(f'formats-{count}-{time.time_ns()}.json',formats);emit('health_stable_30_seconds',count=count);return True
  else:
   if stable is not None:emit('health_stability_reset',count=count)
   stable=None
  time.sleep(5)
 emit('health_deadline_failed',count=count,timeout_seconds=timeout);return False
def add(h):
 emit('join_started',node=h.index)
 h.command('cluster','join','--seed-node-id','n0','--seed-address',hosts[0].address,'--cluster-id','trace-'+run,'--node-id',f'n{h.index}','--address',h.address,'--node-root',h.db,'--capacity-bytes','100000000000',*h.tls,timeout=180)
 emit('join_registered',node=h.index);h.start();active.append(h);emit('serve_started',node=h.index)
def commands(namespace):
 mapping={}
 for h in hosts:
  argv=['ssh','-oBatchMode=yes','-oStrictHostKeyChecking=yes',h.spec['ssh'],' '.join(shlex.quote(x)for x in ['timeout','--signal=TERM','1200',h.root+'/wire-worker',h.db,namespace])];mapping['n'+str(h.index)]=argv
 cp=OUT/('commands-'+hashlib.sha256(namespace.encode()).hexdigest()[:12]);cp.mkdir(exist_ok=True,mode=0o700);save(str(cp.relative_to(OUT)/'application.json'),mapping['n0']);save(str(cp.relative_to(OUT)/'observers.json'),mapping);return cp

def traffic(phase,namespace,seconds,concurrency=4,corpus=None,reconcile=None,join=False,max_operations=1000000,success_control_every=32):
 cp=commands(namespace);dest=OUT/phase
 argv=[sys.executable,str(CLIENT/'trace_client.py'),'traffic','--worker-command',str(cp/'application.json'),'--observer-command-map',str(cp/'observers.json'),'--expected-worker-sha256',WORKER_SHA,'--expected-linked-core-source',SOURCE,'--expected-linked-core-version','1.0.463-beta','--application-gateway-node-id','n0','--namespace',namespace,'--phase',phase,'--seconds',str(seconds),'--concurrency',str(concurrency),'--observer-workers','2','--max-operations',str(max_operations),'--success-control-every',str(success_control_every),'--rpc-timeout-ms','5000','--request-budget-ms','30000','--max-ledger-bytes',str(6*1024**3),'--minimum-free-bytes',str(8*1024**3),'--overall-budget-seconds',str(min(1200,seconds+180)),'--output',str(dest)]
 if corpus:argv+=['--corpus-ack-ledger',str(corpus)]
 for source in reconcile or []:argv+=['--reconcile-journal',str(source)]
 emit('client_launch',phase=phase,argv=argv)
 with (OUT/(phase+'-stdout.txt')).open('w')as stdout,(OUT/(phase+'-stderr.txt')).open('w')as stderr:
  p=subprocess.Popen(argv,stdout=stdout,stderr=stderr,start_new_session=True)
  joined=False; join_error=None
  while p.poll()is None:
   if time.time()>=HARD_STOP:
    emit('hard_application_deadline',phase=phase);os.killpg(p.pid,15);break
   if join and not joined:
    progress=dest/'trace.jsonl'
    # Start membership change only after traffic window marker is durably present.
    if progress.exists():
     with progress.open()as f:
      started=any('"kind": "traffic_window_started"'in line or '"kind":"traffic_window_started"'in line for line in f)
     if started:
      try:add(hosts[4])
      except Exception as error:join_error=str(error);emit('join_failed',node=4,error=join_error)
      joined=True
   time.sleep(2)
  try:code=p.wait(timeout=90)
  except subprocess.TimeoutExpired:os.killpg(p.pid,9);code=p.wait();emit('client_cleanup_forced',phase=phase)
 emit('client_exited',phase=phase,code=code,joined=joined)
 if (dest/'trace.jsonl').exists():
  all_traffic.append(dest/'trace.jsonl');trace_namespaces[dest/'trace.jsonl']=namespace
  reconciliation_attempted_inputs.update(reconcile or [])
  result=subprocess.run([sys.executable,str(CLIENT/'trace_client.py'),'validate',str(dest/'trace.jsonl'),],capture_output=True,text=True,timeout=120)
  save(phase+'-validation.json',{'code':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
 else:result=None
 snapshot('after-'+phase)
 summary=json.loads((dest/'summary.json').read_text())if(dest/'summary.json').exists()else{}
 summary['orchestration_strict_validation_passed']=result is not None and result.returncode==0
 if join_error:summary['join_error']=join_error
 return code,summary

def export():
 export_deadline=time.monotonic()+300
 emit('stop_all_started',export_budget_seconds=300)
 for h in active:
  try:emit('stop_requested',node=h.index,result=h.action('signal',signal='SIGINT',timeout=5))
  except Exception as e:emit('stop_error',node=h.index,error=str(e))
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  if all(not h.action('sample',timeout=5).get('running')for h in active):break
  time.sleep(2)
 for h in active:
  try:
   if h.action('sample',timeout=5).get('running'):emit('forced_stop',node=h.index,result=h.action('signal',signal='SIGKILL',timeout=5))
  except Exception as e:emit('stop_check_error',node=h.index,error=str(e))
 for h in active:
  try:
   remaining=export_deadline-time.monotonic()
   if remaining<50:raise TimeoutError('overall cold export deadline exhausted; source preserved on guest until infrastructure cleanup')
   if h.action('sample',timeout=5).get('running'):raise RuntimeError('engine still running, cold export refused')
   argv=['tar','-czf',h.root+'/cold-light-'+attempt+'.tgz','-C',h.root,'db','server.log','process.json'];r=h.action('run',argv=argv,timeout=max(1,min(90,int(export_deadline-time.monotonic()-25))))
   if r['code']:raise RuntimeError(str(r))
   if export_deadline-time.monotonic()<30:raise TimeoutError('cold archive created but hash/export deadline exhausted')
   remote=h.action('run',argv=['sha256sum',h.root+'/cold-light-'+attempt+'.tgz'],timeout=max(1,min(30,int(export_deadline-time.monotonic()-25))));sha=remote['stdout'].split()[0]
   local=OUT/f'n{h.index}-cold-light-final.private.tgz';subprocess.run(['scp','-q','-oBatchMode=yes','-oStrictHostKeyChecking=yes',h.spec['ssh']+':'+h.root+'/cold-light-'+attempt+'.tgz',str(local)],check=True,timeout=max(1,min(90,int(export_deadline-time.monotonic()))))
   if digest(local)!=sha:raise RuntimeError('cold export hash mismatch')
   save(f'n{h.index}-cold-export.json',{'node':h.index,'sha256':sha,'bytes':local.stat().st_size,'classification':'stopped_engine_database_and_logs_not_raw_disk_image'});emit('cold_export_complete',node=h.index,bytes=local.stat().st_size)
  except Exception as e:emit('cold_export_failed',node=h.index,error=str(e))
 emit('export_done')

try:
 emit('light_restart_admission_started',attempt_id=attempt,data_run=run,classification='separate_short_diagnostic_no_fault_no_capacity_no_strong_read')
 # Original outside-cluster cold archives are immutable and mandatory before reopening.
 assert (OLD/'final.json').exists(), 'first sequence has not finished exporting'
 archive_checks=[]
 for h in hosts[:4]:
  receipt=json.loads((OLD/f'n{h.index}-cold-export.json').read_text());archive=OLD/f'n{h.index}-cold-final.private.tgz'
  assert archive.exists() and digest(archive)==receipt['sha256']
  assert not h.action('sample',timeout=5).get('running'), 'existing owned engine is still running'
  archive_checks.append({'node':h.index,'sha256':receipt['sha256'],'bytes':archive.stat().st_size})
 save('prior-archive-verification.json',archive_checks)
 identities=[h.action('probe')for h in hosts];save('machines.json',identities)
 assert [r['machine_hash']for r in identities]==[r['machine_hash']for r in json.loads((OLD/'machines.json').read_text())]
 for h in hosts:
  staged=h.action('run',argv=['sha256sum',h.binary,h.root+'/wire-worker'],timeout=15)
  assert staged['code']==0 and staged['stdout'].splitlines()[0].split()[0]==ENGINE_SHA and staged['stdout'].splitlines()[1].split()[0]==WORKER_SHA
  save(f'n{h.index}-reused-artifact-hashes.json',staged)
 for h in hosts[:4]:
  save(f'n{h.index}-format-before-reopen.json',json.loads(h.action('read',file='db/format.json',timeout=10)['contents']))
  h.start();active.append(h);emit('owned_engine_restarted',node=h.index)
 obs=threading.Thread(target=observer,daemon=True);obs.start()
 if not wait_ready(4,300):raise RuntimeError('restarted four-node sampled stability gate failed')
 namespace='scale-83da6383fbae4ba38d8d15f621d03cbb'
 original_corpus=P('/tmp/bicdb-azure-instrumented/docs/evidence/azure-long-growth-failure-2026-11-11/growth-4-to-5-ledger.json')
 original=json.loads(original_corpus.read_text())
 fixture_keys=['25d1dbd975814f4f9b0c5225b7658d2d-10-636','25d1dbd975814f4f9b0c5225b7658d2d-14-184']
 fixture={'namespace':namespace,'run':original['run'],'payload_bytes':original['payload_bytes'],'keys':fixture_keys}
 save('two-target-fixture.json',fixture)
 emit('two_target_fixture_declared',keys=fixture_keys,original_corpus_sha256=digest(original_corpus),original_corpus_indices=[original['keys'].index(k)for k in fixture_keys],classification='new explicit fixture writes; not original execution order/reproduction',expected_payload_sha256=hashlib.sha256(json.dumps({'run':original['run'],'payload':'x'*original['payload_bytes']},sort_keys=True,separators=(',',':')).encode()).hexdigest())
 code,summary=traffic('two-target-fixture',namespace,60,1,corpus=OUT/'two-target-fixture.json',max_operations=4,success_control_every=1)
 if code!=0 or not summary.get('orchestration_strict_validation_passed') or not summary.get('workload_passed'):raise RuntimeError('two-target fixture incomplete or unsuccessful')
 code,summary=traffic('light-steady-4',namespace,60,4)
 if code!=0 or not summary.get('orchestration_strict_validation_passed') or not summary.get('workload_passed'):raise RuntimeError('light steady4 gate failed; no join')
 code,summary=traffic('light-growth-4-to-5',namespace,240,4,join=True)
 if not summary.get('telemetry_complete') or not summary.get('orchestration_strict_validation_passed'):raise RuntimeError('light growth telemetry incomplete')
 converged=wait_ready(5,300)
 if converged:traffic('light-steady-5',namespace,60,4)
 else:emit('stable_five_not_run',reason='five-node sampled stability gate failed')
 traffic('two-target-post-growth-read',namespace,60,1,reconcile=[OUT/'two-target-fixture/trace.jsonl'],success_control_every=1)
 inputs=[path for path in all_traffic if trace_namespaces[path]==namespace and 'reconciliation'not in path.parent.name and 'post-growth-read'not in path.parent.name]
 traffic('light-final-reconciliation',namespace,900,4,reconcile=inputs)
 traffic('old-smoke-final-reconciliation','trace-smoke-'+run,120,4,reconcile=[OLD/'cloud-smoke/trace.jsonl'])
 emit('light_sequence_finished')
except BaseException as error:
 emit('sequence_failed',error=repr(error));save('sequence-error.json',{'error':repr(error),'unix_ns':time.time_ns()})
finally:
 fixture_source=OUT/'two-target-fixture/trace.jsonl'
 if fixture_source.exists() and not (OUT/'two-target-post-growth-read/trace.jsonl').exists() and active and time.time()+120<HARD_STOP:
  try:traffic('two-target-finally-reconciliation','scale-83da6383fbae4ba38d8d15f621d03cbb',60,1,reconcile=[fixture_source],success_control_every=1)
  except BaseException as error:emit('dedicated_two_target_finally_failed',error=repr(error))
 grouped={}
 for path in all_traffic:
  if path not in reconciliation_attempted_inputs and 'reconciliation'not in path.parent.name and 'post-growth-read'not in path.parent.name:grouped.setdefault(trace_namespaces[path],[]).append(path)
 emergency_deadline=min(HARD_STOP,time.time()+600)
 for index,(namespace,inputs)in enumerate(grouped.items()):
  remaining=min(300,int(emergency_deadline-time.time()-60))
  if remaining<1:emit('finally_reconciliation_unattempted',namespace=namespace,inputs=[str(p)for p in inputs],reason='declared overall deadline');continue
  try:traffic('light-finally-reconciliation-'+str(index),namespace,remaining,4,reconcile=inputs)
  except BaseException as error:emit('finally_reconciliation_failed',namespace=namespace,error=repr(error),inputs=[str(p)for p in inputs])
 stop.set()
 if 'obs'in locals():obs.join(timeout=45)
 export()
 save('final.json',{'finished_unix_ns':time.time_ns(),'attempt_id':attempt,'data_run':run,'output':str(OUT),'classification':'separate_diagnostic_only','all_trace_paths':[str(p)for p in all_traffic]})
