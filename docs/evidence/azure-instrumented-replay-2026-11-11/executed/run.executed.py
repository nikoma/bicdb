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
run=uuid.uuid4().hex
OUT=BASE/run; OUT.mkdir(mode=0o700)
lock=threading.Lock(); active=[]; stop=threading.Event(); health={}; all_traffic=[]
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
save('run.json',{'run':run,'hosts':[{'index':h.index,'root':h.root,'db':h.db,'address':h.address,'ssh':h.spec['ssh']}for h in hosts], 'engine_sha256':ENGINE_SHA,'worker_sha256':WORKER_SHA,'engine_source':SOURCE,'version':'1.0.463-beta','client_manifest_sha256':digest(CLIENT/'manifest.json'),'runner_sha256':digest(__file__),'host_tools_sha256':digest(BASE/'host-tools.executed.py'),'no_fault':True,'classification':'instrumented_diagnostic_not_capacity_or_strong_read_qualification'})
(BASE/'current-run.json').write_text(json.dumps({'run':run,'output':str(OUT)}))
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
 path=f'snapshots/{label}-{time.time_ns()}.json';save(path,{'started_unix_ns':time.time_ns(),'nodes':rows})
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

def traffic(phase,namespace,seconds,concurrency=16,corpus=None,reconcile=None,join=False):
 cp=commands(namespace);dest=OUT/phase
 argv=[sys.executable,str(CLIENT/'trace_client.py'),'traffic','--worker-command',str(cp/'application.json'),'--observer-command-map',str(cp/'observers.json'),'--expected-worker-sha256',WORKER_SHA,'--expected-linked-core-source',SOURCE,'--expected-linked-core-version','1.0.463-beta','--application-gateway-node-id','n0','--namespace',namespace,'--phase',phase,'--seconds',str(seconds),'--concurrency',str(concurrency),'--observer-workers','2','--max-operations','1000000','--rpc-timeout-ms','5000','--request-budget-ms','30000','--max-ledger-bytes',str(6*1024**3),'--minimum-free-bytes',str(8*1024**3),'--overall-budget-seconds',str(min(1200,seconds+180)),'--output',str(dest)]
 if corpus:argv+=['--corpus-ack-ledger',str(corpus)]
 for source in reconcile or []:argv+=['--reconcile-journal',str(source)]
 emit('client_launch',phase=phase,argv=argv)
 with (OUT/(phase+'-stdout.txt')).open('w')as stdout,(OUT/(phase+'-stderr.txt')).open('w')as stderr:
  p=subprocess.Popen(argv,stdout=stdout,stderr=stderr,start_new_session=True)
  joined=False
  while p.poll()is None:
   if time.time()>=HARD_STOP:
    emit('hard_application_deadline',phase=phase);os.killpg(p.pid,15);break
   if join and not joined:
    progress=dest/'trace.jsonl'
    # Start membership change only after traffic window marker is durably present.
    if progress.exists():
     with progress.open()as f:
      started=any('"kind": "traffic_window_started"'in line or '"kind":"traffic_window_started"'in line for line in f)
     if started:add(hosts[4]);joined=True
   time.sleep(2)
  try:code=p.wait(timeout=90)
  except subprocess.TimeoutExpired:os.killpg(p.pid,9);code=p.wait();emit('client_cleanup_forced',phase=phase)
 emit('client_exited',phase=phase,code=code,joined=joined)
 if (dest/'trace.jsonl').exists():
  all_traffic.append(dest/'trace.jsonl')
  result=subprocess.run([sys.executable,str(CLIENT/'trace_client.py'),'validate',str(dest/'trace.jsonl'),'--allow-incomplete'],capture_output=True,text=True,timeout=120)
  save(phase+'-validation.json',{'code':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
 snapshot('after-'+phase)
 return code,json.loads((dest/'summary.json').read_text())if(dest/'summary.json').exists()else{}

def export():
 emit('stop_all_started')
 for h in active:
  try:emit('stop_requested',node=h.index,result=h.action('signal',signal='SIGINT'))
  except Exception as e:emit('stop_error',node=h.index,error=str(e))
 deadline=time.monotonic()+30
 while time.monotonic()<deadline:
  if all(not h.action('sample').get('running')for h in active):break
  time.sleep(2)
 for h in active:
  try:
   if h.action('sample').get('running'):emit('forced_stop',node=h.index,result=h.action('signal',signal='SIGKILL'))
  except Exception as e:emit('stop_check_error',node=h.index,error=str(e))
 for h in active:
  try:
   if h.action('sample').get('running'):raise RuntimeError('engine still running, cold export refused')
   argv=['tar','-czf',h.root+'/cold-final.tgz','-C',h.root,'db','server.log','process.json'];r=h.action('run',argv=argv,timeout=180)
   if r['code']:raise RuntimeError(str(r))
   remote=h.action('run',argv=['sha256sum',h.root+'/cold-final.tgz'],timeout=30);sha=remote['stdout'].split()[0]
   local=OUT/f'n{h.index}-cold-final.private.tgz';subprocess.run(['scp','-q','-oBatchMode=yes','-oStrictHostKeyChecking=yes',h.spec['ssh']+':'+h.root+'/cold-final.tgz',str(local)],check=True,timeout=180)
   if digest(local)!=sha:raise RuntimeError('cold export hash mismatch')
   save(f'n{h.index}-cold-export.json',{'node':h.index,'sha256':sha,'bytes':local.stat().st_size,'classification':'stopped_engine_database_and_logs_not_raw_disk_image'});emit('cold_export_complete',node=h.index,bytes=local.stat().st_size)
  except Exception as e:emit('cold_export_failed',node=h.index,error=str(e))
 emit('export_done')

try:
 emit('setup_started',run=run)
 identities=[h.action('probe')for h in hosts];assert len({r['machine_hash']for r in identities})==5;save('machines.json',identities)
 for h in hosts:
  h.action('mkdir');h.copy(BINARY,'bicdb');h.copy(WORKER,'wire-worker');h.action('run',argv=['chmod','500',h.binary,h.root+'/wire-worker'])
  r=h.action('run',argv=['sha256sum',h.binary,h.root+'/wire-worker']);assert r['stdout'].splitlines()[0].split()[0]==ENGINE_SHA and r['stdout'].splitlines()[1].split()[0]==WORKER_SHA;save(f'n{h.index}-staged-hashes.json',r)
 (OUT/'tls-private').mkdir(mode=0o700);H.certificates(hosts,OUT/'tls-private')
 hosts[0].command('cluster','init',hosts[0].db,'--cluster-id','trace-'+run,'--node-id','n0','--address',hosts[0].address,'--replication-factor','3','--initial-ranges','64','--capacity-bytes','100000000000','--application-events',*hosts[0].tls)
 hosts[0].start();active.append(hosts[0]);time.sleep(2)
 for h in hosts[1:4]:add(h)
 obs=threading.Thread(target=observer,daemon=True);obs.start()
 if not wait_ready(4):raise RuntimeError('initial four-node stability gate failed')
 smoke_ns='trace-smoke-'+run
 code,summary=traffic('cloud-smoke',smoke_ns,60,4)
 if code!=0 or not summary.get('telemetry_complete')or not summary.get('workload_passed'):raise RuntimeError('cloud smoke gate failed')
 code,summary=traffic('smoke-reconciliation',smoke_ns,120,4,reconcile=[OUT/'cloud-smoke/trace.jsonl'])
 if code!=0 or not summary.get('telemetry_complete')or not summary.get('workload_passed'):raise RuntimeError('smoke reconciliation gate failed')
 emit('cloud_smoke_gate_passed')
 corpora=P('/tmp/bicdb-azure-instrumented/docs/evidence/azure-long-growth-failure-2026-11-11');namespace='scale-83da6383fbae4ba38d8d15f621d03cbb'
 code,summary=traffic('baseline-4',namespace,900,16,corpus=corpora/'steady-4-ledger.json')
 if not summary.get('telemetry_complete') or not summary.get('planned_experiment_complete'):raise RuntimeError('baseline telemetry/planned-completion gate failed; no growth dispatched')
 if time.time()+2100>=HARD_STOP:raise RuntimeError('remaining approved window insufficient for growth plus bounded reconciliation/export')
 traffic('growth-4-to-5',namespace,900,16,corpus=corpora/'growth-4-to-5-ledger.json',join=True)
 converged=wait_ready(5,300)
 if converged and time.time()+1800<HARD_STOP:traffic('stable-5',namespace,600,16)
 else:emit('stable_five_not_run',converged=converged,reason='convergence_or_time_gate')
 inputs=[p for p in all_traffic if p.parent.name in ['baseline-4','growth-4-to-5','stable-5']]
 remaining=min(900,max(1,int(HARD_STOP-time.time()-120)))
 if inputs and remaining>=60:traffic('final-reconciliation',namespace,remaining,16,reconcile=inputs)
 else:emit('reconciliation_not_run',reason='bounded_window_exhausted',inputs=[str(p)for p in inputs])
 emit('planned_sequence_finished')
except BaseException as e:
 emit('sequence_failed',error=repr(e));save('sequence-error.json',{'error':repr(e),'unix_ns':time.time_ns()})
finally:
 stop.set()
 if 'obs'in locals():obs.join(timeout=45)
 export()
 save('final.json',{'finished_unix_ns':time.time_ns(),'run':run,'output':str(OUT),'classification':'diagnostic_only','all_trace_paths':[str(p)for p in all_traffic]})
