import concurrent.futures,hashlib,importlib.util,json,os,shutil,subprocess,time
from pathlib import Path
P=Path('/tmp/bicdb-azure-run');E=P/'long-comparison';E.mkdir(mode=0o700,exist_ok=False);source=Path('/tmp/bicdb-azure-integration/scripts/platform/scale-cluster.py');frozen=E/'scale-cluster.frozen.py';shutil.copy2(source,frozen);frozen.chmod(0o400)
spec=importlib.util.spec_from_file_location('scale',frozen);scale=importlib.util.module_from_spec(spec);spec.loader.exec_module(scale)
request=json.loads((P/'run-request.private.json').read_text());binary=Path(request['binary']);assert hashlib.sha256(binary.read_bytes()).hexdigest()==request['binary_sha256'];env=dict(os.environ);env['PATH']=str(P/'bin')+':'+env['PATH'];os.environ['PATH']=env['PATH']
args=['python3',str(frozen),'--inventory',str(P/'inventory.private.json'),'--binary',str(binary),'--output',str(E/'growth'),'--sizes','4,5','--seconds','900','--failure-seconds','90','--concurrency','16','--rpc-timeout-ms','5000','--max-operations','1000000','--ranges','64','--timeout','1200','--storage-mode','server_paged','--buffer-pool-bytes','268435456','--node-capacity-bytes','100000000000','--source-commit','b05385d4b033edf0db03f3eccf4bdd86f7c8eddb','--build-label','release-thinlto']
(E/'launch-provenance.json').write_text(json.dumps({'unix_ns':time.time_ns(),'argv':args,'harness_sha256':hashlib.sha256(frozen.read_bytes()).hexdigest(),'observer_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'binary_sha256':request['binary_sha256'],'extra_append_workload':False},indent=2)+'\n')
code='import json,pathlib,sys;root=pathlib.Path(sys.argv[1]); names=["cluster-range-write-log.json","cluster-topology.json","cluster-metadata-consensus.json","transactions.log","paged/store.wal","paged/store.pages"];r={};\nfor name in names:\n try:\n  s=(root/name).stat();r[name]={"bytes":s.st_size,"mtime_ns":s.st_mtime_ns}\n except FileNotFoundError:r[name]={"present":False}\nprint(json.dumps(r))'
def capture(report,phase):
 inventory=json.loads((P/'inventory.private.json').read_text());count=4 if phase=='steady-4' else 5
 def node(pair):
  i,h=pair;host=scale.Host(h,i,report['run'],binary,False,'server_paged',268435456)
  try:return {'node':i,'unix_ns':time.time_ns(),'files':host.action('run',argv=['python3','-c',code,host.db]),'sample':host.action('sample')}
  except Exception as error:return {'node':i,'error':str(error)}
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:rows=list(pool.map(node,enumerate(inventory['hosts'][:count])))
 (E/('files-'+phase+'.private.json')).write_text(json.dumps(rows,indent=2)+'\n')
captured=set()
with (E/'stdout').open('w') as stdout,(E/'stderr').open('w') as stderr:
 process=subprocess.Popen(args,stdout=stdout,stderr=stderr,env=env)
 while process.poll() is None:
  try:report=json.loads((E/'growth/report.json').read_text())
  except (FileNotFoundError,json.JSONDecodeError):report={}
  for row in report.get('results',[]):
   if row['phase'] not in captured:capture(report,row['phase']);captured.add(row['phase']);print('phase_complete',row['phase'],flush=True)
  time.sleep(2)
(E/'execution-summary.json').write_text(json.dumps({'exit':process.returncode,'captured_phases':sorted(captured)})+'\n')
print('comparison_exit',process.returncode,flush=True)
raise SystemExit(process.returncode)
