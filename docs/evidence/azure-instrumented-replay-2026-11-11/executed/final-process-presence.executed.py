import concurrent.futures,hashlib,importlib.util,json,os,pathlib,time
P=pathlib.Path;base=P('/tmp/bicdb-azure-repro/experiment');control=json.loads((base/'light-current-run.json').read_text());out=P(control['output'])
spec=importlib.util.spec_from_file_location('owned',base/'host-tools.executed.py');H=importlib.util.module_from_spec(spec);spec.loader.exec_module(H);inventory=json.loads(P('/tmp/bicdb-azure-repro/inventory.private.json').read_text())
SCRIPT="""import pathlib,sys,json,time
root=sys.argv[1];matches=[]
for p in pathlib.Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=p.read_bytes().split(b'\\0')
  if any(v.encode()in args for v in [root+'/bicdb',root+'/wire-worker',root+'/db']):matches.append({'pid':int(p.parent.name),'start_ticks':(p.parent/'stat').read_text().split()[21],'argv':[x.decode(errors='replace')for x in args if x]})
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
print(json.dumps({'unix_ns':time.time_ns(),'matching_task_processes':matches,'database_exists':pathlib.Path(root+'/db').exists(),'process_file_exists':pathlib.Path(root+'/process.json').exists()}))"""
def node(pair):
 i,s=pair;h=H.Host(s,i,control['data_run'],P('/unused'),False);r=h.action('run',argv=['python3','-c',SCRIPT,h.root],timeout=10);return {'node':i,'scan':json.loads(r['stdout'])}
with concurrent.futures.ThreadPoolExecutor(max_workers=3)as pool:rows=list(pool.map(node,enumerate(inventory['hosts'])))
result={'helper_sha256':hashlib.sha256(P(__file__).read_bytes()).hexdigest(),'completed_unix_ns':time.time_ns(),'nodes':rows,'all_task_processes_absent':all(not r['scan']['matching_task_processes']for r in rows)}
with (out/'final-all5-process-presence.json').open('w')as f:json.dump(result,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps({'all_task_processes_absent':result['all_task_processes_absent'],'node_database_exists':[r['scan']['database_exists']for r in rows]}))
