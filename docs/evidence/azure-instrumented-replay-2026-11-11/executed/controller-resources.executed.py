#!/usr/bin/env python3
"""Low-volume read-only host/owned-process counters; no signals or profiling."""
import json,os,pathlib,sys,time
root=pathlib.Path(sys.argv[1]); owner=int(sys.argv[2]); owner_start=sys.argv[3]
def process(pid,expected_start):
 p=pathlib.Path('/proc')/str(pid)
 try:
  raw=(p/'stat').read_text();start=raw.split()[21]
  if start!=expected_start:return {'pid':pid,'identity_changed':True}
  return {'pid':pid,'start_ticks':start,'stat':raw,'io':(p/'io').read_text(),'status':(p/'status').read_text()}
 except (FileNotFoundError,ProcessLookupError):return {'pid':pid,'gone':True}
with (root/'controller-host-resources.jsonl').open('x')as out:
 while True:
  p=process(owner,owner_start);r={'controller_unix_ns':time.time_ns(),'controller_monotonic_ns':time.monotonic_ns(),'classification':'shared_controller_host_not_isolated_workload_cost','orchestrator':p,'cpu_stat':pathlib.Path('/proc/stat').read_text(),'diskstats':pathlib.Path('/proc/diskstats').read_text(),'pressure':{k:(pathlib.Path('/proc/pressure')/k).read_text()for k in ['cpu','io','memory']},'meminfo':pathlib.Path('/proc/meminfo').read_text(),'owned_client_processes':[]}
  for identity_path in root.glob('*/controller-identity.json'):
   try:
    identity=json.loads(identity_path.read_text());targets=[identity]+identity.get('workers',[])
    for item in targets:
     pid=item.get('pid');start=item.get('start_ticks')
     if pid is not None and start is not None:r['owned_client_processes'].append({'phase':identity_path.parent.name,**process(pid,str(start))})
   except (FileNotFoundError,json.JSONDecodeError)as error:r.setdefault('identity_read_errors',[]).append({'phase':identity_path.parent.name,'error':str(error)})
  out.write(json.dumps(r)+'\n');out.flush();os.fsync(out.fileno())
  if p.get('gone')or p.get('identity_changed'):break
  time.sleep(10)
