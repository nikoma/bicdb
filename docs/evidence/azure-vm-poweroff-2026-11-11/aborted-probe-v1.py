import hashlib,importlib.util,json,os,time,uuid
from pathlib import Path
P=Path('/tmp/bicdb-azure-run');E=P/'vm-fault';E.mkdir(mode=0o700,exist_ok=False);S=Path('/tmp/bicdb-azure-integration/scripts/platform');os.environ['PATH']=str(P/'bin')+':'+os.environ['PATH']
def module(name,path):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
scale=module('scale',S/'scale-cluster.py');driver=module('external',S/'external-event-ledger.py')
report=json.loads((P/'experiment/growth/report.json').read_text());inventory=json.loads((P/'inventory.private.json').read_text());request=json.loads((P/'run-request.private.json').read_text());hosts=[scale.Host(h,i,report['run'],request['binary'],False,'server_paged',268435456) for i,h in enumerate(inventory['hosts'][:5])]
result={'qualification_status':'not_qualified','fault_mechanism':'azure_guest_abrupt_power_off','physical_host_power_loss':False,'source_commit':'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb','binary_sha256':request['binary_sha256'],'events':[]}
def save():
 temp=E/'report.json.tmp'
 with temp.open('w') as out:out.write(json.dumps(result,indent=2)+'\n');out.flush();os.fsync(out.fileno())
 os.replace(temp,E/'report.json')
def event(kind,**details):result['events'].append({'kind':kind,'unix_ns':time.time_ns(),**details});save()
ledger=None
try:
 for host in hosts:
  checked=host.action('run',argv=['sha256sum',host.binary])
  if checked['code'] or checked['stdout'].split()[0]!=request['binary_sha256']:raise RuntimeError('member binary identity changed')
  host.start()
 deadline=time.monotonic()+600
 while time.monotonic()<deadline:
  try:
   status=hosts[0].status()
   if scale.capacity_ready(status,5):break
  except Exception:pass
  time.sleep(2)
 else:raise RuntimeError('pre-fault cluster did not converge')
 time.sleep(3);scale.validate_persisted_storage(scale.persisted_storage(hosts),'server_paged')
 selected=next(r for r in status['topology']['ranges'].values() if r['leader']=='n4');namespace='vm-reset-'+uuid.uuid4().hex
 keys=[];index=0
 while len(keys)<1000:
  key='probe-'+str(index);index+=1;n=b'__bic_cluster_events';k=(namespace+'/'+key).encode();token=int.from_bytes(hashlib.sha256(b'bicdb-distribution-key-v1'+bytes([0])+len(n).to_bytes(8,'big')+n+len(k).to_bytes(8,'big')+k).digest()[:8],'big')
  if token>=selected['start_token'] and (selected.get('end_token') is None or token<selected['end_token']):keys.append(key)
 route=json.loads(hosts[0].command('cluster','route',hosts[0].db,'__bic_cluster_events',namespace+'/'+keys[0],'--json'))
 if route['targets'][0]['leader_node']!='n4' or route['targets'][0]['range_id']!=selected['id']:raise RuntimeError('pre-fault affected range route changed')
 result.update(namespace=namespace,selected_range=selected,route_before=route,source_topology=status)
 (E/'members.private.json').write_text(json.dumps({'format':1,'members':[{'ssh':h.spec['ssh'],'binary':h.binary,'member_root':h.db} for h in hosts[:4]]}));remote=driver.RemoteEvents(json.loads((E/'members.private.json').read_text()));ledger=driver.Ledger(E/'ledger.jsonl')
 successes=0;stable=0;stable_recorded=False;started=time.monotonic();last_fault=None;calls=[]
 for i,key in enumerate(keys):
  if time.monotonic()-started>900:break
  control=P/'vm-fault-control.private.json'
  if control.exists():
   fault=json.loads(control.read_text());last_fault=fault
  else:fault={}
  before=time.time_ns();answer=driver.append_once(ledger,remote.call,key,namespace,key,{'probe':namespace,'sequence':i,'payload':'x'*1024},0);after=time.time_ns();calls.append({'sequence':i,'started_unix_ns':before,'completed_unix_ns':after,**answer})
  if answer['acknowledged']:successes+=1
  if successes==5 and not (E/'ready-for-poweroff.json').exists():
   event('baseline_five_acks',range_id=selected['id']);(E/'ready-for-poweroff.json').write_text(json.dumps({'range_id':selected['id'],'leader':'n4','unix_ns':after,'member_root':hosts[4].db,'run_root':hosts[4].root,'binary_sha256':request['binary_sha256']}))
  confirmed=fault.get('power_off_confirmed_unix_ns')
  if confirmed and before>=confirmed:
   stable=stable+1 if answer['acknowledged'] else 0
   if answer['acknowledged'] and 'first_post_off_ack_unix_ns' not in result:result['first_post_off_ack_unix_ns']=after
   if stable>=5 and not stable_recorded:
    stable_recorded=True;result['five_post_off_acks_unix_ns']=after;event('five_affected_range_acks_while_victim_off');(E/'stable-recovery.json').write_text(json.dumps({'unix_ns':after,'range_id':selected['id']}))
  result.update(calls=calls,fault_control=last_fault,acknowledged_calls=successes);save()
  if fault.get('bicdb_restart_ready') and stable_recorded and confirmed:
   break
  time.sleep(1)
 ledger.close();ledger=None
 if not last_fault or not last_fault.get('power_off_confirmed_unix_ns'):raise RuntimeError('no confirmed guest power-off evidence')
 # Infra must restart victim guest and archive its stopped DB before this marker.
 deadline=time.monotonic()+600
 while time.monotonic()<deadline:
  if (P/'vm-fault-control.private.json').exists():
   last_fault=json.loads((P/'vm-fault-control.private.json').read_text())
   if last_fault.get('bicdb_restart_ready'):break
  time.sleep(2)
 else:raise RuntimeError('guest restart/archive readiness was not confirmed')
 hosts[4].start();event('victim_bicdb_restart_requested')
 deadline=time.monotonic()+600
 while time.monotonic()<deadline:
  try:
   after=hosts[0].status()
   if scale.capacity_ready(after,5):break
  except Exception:pass
  time.sleep(2)
 else:raise RuntimeError('post-poweroff five-node convergence failed')
 time.sleep(3)
 result['topology_after']=after
 result['route_after']=json.loads(hosts[0].command('cluster','route',hosts[0].db,'__bic_cluster_events',namespace+'/'+keys[0],'--json'))
 result['targeted_ack_verification']=driver.verify(E/'ledger.jsonl',remote.call)
 result['earlier_external_ack_verification']=driver.verify(P/'experiment/external/ledger.jsonl',remote.call)
 native=[]
 for phase in report['results']:
  name=phase['phase'];v=json.loads(hosts[0].command('cluster','events','verify',hosts[0].db,hosts[0].root+'/'+name+'-ledger.json',timeout=600));native.append({'phase':name,**v})
 result['earlier_native_verification']=native;result['fault_control']=last_fault;result['complete']=True;event('all_ack_rechecks_finished')
except Exception as error:result['error']=str(error);save();raise
finally:
 if ledger is not None:ledger.close()
 for host in hosts:
  try:host.action('signal',signal='SIGINT')
  except Exception:pass
 time.sleep(4)
 for host in hosts:
  try:host.action('signal',signal='SIGKILL')
  except Exception:pass
 save()
