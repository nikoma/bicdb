import concurrent.futures,hashlib,importlib.util,json,os,pathlib,time,traceback
P=pathlib.Path('/tmp/bicdb-azure-run');E=P/'long-comparison';D=E/'post-failure-diagnosis';D.mkdir(mode=0o700,exist_ok=False)
os.environ['PATH']=str(P/'bin')+':'+os.environ['PATH']
frozen=E/'scale-cluster.frozen.py';spec=importlib.util.spec_from_file_location('scale',frozen);scale=importlib.util.module_from_spec(spec);spec.loader.exec_module(scale)
r=json.loads((E/'growth/report.json').read_text());failed=json.loads((E/'failed-growth-workload.json').read_text());request=json.loads((P/'run-request.private.json').read_text());inventory=json.loads((P/'inventory.private.json').read_text());receipt=json.loads((E/'diagnostic-databases.private/receipt.json').read_text());assert len(receipt)==5 and all(v['status']=='exported' and v['sha256']==v['remote_sha256'] and v['owned_engine_stopped_before_archive'] for v in receipt)
hosts=[scale.Host(h,i,r['run'],request['binary'],False,'server_paged',268435456) for i,h in enumerate(inventory['hosts'])];summary={'original_failure_preserved':True,'recovery_converged':False,'new_writes':False};started=[]
def save(name,value):(D/name).write_text(json.dumps(value,indent=2)+'\n')
save('launch.json',{'unix_ns':time.time_ns(),'helper_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'harness_sha256':hashlib.sha256(frozen.read_bytes()).hexdigest(),'binary_sha256':request['binary_sha256'],'run':r['run'],'purpose':'bounded persisted-cluster restart and known ACK reads after five cold exports'})
try:
 for host in hosts:
  assert not host.action('sample').get('running'),'refuse duplicate engine launch'
  digest=host.action('run',argv=['sha256sum',host.binary]);assert digest['code']==0 and digest['stdout'].split()[0]==request['binary_sha256'],'binary identity mismatch'
  host.start();started.append(host)
 summary['restart_complete_unix_ns']=time.time_ns();observations=[];deadline=time.monotonic()+300
 while time.monotonic()<deadline:
  row={'started_unix_ns':time.time_ns()}
  try:
   status=hosts[0].status();row.update(completed_unix_ns=time.time_ns(),metrics=status['metrics'],metadata_consensus=status.get('metadata_consensus'));save('latest-topology.private.json',status)
   if scale.capacity_ready(status,5):summary['recovery_converged']=True;observations.append(row);break
  except Exception as error:row.update(completed_unix_ns=time.time_ns(),error=str(error))
  observations.append(row);save('convergence-observations.json',observations);time.sleep(3)
 save('convergence-observations.json',observations);summary['recovery_observation_completed_unix_ns']=time.time_ns()
 expected={'run':failed['run'],'payload':'x'*failed['payload_bytes']}
 def point(pair):
  host,key=pair;row={'node':host.index,'key':key,'started_unix_ns':time.time_ns()}
  try:
   response=host.action('run',argv=[*host.engine_env,host.binary,'cluster','events','get',host.db,failed['namespace'],key],timeout=180);row.update(response=response,completed_unix_ns=time.time_ns());row['matches_expected']=response['code']==0 and json.loads(response['stdout']).get('value')==expected
  except Exception as error:row.update(error=str(error),completed_unix_ns=time.time_ns(),matches_expected=False)
  return row
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:points=list(pool.map(point,[(h,k) for k in failed['unverified_writes'] for h in hosts]))
 save('original-unverified-point-reads.private.json',points);summary['original_unverified_point_reads_matching']=sum(x['matches_expected'] for x in points);summary['original_unverified_point_reads_total']=len(points)
 checks=[]
 for phase in ['steady-4','growth-4-to-5']:
  row={'phase':phase,'started_unix_ns':time.time_ns()}
  try:row['response']=hosts[0].action('run',argv=[*hosts[0].engine_env,hosts[0].binary,'cluster','events','verify',hosts[0].db,hosts[0].root+'/'+phase+'-ledger.json'],timeout=1200)
  except Exception as error:row['error']=str(error)
  row['completed_unix_ns']=time.time_ns();checks.append(row);save('all-ack-ledger-checks.private.json',checks)
 summary['ledger_checks_completed']=len(checks)
except Exception as error:summary.update(error=str(error),traceback=traceback.format_exc())
finally:
 cleanup=[]
 for host in started:
  row={'node':host.index}
  try:
   host.action('signal',signal='SIGINT');time.sleep(4)
   if host.action('sample').get('running'):host.action('signal',signal='SIGKILL')
   row['stopped']=not host.action('sample').get('running')
  except Exception as error:row['error']=str(error)
  cleanup.append(row)
 summary['cleanup']=cleanup;summary['completed_unix_ns']=time.time_ns();save('summary.json',summary);print(json.dumps(summary),flush=True)
