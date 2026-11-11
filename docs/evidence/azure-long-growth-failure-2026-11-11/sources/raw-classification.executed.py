import concurrent.futures,hashlib,importlib.util,json,math,os,pathlib,tarfile,time
P=pathlib.Path('/tmp/bicdb-azure-run');B=P/'long-comparison';D=B/'raw-baseline-sample-classification';D.mkdir(mode=0o700,exist_ok=False);os.environ['PATH']=str(P/'bin')+':'+os.environ['PATH']
spec=importlib.util.spec_from_file_location('scale',B/'scale-cluster.frozen.py');scale=importlib.util.module_from_spec(spec);spec.loader.exec_module(scale);r=json.loads((B/'growth/report.json').read_text());inventory=json.loads((P/'inventory.private.json').read_text());request=json.loads((P/'run-request.private.json').read_text());host=scale.Host(inventory['hosts'][0],0,r['run'],request['binary'],False,'server_paged',268435456);check=json.loads((B/'post-failure-diagnosis/all-ack-ledger-checks.private.json').read_text())[0];samples=json.loads(check['response']['stdout'])['unverified_key_samples'];baseline=json.loads((B/'growth/steady-4-ledger.json').read_text());expected={'run':baseline['run'],'payload':'x'*baseline['payload_bytes']}
with tarfile.open(B/'diagnostic-databases.private/node-0.tar.gz') as a:transport=json.load(a.extractfile('db/bicdb-distribution.json'))['transport']
launch=time.time_ns();deadline=time.monotonic()+180
(D/'launch.json').write_text(json.dumps({'started_unix_ns':launch,'helper_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'harness_sha256':hashlib.sha256((B/'scale-cluster.frozen.py').read_bytes()).hexdigest(),'binary_sha256':request['binary_sha256'],'overall_dispatch_and_completion_budget_seconds':180,'maximum_parallelism':4,'new_application_event_writes':False,'client_path':'fresh CLI cluster events get using node0 persisted member context; no explicit refresh override','persisted_get_connect_timeout_ms':transport['connect_timeout_ms'],'persisted_get_io_timeout_ms':transport['io_timeout_ms'],'verify_rpc_io_timeout_ms':1000,'overlap':'concurrent with diagnostic growth1223 full-ledger verification; adds diagnostic SSH/process/read load','per_command_timeout_seconds_max':30,'ssh_timeout_grace_seconds':20,'sample_scope':'only retained20 baseline111unverified samples; cannot classify omitted91 or retroactively resolve original failure'},indent=2)+'\n')
def call(key):
 row={'key':key,'node_context':0,'started_unix_ns':time.time_ns()};remaining=deadline-time.monotonic()
 if remaining<=25:return dict(row,classification='not_dispatched_budget_unknown',completed_unix_ns=time.time_ns())
 timeout=min(30,max(1,math.floor(remaining-25)));row['remote_cli_timeout_seconds']=timeout
 try:
  response=host.action('run',argv=[*host.engine_env,host.binary,'cluster','events','get',host.db,baseline['namespace'],key],timeout=timeout);row['response']=response
  if response['code']!=0:kind='cli_error'
  else:
   value=json.loads(response['stdout'])['value'];kind='exact_match' if value==expected else ('absent_at_observation' if value is None else 'mismatch_at_observation')
  row['classification']=kind
 except Exception as error:row.update(error=str(error),classification='timeout_unknown' if 'timed out' in str(error).lower() or 'timeoutexpired' in str(error).lower() else 'transport_or_helper_error')
 row['completed_unix_ns']=time.time_ns();return row
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:rows=list(pool.map(call,samples))
(D/'results.private.json').write_text(json.dumps(rows,indent=2)+'\n');summary={'started_unix_ns':launch,'completed_unix_ns':time.time_ns(),'sample_count':len(samples),'classifications':{name:sum(v['classification']==name for v in rows) for name in sorted({v['classification'] for v in rows})},'new_application_event_writes':False,'scope':'fresh Get observations during ongoing recovery and diagnostic growth ledger verification, not original111failure classification'};(D/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
