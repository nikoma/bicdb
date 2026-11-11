import base64,copy,hashlib,json,os,pathlib,signal,socket,struct,subprocess,time,uuid
ROOT=pathlib.Path('/tmp/bicdb-protocol19-compat-evidence')
RUN=ROOT/('run-'+uuid.uuid4().hex);RUN.mkdir(mode=0o700)
BIN=pathlib.Path('/tmp/bicdb-azure-artifact-463-evidence/bicdb-1.0.463-beta-d49a391b')
SHA='840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d'
DB=RUN/'db';CLUSTER='protocol19-compat-'+uuid.uuid4().hex[:12]
env={k:v for k,v in os.environ.items() if not k.startswith('BICDB_')}
def persist(name,data):
 p=RUN/name;b=json.dumps(data,indent=2).encode()+b'\n';p.write_bytes(b)
 with p.open('rb') as f:os.fsync(f.fileno())
 return p
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def state(cut):
 p=DB/'cluster-metadata-consensus.json';raw=p.read_bytes();(RUN/(cut+'.metadata.raw.json')).write_bytes(raw)
 top=DB/'cluster-topology.json'
 if top.exists():(RUN/(cut+'.topology.raw.json')).write_bytes(top.read_bytes())
 value=json.loads(raw);return value,hashlib.sha256(raw).hexdigest()
def recv_exact(s,n):
 out=b''
 while len(out)<n:
  b=s.recv(n-len(out))
  if not b:raise RuntimeError('unexpected EOF')
  out+=b
 return out
def call(label,version,request):
 body=json.dumps({'protocol_version':version,'cluster_id':CLUSTER,'caller_node_id':'n1','destination_node_id':'n1','request_id':len(calls)+1,'request':request},separators=(',',':')).encode();wire=struct.pack('>I',len(body))+body
 before=time.time_ns()
 with socket.create_connection(('127.0.0.1',RPC),timeout=2) as s:
  s.settimeout(2);s.sendall(wire);head=recv_exact(s,4);size=struct.unpack('>I',head)[0];assert 0<size<=2*1024*1024;response=recv_exact(s,size)
 after=time.time_ns();(RUN/(label+'.request.bin')).write_bytes(wire);(RUN/(label+'.response.bin')).write_bytes(head+response)
 decoded=json.loads(response);row={'label':label,'protocol':version,'started_unix_ns':before,'ended_unix_ns':after,'request':json.loads(body),'response':decoded,'request_sha256':hashlib.sha256(wire).hexdigest(),'response_sha256':hashlib.sha256(head+response).hexdigest()};calls.append(row);persist('calls.json',calls);return decoded
(RUN/'executed-runner.py').write_bytes(pathlib.Path(__file__).read_bytes())

def interrupted(signum,frame):raise TimeoutError('bounded compatibility helper interrupted')
signal.signal(signal.SIGALRM,interrupted);signal.signal(signal.SIGTERM,interrupted);signal.alarm(90)
calls=[];server=None;report={'scope':'fresh single-process loopback protocol compatibility; synthetic self-addressed Append; no TLS/physical/HA qualification','run_root':str(RUN),'binary':str(BIN),'expected_binary_sha256':SHA,'source_commit':'d49a391b3c6157f90f29e9f2f5214143c60f9f4b','started_unix_ns':time.time_ns()};teardown={}
try:
 available=int(next(x.split()[1] for x in pathlib.Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024
 if available<12*1024**3:raise RuntimeError('less than12GiB RAM available; refusing fixture')
 report['binary_sha256']=digest(BIN);assert report['binary_sha256']==SHA
 version=subprocess.run([str(BIN),'--version'],env=env,capture_output=True,text=True,timeout=10);assert version.returncode==0;assert version.stdout.strip()=='bicdb 1.0.463-beta';report['version_stdout']=version.stdout
 RPC=port();SQL=port();assert RPC!=SQL
 report['rpc_address']=['127.0.0.1',RPC];report['sql_address']=['127.0.0.1',SQL]
 init=[str(BIN),'cluster','init',str(DB),'--cluster-id',CLUSTER,'--node-id','n1','--address',f'127.0.0.1:{RPC}','--replication-factor','1','--initial-ranges','1','--capacity-bytes','268435456','--application-events','--json']
 done=subprocess.run(init,env=env,capture_output=True,timeout=30);(RUN/'init.stdout').write_bytes(done.stdout);(RUN/'init.stderr').write_bytes(done.stderr);report['init_argv']=init;report['init_exit_code']=done.returncode;assert done.returncode==0
 argv=[str(BIN),'serve',str(DB),'--host','127.0.0.1','--port',str(SQL),'--cluster-controller','balanced'];report['server_argv']=argv
 out=(RUN/'server.stdout').open('wb');err=(RUN/'server.stderr').open('wb');server=subprocess.Popen(argv,env=env,stdout=out,stderr=err,start_new_session=True);report['pid']=server.pid;report['pgid']=os.getpgid(server.pid);persist('started.json',report)
 deadline=time.monotonic()+30
 while True:
  if server.poll() is not None:raise RuntimeError('server exited before healthy metadata voter')
  try:
   raw=json.loads((DB/'cluster-metadata-consensus.json').read_bytes());s=raw['state']
   if s['role']=='leader' and s['commit_index']>0:
    with socket.create_connection(('127.0.0.1',RPC),timeout=.1):pass
    break
  except (FileNotFoundError,ConnectionError,socket.timeout,json.JSONDecodeError):pass
  if time.monotonic()>deadline:raise TimeoutError('bounded metadata-voter startup expired')
  time.sleep(.05)
 ping=call('01-v19-ping',19,'Ping');assert ping['response']=={'Ok':'Pong'}
 baseline,before_hash=state('before-v20');s=baseline['state'];term=s['current_term'];idx=s['base_index']
 topology=copy.deepcopy(s['committed_topology']);domain=str(uuid.uuid4());topology['range_image_bootstrap_id']=domain
 append={'MetadataAppend':{'request':{'cluster_id':CLUSTER,'term':term,'leader_id':'n1','prev_log_index':idx,'prev_log_term':s['base_term'],'entries':[],'leader_commit':s['commit_index'],'snapshot':{'last_included_index':idx,'last_included_term':s['base_term'],'topology':topology}}}}
 refusal=call('02-v20-append',20,append)
 message=refusal['response']['Err']['message'];assert 'unsupportedclusterdataprotocol20' in message.replace(' ','');assert 'expected19' in message.replace(' ','');assert 'MetadataAppend' not in refusal['response'].get('Ok',{})
 after,after_hash=state('after-v20');report['v20_header_refusal']='PASS';report['metadata_bytes_identical_across_v20']=before_hash==after_hash;report['before_v20_sha256']=before_hash;report['after_v20_sha256']=after_hash;report['concurrent_metadata_change_observed']=before_hash!=after_hash
 accepted=call('03-v19-same-append',19,append);after19,h19=state('after-v19');ok=accepted['response'].get('Ok',{}).get('MetadataAppend',{})
 if ok.get('success') is True:
  retained=after19['state']['committed_topology'];assert 'range_image_bootstrap_id' not in retained
  report['v19_append_ack_and_domain_drop']='PASS';report['synthetic_domain_uuid']=domain;report['v19_response_match_index']=ok.get('match_index');report['v19_state_commit_index']=after19['state']['commit_index'];report['v19_domain_retained']=False
 else:report['v19_append_ack_and_domain_drop']='NOT_TESTED_PRECONDITION_OR_CONCURRENT_CHANGE';report['v19_non_ack_response']=accepted
 ping=call('04-v19-ping-after',19,'Ping');assert ping['response']=={'Ok':'Pong'}
 report['healthy_v19_control']='PASS';report['outcome']='PASS'
except BaseException as e:
 report['outcome']='FAIL';report['error']={'type':type(e).__name__,'message':str(e)}
finally:
 if server is not None:
  teardown={'pid':server.pid,'pgid':report['pgid'],'stop_requested_unix_ns':time.time_ns(),'signal':'SIGTERM'}
  if server.poll() is None:
   os.killpg(server.pid,signal.SIGTERM)
   try:server.wait(timeout=15)
   except subprocess.TimeoutExpired:
    teardown['fallback_signal']='SIGKILL';os.killpg(server.pid,signal.SIGKILL);server.wait(timeout=5)
  teardown['exit_code']=server.returncode;teardown['reaped_unix_ns']=time.time_ns();out.close();err.close();teardown['proc_pid_absent']=not pathlib.Path(f'/proc/{server.pid}').exists()
  for name,p in [('rpc',RPC),('sql',SQL)]:
   with socket.socket() as s:s.settimeout(.2);teardown[name+'_connection_result']=s.connect_ex(('127.0.0.1',p))
 report['ended_unix_ns']=time.time_ns();report['teardown']=teardown;report['runner_sha256']=digest(RUN/'executed-runner.py');signal.alarm(0);persist('report.json',report)
 files=[p for p in RUN.rglob('*') if p.is_file()];manifest={str(p.relative_to(RUN)):digest(p) for p in files};persist('manifest.json',manifest)
 for path in files:
  with path.open('rb') as f:os.fsync(f.fileno())
 for directory in sorted([RUN,*[p for p in RUN.rglob('*') if p.is_dir()]],key=lambda p:len(p.parts),reverse=True):
  fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(fd)
  finally:os.close(fd)
 print(json.dumps({'run_root':str(RUN),'outcome':report['outcome'],'error':report.get('error'),'teardown':teardown}))
 if report['outcome']!='PASS':raise SystemExit(1)
