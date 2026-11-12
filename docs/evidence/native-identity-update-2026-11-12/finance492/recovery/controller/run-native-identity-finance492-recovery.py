"""Qualify real signed SQL key replacement and its durable audit; copies only."""
import argparse,hashlib,json,os,pathlib,resource,shutil,signal,subprocess,time
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
a=argparse.ArgumentParser();a.add_argument('--source',required=True);a.add_argument('--build',required=True);a.add_argument('--tag',required=True);a.add_argument('--custody');a.add_argument('--runtime',default='0031d1976ece1be2d6139ea9972fbf49069b1590');args=a.parse_args()
root=pathlib.Path('/root/bicdb-durability-s3');out=root/args.tag;out.mkdir(exist_ok=False)
record={'source':args.source,'production_equivalent':args.runtime,'status':'waiting_for_build','started_unix':time.time(),'scope':'ActualNative actor+oneUPDATEgrant throughSQL generatedkey change; originalacknowledgedNativevector/timestamp/payload/unknownmetadata anduntouchedrow preserved exactly. Atomicold/newfullrecords+schema+policy+one matchingaudit againstindependentprecommitNativeTXwitness, 3stableopens/indexes/grantlessrefusals. Processabort+explicitpage/directoryvolatileemulation andactualEIO, notVM/HA/fullapplication. Current-source seedfixtures, not old482.','fixtures':[],'controls':[],'process_cases':[],'io_failure_cases':[]}
def save():(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
def hashes(p):return {str(f.relative_to(p)):{'bytes':f.stat().st_size,'sha256':sha(f)} for f in p.rglob('*') if f.is_file() and not f.is_symlink()}
def env():return {k:v for k,v in os.environ.items() if not k.startswith(('BICDB_TEST_','IDENTITY_','PARTITION_'))}
def messages(s,marker):return [json.loads(x.split(marker+' ',1)[1]) for x in s.splitlines() if x.startswith(marker+' ')]
save()
try:
 if args.custody:
  while True:
   receipt=root/('report-'+args.build+'.json')
   progress=json.loads(receipt.read_text()) if receipt.exists() else {}
   if len(progress.get('steps',[]))>=3 and all(step.get('exit')==0 for step in progress['steps'][:3]) and (root/args.custody).exists():
    build=json.loads((root/args.custody).read_text());assert build['source']==args.source and build['status']=='probe_built' and progress['source']==args.source;break
   assert subprocess.check_output(['systemctl','show','bicdb-'+args.build,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading'],progress
   time.sleep(3)
  record['scope']='Versionedfinance492candidateCore-onlyNativeidentityUPDATE withactualmain490finance issuercontracts and6implementedreaderrequirements; currentcandidateexactsource, no privateSQLpartition/generated-columncode. Original4seedACKs/fullNativeattributes/indexdefinitions+policy, oneactualUPDATEgrant+audit boundtoindependentNativeTXwitness; exact38processcuts+8EIO and3stableopens. NotVM/HA/fullapplication. FullCore/SQLauthority/CI separate.'
 else:
  while subprocess.check_output(['systemctl','show','bicdb-'+args.build,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(3)
  build=json.loads((root/('report-'+args.build+'.json')).read_text());assert build['source']==args.source and build['status']=='focused_passed',build
 binary=pathlib.Path(build['binary']);assert sha(binary)==build['binary_sha256'];record.update(status='running',binary=str(binary),binary_sha256=build['binary_sha256']);save()
 def run(argv,name,e=None):
  try:r=subprocess.run([str(binary),*map(str,argv)],capture_output=True,text=True,env=e or env(),timeout=60)
  except subprocess.TimeoutExpired as error:
   def text(b):return b.decode() if isinstance(b,bytes) else b or ''
   r=subprocess.CompletedProcess(argv,124,text(error.stdout),text(error.stderr)+'\nowned_probe_60s_timeout')
  (out/(name+'.stdout')).write_text(r.stdout);(out/(name+'.stderr')).write_text(r.stderr);return r
 def verify(mode,dest,before,started,name,ack=None,require_new=False):
  e=env();e['IDENTITY_EXPECTED_TX_ID']=str(started['native_tx_id'])
  if ack is not None:e['IDENTITY_ACK']=str(ack)
  if require_new:e['IDENTITY_REQUIRE_NEW']='1'
  r=run([mode,'verify',dest,before],name,e);obs=messages(r.stdout,'IDENTITY_VERIFIED')
  if r.returncode==0:
   assert len(obs)==3 and [x['reopen'] for x in obs]==[0,1,2] and len({x['state'] for x in obs})==1
   assert all(x['full_records_schema_policy_equal'] and x['native_indexes_valid'] and x['grantless_write_refused'] for x in obs)
   assert all(x['audit_messages']==obs[0]['audit_messages'] and x['row_hashes']==obs[0]['row_hashes'] for x in obs)
  return r,obs
 for mode in ['embedded_memory','server_paged']:
  seed_dir=out/(mode+'-seed');seed_dir.mkdir();db=seed_dir/'db';before=seed_dir/'original-ACK.json';r=run([mode,'seed',db,before],mode+'-seed');assert r.returncode==0,r.stderr
  witness=messages(r.stdout,'IDENTITY_SEED_ACK');assert len(witness)==1 and len(witness[0]['acknowledged_rows'])==2
  fixture={'mode':mode,'database':str(db),'original_ACK':str(before),'original_ACK_sha256':sha(before),'ACKs':witness[0]['acknowledged_rows'],'files':hashes(db)};record['fixtures'].append(fixture);save()
  control=out/(mode+'-control-db');shutil.copytree(db,control);ack=out/(mode+'-control-ACK.json');r=run([mode,'apply',control,ack],mode+'-control-apply');assert r.returncode==0,r.stderr
  started=messages(r.stdout,'IDENTITY_TX_STARTED');assert len(started)==1;started=started[0];accepted=json.loads(ack.read_text());assert accepted['update_acknowledged'] is True and accepted['native_tx_id']==started['native_tx_id'];span=accepted['after_native_sequence']-accepted['before_native_sequence'];assert span==1,span
  r,obs=verify(mode,control,before,started,mode+'-control-verify',ack,True);assert r.returncode==0,r.stderr
  record['controls'].append({'mode':mode,'database':str(control),'ack':str(ack),'ack_sha256':sha(ack),'native_commit_span':span,'tx_started':started,'observations':obs});save()
  points=[('crash',x+':1') for x in ['identity.after_staging','identity.after_commit','identity.after_ack','commit.before_broker_publish','commit.after_broker_publish']]+[('io',x+':1') for x in ['transaction_log.write','transaction_log.sync']]
  if mode=='server_paged':points += [('crash','commit.'+x+':1') for x in ['before_decision','after_txlog_durable','after_page_apply','after_marker','after_sync_publish']]
  for volatile in [False,True]:
   for kind,point in points:
    name=mode+'-'+('volatile-' if volatile else 'process-')+point.replace('.','_').replace(':','-');dest=out/(name+'-db');shutil.copytree(db,dest);ack=out/(name+'-ACK.json');e=env();e['IDENTITY_CRASH_IO' if kind=='io' else 'BICDB_TEST_CRASH_AT']=point
    if volatile:e.update(BICDB_TEST_WAL_VOLATILE_TAIL='1',BICDB_TEST_DIR_VOLATILE='1')
    cut=run([mode,'apply',dest,ack],name+'-cut',e);reached=cut.returncode==-signal.SIGABRT and 'bicdb test crash point reached' in cut.stderr;started=messages(cut.stdout,'IDENTITY_TX_STARTED');assert len(started)==1;started=started[0]
    require_new=point.split(':')[0] in ['identity.after_commit','identity.after_ack','commit.before_broker_publish','commit.after_broker_publish','commit.after_txlog_durable','commit.after_page_apply','commit.after_marker','commit.after_sync_publish']
    item={'mode':mode,'kind':kind,'point':point,'volatile_page_and_directory_emulation':volatile,'database':str(dest),'tx_started':started,'cut_exit':cut.returncode,'cut_reached':reached,'acknowledged':ack.exists(),'requires_complete_new':require_new,'passed':False}
    if reached:
     r,obs=verify(mode,dest,before,started,name+'-verify',ack,require_new);item.update(verify_exit=r.returncode,observations=obs)
     item['passed']=r.returncode==0 and (point!='identity.after_staging:1' or all(x['state']=='complete_old' for x in obs)) and item['acknowledged']==(point=='identity.after_ack:1')
    record['process_cases'].append(item);save()
  for persistent in [False,True]:
   for site in ['transaction_log.write','transaction_log.sync']:
    name=mode+'-'+('persistent-' if persistent else 'one-shot-')+site.replace('.','_');dest=out/(name+'-db');shutil.copytree(db,dest);failure=out/(name+'-failure.json');e=env();e.update(IDENTITY_FAIL_SITE=site,IDENTITY_FAIL_NTH='1',IDENTITY_FAIL_PERSISTENT=str(int(persistent)))
    r=run([mode,'io_fail',dest,failure],name+'-failure',e);started=messages(r.stdout,'IDENTITY_TX_STARTED');assert len(started)==1;started=started[0];report=json.loads(failure.read_text()) if failure.exists() else None
    reached=r.returncode==0 and report is not None and report.get('fault_fired',0)>0 and report.get('update_acknowledged') is False and report.get('after_failure_write_refused') is True
    item={'mode':mode,'site':site,'persistent':persistent,'database':str(dest),'tx_started':started,'failure_exit':r.returncode,'fault_reached':reached,'failure_report':report,'passed':False}
    if reached:
     r,obs=verify(mode,dest,before,started,name+'-verify');item.update(verify_exit=r.returncode,observations=obs,passed=r.returncode==0)
    record['io_failure_cases'].append(item);save()
  assert hashes(db)==fixture['files'] and sha(before)==fixture['original_ACK_sha256']
 assert len(record['process_cases'])==38 and len(record['io_failure_cases'])==8
 record.update(status='recovery_passed' if all(c['passed'] for c in record['process_cases']+record['io_failure_cases']) else 'recovery_failed',finished_unix=time.time(),originals_unchanged=True)
except BaseException as error:record.update(status='recovery_harness_failed',error=repr(error),finished_unix=time.time())
save();print(json.dumps({k:v for k,v in record.items() if k not in ['process_cases','io_failure_cases','fixtures','controls']}));raise SystemExit(0 if record['status']=='recovery_passed' else 1)
