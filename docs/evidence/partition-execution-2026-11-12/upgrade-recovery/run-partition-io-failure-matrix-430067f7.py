import pathlib,json,subprocess,time,hashlib,shutil,os,sys,signal,resource
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
root=pathlib.Path('/root/bicdb-durability-s3');label='partition-old-io-failure-probe-build';unit='bicdb-'+label
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
build=json.loads((root/('report-'+label+'.json')).read_text());assert build['status']=='focused_passed',build
binary=pathlib.Path(build['binary']);assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==build['binary_sha256']
old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());ledger=[json.loads(x) for x in (old/'ack-ledger.jsonl').read_text().splitlines()];out=root/'partition-io-failure-matrix-430067f7';out.mkdir(exist_ok=False)
record={'status':'running','source':build['source'],'production_equivalent':'6c16e66e742bb575041aebce4086ebf434b9fe64','binary':str(binary),'binary_sha256':build['binary_sha256'],'started_unix':time.time(),'scope':'Disposable genuineold482parentheap/badbound/independentpolicy copies; all14sourceACKs independentlylinked to original14ACKledger and oldfilemanifest; bothreviewedpolicychoices. Countsareoverlapping invocations, notuniqueACKs. FullNativeold/newrows+rawschemassnapshots, actualfencewrite/sync/rename/dirsync andNativepublicationtransactionlogwrite/syncfailures, 3reopens+Nativeindexes+oldreaderbyteidentity. ActualEIO one-shotandpersistent scopedafteropen. Requiresfaultfired+applyErr/noACK; dropsinjectionandrequiresfailedhandle refuseslegitimateNativewrite. Independentfreshprocesswholeold/newverification,notVM/HA/fullapplication.','controls':[],'cases':[]}
def save():(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
def hashes(path):return {str(p.relative_to(path)):{'bytes':p.stat().st_size,'sha256':hashlib.file_digest(p.open('rb'),'sha256').hexdigest()} for p in path.rglob('*') if p.is_file() and not p.is_symlink()}
def verify_original(f):assert hashes(pathlib.Path(f['directory'])/'db')==f['files']
def cleanenv():return {k:v for k,v in os.environ.items() if not k.startswith('BICDB_TEST_') and not k.startswith('PARTITION_CRASH_')}
def run(argv,name,env=None):
 
 try:r=subprocess.run([str(binary),*map(str,argv)],capture_output=True,text=True,env=env or cleanenv(),timeout=60)
 except subprocess.TimeoutExpired as error:r=subprocess.CompletedProcess(argv,124,(error.stdout or b'').decode() if isinstance(error.stdout,bytes) else (error.stdout or ''),((error.stderr or b'').decode() if isinstance(error.stderr,bytes) else (error.stderr or ''))+'\ncontroller_owned_probe_60s_timeout')
 (out/(name+'.stdout')).write_text(r.stdout);(out/(name+'.stderr')).write_text(r.stderr);return r
save()
try:
 for mode,case,original_case in [(m,c,o) for m in ['embedded_memory','server_paged'] for c,o in [('parent_heap','parent_heap'),('bad_child_bound','bad_child_bound'),('policy_keep_stored','independent_child_policy'),('policy_preserve_historical','independent_child_policy')]]:
  prefix=mode+'-'+case
  fixture=next(f for f in fixtures['cases'] if f['mode']==mode and f['name']==original_case);verify_original(fixture);control=out/(prefix+'-control-db');shutil.copytree(pathlib.Path(fixture['directory'])/'db',control)
  before=out/(prefix+'-old.json');after=out/(prefix+'-new.json');ack=out/(prefix+'-control-ack.json')
  r=run([mode,'snapshot',control,before],prefix+'-capture-old');assert r.returncode==0,r.stderr
  original=json.loads(before.read_text());acks=[x for x in ledger if x['mode']==mode and x['case']==original_case];assert len(acks)==len(original['rows']) and len(acks)>0
  for a in acks:
   row=next(x for x in original['rows'] if x[0]==a['relation'] and x[1]==str(a['k']));assert row[3]['metadata']['payload']==a['payload'];assert row[3]['id']==str(a['k'])
  r=run([mode,'apply',control,case,ack],prefix+'-control-apply');assert r.returncode==0,r.stderr
  r=run([mode,'snapshot',control,after],prefix+'-capture-new');assert r.returncode==0,r.stderr
  control_ack=json.loads(ack.read_text());span=control_ack['after_native_sequence']-control_ack['before_native_sequence'];assert 0<span<=8,span
  env=cleanenv();env['PARTITION_CRASH_ACK']=str(ack);r=run([mode,'verify',control,before,after],prefix+'-control-verify',env);assert r.returncode==0,r.stderr
  record['controls'].append({'mode':mode,'case':case,'old_snapshot':before.name,'new_snapshot':after.name,'ack':control_ack,'native_commit_span':span,'original_ACKs_linked':len(acks)});save()
  cuts=[('iofailure',site+':1') for site in ['sql_partition.format_fence.write','sql_partition.format_fence.sync','sql_partition.format_fence.rename','sql_partition.format_fence.dir_sync']]+[('iofailure',site+':'+str(n)) for site in ['transaction_log.write','transaction_log.sync'] for n in range(1,span+1)]
  for volatile in [False,True]:
   for kind,point in cuts:
    name=prefix+'-'+('persistent-' if volatile else 'one-shot-')+kind+'-'+point.replace('.','_').replace(':','-');dest=out/(name+'-db');shutil.copytree(pathlib.Path(fixture['directory'])/'db',dest);ack=out/(name+'-failure.json');env=cleanenv();site,nth=point.rsplit(':',1);env.update(PARTITION_FAIL_SITE=site,PARTITION_FAIL_NTH=nth,PARTITION_FAIL_PERSISTENT='1' if volatile else '0')
    crash=run([mode,'io_fail',dest,case,ack],name+'-failure',env);failure=json.loads(ack.read_text()) if ack.exists() else None;reached=crash.returncode==0 and failure is not None and failure.get('upgrade_acknowledged') is False and failure.get('after_failure_write_refused') is True
    item={'mode':mode,'kind':kind,'point':point,'persistent_fault':volatile,'failure_exit':crash.returncode,'fault_reached':reached,'upgrade_acknowledged':False if failure else None,'failure_report':failure,'database_copy':str(dest)}
    if reached:
     env=cleanenv();verified=run([mode,'verify',dest,before,after],name+'-verify',env);item.update(verify_exit=verified.returncode)
     if verified.returncode==0:
      observations=[json.loads(line) for line in verified.stdout.splitlines() if line.startswith('{')];assert len(observations)==3;item['observations']=observations
      if any(o['reader_fence'] for o in observations):
       before_reader=hashes(dest);reader=subprocess.run([fixtures['binary'],'sql',str(dest),'SELECT k,payload FROM p ORDER BY k','--json'],capture_output=True,text=True,env=cleanenv());after_reader=hashes(dest);(out/(name+'-old-reader.stderr')).write_text(reader.stderr);item.update(old_reader_exit=reader.returncode,old_reader_refused=reader.returncode!=0,old_reader_byte_identity=before_reader==after_reader)
    item['passed']=reached and item.get('verify_exit')==0 and (not item.get('observations') or not any(o['reader_fence'] for o in item['observations']) or (item.get('old_reader_refused') and item.get('old_reader_byte_identity')))
    record['cases'].append(item);save()
  verify_original(fixture)
 record.update(status='io_failure_matrix_passed' if all(c['passed'] for c in record['cases']) else 'io_failure_matrix_failed',finished_unix=time.time(),originals_unchanged=True)
except BaseException as error:record.update(status='io_failure_matrix_harness_failed',error=repr(error),finished_unix=time.time())
save();print(json.dumps({k:v for k,v in record.items() if k not in ['cases','controls']}));print('cases',len(record['cases']),'passed',sum(c['passed'] for c in record['cases']));sys.exit(0 if record['status']=='io_failure_matrix_passed' else 1)
