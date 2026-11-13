import pathlib,json,subprocess,time,hashlib,sys
root=pathlib.Path('/root/bicdb-durability-s3');unit='bicdb-partition-legacy-qualification-6c16e66e'
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
source='6c16e66e742bb575041aebce4086ebf434b9fe64';out=root/'report-partition-legacy-qualification-6c16e66e-verified.json';record={'source':source,'status':'verification_failed','recorded_unix':time.time(),'scope':'Independently verify canonical percase results, not only wrapper process exits. Actualold482immutablefilemanifest+14sourceACKs, default8/explicit8/entry-startup14, originalcaseassertions/reopens/content/indexes/oldreaderbyteidentity. Not fullapplication/fault/VM/HA.','matrices':[]}
try:
 build=json.loads((root/'report-partition-upgrade-all-acks-probes.json').read_text());assert build['source']==source and build['status']=='focused_passed'
 for item in [build,*build['additional_binaries'].values()]:
  assert hashlib.file_digest(pathlib.Path(item['binary']).open('rb'),'sha256').hexdigest()==item['binary_sha256']
 expected=[('partition-old-store-baseline-6c16e66e','all_requirements_passed',8),('partition-upgrade-api-6c16e66e','explicit_apply_passed',8),('partition-entry-startup-6c16e66e','entry_startup_passed',14)]
 for folder,status,count in expected:
  result=json.loads((root/folder/'result.json').read_text());assert result['source']==source and result['status']==status and len(result['cases'])==count,(folder,result.get('status'),len(result.get('cases',[])))
  assert result.get('originals_unchanged') is True
  for case in result['cases']:
   if case.get('case')=='old_reader_fence':assert case['candidate_exit']==0 and case['old_reader_exit']!=0 and case['byte_identity_preserved'] is True and case['changed_files']==[]
   else:assert case['exit']==0,case
   if 'old_reader_refused' in case:assert case['old_reader_refused'] is True and case['byte_identity_preserved'] is True and case['changed_files']==[],case
  record['matrices'].append({'path':str(root/folder/'result.json'),'status':status,'cases':count,'sha256':hashlib.file_digest((root/folder/'result.json').open('rb'),'sha256').hexdigest()})
 old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());assert sum(1 for _ in (old/'ack-ledger.jsonl').open())==14
 for case in fixtures['cases']:
  path=pathlib.Path(case['directory'])/'db';actual={str(p.relative_to(path)):{'bytes':p.stat().st_size,'sha256':hashlib.file_digest(p.open('rb'),'sha256').hexdigest()} for p in path.rglob('*') if p.is_file() and not p.is_symlink()};assert actual==case['files']
 record.update(status='legacy_controls_verified_passed',total_case_invocations=30,independent_old_ACKs=14,originals_unchanged=True)
except BaseException as error:record['error']=repr(error)
out.write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record));sys.exit(0 if record['status']=='legacy_controls_verified_passed' else 1)
