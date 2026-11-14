#!/usr/bin/env python3
"""Verify only retained logical evidence and hashes; never reopen a DB or mint grants."""
import argparse,copy,gzip,hashlib,json,pathlib,re,runpy,subprocess,sys,zipfile
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit("Run without Python -O; assertions are required")
SOURCE='d6908095e0d347e563ef68801f49e52dba35452d'
BASE_CASES={'TargetNativeBase','TargetNativeBaseCanceled','TargetNativeBaseWithoutFinal','TargetNativeBaseInvalidated'}
def sha(data):return hashlib.sha256(data).hexdigest()
def check_base(case,d,extra=None):
 if case=='TargetNativeBaseWithoutFinal':
  assert 'requires actual final image receipt' in d['exact_error'] and d['no_native_floor_or_serving']
  assert d['owner_before']==d['owner_after'] and d['owner_after']['native_base'] is None
  assert d['before']['points']['1']['native_base_index'] is None
  return {'classification':'passed','native_base_published':False,'expected_refusal':d['exact_error']}
 native=d['actual_native'];b=native['base'];assert b==native['archive']
 assert b['format_version']==2 and b['range_index']==b['history_verified_through']==1
 assert b['native_page_xid']==21 and b['registered_final_image']==d['expected_final_pointer'] and b['registered_final_image']['native_page_xid']==20
 assert b['attempt_id']==b['registered_final_image']['attempt_id'] and b['target_node_id']=='n4'
 assert b['record_count']==1 and b['serialized_record_bytes']==177
 assert d['first_progress']==d['second_progress'] and d['first_progress']['NativeBase']['native_page_xid']==b['native_page_xid']
 assert d['native_floor_published'] and not d['catalogue_floor_adopted'] and not d['serving'] and not d['cold_recovery_or_physical_qualification']
 assert d['before']['B']==d['after']['B']==1 and not d['after']['writer_installed']
 assert d['before']['points']['1']['native_base_index'] is None and d['after']['points']['1']['native_base_index']==1
 assert d['after']['points']['1']['native_receipt_sha256']==b['checksum_sha256'] and d['before']['points']['2']==d['after']['points']['2']
 for key in ('raw','rows','namespace_ids','schema','birth','open','slots','protected_catalogue_uuid'):assert d['before'][key]==d['after'][key]
 assert d['owner_before']['native_base'] is None and d['owner_after']['native_base']['verified']
 for key in ('history','rows','final_image','verification'):assert d['owner_before'][key]==d['owner_after'][key]
 if case=='TargetNativeBaseCanceled':
  assert extra['exact_error']=='canceling statement due to user request' and not extra['catalogue_adoption_or_serving']
  pending=extra['retained']['native_base'];assert pending['attempted'] and not pending['verified'] and pending['actual_commit_returned']==21
  assert pending['receipt']==b and extra['actual_native']==native
 if case=='TargetNativeBaseInvalidated':
  assert extra['actual_image_generations']['before']!=extra['actual_image_generations']['after']
  assert extra['actual_native']==extra['original_native']==native and extra['integrity_range_index']==1
  assert extra['durable_floor_survives_image_invalidation'] and not extra['cold_owner_reconstruction'] and not extra['serving']
  assert 'installed image local/native namespace contract changed' in extra['current_image_error']
 return {'classification':'passed','native_base_published':True,'native_base_xid':21,'final_pointer_xid':20,'range_index':1,'record_count':1,'bytes':177,'catalogue_adopted':False,'serving':False}
def read_projection(root,label,index):
 manifest=json.loads((root/f'artifacts/{label}-projection-manifest.json').read_text());p=root/f'artifacts/{label}-public.zip'
 assert manifest['source_commit']==SOURCE and sha(p.read_bytes())==manifest['projection_zip_sha256']
 assert not manifest['projection_fixture_db_custody_present'] and manifest['original_archive_unchanged']
 with zipfile.ZipFile(p) as z:
  assert len(z.namelist())==len(set(z.namelist()))==len(manifest['included_members'])
  for member in manifest['included_members']:
   assert member['path'] in z.namelist() and '/custody/' not in member['path'] and not member['path'].endswith('custody-manifest.json')
   data=z.read(member['path']);assert len(data)==member['bytes'] and sha(data)==member['sha256']
   assert b'PRIVATE KEY-----' not in data
  for member in manifest['omitted_members']:
   assert member['path'] not in z.namelist() and re.fullmatch('[a-f0-9]{64}',member['sha256'])
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  assert f"test result: ok. {index['passed']} passed; 0 failed;" in z.read('tests.log').decode()
 return p,manifest
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=pathlib.Path,default=pathlib.Path(__file__).resolve().parent);parser.add_argument('--repo',type=pathlib.Path);parser.add_argument('--self-test',action='store_true');args=parser.parse_args();root=args.root.resolve()
 manifest=json.loads((root/'manifest.json').read_text())
 assert manifest['source_revision']==SOURCE and len(manifest['runs'])==2
 assert not any(f.is_symlink() for f in root.rglob('*'))
 assert {str(f.relative_to(root)) for f in root.rglob('*') if f.is_file()}==set(manifest['files'])|{'manifest.json'}
 for name,identity in manifest['files'].items():
  p=pathlib.PurePosixPath(name);assert not p.is_absolute() and '..' not in p.parts
  data=(root/name).read_bytes();assert len(data)==identity['bytes'] and sha(data)==identity['sha256'],name
 for run in manifest['runs']:
  actual=json.loads((root/run['metadata']).read_text());assert actual['head_sha']==SOURCE and actual['id']==run['run_id'] and actual['status']=='completed' and actual['conclusion']=='success'
  for name in run['job_indices']:
   j=json.loads((root/name).read_text());assert j['source_revision']==SOURCE and j['run_id']==run['run_id'] and j['job_status']=='completed' and j['job_conclusion']=='success' and j['authoritative_source_verified']
   data=gzip.decompress((root/'jobs'/j['compressed_file']).read_bytes());assert len(data)==j['raw_bytes'] and sha(data)==j['raw_sha256']
 base_run=next(v for v in manifest['runs'] if v['kind']=='base4');base_path,_=read_projection(root,'base4',base_run)
 rows_run=next(v for v in manifest['runs'] if v['kind']=='rows5');read_projection(root,'rows5',rows_run)
 results={};ack=0;closed=0;sample=None
 with zipfile.ZipFile(base_path) as z:
  for name in z.namelist():
   if not name.endswith('/execution-identity.json'):continue
   prefix=name.rsplit('/',1)[0]+'/';read=lambda leaf:json.loads(z.read(prefix+leaf));identity=read('execution-identity.json');case=identity['source_case']
   assert case in BASE_CASES and case not in results and identity['source_commit']==SOURCE and identity['package_version']=='1.0.494-beta'
   terminal=read('terminal.json');assert terminal['required_baseline_succeeded'] and terminal['primary_error'] is None and terminal['custody_error'] is None
   assert read('cleanup.json')['errors']==[] and read('evidence-errors.json')['errors']==[]
   acks=read('ack-ledger.json');assert len(acks)==2 and all(a['outcome']=='returned' and a['actual_api_inserted'] and a['actual_api_error'] is None and not a['effect_unknown_on_error'] for a in acks);ack+=2
   journal=read('remote-old-ledger.json');closes=[v for v in journal if v['operation']=='remote_old_close'];assert len(closes)==3 and all(c['outcome']=='returned_actual_closure' and c['exact_error'] is None for c in closes);closed+=3
   c=next(v for v in journal if v['operation']=='remote_old_fresh_all_closed_collection');assert c['native_before']==c['native_after'] and c['native_receipts_unchanged'] and len(c['native_before'])==3 and c['outcome']=='returned_actual_all_old_certificate'
   rows=read('registered-image-history-and-rows.json');matching=[a for a in acks if a['range']==1];assert len(matching)==1 and rows['acknowledged_record_verified_locally']==matching[0]['actual_full_record']
   if case=='TargetNativeBaseWithoutFinal':d=read('registered-native-base-missing-final.json');extra=None
   else:
    d=read('registered-native-base-before-assertions.json');extra=read('registered-native-base-canceled.json') if case=='TargetNativeBaseCanceled' else read('registered-native-base-invalidated-integrity.json') if case=='TargetNativeBaseInvalidated' else None
    f=read('registered-final-image-before-assertions.json');assert f['independent_actual_primary_scan'] and not f['independent_index_parity'] and f['pointer']==d['expected_final_pointer']
   results[case]=check_base(case,d,extra)
   if case=='TargetNativeBase':sample=copy.deepcopy(d)
 assert set(results)==BASE_CASES and ack==8 and closed==12
 rows_globals=runpy.run_path(str(root/'verify_rows.py'));rows=rows_globals['report'];assert len(rows['same_open_wal_retention_controls'])==3
 findings=json.loads((root/'source-index.json').read_text())
 if args.repo:
  for entry in findings['source_pins']:
   for name,expected in entry['files_sha256'].items():assert sha(subprocess.check_output(['git','show',entry['commit']+':'+name],cwd=args.repo))==expected
 negative=[]
 if args.self_test:
  mutated=copy.deepcopy(sample);mutated['actual_native']['archive']['range_index']=99
  try:check_base('TargetNativeBase',mutated)
  except AssertionError:negative.append('different_archive_refused')
  else:raise AssertionError('mismatched archive accepted')
  mutated=copy.deepcopy(sample);mutated['actual_native']['base']['registered_final_image']['native_page_xid']=99;mutated['actual_native']['archive']=copy.deepcopy(mutated['actual_native']['base'])
  try:check_base('TargetNativeBase',mutated)
  except AssertionError:negative.append('wrong_final_pointer_refused')
  else:raise AssertionError('wrong final pointer accepted')
 print(json.dumps({'classification':'passed_portable_logical_evidence_only','source':SOURCE,'base4':results,'rows5_cases':len(rows['cases']),'source_APP_API_calls':{'acknowledged':18,'failed':0,'uncertain':0,'recorded_caller_retry_calls':0,'internal_wire_retry_count':None},'internal_target_unknown_outcomes':rows['target_control_outcomes']['apply_commit_unknown_retained_fenced'],'rows_same_open_wal_controls':rows['same_open_wal_retention_controls'],'self_test_controls':negative,'database_reopened':False,'native_grant_reconstructed':False,'production_qualified':False},indent=2))
if __name__=='__main__':main()
