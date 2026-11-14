#!/usr/bin/env python3
"""Verify retained public observations, not omitted native WAL; never open a DB."""
import argparse,copy,hashlib,json,pathlib,re,subprocess,sys
if not __debug__:raise SystemExit('Run without Python -O; assertions required')
SOURCE='e1ecfeab71d3f1e104530e9d6aa3bf37971abd65'
def sha(b):return hashlib.sha256(b).hexdigest()
def check(d):
 assert d['source_revision']==SOURCE and d['source_package_version']=='1.0.494-beta'
 assert d['original_run']==43493938281 and d['original_artifact']==14070838685
 assert d['original_zip_sha256']=='5445c509695372da8f5038c5e8073e9e8ce9619349c311221dbed743d97ab026'
 assert not d['database_opened']and not d['engine_recovery_or_checkpoint_invoked']and not d['raw_or_secret_bytes_published']and not d['frozen_packet_modified']and not d['production_qualified']
 assert d['original_two_tests_remain_failed']and d['original_named_fault_controls_remain_not_tested']
 assert {c['source_case']for c in d['cases']}=={'TargetCatalogueFloorDirSyncRetry','TargetCatalogueFloorCanceled'}and len(d['cases'])==2
 assert len(d['self_test_controls'])==6
 for c in d['cases']:
  assert c['custody_mode']=='observed_stable_with_live_actual_owners'
  assert c['full_retained_chain_CRC_checked']and c['retained_chain_records']==73 and [c['first_lsn'],c['last_lsn']]==[25,97]
  assert not c['retained_prefix_before_first_lsn_available']and not c['original_post_error_API_owner_state_available']
  assert not c['native_commit_API_return_or_fsync_receipt_observed']and not c['actual_root_reachability_or_current_native_key_visibility_proven']and not c['powerloss_durability_proven']and not c['serving_or_read_consistency_proven']and not c['named_catalogue_fault_seam_reached']
  xid=c['native_xid_identified_from_exact_key_and_receipt'];commit=c['commit_record'];r=c['receipt'];assert xid==commit['xid']==21 and commit['kind']==2 and commit['lsn']==97 and commit['offset']==469824 and commit['bytes']==40
  assert r['range_id']==r['range_index']==1 and r['record_count']==1 and r['serialized_record_bytes']==177 and r['target_node_id']=='n4'
  assert r['full_manifest_and_final_report_binding_checked']and len(c['base_and_archive'])==2
  a,b=c['base_and_archive'];assert a['native_key']=='install:base:1'and b['native_key']==f"registered-remote-base/{r['attempt_id']}/archive/{xid}"
  assert a['receipt_sha256']==b['receipt_sha256']and a['receipt_bytes']==b['receipt_bytes']==3914
  for v in c['base_and_archive']:
   assert v['mvcc']['xmin']==xid and v['mvcc']['xmax']==0 and v['mvcc']['previous_is_none']
   assert v['mvcc']['locator']['generation']==1
  assert len(c['WAL_members'])==1 and c['WAL_members'][0]['bytes']==469864
  assert c['WAL_members'][0]['records']==c['retained_chain_records']and c['WAL_members'][0]['first_lsn']==25 and c['WAL_members'][0]['last_lsn']==97
  for f in c['relevant_page_image_frames']:
   assert f['xid']==xid and f['kind']==1 and f['lsn']<commit['lsn']and re.fullmatch('[0-9a-f]{64}',f['frame_sha256'])
  f=c['native_final_image_WAL_correlation'];assert f['pointer_from_exact_base_receipt']==r['registered_final_pointer']and f['commit_record']['xid']==r['registered_final_pointer']['native_page_xid']==20 and f['commit_record']['kind']==2 and f['commit_record']['lsn']<commit['lsn']
  assert f['actual_current_and_archive_bytes_equal']and f['checksummed_native_final_record_matches_report']and f['native_source_manifest_checksum_independently_recomputed']and not f['current_root_visibility_proven']
  assert len(f['key_and_tuple_bindings'])==2
  assert f['key_and_tuple_bindings'][0]['record_sha256']==f['key_and_tuple_bindings'][1]['record_sha256']
  for v in f['key_and_tuple_bindings']:assert v['mvcc']['xmin']==20 and v['mvcc']['xmax']==0 and v['mvcc']['previous_is_none']
 return {'source':SOURCE,'cases':2,'classification':'retained public receipt of exact publication bytes logged under committed native xids','raw_WAL_independently_reparsed_by_this_public_verifier':False,'source_tests_still_failed':2,'named_fault_controls_still_not_tested':2,'current_root_visibility_or_fsync_or_powerloss_proven':False,'database_opened':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--self-test',action='store_true');a=p.parse_args();root=pathlib.Path(__file__).resolve().parent
 manifest=json.loads((root/'manifest.json').read_text());assert manifest['source_revision']==SOURCE
 assert not any(p.is_symlink()for p in root.rglob('*'))
 assert {str(p.relative_to(root))for p in root.rglob('*')if p.is_file()}==set(manifest['files'])|{'manifest.json'}
 for n,v in manifest['files'].items():
  name=pathlib.PurePosixPath(n);assert not name.is_absolute()and '..'not in name.parts
  b=(root/n).read_bytes();assert len(b)==v['bytes']and sha(b)==v['sha256']
 d=json.loads((root/'observation.json').read_text());assert d['inspector_sha256']==sha((root/'inspect-native-wal.py').read_bytes());result=check(d)
 if a.repo:
  for n,v in d['source_pins'].items():
   assert sha(subprocess.check_output(['git','show',SOURCE+':'+n],cwd=a.repo))==v['sha256']
   assert subprocess.check_output(['git','rev-parse',SOURCE+':'+n],cwd=a.repo,text=True).strip()==v['git_blob']
 negative=[]
 if a.self_test:
  for case in ['wrong_MVCC_xid','invented_current_visibility']:
   changed=copy.deepcopy(d)
   if case=='wrong_MVCC_xid':changed['cases'][0]['base_and_archive'][0]['mvcc']['xmin']=22
   else:changed['cases'][0]['actual_root_reachability_or_current_native_key_visibility_proven']=True
   try:check(changed)
   except AssertionError:negative.append(case+'_refused')
   else:raise AssertionError('altered public assertion accepted: '+case)
 result['public_negative_controls']=negative;print(json.dumps(result,indent=2))
if __name__=='__main__':main()
