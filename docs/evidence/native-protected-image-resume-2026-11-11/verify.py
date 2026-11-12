#!/usr/bin/env python3
"""Verify retained evidence and optionally reconstruct source; does not run Rust."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
def sha(x):return hashlib.sha256(x).hexdigest()
def parsed(d,suffix):return gzip.decompress((d/suffix).read_bytes()).decode()
def verify(directory,repo=None):
 root=Path(directory).resolve();assert not any(p.is_symlink() for p in root.rglob('*'))
 rows=json.loads((root/'MANIFEST.json').read_text());files={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
 assert len(rows)==len(files) and {r['path'] for r in rows}==files
 for r in rows:
  p=(root/r['path']).resolve();assert p.is_relative_to(root);b=p.read_bytes();assert (len(b),sha(b))==(r['bytes'],r['sha256']),r['path']
 c=json.loads((root/'claims.json').read_text());assert c['source_version']=='1.0.471-beta'
 for k in ['independent_processes_tested','physical_qualification','production_qualified','strong_get_tested','source_main_at_measurement','operator_binary_produced']:assert c[k] is False,k
 run_ids={};all_results={}
 for r in c['runs']:
  d=root/'runs'/r['name'];a=json.loads((d/'inputs.json').read_text());b=json.loads((d/'result.json').read_text());all_results[r['name']]=b
  assert all(b[k]==v for k,v in a.items())
  assert b['source_revision']==r['source_revision'] and b['exit_code']==r['exit_code'] and b['dirty_diff_sha256']==sha(b'')
  assert b['guard_stop_reason'] is None and b['pause_episodes']==0
  assert b['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
  for flag in ['--release','--locked','profile.release.package.bicdb-core.opt-level=0','profile.release.package.bicdb-core.debug=0']:assert flag in b['command']
  samples=[json.loads(l) for l in parsed(d,'resources.jsonl.gz').splitlines()];assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
  assert all(s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 and not s['paused'] for s in samples)
  out=(d/'stdout').read_text();ok=re.findall(r'^test (\S+) \.\.\. ok$',out,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',out,re.M)
  assert ok==r['passed_test_ids'] and bad==r['failed_test_ids'];run_ids[r['name']]=ok
  if ok or bad:assert re.search(rf'test result: (?:ok|FAILED)\. {len(ok)} passed; {len(bad)} failed; 0 ignored;',out)
  else:assert 'test result:' not in out
 selected=[x for n in c['selected_passing_runs'] for x in run_ids[n]];assert len(set(selected))==c['distinct_passing_tests']
 for n in c['selected_passing_runs']:assert all_results[n]['exit_code']==0
 if c['kind']=='protected-cold-image':
  assert c['source_revision']=='4de59df7e36672061a37964bec821988f44975fc' and c['distinct_passing_tests']==7 and len(selected)==7
  for k in ['tls_exercised','replacement_source_tested','higher_data_epoch_tested','joint_membership_tested','activation_tested','new_application_ack_tested']:assert c[k] is False
  assert c['cold_shutdown']=='orderly_all_owner_drop_then_strict_copied_reopen'
  expected={'611-first-cold':(101,0,0),'150-second-cold':(101,0,0),'2bb-third-cold':(101,0,0),'0ca-fourth-cold':(0,1,0),'ec19-protected-controls':(101,3,1),'ec19-existing-r1':(0,2,0),'ec19-global-format':(0,1,0),'4de-corrected-protected-controls':(0,4,0)}
  assert set(expected)==set(all_results)
  for r in c['runs']:assert (r['exit_code'],len(r['passed_test_ids']),len(r['failed_test_ids']))==expected[r['name']]
  assert all(all_results[n]['source_revision']=='ec19eb3d599bc4735e67e392cff6cc0797a229b3' for n in ['ec19-global-format','ec19-existing-r1'])
  assert 'range image schema has no current verified digest' in parsed(root/'runs/ec19-protected-controls','stderr.gz')
  assert all(x.endswith(('/proposal_tests.rs','/interrupted_normal_repair.rs','/protected_resume_positive.rs')) for x in c['related_to_final_changed_paths']) and len(c['related_to_final_changed_paths'])==3
  data=json.loads((root/'actual-results.json').read_text());cold=[x for x in data if 'purpose_specific_resume' in x];orph=[x for x in data if x.get('result','').startswith('ORPHAN_')];assert len(cold)==3 and len(orph)==1
  assert {x['stop'] for x in cold}=={'AfterClear','BeginBeforeStage','CatalogueBeforeMarker'}
  for x in cold:
   p=x['purpose_specific_resume'];assert x['outer_snapshot_phase']=='COLD_BEFORE_EXPLICIT_RESUME' and x['ordinary_path_still_refuses'] and x['original_catalogues_unchanged'] and not x['stale_authority_reused'] and not x['new_genesis_created_after_reopen']
   assert x['source_identity_before_close']['birth']==x['source_identity_after_close']['birth'] and x['source_identity_before_close']['items_local_uid']==x['source_identity_after_close']['items_local_uid']
   assert x['source_identity_before_close']['db_open']!=x['source_identity_after_close']['db_open'] and x['original_target_open']!=x['current_target_open']
   assert p['result']=='ACTUAL_NATIVE_PREPARED_READY_RECOVERED' and p['final_snapshot_phase']=='NATIVE_PREPARED_READY_STILL_FROZEN' and p['target_range_index']==2 and p['target_actual_local_visibility']==1
   assert p['acknowledged_values_reconciled_on_target']==1 and p['unknown2_resolved_but_not_new_ack'] and p['fresh_mixed_qq_and_full_history'] and p['duplicate_actual_reply_rejected']
   assert p['final_exact_image_intent_removed'] and p['final_native_base_published'] and p['target_uid']==x['target_uid'] and p['logical_history_sha256']==x['source_logical_history_sha256']
   for k in ['ordinary_no_image_relaxed','activation_proven','new_application_ack_tested','strong_get_tested','physical_qualification']:assert p[k] is False
  assert not orph[0]['published_intent_present'] and not orph[0]['fresh_authority_from_orphan'] and not orph[0]['native_ready_proven'] and orph[0]['native_range_index']==0
  custody=json.loads((root/'custody-receipt.json').read_text());assert (custody['root_count'],custody['files'],custody['bytes'],custody['manifest_sha256'])==(18,997,17594774,'0607e176907059dfdf602bc75351db7e0bdee12411f3cccb354b1b27c764d822')
  assert custody['all_source_destination_hashes_match'] and custody['files_and_directories_fsynced'] and not custody['global_credentials_or_binaries_copied'] and not custody['engine_reopened_for_custody']
  assert f"Compiling bicdb-core v1.0.471-beta ({c['worktree']}/crates/bicdb-core)" in parsed(root/'runs/4de-corrected-protected-controls','stderr.gz')
 else:
  assert c['kind']=='host-metadata' and c['source_revision']=='e6a4d6368fb961b6da0d13394e4bc0902b7d6361' and c['distinct_passing_tests']==23 and len(selected)==c['executions']==24
  for k in ['native_writer_admission_tested','database_roots_retained','external_ack_ledger']:assert c[k] is False
  assert c['tls_exercised'] is True
  s=json.loads((root/'actual-summary.json').read_text());assert s['source']==c['source_revision'] and s['distinct_tests']==23 and s['executions']==24 and s['test_executable']==c['test_executable']
  for r in s['runs']:assert sorted(r['test_ids'])==sorted(run_ids[r['run']])
  assert all_results['host-metadata-no-default-check']['exit_code']==101 and all_results['host-metadata-no-default-check-v2']['exit_code']==0
  assert '--no-default-features' in all_results['host-metadata-no-default-check-v2']['command']
  assert all_results['host-metadata-no-default-check-v2']['source_revision']=='79593853e9a25122fc59c64355a0d9dca8fb43d8'
  assert parsed(root/'runs/host-metadata-no-default-check','stderr.gz').count('error[E0433]')==6
  assert f"Compiling bicdb-core v1.0.471-beta ({c['worktree']}/crates/bicdb-core)" in parsed(root/'runs/host-metadata-v2','stderr.gz')
 reconstructed=False
 for src in c['sources']:assert sha(gzip.decompress((root/src['patch_file']).read_bytes()))==src['patch_sha256']
 if repo:
  with tempfile.TemporaryDirectory(prefix='bicdb-evidence-source-') as td:
   td=Path(td);archive=td/'base.tar'
   with archive.open('wb') as f:subprocess.run(['git','archive',c['public_patch_base'],'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=f,check=True,timeout=60)
   with tarfile.open(archive) as t:t.extractall(td,filter='data')
   for src in c['sources']:
    subprocess.run(['git','apply','--whitespace=nowarn','-'],input=gzip.decompress((root/src['patch_file']).read_bytes()),cwd=td,check=True,capture_output=True,timeout=60)
    rows=json.loads((root/src['source_files']).read_text());assert {r['path'] for r in rows}=={p.relative_to(td).as_posix() for p in (td/'crates/bicdb-core/src').rglob('*') if p.is_file()}
    for r in rows:
     b=(td/r['path']).read_bytes();assert (len(b),sha(b))==(r['bytes'],r['sha256']),r['path']
   reconstructed=True
 return {'kind':c['kind'],'source':c['source_revision'],'distinct_passing_tests':c['distinct_passing_tests'],'all_retained_runs':len(c['runs']),'source_reconstructed':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
