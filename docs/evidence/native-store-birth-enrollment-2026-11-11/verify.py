#!/usr/bin/env python3
"""Verify retained source/results; never opens a DB, builds Rust or grants HA qualification."""
import argparse,gzip,hashlib,json,os,re,subprocess,sys,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
PRIMARY='ee5cb1880a946b97816a15aa4e0cb12f7d03778d'
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
DOCBASE='f3a063902d2fe316c29264fe20549c2fdd270242'
TREE='/tmp/bicdb-native-store-voter-admission'
PREFIXES=['Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions']
SOURCES={'ee5cb1880a946b97816a15aa4e0cb12f7d03778d': ('b0e2498842be7b01e355bb8da1cc249ff16be056', '30d1cede82b3bebb2859e881fced41094e8b5c75f16cd4239ac702d6898a5887', 'e40f71a37f245f66fc99d45f3657198b4dbcf303a0dfd6acadaefcfc940ac078'), '06da717949a062319646be72d14b31273321540a': ('ee5cb1880a946b97816a15aa4e0cb12f7d03778d', '6a78802868cd5564347a7e4ebc23d8a1e496a9754e766e0c0eadd9e98877030c', '4d0cf464f8c85eb95a46779b143e31206a3ff91d4163c1f3af4d653ee053f5e5'), '256d797860b740884ffea082a6b77f83aaf036b7': ('ee5cb1880a946b97816a15aa4e0cb12f7d03778d', '5ebb338694d2f4f5c479edbbe0fdf645319fb3b8a2614d4de1871e312893723a', '30bb6018d4c9cc034ffaf8ffeda9616e7045a53c5241d3580d76e6b00da9e248'), '6641e13bc2a1f95773cc416929a1977b91340ffa': ('ee5cb1880a946b97816a15aa4e0cb12f7d03778d', 'dd3f8503b22add723fb231bb2db525cb27c299ce7444db2a5ac81c612a110013', '30da5adb6fbc645211038f20c17567ae2f0746b7c1625988a916acb1dfdb352e'), '8a021a49c546d7d762650cf08c82a0c568a7c618': ('ee5cb1880a946b97816a15aa4e0cb12f7d03778d', '2783b157a4cf3b060acd6a3426092157136bf7fc2f37711b6f9f7ff99989f39f', '88450eead0b1813305ec26c529fe0bfab5bbc5185e0cbf035111e68451c3bb78'), '0c687a9692667edbabf7b78b87118d839b4d26ce': ('ee5cb1880a946b97816a15aa4e0cb12f7d03778d', '4144d6a42d839ffb2631648835e9f7148c73fe8094212e01a0a4d07fd95f4bb0', 'd4a90fdf7dbfc9dccb990ac4c9c24bdbc34fcccc0f89284618154cea884085b9')}
# source, exit code, passed, failed, pause episodes; earlier attempts never inflate final counts.
RUNS={
 'native-enrollment-v1':('06da717949a062319646be72d14b31273321540a',101,0,0,0),
 'native-enrollment-v2':('256d797860b740884ffea082a6b77f83aaf036b7',101,1,2,1),
 'native-enrollment-v3':('6641e13bc2a1f95773cc416929a1977b91340ffa',101,1,2,0),
 'native-enrollment-v4':('8a021a49c546d7d762650cf08c82a0c568a7c618',101,2,1,0),
 'native-enrollment-v5':('0c687a9692667edbabf7b78b87118d839b4d26ce',0,3,0,0),
 'native-metadata-v1':('0c687a9692667edbabf7b78b87118d839b4d26ce',0,9,0,0),
 'native-read-enrolled-v1':('0c687a9692667edbabf7b78b87118d839b4d26ce',101,16,2,0),
 'native-birth-final-metadata':(PRIMARY,0,11,0,0),
 'native-birth-final-reads':(PRIMARY,0,18,0,0),
 'native-birth-final-factory':(PRIMARY,0,3,0,0),
}
FINAL=['native-birth-final-metadata','native-birth-final-reads','native-birth-final-factory']
def sha(b):return hashlib.sha256(b).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def unpack(root,name):return gzip.decompress((root/name).read_bytes())
def reconstruct(root,repo,rows):
 common=Path(subprocess.check_output(['git','rev-parse','--path-format=absolute','--git-common-dir'],cwd=repo,text=True).strip())
 assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
 with tempfile.TemporaryDirectory(prefix='native-birth-source-') as temporary:
  temp=Path(temporary);gitdir=temp/'isolated.git';subprocess.run(['git','init','--bare','--quiet',str(gitdir)],check=True,capture_output=True)
  env=os.environ.copy();env.update(GIT_INDEX_FILE=str(temp/'index'),GIT_ALTERNATE_OBJECT_DIRECTORIES=str(common/'objects'))
  cmd=['git','--git-dir',str(gitdir)]
  primary_tree=None
  for row in rows:
   subprocess.run(cmd+['read-tree',BASE if row['revision']==PRIMARY else primary_tree],check=True,env=env,capture_output=True)
   subprocess.run(cmd+['apply','--cached','--whitespace=nowarn','-'],input=unpack(root,row['patch_path']),check=True,env=env,capture_output=True)
   listing=subprocess.check_output(cmd+['ls-files','--stage','--',*PREFIXES],env=env,text=True)
   staged={line.split('\t',1)[1]:line.split('\t',1)[0].split()[:2] for line in listing.splitlines()}
   files=read(root,row['source_files_path']);assert set(staged)=={f['path'] for f in files}
   payload=subprocess.check_output(cmd+['cat-file','--batch'],input=('\n'.join(staged[f['path']][1] for f in files)+'\n').encode(),env=env)
   offset=0
   for f in files:
    end=payload.index(b'\n',offset);header=payload[offset:end].decode().split();assert header[1]=='blob';n=int(header[2]);b=payload[end+1:end+1+n];offset=end+n+2
    assert n==f['bytes'] and sha(b)==f['sha256'] and staged[f['path']][0]==f['mode'],f['path']
   assert offset==len(payload)
   if row['revision']==PRIMARY:primary_tree=subprocess.check_output(cmd+['write-tree'],env=env,text=True).strip()
def verify(directory,repo=None,private_dir=None):
 root=Path(directory).resolve();assert not any(p.is_symlink() for p in root.rglob('*'))
 manifest=read(root,'MANIFEST.json');actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.relative_to(root).as_posix()!='MANIFEST.json'}
 assert len(manifest)==len({r['path'] for r in manifest}) and {r['path'] for r in manifest}==actual,'manifest exact coverage'
 for r in manifest:
  path=(root/r['path']).resolve();assert path.is_relative_to(root);b=path.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
 c=read(root,'claims.json');assert c['source_revision']==PRIMARY and c['source_version']=='1.0.471-beta' and c['documentation_base']==DOCBASE and c['documentation_base_version']=='1.0.478-beta' and c['public_patch_base']==BASE and c['worktree']==TREE
 for k in ['earlier_attempts_added_to_final_count','enrolled_read_result_grants_value_or_absence','strong_get_tested','host_writer_startup_all_voter_birth_enforcement','operator_four_to_five_tested','cross_process_node_cluster_tested','broad_core_suite_tested','physical_failure_tested','effects_issue_148_closed','production_qualified','public_engine_merge_tested','ci_tested','release_binary_produced','external_ack_ledger_retained','new_factory_fixture_roots_retained','new_enrollment_tls_fixture_roots_retained']:assert c[k] is False,k
 for k in ['first_compile_failure_preserved','earlier_factory_and_read_fixture_failures_preserved','earlier_v2_psi_pause_preserved','known_none_voter_live_upgrade_refused','same_incarnation_demotion_enrollment_refused','unbound_voter_deletion_readd_refused','uncommitted_birth_can_be_replaced_by_valid_different_term_or_snapshot','same_term_same_index_conflict_refused','genuine_wrong_actual_birth_in_fresh_metadata_refused','generic_legacy_demotion_preserved','birth_record_is_data_only']:assert c[k] is True,k
 assert c['final_run_names']==FINAL and c['final_group_counts']=={'metadata':11,'reads':18,'factory':3}
 assert len(c['runs'])==len(RUNS) and {r['name'] for r in c['runs']}==set(RUNS)
 ids=[];minimum=10**30;diagnostics=0;run_ids={}
 for row in c['runs']:
  name=row['name'];rev,code,passed,failed,pauses=RUNS[name];d=root/'runs'/name;a=read(d,'inputs.json');r=read(d,'result.json')
  assert all(r[k]==v for k,v in a.items()) and r['source_revision']==row['source_revision']==rev and r['dirty_diff_sha256']==sha(b'')
  assert r['exit_code']==row['exit_code']==code and r['guard_stop_reason'] is None and r['pause_episodes']==row['pause_episodes']==pauses
  assert r['external_fixture'] is None and r['compiler']=='rustc 1.96.0 (ac68faa20 2026-05-25)'
  assert r['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
  expected_filter='native_birth_' if 'metadata' in name else ('native_read_' if 'read' in name else 'native_enrollment_')
  assert r['command']==['cargo','test','--release','--locked','-p','bicdb-core','--config','profile.release.package.bicdb-core.opt-level=0','--config','profile.release.package.bicdb-core.debug=0','--lib',expected_filter,'--features','tls-replication','--','--nocapture']
  samples=[json.loads(line) for line in unpack(d,'resources.jsonl.gz').decode().splitlines()];assert samples and min(s['available_bytes'] for s in samples)==r['minimum_available_bytes']
  assert all(s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
  if pauses:
   assert any(s['paused'] for s in samples) and not samples[-1]['paused']
   assert any(float(next(line for line in s['memory_pressure'].splitlines() if line.startswith('full ')).split('avg10=')[1].split()[0])>1.0 for s in samples)
  else:assert all(not s['paused'] for s in samples)
  stdout=unpack(d,'stdout.gz').decode();stderr=unpack(d,'stderr.gz').decode();ok=re.findall(r'^test (\S+) \.\.\. ok$',stdout,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',stdout,re.M)
  assert len(ok)==passed and len(bad)==failed and row['passed_test_ids']==ok and row['failed_test_ids']==bad
  assert stderr.count('Broken pipe')==row['broken_pipe_diagnostics'];assert row['wall_seconds']==r['end_unix']-r['start_unix']
  if name=='native-enrollment-v1':assert not ok and not bad and 'error[E0425]' in stderr and 'cannot find value `scope`' in stderr and 'test result:' not in stdout
  else:assert re.search(rf'test result: (?:ok|FAILED)\. {passed} passed; {failed} failed; 0 ignored;',stdout)
  if name=='native-enrollment-v2':assert 'actual unwritten native genesis' in stderr
  if name=='native-enrollment-v3':assert 'range image schema has no current verified digest' in stderr
  if name=='native-enrollment-v4':assert 'namespace `items` is not server_paged' in stderr
  if name=='native-read-enrolled-v1':assert 'actual persisted recovery fence scope changed' in stderr and stderr.count('panicked at')==2
  if name in ['native-enrollment-v1','native-enrollment-v2','native-enrollment-v3','native-enrollment-v4','native-enrollment-v5','native-birth-final-metadata']:assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in stderr
  if name in FINAL:assert row['classification']=='final_exact_source_passing';ids+=ok;minimum=min(minimum,r['minimum_available_bytes']);diagnostics+=stderr.count('Broken pipe');run_ids[name]=ok
  else:assert row['classification']==('compile_failure_excluded' if name=='native-enrollment-v1' else 'earlier_source_excluded')
 assert len(ids)==len(set(ids))==c['distinct_passing_tests']==c['passing_executions']==32 and sorted(ids)==c['passing_test_ids']
 assert minimum==c['minimum_available_bytes']==21637189632 and diagnostics==c['final_broken_pipe_diagnostics']==522 and c['final_pause_episodes']==c['final_guard_stops']==0
 summary=read(root,'final-source32-summary.json');assert summary['source_revision']==PRIMARY and summary['source_version']=='1.0.471-beta' and summary['final_distinct_tests']==32
 assert len(summary['cohorts'])==3
 for row in summary['cohorts']:assert row['run'] in FINAL and row['ids']==run_ids[row['run']] and row['tests']==len(row['ids'])
 exe=summary['executable'];assert exe['sha256']=='b8ba1c2b7c0d9bcdba51b4b09b6f15359b1757d0edf6b2e6b9d2c5c4462a3fd3' and exe['bytes']==136680992 and exe['mutable_shared_libtest'] is True and exe['operator_binary'] is False
 for k in ['broad_core_suite_run_on_this_source','strong_get_tested','host_writer_enrollment_enforced','operator_integration_tested','physically_qualified']:assert summary[k] is False,k
 assert sha((root/'run_cargo_guarded.py').read_bytes())==c['runner_sha256']
 exports=read(root,'export-provenance.json');assert exports['raw_files_modified'] is False and exports['redactions_applied'] is False
 expected={f'runs/{name}/{file}' for name in RUNS for file in ['inputs.json','result.json','stdout.gz','stderr.gz','resources.jsonl.gz']};assert len(exports['exports'])==len(expected) and {r['export_path'] for r in exports['exports']}==expected
 for row in exports['exports']:
  b=(root/row['export_path']).read_bytes();assert len(b)==row['export_bytes'] and sha(b)==row['export_sha256'];assert row['transformation'] in ['gzip_mtime_0','unchanged'];raw=gzip.decompress(b) if row['transformation']=='gzip_mtime_0' else b;assert len(raw)==row['original_bytes'] and sha(raw)==row['original_sha256']
 source=read(root,'source-provenance.json');assert source['primary_revision']==PRIMARY and source['public_patch_base']==BASE and source['documentation_base']==DOCBASE and source['reconstruction_prefixes']==PREFIXES
 assert len(source['sources'])==len(SOURCES) and source['sources'][0]['revision']==PRIMARY and {row['revision'] for row in source['sources']}==set(SOURCES)
 for row in source['sources']:
  anchor,patchsha,filessha=SOURCES[row['revision']];assert row['patch_base']==anchor and sha(unpack(root,row['patch_path']))==row['patch_uncompressed_sha256']==patchsha
  assert sha((root/row['source_files_path']).read_bytes())==row['source_files_sha256']==filessha
  files=read(root,row['source_files_path']);assert len(files)==len({f['path'] for f in files})==row['files']==1372
 if repo:reconstruct(root,repo,source['sources'])
 custody=read(root,'private-custody.json');assert custody['source_revision']==PRIMARY and custody['files']==47 and custody['bytes']==824217 and custody['manifest_sha256']=='63e2da1e0da7e313469c0a4f28e23ab8f06b866620dd4ecccfc8c5dd18a9d056'
 for k in ['database_opened_for_copy','external_ack_ledger_retained','new_factory_fixture_roots_retained','new_enrollment_tls_fixture_roots_retained']:assert custody[k] is False,k
 for k in ['source_destination_hash_parity','source_unchanged_after_copy','directories_fsynced','files_fsynced','contains_private_generated_database_keys']:assert custody[k] is True,k
 assert custody['active_source_fd_or_map_users']==0 and custody['directory_mode']=='0700' and custody['file_mode']=='0600'
 assert f"CURRENT_NATIVE_READ_RPLUSONE_ROOT {custody['source_root_printed_in_final_reads_stderr']}" in unpack(root/'runs/native-birth-final-reads','stderr.gz').decode()
 # Without --private-dir, verification never reads or stats the private archive.
 if private_dir:
  p=Path(private_dir);assert not any(f.is_symlink() for f in p.rglob('*'));m=(p/'MANIFEST.json').read_bytes();assert sha(m)==custody['manifest_sha256'];rows=json.loads(m)['files']
  assert len(rows)==47 and sum(r['bytes'] for r in rows)==824217 and {f.relative_to(p).as_posix() for f in p.rglob('*') if f.is_file()}=={'MANIFEST.json',*[r['path'] for r in rows]}
  for row in rows:
   f=(p/row['path']).resolve();assert f.is_relative_to(p.resolve());b=f.read_bytes();assert sha(b)==row['sha256'] and len(b)==row['bytes'] and f.stat().st_mode&0o777==0o600
  assert all(d.stat().st_mode&0o777==0o700 for d in [p,*[d for d in p.rglob('*') if d.is_dir()]])
 return {'source_revision':PRIMARY,'distinct_tests':32,'passing_executions':32,'source_states_reconstructed':6 if repo else 0,'private_custody_verified':bool(private_dir),'rust_tests_rerun':False,'strong_get_tested':False,'operator_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');p.add_argument('--private-dir');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo,a.private_dir),sort_keys=True))
