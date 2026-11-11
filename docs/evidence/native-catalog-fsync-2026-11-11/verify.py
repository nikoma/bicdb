#!/usr/bin/env python3
"""Read-only retained-evidence verification, not a Rust/DB/HA rerun."""
import argparse,gzip,hashlib,json,re,subprocess,sys,tarfile,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
REV='574ea56074a991368e9dae2129a0734a23bc1be3';EARLY='6141729b928e91964160d91ac9f4c017fe838488';BASE='b0e2498842be7b01e355bb8da1cc249ff16be056';TREE='/tmp/bicdb-native-fsync-fence'
PREFIXES=['Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions']
SOURCES={'primary':(REV,BASE,'cc803a3df7dcc90992fa7b0738dd72ff927bc2a9d1beab5f7a363990281c4851','7b9564f1dca4dd6e68686039da0cbb9b99eb646e77c2186a6e1de4c3c37fbe05'),'compile-failure':(EARLY,REV,'17ff95cc2476f7f62146c3ebacc1b574599816a5ea37c91f60edec7dc2ebc9fc','f92a9567c391d09bee9a93a191242d882f6c6078c447515aad57031ffc5ae02f')}
RUNS={'native-fsync-v2':(REV,0,6,'exact-source-passing'),'native-fsync-read-controls':(REV,0,9,'exact-source-passing'),'native-fsync-origin-controls':(REV,0,7,'exact-source-passing'),'native-fsync-actual-mtls-available':(REV,0,5,'exact-source-passing'),'native-fsync-normal-embedded':(REV,0,1,'exact-source-passing'),'native-fsync-tls-controls':(REV,0,0,'zero-match-excluded'),'native-fsync-v1':(EARLY,101,0,'compile-failure-excluded')}
def sha(b):return hashlib.sha256(b).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def source_paths(root):
 names=set()
 for name in PREFIXES:
  p=root/name
  if p.is_file():names.add(name)
  elif p.is_dir():names.update(f.relative_to(root).as_posix() for f in p.rglob('*') if f.is_file())
 return names
def verify(root,repo=None):
 root=Path(root).resolve();assert not any(p.is_symlink() for p in root.rglob('*'))
 manifest=read(root,'MANIFEST.json');actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
 assert len(manifest)==len(actual) and {r['path'] for r in manifest}==actual,'manifest exact coverage'
 for r in manifest:
  p=(root/r['path']).resolve();assert p.is_relative_to(root);b=p.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
 c=read(root,'claims.json');assert c['source_revision']==REV and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE and c['worktree']==TREE and c['reconstruction_prefixes']==PREFIXES
 for k in ['zero_match_counted_as_pass','pre_fix_dynamic_unsafe_ack_run','acknowledged_data_loss_observed','source_issue_is_new_azure_cause','main_merged_at_measurement','binary_release_produced','physical_failure_tested','production_qualified','strong_get_tested','read_quorum_rpc_tested','effects_issue_148_closed','config_test_database_roots_retained','external_ack_ledger_retained']:assert c[k] is False,k
 for k in ['explicit_legacy_async_behavior_preserved','protected_native_false_fsync_refusal_tested','existing_legacy_service_after_actual_format_activation_tested','native_foreign_root_refusal_tested','restricted_custody_contains_database_keys']:assert c[k] is True,k
 assert (c['native_config_tests'],c['read_foundation_controls'],c['origin_controls'],c['historical_mtls_controls'],c['metadata_mtls_controls'],c['embedded_normal_apply_controls'],c['normal_apply_mtls_specific_tests'])==(6,9,7,3,2,1,0)
 assert len(c['runs'])==len(RUNS) and {r['name'] for r in c['runs']}==set(RUNS)
 ids=[];minimum=10**30;diagnostics=0
 for report in c['runs']:
  name=report['name'];rev,code,passed,classification=RUNS[name];d=root/'runs'/name;a=read(d,'inputs.json');b=read(d,'result.json')
  assert all(b[k]==v for k,v in a.items()) and b['source_revision']==report['source_revision']==rev and b['exit_code']==report['exit_code']==code and report['classification']==classification
  assert b['dirty_diff_sha256']==sha(b'') and b['pause_episodes']==0 and b['guard_stop_reason'] is None
  assert b['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
  for flag in ['--release','--locked','profile.release.package.bicdb-core.opt-level=0','profile.release.package.bicdb-core.debug=0']:assert flag in b['command']
  samples=[json.loads(x) for x in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()];assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
  assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
  stdout=gzip.decompress((d/'stdout.gz').read_bytes()).decode();stderr=gzip.decompress((d/'stderr.gz').read_bytes()).decode();ok=re.findall(r'^test (\S+) \.\.\. ok$',stdout,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',stdout,re.M)
  assert len(ok)==passed and not bad and ok==report['passed_test_ids'] and bad==report['failed_test_ids'] and report['broken_pipe_diagnostics']==stderr.count('Broken pipe')
  if code==0:assert re.search(rf'test result: ok\. {passed} passed; 0 failed; 0 ignored;',stdout)
  else:assert not re.search(r'^running \d+ tests?$',stdout,re.M) and 'error[E0308]' in stderr and 'ServiceStoragePolicy::capture' in stderr and 'could not compile `bicdb-core`' in stderr
  if classification=='exact-source-passing':ids+=ok;minimum=min(minimum,b['minimum_available_bytes']);diagnostics+=stderr.count('Broken pipe')
 assert len(ids)==len(set(ids))==c['distinct_passing_tests']==c['passing_executions']==28 and sorted(ids)==c['passing_test_ids'] and minimum==c['minimum_available_bytes']==21236314112 and diagnostics==88
 zero=read(root/'runs/native-fsync-tls-controls','result.json');assert 'native_normal_actual_mtls' in zero['command']
 exe=read(root,'test-executable.json');assert exe['source_revision']==REV and exe['bytes']==135514536 and exe['sha256']=='25346bbcb2337b1ca2166e24b1e966ee4b09cd58672e9392c87359bdb7578908' and exe['mutable_shared_target_path'] is True and exe['binary_copied'] is False and exe['release_operator_binary'] is False
 compiler=gzip.decompress((root/'runs/native-fsync-v2/stderr.gz').read_bytes()).decode();assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in compiler and f"Running unittests src/lib.rs ({exe['path']})" in compiler
 exports=read(root,'export-provenance.json');assert exports['raw_files_modified'] is False and exports['redactions_applied'] is False
 expected={f'runs/{n}/{f}' for n in RUNS for f in ['inputs.json','result.json','stdout.gz','stderr.gz','resources.jsonl.gz']};assert len(exports['exports'])==len(expected) and {r['export_path'] for r in exports['exports']}==expected
 for r in exports['exports']:
  b=(root/r['export_path']).read_bytes();raw=gzip.decompress(b) if r['transformation']=='gzip_mtime_0' else b;assert r['transformation'] in ['gzip_mtime_0','unchanged'] and len(b)==r['export_bytes'] and sha(b)==r['export_sha256'] and len(raw)==r['original_bytes'] and sha(raw)==r['original_sha256']
 finding=read(root,'source-finding.json');assert finding['classification']=='SOURCE_LEVEL_UNSAFE_NATIVE_SERVICE_CONFIGURATION' and finding['observed_lost_ack'] is False and finding['before_fix_dynamic_experiment_run'] is False
 assert {r['revision'] for r in finding['source_inputs']}=={'18723cf4f1ddbb01e37f3762067b30000707cadb','6d07ded1ec9e44d688f2263a7804c236b3aa9f14'}
 custody=read(root,'custody-receipt.json');assert custody['source_revision']==REV and (custody['files'],custody['bytes'],custody['manifest_sha256'])==(84,1725403,'dd48abef28f9268b0182830d4de14f82014f1db923a087f4636ab9f69b4d7725')
 for k in ['source_destination_hash_parity','files_and_directories_fsynced','database_keys_restricted']:assert custody[k] is True,k
 for k in ['database_engine_opened','global_credentials_included','separate_tls_key_and_metadata_consensus_tempdirs_retained','external_ack_ledger_retained','custody_is_additional_qualification']:assert custody[k] is False,k
 assert custody['stopped_native_r2_roots']==c['retained_r2_root_count']==1 and custody['file_mode']=='0600' and custody['directory_mode']=='0700'
 assert len(c['sources'])==2 and {s['name'] for s in c['sources']}==set(SOURCES)
 for source in c['sources']:
  rev,parent,pdigest,indexdigest=SOURCES[source['name']];d=root/source['directory'];assert source['revision']==rev and source['patch_parent']==parent and source['patch_sha256']==pdigest and source['input_files']==1364 and source['directory']=='source/'+source['name']
  assert sha(gzip.decompress((d/'engine.patch.gz').read_bytes()))==pdigest and sha((d/'source-inputs.json').read_bytes())==indexdigest
 reconstructed=False
 if repo:
  assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
  with tempfile.TemporaryDirectory(prefix='bicdb-fsync-source-') as tmp:
   tmp=Path(tmp);archive=tmp/'base.tar'
   with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--',*PREFIXES],cwd=repo,stdout=out,check=True,timeout=60)
   for name in SOURCES:
    tree=tmp/name;tree.mkdir()
    with tarfile.open(archive) as t:t.extractall(tree,filter='data')
    for p in (['primary'] if name=='primary' else ['primary',name]):subprocess.run(['git','apply','--whitespace=nowarn','-'],input=gzip.decompress((root/'source'/p/'engine.patch.gz').read_bytes()),cwd=tree,check=True,capture_output=True,timeout=60)
    rows=read(root/'source'/name,'source-inputs.json');assert len(rows)==1364 and {r['path'] for r in rows}==source_paths(tree)
    for r in rows:
     b=(tree/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
  reconstructed=True
 return {'source_revision':REV,'passing_tests':28,'native_config_tests':6,'zero_match_filter_excluded':True,'first_compile_failure_preserved':True,'both_sources_reconstructed':reconstructed,'retained_native_r2_roots':1,'external_ack_ledger_retained':False,'rust_tests_rerun':False,'database_opened':False,'production_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
