#!/usr/bin/env python3
"""Verify retained revision-scoped evidence; never rerun Rust or open a database."""
import argparse,gzip,hashlib,json,re,subprocess,sys,tarfile,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
REV='18723cf4f1ddbb01e37f3762067b30000707cadb'
PRIOR='87f1da00c53126d86796abe8048e36098d9d05f7'
TREE='/tmp/bicdb-native-normal-apply'
PREFIXES=['Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions']
SOURCES={
 'primary':(REV,BASE,1350,'786e078a85e25ba2d8f03a6447182504c2fc39079386901c1871495e2e49e81c','97b666b0c89c576a172c47a7706e11f6f47e06d59fcc239a561d116b19aa967e'),
 'prior-origin':(PRIOR,REV,1349,'9e447e239dc0020c1792c9a7b2307e6b1caa9fffeaee0ceb3f3e7aa6349150b3','551b4f6454ea402327cb8ce8515d1bce409d80c4702bd5d14a79060aab1698f9'),
 'compile-first':('73ab4f636e66f9e165e30b7736b7d263f812d6fc',REV,1349,'3ef9e92fb98c0a08a673c9b48ff02e680f7f0f7dc306aa86d1b590201d9ab4c5','0f5f8a36d0e33b8455ec29967c610949944f4d19df0b28cd78752d7a0f997ce5'),
 'compile-mtls':('7b8e5229f3d3572c6c512b1d4c471649a1909bd7',REV,1350,'54df3a9a4d25bb4d10bf75d19a6f38c88bd356db1eec200061809681f4cb0027','9bd66837a03423186863a1a13bf92e4e1df96d90b831be24a588ef3ebe26b6d3')}
RUNS={
 'actual-mtls-v2':(REV,0,2,'primary-final'),
 'final-bare-controls':(REV,0,2,'primary-final'),
 'final-embedded-positive':(REV,0,1,'primary-final'),
 'prior-origin-controls':(PRIOR,0,7,'prior-origin-only'),
 'second-coherence':(PRIOR,0,1,'earlier-supporting-not-added'),
 'prior-rf3-controls':(PRIOR,0,2,'earlier-supporting-not-added'),
 'first-coherence':(SOURCES['compile-first'][0],101,0,'compile-failure'),
 'actual-mtls-v1':(SOURCES['compile-mtls'][0],101,0,'compile-failure')}
def sha(b):return hashlib.sha256(b).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def input_paths(root):
 out=set()
 for name in PREFIXES:
  p=root/name
  if p.is_file():out.add(name)
  elif p.is_dir():out.update(f.relative_to(root).as_posix() for f in p.rglob('*') if f.is_file())
 return out
def verify(root,repo=None):
 root=Path(root).resolve()
 assert not any(p.is_symlink() for p in root.rglob('*')),'symlink in bundle'
 manifest=read(root,'MANIFEST.json');actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
 assert len(manifest)==len(actual) and {r['path'] for r in manifest}==actual,'manifest exact coverage'
 for r in manifest:
  p=(root/r['path']).resolve();assert p.is_relative_to(root);b=p.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
 c=read(root,'claims.json');assert c['source_revision']==REV and c['prior_origin_revision']==PRIOR and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE and c['worktree']==TREE and c['reconstruction_prefixes']==PREFIXES
 false=['cohorts_combined_into_total','changed_epoch_command_checksum_recomputed_in_negative','successful_database_archive_retained','external_ack_ledger_retained','raw_individual_rpc_trace_retained','successful_root_identity_retained','test_executable_digest_retained','original_035_baseline_modified','source_package_version_bumped','main_merged_at_measurement','binary_release_produced','strong_get_tested','automatic_election_tested','effects_issue_148_closed','compacted_catchup_tested','aborted_slot_catchup_tested','membership_transition_tested','physical_fault_tested','production_qualified']
 for k in false:assert c[k] is False,k
 assert c['actual_loopback_mtls_tested'] is True and c['same_epoch_retained_applied_catchup_tested'] is True
 assert {r['name'] for r in c['runs']}==set(RUNS) and len(c['runs'])==len(RUNS)
 primary=[];prior=[];minimum=10**30;errors=0
 for report in c['runs']:
  name=report['name'];revision,code,passed,cohort=RUNS[name];d=root/'runs'/name;a=read(d,'inputs.json');b=read(d,'result.json')
  assert all(b[k]==v for k,v in a.items()) and report['source_revision']==b['source_revision']==revision and report['exit_code']==b['exit_code']==code and report['cohort']==cohort
  assert b['dirty_diff_sha256']==sha(b'') and b['pause_episodes']==0 and b['guard_stop_reason'] is None
  assert b['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
  assert b['command'][:7]==['cargo','test','--release','--locked','-p','bicdb-core','--config']
  for flag in ['profile.release.package.bicdb-core.opt-level=0','profile.release.package.bicdb-core.debug=0']:assert flag in b['command']
  samples=[json.loads(x) for x in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
  assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
  assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
  text=gzip.decompress((d/'stdout.gz').read_bytes()).decode();stderr=gzip.decompress((d/'stderr.gz').read_bytes()).decode();ok=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',text,re.M)
  assert len(ok)==passed and not bad and ok==report['passed_test_ids'] and bad==report['failed_test_ids']
  assert report['rust_tests_started']==bool(re.search(r'^running \d+ tests?$',text,re.M))
  assert report['broken_pipe_diagnostics']==stderr.count('Broken pipe')
  if code==0:assert re.search(rf'test result: ok\. {passed} passed; 0 failed; 0 ignored;',text)
  else:
   assert not report['rust_tests_started'] and 'could not compile `bicdb-core`' in stderr
   if name=='first-coherence':assert 'error[E0433]' in stderr and 'error[E0308]' in stderr
   else:assert 'error[E0046]' in stderr and 'apply_range_write_repair' in stderr
  if cohort=='primary-final':primary+=ok;minimum=min(minimum,b['minimum_available_bytes']);errors+=stderr.count('Broken pipe')
  if cohort=='prior-origin-only':prior+=ok
 assert len(primary)==len(set(primary))==c['primary_distinct_passing_tests']==5 and sorted(primary)==c['primary_passing_test_ids']
 assert len(prior)==len(set(prior))==c['prior_origin_distinct_passing_tests']==7 and sorted(prior)==c['prior_origin_passing_test_ids']
 assert minimum==c['minimum_primary_available_bytes'] and errors==c['broken_pipe_diagnostics_primary']==5
 for name in ['actual-mtls-v2','second-coherence']:
  compiler=gzip.decompress((root/'runs'/name/'stderr.gz').read_bytes()).decode();assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in compiler
 provenance=read(root,'export-provenance.json');assert provenance['raw_files_modified'] is False and provenance['redactions_applied'] is False and provenance['generated_credentials_in_public_bundle'] is False
 expected={f'runs/{name}/{f}' for name in RUNS for f in ['inputs.json','result.json','stdout.gz','stderr.gz','resources.jsonl.gz']}
 assert len(provenance['exports'])==len(expected) and {r['export_path'] for r in provenance['exports']}==expected
 for r in provenance['exports']:
  exported=(root/r['export_path']).read_bytes();raw=gzip.decompress(exported) if r['transformation']=='gzip_mtime_0' else exported
  assert r['transformation'] in ['gzip_mtime_0','unchanged'] and len(exported)==r['export_bytes'] and sha(exported)==r['export_sha256'] and len(raw)==r['original_bytes'] and sha(raw)==r['original_sha256']
 custody=read(root,'custody-assessment.json')
 for k in ['successful_stopped_database_roots','external_ack_ledgers','individual_rpc_reports']:assert custody[k]=='NOT_RETAINED'
 for k in ['other_task_roots_opened','database_engine_opened_for_packaging','public_credentials_included','restricted_database_custody_created']:assert custody[k] is False
 assert custody['raw_build_and_test_receipts_retained'] is True
 assert len(c['sources'])==len(SOURCES) and {s['name'] for s in c['sources']}==set(SOURCES)
 for source in c['sources']:
  rev,parent,count,patchsha,indexsha=SOURCES[source['name']];d=root/source['directory'];assert source['directory']=='source/'+source['name']
  assert source['revision']==rev and source['patch_parent']==parent and source['patch_sha256']==patchsha and source['input_files']==count
  assert sha(gzip.decompress((d/'engine.patch.gz').read_bytes()))==patchsha and sha((d/'source-inputs.json').read_bytes())==indexsha
  rows=read(d,'source-inputs.json');assert len(rows)==len({r['path'] for r in rows})==count
 reconstructed=False
 if repo:
  assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
  with tempfile.TemporaryDirectory(prefix='bicdb-normal-source-') as tmp:
   tmp=Path(tmp);archive=tmp/'base.tar'
   with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--',*PREFIXES],cwd=repo,stdout=out,check=True,timeout=60)
   for name in SOURCES:
    tree=tmp/name;tree.mkdir()
    with tarfile.open(archive) as t:t.extractall(tree,filter='data')
    for patchname in (['primary'] if name=='primary' else ['primary',name]):
     patch=gzip.decompress((root/'source'/patchname/'engine.patch.gz').read_bytes());subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tree,check=True,capture_output=True,timeout=60)
    rows=read(root/'source'/name,'source-inputs.json');assert {r['path'] for r in rows}==input_paths(tree)
    for r in rows:
     b=(tree/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],(name,r['path'])
  reconstructed=True
 return {'source_revision':REV,'primary_passing_tests':5,'separate_prior_origin_revision':PRIOR,'separate_prior_origin_passing_tests':7,'cohorts_combined':False,'compile_failures_preserved':2,'broken_pipe_diagnostics':5,'all_four_sources_reconstructed':reconstructed,'rust_tests_rerun':False,'database_opened':False,'production_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
