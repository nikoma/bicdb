#!/usr/bin/env python3
"""Read-only revision-scoped evidence verification, not runtime qualification."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
REV='0d58359b07c31b38ed432e0a72ef7996ee2544b7'
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
TREE='/tmp/bicdb-native-sparse-current-certification'
RUNS={'sparse-codec':('b435f68c5d9bda8d4f87b9d05f2d84a8bdcf653b',101,0,1),'sparse-codec-v2':('1e6e3a0a0e60fbdba8abcfed3b78bdd82535c1f5',0,1,0),'copied-sparse-actual1025':('1e6e3a0a0e60fbdba8abcfed3b78bdd82535c1f5',101,0,1),'two-range-domain':(REV,0,2,0),'copied-sparse-actual1025-v2':(REV,0,1,0)}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
 root=Path(root).resolve();assert not any(p.is_symlink() for p in root.rglob('*'))
 rows=json.loads((root/'MANIFEST.json').read_text());files={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
 assert len(rows)==len(files) and {r['path'] for r in rows}==files
 for r in rows:
  p=(root/r['path']).resolve();assert p.is_relative_to(root);b=p.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
 c=json.loads((root/'claims.json').read_text());assert c['source_revision']==REV and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE and c['worktree']==TREE
 for k in ['original_unknown_tail_is_new_ack','target_image_install_tested','activation_tested','new_application_ack_tested','strong_get_tested','physical_qualification','production_qualified','main_merged_at_measurement','binary_release_produced']:assert c[k] is False,k
 assert c['source_native_applied_after']==c['source_native_ready_after']==1026 and c['behind_raw_and_native_after']==0 and c['original_ack_values_reconciled']==1025
 assert {r['name'] for r in c['runs']}==set(RUNS)
 ids=[]
 for r in c['runs']:
  rev,code,passed,failed=RUNS[r['name']];d=root/'runs'/r['name'];a=json.loads((d/'inputs.json').read_text());b=json.loads((d/'result.json').read_text())
  assert all(b[k]==v for k,v in a.items()) and b['source_revision']==r['source_revision']==rev and b['exit_code']==r['exit_code']==code
  assert b['dirty_diff_sha256']==sha(b'') and b['pause_episodes']==0 and b['guard_stop_reason'] is None
  assert b['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
  for flag in ['--release','--locked','profile.release.package.bicdb-core.opt-level=0','profile.release.package.bicdb-core.debug=0']:assert flag in b['command']
  samples=[json.loads(x) for x in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()];assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
  assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
  text=(d/'stdout').read_text();ok=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',text,re.M)
  assert len(ok)==passed and len(bad)==failed and ok==r['passed_test_ids'] and bad==r['failed_test_ids']
  assert re.search(rf'test result: (?:ok|FAILED)\. {passed} passed; {failed} failed; 0 ignored;',text)
  if rev==REV and code==0:ids+=ok
 assert len(ids)==len(set(ids))==c['distinct_passing_tests']==3 and sorted(ids)==c['passing_test_ids']
 compiler=gzip.decompress((root/'runs/two-range-domain/stderr.gz').read_bytes()).decode();assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in compiler
 assert 'unknown native catalogue format' in gzip.decompress((root/'runs/sparse-codec/stderr.gz').read_bytes()).decode()
 assert 'metadata recovery-role identity, purpose or committed configuration changed' in gzip.decompress((root/'runs/copied-sparse-actual1025/stderr.gz').read_bytes()).decode()
 a=json.loads((root/'actual-result.json').read_text());assert a['classification']=='SPARSE_CURRENT_CERTIFICATION_FINISHES_NATIVE_SOURCE_TAIL'
 for k in ['activation_proven','behind_image_install_tested','new_application_ack_tested','physical_qualification']:assert a[k] is False,k
 assert a['acknowledged_values_reconciled']==1025 and a['unknown_tail_is_not_new_application_ack'] is True and a['original_catalog_and_refusal_bytes_unchanged'] is True
 assert a['source_native_after']['native_applied_index']==a['source_progress_after']['resolved_through']==a['source_ready_index']==1026
 assert a['behind_native_after_sparse']['native_applied_index']==a['behind_progress_after_sparse']['last_index']==a['behind_progress_after_sparse']['resolved_through']==0
 assert a['behind_obligations']['format_version']==2 and set(a['behind_obligations']['irreversible_command_sha256'])=={'1026'}
 assert a['behind_obligations']['sparse_committed_command_sha256']==a['behind_obligations']['irreversible_command_sha256']
 assert 'committed sparse history is not fully materialized' in a['behind_ready_error']
 assert 'compacted/unavailable gap' in a['normal_tail_gap_refusal']
 assert a['source_native_before']['n2']['native_applied_index'] is None and a['source_native_before']['n2']['invalidated'] is True
 custody=json.loads((root/'custody-receipt.json').read_text());assert (custody['files'],custody['bytes'],custody['manifest_sha256'])==(137,16401076,'40305c743676d805d2fde2e0e910592c573cd9727052e27ef0add0952d2646e6')
 assert custody['files_and_directories_fsynced'] is True and custody['source_destination_hash_parity'] is True and custody['global_credentials_included'] is False
 patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['patch_sha256'];reconstructed=False
 if repo:
  assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
  with tempfile.TemporaryDirectory(prefix='bicdb-sparse-source-') as tmp:
   tmp=Path(tmp);archive=tmp/'base.tar'
   with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
   with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
   subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,capture_output=True,check=True,timeout=60)
   source=json.loads((root/'source-files.json').read_text());assert {r['path'] for r in source}=={p.relative_to(tmp).as_posix() for p in (tmp/'crates/bicdb-core/src').rglob('*') if p.is_file()}
   for r in source:
    b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
  reconstructed=True
 return {'source_revision':REV,'passing_tests':3,'separate_codec_pass':1,'preserved_failures':2,'ack_values_reconciled':1025,'behind_native':0,'source_native':1026,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
