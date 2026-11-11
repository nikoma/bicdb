#!/usr/bin/env python3
"""Verify retained namespace evidence; optionally reconstruct its exact source."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
REV='24aa180cdce2704fe313852c11177c238b403658'
BASE='149a09b812db56b646d25104c5aebfac9e00ed0c'
EMPTY=hashlib.sha256(b'').hexdigest()
EXPECTED={('v1','namespace-identity-focus'):11,('v1','namespace-old-reader-fence'):1,('v1','all-core-units'):535,('v1','integration-collection_rename'):3,('v1','integration-collection_rename_admission'):4,('v1','integration-collection_rename_paged_stubs'):3,('v1','integration-snapshot'):10,('v1','integration-distribution_snapshot'):1,('v1','integration-ha_state_durability'):8,('v1','integration-ha_state_crash'):0,('v2','build-ha-crash-child'):0,('v2','integration-ha_state_crash'):7,('v2','integration-log_archive_pitr'):8,('v3','downstream-sql-version-identity'):2}
def sha(b):return hashlib.sha256(b).hexdigest()
def read(root,p):
 f=root/p;b=f.read_bytes();return gzip.decompress(b) if f.suffix=='.gz' else b
def verify(root,repo=None):
 root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*')),'symlink'
 manifest=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p!=root/'MANIFEST.json'}
 assert len(manifest)==len(actual) and {r['path'] for r in manifest}==actual,'exact manifest coverage'
 for r in manifest:
  p=root/r['path'];assert p.resolve().is_relative_to(root.resolve())
  b=p.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
 c=json.loads((root/'claims.json').read_text());assert c['source_revision']==REV and c['source_version']=='1.0.475-beta' and c['public_patch_base']==BASE
 for k in ['ci_tested','physical_qualified','production_qualified','binary_published','deployed','rust_tests_rerun_for_export']:assert c[k] is False,k
 assert c['focus_overlaps_broad'] and c['no_aggregate_test_total_claimed']
 assert [c[k] for k in ['broad_core_passed','broad_core_ignored','broad_core_nested_child_extra_execution','focus_passed','focus_ignored','old_reader_negative_passed','integration_passed','initial_missing_child_failed','corrected_ha_crash_passed','downstream_sql_version_identity_passed']]==[534,4,1,11,1,1,44,7,7,2]
 assert c['broad_core_environment']=={'BICDB_PAGED_UNIQUE_APPLY_PROBE':'1'}
 provenance=json.loads((root/'raw-provenance.json').read_text());assert len(provenance)==62 and len({r['retained_path'] for r in provenance})==62
 for r in provenance:
  b=read(root,r['retained_path']);assert len(b)==r['original_bytes'] and sha(b)==r['original_sha256']
 assert {(r['group'],r['name']) for r in c['runs']}==set(EXPECTED)
 minimum=10**30;byrun={}
 for r in c['runs']:
  group,name=r['group'],r['name'];prefix=f'raw/{group}/{name}'
  result=json.loads(read(root,prefix+'-result.json'));text=read(root,prefix+'.stdout.gz').decode()
  assert result['git_head']==REV and result['source_version']=='1.0.475-beta' and result['uncommitted_patch_sha256']==EMPTY
  assert result['physical_qualification'] is False and result['interrupted_reason'] is None
  expected_exit=101 if (group,name)==('v1','integration-ha_state_crash') else 0
  assert result['exit_code']==expected_exit==r['exit_code']
  ok=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M);ignored=re.findall(r'^test (\S+) \.\.\. ignored.*$',text,re.M);failed=re.findall(r'^test (\S+) \.\.\. FAILED$',text,re.M)
  assert len(ok)==EXPECTED[group,name] and ok==r['passing_execution_ids'] and sorted(set(ok))==r['distinct_passing_ids']
  assert ignored==r['ignored_ids'] and failed==r['failed_ids']
  summaries=re.findall(r'^test result:.*$',text,re.M);assert summaries==r['summaries']
  if name=='all-core-units':
   assert len(set(ok))==534 and len(ignored)==4 and '534 passed; 0 failed; 4 ignored;' in summaries[-1]
   assert result['environment_override']=={'BICDB_PAGED_UNIQUE_APPLY_PROBE':'1'}
  elif name=='namespace-identity-focus':assert len(ignored)==1
  elif expected_exit==101:
   assert len(failed)==7 and '0 passed; 7 failed;' in summaries[-1]
   assert 'build the child first: cargo build -p bicdb-core --example ha_state_child' in read(root,prefix+'.stderr.gz').decode()
  elif name!='build-ha-crash-child':assert f'{len(ok)} passed; 0 failed;' in summaries[-1]
  if name=='namespace-old-reader-fence':assert result['environment_override']['BICDB_OLD_READER_BINARY'].endswith('bicdb-1.0.463-beta-d49a391b') and '--ignored' in result['command']
  samples=json.loads(read(root,prefix+'-resources.json'));assert samples and all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
  assert min(s['available_bytes'] for s in samples)==result['minimum_available_bytes']==r['minimum_available_bytes'];minimum=min(minimum,r['minimum_available_bytes'])
  assert '--release' in result['command'] and '--locked' in result['command']
  byrun[group,name]=set(ok)
 assert byrun['v1','namespace-identity-focus']<=byrun['v1','all-core-units']
 assert sum(len(v) for (group,name),v in byrun.items() if name.startswith('integration-') and not(group=='v1' and name=='integration-ha_state_crash'))==44
 assert minimum==c['minimum_available_bytes']
 first=read(root,'raw/v1/namespace-identity-focus.stderr.gz').decode();assert 'Compiling bicdb-core v1.0.475-beta (/tmp/bicdb-local-namespace-public/crates/bicdb-core)' in first
 sql=read(root,'raw/v3/downstream-sql-version-identity.stderr.gz').decode();assert 'bicdb-sql v1.0.475-beta (/tmp/bicdb-local-namespace-public/crates/bicdb-sql)' in sql
 old=json.loads((root/'old-reader-input.json').read_text());assert old['sha256']=='840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d' and old['binary_retained_in_this_bundle'] is False
 patch=read(root,'engine.patch.gz');assert sha(patch)==c['engine_patch_sha256']
 reconstructed=False
 if repo:
  assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
  with tempfile.TemporaryDirectory(prefix='bicdb-namespace-reconstruction-') as tmp:
   tmp=Path(tmp);archive=tmp/'base.tar'
   with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--',*c['engine_paths']],cwd=repo,stdout=out,check=True,timeout=60)
   with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
   archive.unlink();subprocess.run(['git','apply','--whitespace=nowarn','-'],cwd=tmp,input=patch,capture_output=True,check=True,timeout=60)
   source=json.loads((root/'source-files.json').read_text());assert len(source)==c['source_file_count']==1282
   assert {p.relative_to(tmp).as_posix() for p in tmp.rglob('*') if p.is_file()}=={r['path'] for r in source}
   for r in source:
    b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
  reconstructed=True
 return {'source_revision':REV,'broad_core_passed':534,'broad_core_ignored':4,'integration_passed':44,'failed_setup_tests_retained':7,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence-root',type=Path,default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.evidence_root,a.repo),sort_keys=True))
