#!/usr/bin/env python3
"""Retained local evidence verification; optional offline public-base reconstruction."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path

def sha(data):return hashlib.sha256(data).hexdigest()
def verify(root,repo=None):
    root=Path(root)
    assert not any(p.is_symlink() for p in root.rglob('*'))
    manifest=json.loads((root/'MANIFEST.json').read_text())
    actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p!=root/'MANIFEST.json'}
    assert actual=={r['path'] for r in manifest} and len(actual)==len(manifest),'exact manifest coverage'
    for r in manifest:
        assert not Path(r['path']).is_absolute() and '..' not in Path(r['path']).parts
        data=(root/r['path']).read_bytes();assert sha(data)==r['sha256'] and len(data)==r['bytes'],r['path']
    c=json.loads((root/'claims.json').read_text())
    assert c['source']=='d440c58ebc962f1e62374d86faecbfcc051e2bbf' and c['source_version']=='1.0.471-beta'
    assert c['distinct_focused_tests']==c['executions']==40 and c['native_catalogue_format']==7
    assert c['actual_historical_mtls_receiver_exercised'] and c['recovery_owned_image_lease_components_exercised']
    for key in ['complete_nonempty_current_voter_repair_exercised','native_ready_quorum_exercised','recovery_complete_exercised','new_write_after_recovery_exercised','strong_get_exercised','full_effects_qualified','ci','physical_failure_tested','production_qualified','deployed','test_executable_archived','test_executable_hash_recomputed_for_export','release_binary']:
        assert c[key] is False,key
    patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['engine_patch_sha256']
    source=(root/'source-files.json').read_bytes();assert sha(source)==c['source_files_sha256']
    files=json.loads(source);assert len(files)==154 and len({r['path'] for r in files})==154
    results=json.loads((root/'candidate-validation.json').read_text());assert len(results)==len(c['runs'])==9
    ids=set();executions=0;minimum=None
    for run,claim in zip(results,c['runs']):
        name=run['name'];assert name==claim['name']
        retained=json.loads((root/'runs'/f'{name}-result.json').read_text());assert retained==run
        assert run['git_head']==c['source'] and run['source_version']==c['source_version'] and run['uncommitted_patch_sha256']==sha(b'')
        assert run['exit_code']==0 and run['interrupted_reason'] is None and run['physical_qualification'] is False
        assert 'profile.release.package.bicdb-core.opt-level=0' in run['command'] and 'profile.release.package.bicdb-core.debug=0' in run['command']
        samples=json.loads(gzip.decompress((root/'runs'/f'{name}-resources.json.gz').read_bytes()))
        assert samples and min(x['available_bytes'] for x in samples)==run['minimum_available_bytes']
        assert all(not x['paused'] and x['available_bytes']>=12*2**30 for x in samples)
        minimum=min(minimum or run['minimum_available_bytes'],run['minimum_available_bytes'])
        text=(root/'runs'/f'{name}.stdout').read_text();tests=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M)
        assert tests==claim['test_ids'] and len(tests)==claim['count'] and f'test result: ok. {len(tests)} passed; 0 failed;' in text
        ids.update(tests);executions+=len(tests)
    assert len(ids)==executions==40 and sorted(ids)==c['test_ids'] and minimum==c['minimum_available_bytes']==22258626560
    assert sum('::native_historical_tls::' in name for name in ids)==3
    first=gzip.decompress((root/'runs/native-ready-all-local-controls.stderr.gz').read_bytes()).decode()
    assert 'Compiling bicdb-core v1.0.471-beta (/tmp/bicdb-native-catalogue-adoption/crates/bicdb-core)' in first
    reconstructed=False
    if repo is not None:
        repo=Path(repo).resolve();base=c['public_base']
        assert subprocess.check_output(['git','rev-parse',base+'^{commit}'],cwd=repo,text=True).strip()==base
        with tempfile.TemporaryDirectory(prefix='bicdb-native-prepared-source-') as tmp:
            tmp=Path(tmp);archive=tmp/'base.tar'
            with archive.open('wb') as out:
                subprocess.run(['git','archive',base,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
            with tarfile.open(archive) as tar:tar.extractall(tmp,filter='data')
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,check=True,timeout=60,capture_output=True)
            for r in files:
                path=Path(r['path']);assert not path.is_absolute() and '..' not in path.parts
                data=(tmp/path).read_bytes();assert sha(data)==r['sha256'] and len(data)==r['bytes'],r['path']
            assert 'NATIVE_READY_CATALOG_FORMAT: u32 = 7' in (tmp/'crates/bicdb-core/src/distribution_range_consensus/native_ready_prepare.rs').read_text()
            reconstructed=True
    return {'distinct_focused_tests':40,'retained_evidence_verified':True,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
