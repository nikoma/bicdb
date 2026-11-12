#!/usr/bin/env python3
"""Check retained component-test evidence; optionally reconstruct exact source."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
REV='cad84207e8a4329db47449e43f658325d8f8cf55'
WORKTREE='/tmp/bicdb-native-completion-participants'
COUNTS={'completion':3,'floor-witness':1,'floor-negative':1,'ready':3,'local-ready':19,'current-repair':4,'historical-tls':5,'origin':7,'gate':9,'state':5,'quorum':7,'lease':3}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
    root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*'))
    manifest=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p!=root/'MANIFEST.json'}
    assert len(manifest)==len(actual) and {r['path'] for r in manifest}==actual,'manifest exact coverage'
    for r in manifest:
        assert (root/r['path']).resolve().is_relative_to(root.resolve())
        b=(root/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
    c=json.loads((root/'claims.json').read_text());assert c['source_revision']==REV and c['source_version']=='1.0.471-beta'
    assert c['distinct_tests']==66 and c['test_executions']==67 and c['worktree']==WORKTREE
    for k in ['incoming_complete_rpc_tested','readonly_completion_rpc_tested','frozen_retained','actual_post_persistence_client_socket_cancellation_tested']:assert c[k] is True,k
    for k in ['native_activation_tested','new_application_write_after_recovery_tested','strong_get_tested','physical_failure_tested','ci_tested','production_qualified','deployed','higher_prepared_minority_veto_negative_fixture_tested','green_witness_database_archive_retained']:assert c[k] is False,k
    assert {r['name'] for r in c['runs']}=={'cad84207-'+k for k in COUNTS}
    ids=[];minimum=10**30
    for r in c['runs']:
        d=root/'runs'/r['name'];a=json.loads((d/'inputs.json').read_text());b=json.loads((d/'result.json').read_text())
        assert all(b[k]==v for k,v in a.items()) and b['source_revision']==REV and b['dirty_diff_sha256']==sha(b'')
        assert b['exit_code']==0 and b['guard_stop_reason'] is None and b['pause_episodes']==0
        assert b['command'][:7]==['cargo','test','--release','--locked','-p','bicdb-core','--config']
        assert 'profile.release.package.bicdb-core.opt-level=0' in b['command'] and 'profile.release.package.bicdb-core.debug=0' in b['command']
        assert b['environment']['CARGO_BUILD_JOBS']=='1' and b['environment']['CARGO_INCREMENTAL']=='0' and b['environment']['RUST_TEST_THREADS']=='2'
        samples=[json.loads(line) for line in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
        assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
        minimum=min(minimum,b['minimum_available_bytes']);text=(d/'stdout').read_text();tests=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M)
        assert len(tests)==r['count']==COUNTS[r['name'].removeprefix('cad84207-')] and tests==r['test_ids']
        assert re.search(rf'test result: ok\. {len(tests)} passed; 0 failed; 0 ignored;',text)
        ids+=tests
    assert len(ids)==67 and len(set(ids))==66 and sorted(set(ids))==c['test_ids']
    assert minimum==c['minimum_available_bytes'] and c['pause_episodes']==0
    compiler=gzip.decompress((root/'runs'/c['compile_run']/'stderr.gz').read_bytes()).decode()
    assert f'Compiling bicdb-core v1.0.471-beta ({WORKTREE}/crates/bicdb-core)' in compiler,'intended forced compile path'
    exe=json.loads((root/'test-executable.json').read_text());assert exe['bytes']>0 and len(exe['sha256'])==64
    assert f"Running unittests src/lib.rs ({exe['path']})" in compiler
    patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['patch_sha256']
    reconstructed=False
    if repo:
        base=c['public_patch_base'];assert subprocess.check_output(['git','rev-parse',base+'^{commit}'],cwd=repo,text=True).strip()==base
        with tempfile.TemporaryDirectory(prefix='bicdb-completion-source-') as tmp:
            tmp=Path(tmp);archive=tmp/'base.tar'
            with archive.open('wb') as out:subprocess.run(['git','archive',base,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
            with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,capture_output=True,check=True,timeout=60)
            source=json.loads((root/'source-files.json').read_text())
            assert {r['path'] for r in source}=={p.relative_to(tmp).as_posix() for p in (tmp/'crates/bicdb-core/src').rglob('*') if p.is_file()},'exact core source coverage'
            for r in source:
                b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
        reconstructed=True
    return {'source_revision':REV,'distinct_tests':66,'test_executions':67,'retained_evidence_verified':True,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'native_activation_tested':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
