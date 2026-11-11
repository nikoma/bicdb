#!/usr/bin/env python3
"""Integrity and retained-result checks; optional offline public-base reconstruction."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path

def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
    root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*'))
    manifest=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p!=root/'MANIFEST.json'}
    assert actual=={r['path'] for r in manifest} and len(actual)==len(manifest),'manifest exact coverage'
    for r in manifest:
        b=(root/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
    c=json.loads((root/'claims.json').read_text());assert c['unique_tests']==55 and c['executions']==55
    assert all(c[k] is False for k in ['ci','physical_failure_tested','production_qualified','new_receiver_rpc_exercised','recovery_owned_image_lease_exercised'])
    patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['patch_sha256']
    ids=set();executions=0
    for r in c['runs']:
        p=root/'runs'/r['name'];x=json.loads((p/'result.json').read_text());inputs=json.loads((p/'inputs.json').read_text())
        assert all(x[k]==v for k,v in inputs.items())
        assert x['source_revision']==c['compiled_source'] and x['dirty_diff_sha256']==sha(b'')
        assert x['exit_code']==0 and x['guard_stop_reason'] is None and x['pause_episodes']==0
        samples=[json.loads(line) for line in gzip.decompress((p/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
        assert samples and min(s['available_bytes'] for s in samples)==x['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 for s in samples)
        text=(p/'stdout').read_text();ts=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M)
        assert ts==r['test_ids'] and len(ts)==r['count'] and re.search(rf'test result: ok\. {len(ts)} passed; 0 failed; 0 ignored;',text)
        ids.update(ts);executions+=len(ts)
    assert sorted(ids)==c['test_ids'] and len(ids)==55 and executions==55
    compiled=gzip.decompress((root/'runs/receiver-participant-first-v2/stderr.gz').read_bytes()).decode()
    assert 'Compiling bicdb-core v1.0.471-beta (/tmp/bicdb-range-recovery-authority/crates/bicdb-core)' in compiled
    prior=json.loads((root/'prior-compile/receiver-participant-first-v1/result.json').read_text());assert prior['exit_code']==101
    reconstructed=False
    if repo is not None:
        repo=Path(repo).resolve();base=c['public_patch_base']
        assert subprocess.check_output(['git','rev-parse',base+'^{commit}'],cwd=repo,text=True).strip()==base
        with tempfile.TemporaryDirectory(prefix='bicdb-receiver-source-') as tmp:
            tmp=Path(tmp);archive=tmp/'base.tar'
            with archive.open('wb') as output:
                subprocess.run(['git','archive',base,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=output,check=True,timeout=60)
            with tarfile.open(archive) as tar:tar.extractall(tmp,filter='data')
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,check=True,timeout=60,capture_output=True)
            for r in json.loads((root/'source-files.json').read_text()):
                b=(tmp/r['path']).read_bytes();assert sha(b)==r['sha256'] and len(b)==r['bytes'],r['path']
            reconstructed=True
    return {'unique_focused_tests':55,'retained_results_verified':True,'source_reconstructed_and_hash_checked':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
