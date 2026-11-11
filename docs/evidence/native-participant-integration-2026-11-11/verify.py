#!/usr/bin/env python3
"""Verify revision-scoped retained tests; optional offline source reconstruction."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
EXPECTED={'historical_and_owned_lease':10,'closed_readiness_components':38,'native_ready_tls':8}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
    root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*'))
    m=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p!=root/'MANIFEST.json'}
    assert len(m)==len(actual) and actual=={r['path'] for r in m},'manifest exact coverage'
    for r in m:
        b=(root/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
    c=json.loads((root/'claims.json').read_text());assert c['same_source_combined_test_total'] is None
    for k in ['incoming_complete_rpc_tested','native_activation_tested','new_application_writes_after_recovery_tested','strong_get_tested','production_qualified']:assert c[k] is False,k
    assert {g['group'] for g in c['groups']}==set(EXPECTED)
    output=[]
    for g in c['groups']:
        p=root/g['group'];ids=set();executions=0
        assert g['unique_tests']==EXPECTED[g['group']] and g['source_version']=='1.0.471-beta'
        assert g['new_native_ready_wire_exercised']==(g['group']=='native_ready_tls')
        assert all(g[k] is False for k in ['physical_failure_tested','ci_tested','production_qualified','deployed'])
        for r in g['runs']:
            d=p/'runs'/r['name'];result=json.loads((d/'result.json').read_text());inputs=json.loads((d/'inputs.json').read_text())
            assert all(result[k]==v for k,v in inputs.items()) and result['source_revision']==g['source_revision'] and result['dirty_diff_sha256']==sha(b'')
            assert result['exit_code']==0 and result['guard_stop_reason'] is None and result['pause_episodes']==0
            samples=[json.loads(l) for l in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
            assert samples and min(s['available_bytes'] for s in samples)==result['minimum_available_bytes']
            assert all(not s['paused'] and s['available_bytes']>=12*2**30 for s in samples)
            text=(d/'stdout').read_text();ts=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M)
            assert ts==r['test_ids'] and len(ts)==r['count'] and re.search(rf'test result: ok\. {len(ts)} passed; 0 failed; 0 ignored;',text)
            ids.update(ts);executions+=len(ts)
        assert sorted(ids)==g['test_ids'] and len(ids)==g['unique_tests'] and executions==g['executions']
        forced={'historical_and_owned_lease':('actual-owned-lease-v2','/tmp/bicdb-range-recovery-authority'),
                'closed_readiness_components':('ready-wire-first-v2','/tmp/bicdb-native-ready-participants'),
                'native_ready_tls':('actual-native-ready-tls-v1','/tmp/bicdb-native-ready-participants')}[g['group']]
        compiler=gzip.decompress((p/'runs'/forced[0]/'stderr.gz').read_bytes()).decode()
        assert f'Compiling bicdb-core v1.0.471-beta ({forced[1]}/crates/bicdb-core)' in compiler
        patch=gzip.decompress((p/'engine.patch.gz').read_bytes());assert sha(patch)==g['patch_sha256']
        rebuilt=False
        if repo:
            base=c['public_patch_base'];assert subprocess.check_output(['git','rev-parse',base+'^{commit}'],cwd=repo,text=True).strip()==base
            with tempfile.TemporaryDirectory(prefix='bicdb-native-evidence-') as tmp:
                tmp=Path(tmp);archive=tmp/'base.tar'
                with archive.open('wb') as out:subprocess.run(['git','archive',base,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
                with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
                subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,capture_output=True,check=True,timeout=60)
                source=json.loads((p/'source-files.json').read_text())
                assert {r['path'] for r in source}=={f.relative_to(tmp).as_posix() for f in (tmp/'crates/bicdb-core/src').rglob('*') if f.is_file()},'exact reconstructed core source coverage'
                for r in source:
                    b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
            rebuilt=True
        output.append({'group':g['group'],'source':g['source_revision'],'tests':len(ids),'source_reconstructed':rebuilt})
    prior=root/'prior-attempts';assert json.loads((prior/'owned-lease-wrong-guard-v1/result.json').read_text())['exit_code']==101
    assert 'test result: FAILED. 2 passed; 1 failed;' in (prior/'owned-lease-wrong-guard-v1/stdout').read_text()
    assert json.loads((prior/'ready-dto-privacy-compile-v1/result.json').read_text())['exit_code']==101
    return {'groups':output,'retained_evidence_verified':True,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
