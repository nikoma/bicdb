#!/usr/bin/env python3
"""Verify retained claims/source integrity; never a wire attestation or Rust rerun."""
import argparse, gzip, hashlib, json, os, re, subprocess, sys, tempfile
from pathlib import Path
sys.dont_write_bytecode = True
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
REV='daa77b42cd8f71a212de06596a249bfbce4d36e9'
PRIOR='453b86822bdc1a216e57b03d251dd42b573ad746'
PENDING='8ce4938c0ba44135f4d62745bdd414aa13e3a6c5'
TREE='/tmp/bicdb-native-read-quorum'
RUNS={'read-rpc-v1':(PRIOR,5,'native_read_rpc_'),'read-rpc-v2':(REV,15,'native_read_')}
EXES=[('7eca5958d8fc191597f3c8f0d04b48ea3e869bde75dbca70901c779d6ac98425',136206384),('f7cd6056cc638b60b15e526cd584028412e213457146ee83d2d08d1e014e36cf',136252624)]
def sha(b):return hashlib.sha256(b).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def reconstruct(root,repo,patches):
    # An isolated index/object database avoids checkout/caches or modifying the
    # supplied repository. Only BASE objects are borrowed read-only; each patch
    # is actually applied and every indexed source blob is checked afterwards.
    common=Path(subprocess.check_output(['git','rev-parse','--path-format=absolute','--git-common-dir'],cwd=repo,text=True).strip())
    assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
    with tempfile.TemporaryDirectory(prefix='bicdb-read-rpc-index-') as tmp:
        tmp=Path(tmp);gitdir=tmp/'isolated.git'
        subprocess.run(['git','init','--bare','--quiet',str(gitdir)],check=True,capture_output=True)
        env=os.environ.copy();env.update(GIT_INDEX_FILE=str(tmp/'index'),GIT_ALTERNATE_OBJECT_DIRECTORIES=str(common/'objects'))
        cmd=['git','--git-dir',str(gitdir)]
        subprocess.run(cmd+['read-tree',BASE],check=True,env=env,capture_output=True)
        for patch,source in patches:
            subprocess.run(cmd+['apply','--cached','--whitespace=nowarn','-'],input=patch,check=True,env=env,capture_output=True)
            listing=subprocess.check_output(cmd+['ls-files','--stage','--','crates/bicdb-core/src'],env=env,text=True)
            staged={line.split('\t',1)[1]:line.split()[1] for line in listing.splitlines()}
            rows=read(root,source);assert len(rows)==len({r['path'] for r in rows})
            assert set(staged)=={r['path'] for r in rows}
            payload=subprocess.check_output(cmd+['cat-file','--batch'],input=('\n'.join(staged[r['path']] for r in rows)+'\n').encode(),env=env)
            offset=0
            for row in rows:
                end=payload.index(b'\n',offset);header=payload[offset:end].decode().split();assert header[1]=='blob'
                size=int(header[2]);body=payload[end+1:end+1+size];offset=end+size+2
                assert len(body)==row['bytes']==size and sha(body)==row['sha256'],row['path']
            assert offset==len(payload)
def verify(directory,repo=None):
    root=Path(directory);manifest=read(root,'MANIFEST.json')
    assert len(manifest)==len({r['path'] for r in manifest})
    actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
    assert actual=={r['path'] for r in manifest},'manifest exact file set'
    for row in manifest:
        b=(root/row['path']).read_bytes();assert len(b)==row['bytes'] and sha(b)==row['sha256'],row['path']
    c=read(root,'claims.json');assert c['source_revision']==REV and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE
    assert c['passing_tests']==15 and c['total_executions']==20
    for key in ['authenticated_read_probe_rpc_tested','actual_fresh_mq_factual_candidate_tested','candidate_is_unadmitted']:assert c[key] is True,key
    for key in ['admitted_read_quorum_tested','strong_get_tested','row_snapshot_barrier_tested','final_fresh_metadata_barrier_tested','legacy_get_changed','host_new_birth_continuation_qualification','second_actual_rpc_counter_assertion_tested','lower_peer_history_comparison_dynamic_tested','external_ack_ledger_written','production_qualified','deployed','ci_tested','public_engine_merge_tested','hard_os_io_deadline_qualified']:assert c[key] is False,key
    assert {r['name'] for r in c['runs']}==set(RUNS)
    all_ids=[]
    for report in c['runs']:
        name=report['name'];rev,count,selector=RUNS[name];d=root/'runs'/name
        inputs=read(d,'inputs.json');result=read(d,'result.json')
        assert all(result[k]==v for k,v in inputs.items())
        assert result['source_revision']==report['source_revision']==rev
        assert result['dirty_diff_sha256']==sha(b'') and result['exit_code']==report['exit_code']==0
        assert result['pause_episodes']==0 and result['guard_stop_reason'] is None
        assert result['command']==['cargo','test','--release','--locked','-p','bicdb-core','--config','profile.release.package.bicdb-core.opt-level=0','--config','profile.release.package.bicdb-core.debug=0','--lib',selector,'--features','tls-replication','--','--nocapture']
        assert result['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
        samples=[json.loads(s) for s in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
        assert samples and min(s['available_bytes'] for s in samples)==result['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
        output=(d/'stdout').read_text();ids=re.findall(r'^test (\S+) \.\.\. ok$',output,re.M)
        assert len(ids)==len(set(ids))==report['passing_tests']==count and ids==report['passed_test_ids']
        assert re.search(rf'test result: ok\. {count} passed; 0 failed; 0 ignored;',output)
        assert len([i for i in ids if '::native_read_rpc_tests::' in i])==(5 if count==5 else 6)
        stderr=gzip.decompress((d/'stderr.gz').read_bytes()).decode()
        assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in stderr
        assert 'error[E' not in stderr
        all_ids+=ids
    assert len(all_ids)==20 and len(set(all_ids))==15
    for executable,expected in zip(c['test_executables'],EXES):
        assert (executable['sha256'],executable['bytes'])==expected
        assert executable['mutable_shared_test_executable'] is True
        assert executable['path']=='/tmp/bicdb-azure-artifact-target/release/deps/bicdb_core-b0702f49611c59ed'
    custody=c['private_custody'];assert custody=={'manifest_sha256':'9fb6d0825bf61bef1ed5835b65df114afdb57f0760799af2e016d952691905f7','files':47,'bytes':824217,'raw_database_keys_private':True,'source_databases_reopened':False}
    final=gzip.decompress((root/'engine.patch.gz').read_bytes());old=gzip.decompress((root/'prior-453.patch.gz').read_bytes());pending=gzip.decompress((root/'pending-retry-counter.patch.gz').read_bytes())
    assert sha(final)==c['patch_sha256'] and sha(old)==c['prior_source']['patch_sha256'] and c['prior_source']['source_revision']==PRIOR
    follow=c['pending_test_only_followup'];assert follow['source_revision']==PENDING and sha(pending)==follow['patch_sha256'] and follow['compiled'] is False and follow['tested'] is False
    if repo:
        reconstruct(root,repo,[(final,'source-files.json'),(old,'prior-source-files.json')])
        reconstruct(root,repo,[(final,'source-files.json'),(pending,'pending-source-files.json')])
    return {'source_revision':REV,'passing_tests':15,'executions':20,'source_reconstructed':bool(repo),'pending_counter_reconstructed_not_tested':bool(repo),'rust_tests_rerun':False,'candidate_is_unadmitted':True,'strong_get_tested':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
