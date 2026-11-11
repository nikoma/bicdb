#!/usr/bin/env python3
"""Verify revision-scoped component receipts, not production or packet authenticity."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
REV='7b36642b444dc57051dde1fbb369f15ddb3cc3e1'
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
TREE='/tmp/bicdb-native-activation-participants'
RUNS={'selected-key-decision':(REV,0,4,0),'decision-activation-controls':(REV,0,2,0),'decision-late-control':(REV,0,1,0),'decision-incoming-complete-controls':(REV,0,3,0),'activated-rplusone':(REV,101,0,1),'d77f4743-native-decision':('d77f474397c5beeea42e26bbde06552dd0099c17',101,3,1),'corrected-native-decision':('958d087d522e53a32b97cc1daa74f69250e549db',101,3,1)}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
    root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*'))
    manifest=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
    assert {r['path'] for r in manifest}==actual and len(manifest)==len(actual),'manifest exact coverage'
    for r in manifest:
        path=root/r['path'];assert path.resolve().is_relative_to(root.resolve());b=path.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
    c=json.loads((root/'claims.json').read_text());assert c['source_revision']==REV and c['public_patch_base']==BASE and c['source_version']=='1.0.471-beta' and c['worktree']==TREE
    for k in ['native_activation_tested','late_prepared_after_one_endpoint_loss_tested','protected_native_decision_cold_reissue_tested']:assert c[k] is True,k
    for k in ['incoming_activate_rpc_tested','new_application_write_after_activation_tested','strong_get_tested','physical_failure_tested','production_qualified','main_merged','binary_release_produced','green_database_archive_retained']:assert c[k] is False,k
    assert {r['name'] for r in c['runs']}==set(RUNS)
    ids=[];minimum=10**30
    for report in c['runs']:
        name=report['name'];rev,exitcode,passed,failed=RUNS[name];d=root/'runs'/name
        a=json.loads((d/'inputs.json').read_text());b=json.loads((d/'result.json').read_text());assert all(b[k]==v for k,v in a.items())
        assert b['source_revision']==report['source_revision']==rev and b['exit_code']==report['exit_code']==exitcode and b['dirty_diff_sha256']==sha(b'')
        assert b['guard_stop_reason'] is None and b['pause_episodes']==0
        assert b['command'][:7]==['cargo','test','--release','--locked','-p','bicdb-core','--config']
        for flag in ['profile.release.package.bicdb-core.opt-level=0','profile.release.package.bicdb-core.debug=0']:assert flag in b['command']
        assert b['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
        samples=[json.loads(line) for line in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()];assert samples and min(s['available_bytes'] for s in samples)==b['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
        text=(d/'stdout').read_text();ok=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',text,re.M)
        assert len(ok)==passed and len(bad)==failed and ok==report['passed_test_ids'] and bad==report['failed_test_ids']
        assert re.search(rf'test result: (?:ok|FAILED)\. {passed} passed; {failed} failed; 0 ignored;',text)
        if rev==REV and exitcode==0:ids+=ok;minimum=min(minimum,b['minimum_available_bytes'])
    assert len(ids)==len(set(ids))==c['distinct_passing_tests']==c['passing_executions']==10
    assert sorted(ids)==c['passing_test_ids'] and minimum==c['minimum_available_bytes']
    compiler=gzip.decompress((root/'runs'/'selected-key-decision'/'stderr.gz').read_bytes()).decode();assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in compiler
    exe=json.loads((root/'test-executable.json').read_text());assert exe['bytes']>0 and len(exe['sha256'])==64 and exe['mutable_shared_target_path'] is True and exe['release_binary'] is False
    assert f"Running unittests src/lib.rs ({exe['path']})" in compiler
    rplus=gzip.decompress((root/'runs'/'activated-rplusone'/'stderr.gz').read_bytes()).decode();assert '"cached_epoch_seeded":false,"error":null' in rplus and '"acknowledged":false' in rplus and 'range-quorum commit authority is not ready' in rplus
    assert 'schema has no current verified digest' in gzip.decompress((root/'runs'/'d77f4743-native-decision'/'stderr.gz').read_bytes()).decode()
    assert 'stage-actual-origin-0' in gzip.decompress((root/'runs'/'corrected-native-decision'/'stderr.gz').read_bytes()).decode()
    patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['patch_sha256']
    for d in c['earlier_source_deltas']:assert sha(gzip.decompress((root/d['path']).read_bytes()))==d['sha256']
    follow=root/'followup-1cd';fc=json.loads((follow/'claims.json').read_text());fr=json.loads((follow/'result.json').read_text());fi=json.loads((follow/'inputs.json').read_text())
    assert fc['source_revision']==fr['source_revision']=='1cd28523c2f8a3970635dcae4ca43977727b1afe' and all(fr[k]==v for k,v in fi.items())
    assert fc['test_count']==1 and fc['actual_new_application_ack'] is True
    for key in ['late_follower_caught_up_r2','incoming_activate_rpc_tested','strong_get_tested','physical_qualification','production_qualified']:assert fc[key] is False,key
    assert fr['exit_code']==0 and fr['dirty_diff_sha256']==sha(b'') and fr['guard_stop_reason'] is None and fr['pause_episodes']==0
    assert 'test result: ok. 1 passed; 0 failed; 0 ignored;' in (follow/'stdout').read_text()
    fs=[json.loads(s) for s in gzip.decompress((follow/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
    assert fs and min(s['available_bytes'] for s in fs)==fr['minimum_available_bytes'] and all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in fs)
    assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in gzip.decompress((follow/'stderr.gz').read_bytes()).decode()
    recover=json.loads((follow/'rplusone-coordinator-recovery.json').read_text());origin=json.loads((follow/'rplusone-origin-result.json').read_text());late=json.loads((follow/'rplusone-late-activation-report.json').read_text())
    assert recover['result']==1 and recover['error'] is None and recover['cached_epoch_seeded'] is False
    assert origin['acknowledged'] is True and origin['source_native']['native_applied_index']==2 and origin['late_node_native']['native_applied_index']==origin['late_node_raw']['last_index']==1
    assert late['actual_new_application_ack'] is True and late['source_actual_native']==2 and late['late_actual_native']==1 and late['late_row2_present'] is False and late['current_native_catchup_of_late_follower_proven'] is False
    assert late['source_activated_anchor']['range_index']==late['late_activated_anchor']['range_index']==1 and late['old_r1_decision_not_ready_or_current_head'] is True
    ledger=[json.loads(s) for s in (follow/'controller-rplusone-ack.jsonl').read_text().splitlines()];assert len(ledger)==1 and ledger[0]['range_index']==2 and ledger[0]['key']=='actual-rplusone-after-activation'
    followpatch=gzip.decompress((follow/'source-from-7b.patch.gz').read_bytes());assert sha(followpatch)==fc['patch_sha256']
    reconstructed=False
    if repo:
        assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
        with tempfile.TemporaryDirectory(prefix='bicdb-activation-source-') as tmp:
            tmp=Path(tmp);archive=tmp/'base.tar'
            with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
            with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,capture_output=True,check=True,timeout=60)
            source=json.loads((root/'source-files.json').read_text());assert {r['path'] for r in source}=={p.relative_to(tmp).as_posix() for p in (tmp/'crates/bicdb-core/src').rglob('*') if p.is_file()}
            for r in source:
                b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=followpatch,cwd=tmp,capture_output=True,check=True,timeout=60)
            follow_source=json.loads((follow/'source-files.json').read_text());assert {r['path'] for r in follow_source}=={p.relative_to(tmp).as_posix() for p in (tmp/'crates/bicdb-core/src').rglob('*') if p.is_file()}
            for r in follow_source:
                b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
        reconstructed=True
    return {'source_revision':REV,'passing_tests':10,'followup_revision':fc['source_revision'],'followup_passing_tests':1,'followup_actual_ack_r2':True,'late_follower_caught_up_r2':False,'separate_expected_or_fixture_failures':3,'retained_evidence_verified':True,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
