#!/usr/bin/env python3
"""Check retained local native read evidence; this does not rerun Rust."""
import argparse,gzip,hashlib,json,re,subprocess,tarfile,tempfile
from pathlib import Path
REV='6d07ded1ec9e44d688f2263a7804c236b3aa9f14'
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
TREE='/tmp/bicdb-native-range-read-observation'
RUNS={'read-foundation-v1':('2b99c1e56f6d9f0429af4c0a9b8648e9e44c8dc3',101,0,0),'read-foundation-v2':('1c7d5ef917435a7ff2c1c27a96f43977deb2d80e',101,8,1),'read-foundation-v3':(REV,0,9,0)}
TESTS={'native_read_observation_recovered_serving_requires_actual_activated_open','native_read_observation_uses_current_r2_not_activation_anchor_and_refuses_cq_only_peer','native_read_observation_actual_cold_open_invalidates_old_lifetime','native_read_observation_actual_genesis_and_r1_use_target_local_data_b','native_read_observation_failed_mutation_and_health_invalidate_owned_observation','native_read_observation_rejects_actual_raw_prepare_and_cq_ahead_of_native','native_read_stamp_invalidates_database_freeze_and_controlled_paths','native_read_stamp_never_allocates_and_failed_writer_cannot_aba','native_read_observation_scope_cancellation_and_store_replacement_are_not_permissions'}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(root,repo=None):
    root=Path(root);assert not any(p.is_symlink() for p in root.rglob('*'))
    manifest=json.loads((root/'MANIFEST.json').read_text());actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
    assert len(manifest)==len(actual) and {r['path'] for r in manifest}==actual,'manifest exact coverage'
    for r in manifest:
        p=root/r['path'];assert p.resolve().is_relative_to(root.resolve());b=p.read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
    c=json.loads((root/'claims.json').read_text());assert c['source_revision']==REV and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE and c['worktree']==TREE and c['passing_tests']==9
    for k in ['local_read_observation_tested','current_r2_not_old_activation_anchor_tested','target_local_b_not_range_r_tested','actual_raw_applied_cannot_mint_native_floor_tested','gate_stamp_aba_negative_tested','request_scope_is_untrusted']:assert c[k] is True,k
    for k in ['fresh_membership_authority_from_observation','authenticated_read_probe_rpc_tested','read_quorum_tested','row_snapshot_barrier_tested','strong_get_tested','legacy_get_changed','production_qualified','deployed','public_engine_merge_tested','large_schema_or_range_latency_bound_tested','healthy_closed_sparse_read_birth_negative_tested','host_new_birth_continuation_qualification','external_ack_ledger_written','native_catalog_fsync_false_negative_tested','ci_tested']:assert c[k] is False,k
    assert {r['name'] for r in c['runs']}==set(RUNS)
    for report in c['runs']:
        name=report['name'];rev,exitcode,passed,failed=RUNS[name];d=root/'runs'/name
        a=json.loads((d/'inputs.json').read_text());r=json.loads((d/'result.json').read_text());assert all(r[k]==v for k,v in a.items())
        assert r['source_revision']==report['source_revision']==rev and r['dirty_diff_sha256']==sha(b'') and r['exit_code']==report['exit_code']==exitcode
        assert r['pause_episodes']==0 and r['guard_stop_reason'] is None
        assert r['command'][:7]==['cargo','test','--release','--locked','-p','bicdb-core','--config'] and '--features' in r['command'] and 'tls-replication' in r['command']
        assert 'profile.release.package.bicdb-core.opt-level=0' in r['command'] and 'profile.release.package.bicdb-core.debug=0' in r['command']
        assert r['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
        samples=[json.loads(s) for s in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()];assert samples and min(s['available_bytes'] for s in samples)==r['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 and s['disk_free_bytes']>=8*2**30 for s in samples)
        text=(d/'stdout').read_text();ok=re.findall(r'^test (\S+) \.\.\. ok$',text,re.M);bad=re.findall(r'^test (\S+) \.\.\. FAILED$',text,re.M)
        assert len(ok)==passed and len(bad)==failed and ok==report['passed_test_ids'] and bad==report['failed_test_ids']
        stderr=gzip.decompress((d/'stderr.gz').read_bytes()).decode();assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in stderr
        if name=='read-foundation-v1':assert 'error[E0063]' in stderr and 'error[E0596]' in stderr and 'running 9 tests' not in text
        else:
            assert re.search(rf'test result: (?:ok|FAILED)\. {passed} passed; {failed} failed; 0 ignored;',text)
            assert {t.rsplit('::',1)[-1] for t in ok+bad}==TESTS
            if name=='read-foundation-v2':assert 'outside r1' in stderr
    exe=c['test_executable'];assert exe['bytes']==135192976 and exe['sha256']=='d960579506a7a13435dc75db18ebbcfd33a29f0b4dfd8abaf82a6a7c6b89e3f6' and exe['mutable_shared_path'] is True and exe['release_binary'] is False
    compiler=gzip.decompress((root/'runs/read-foundation-v3/stderr.gz').read_bytes()).decode();assert f"Running unittests src/lib.rs ({exe['path']})" in compiler
    custody=c['private_custody'];assert custody['only_retained_successful_r2_roots']==['v2','v3'] and custody['files']==94 and custody['bytes']==1648434 and custody['manifest_sha256']=='07296d900ed993c48bc4978b6e8271273bbd4baae6a71e2ed9d0274af2435618'
    assert custody['raw_database_keys_private'] is True and custody['other_successful_temporary_databases_archived'] is False and custody['source_databases_reopened_for_custody'] is False
    patch=gzip.decompress((root/'engine.patch.gz').read_bytes());assert sha(patch)==c['patch_sha256']
    for delta in c['prior_source_deltas']:assert sha(gzip.decompress((root/delta['path']).read_bytes()))==delta['patch_sha256']
    reconstructed=False
    if repo:
        assert subprocess.check_output(['git','rev-parse',BASE+'^{commit}'],cwd=repo,text=True).strip()==BASE
        with tempfile.TemporaryDirectory(prefix='bicdb-native-read-source-') as tmp:
            tmp=Path(tmp);archive=tmp/'base.tar'
            with archive.open('wb') as out:subprocess.run(['git','archive',BASE,'--','Cargo.toml','Cargo.lock','.cargo','crates','examples/extensions'],cwd=repo,stdout=out,check=True,timeout=60)
            with tarfile.open(archive) as t:t.extractall(tmp,filter='data')
            subprocess.run(['git','apply','--whitespace=nowarn','-'],input=patch,cwd=tmp,capture_output=True,check=True,timeout=60)
            files=json.loads((root/'source-files.json').read_text());assert {r['path'] for r in files}=={p.relative_to(tmp).as_posix() for p in (tmp/'crates/bicdb-core/src').rglob('*') if p.is_file()}
            for r in files:
                b=(tmp/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
        reconstructed=True
    return {'source_revision':REV,'passing_tests':9,'earlier_compile_and_fixture_failures_retained':True,'source_reconstructed':reconstructed,'rust_tests_rerun':False,'authenticated_read_rpc_tested':False,'strong_get_tested':False,'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo),sort_keys=True))
