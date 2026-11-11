#!/usr/bin/env python3
"""Retained characterization integrity only; passing test != recovered scenario."""
import argparse,gzip,hashlib,json,os,re,subprocess,sys,tempfile
from pathlib import Path
sys.dont_write_bytecode=True
BASE='b0e2498842be7b01e355bb8da1cc249ff16be056'
REV='8707ffa42fc0b20ff30e0dd5ebd8989e20207a0d'
PRIOR='5c65eb9b6f785f18dedbe483b274805ed454e8ef'
TREE='/tmp/bicdb-native-sparse-current-certification'
TEST='actual_interrupted_normal_image_cold_resume_is_characterized_without_forged_cut'
RUNS={'coherent-interrupted-normal-cold':(PRIOR,101,None),'coherent-interrupted-normal-cold-v2':(REV,-15,'resource_or1800s_wall_guard'),'coherent-interrupted-normal-cold-v3':(REV,0,None)}
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
def verify(directory,repo=None,private_dir=None):
    root=Path(directory);manifest=read(root,'MANIFEST.json')
    actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
    assert len(manifest)==len({r['path'] for r in manifest}) and actual=={r['path'] for r in manifest}
    for row in manifest:
        b=(root/row['path']).read_bytes();assert len(b)==row['bytes'] and sha(b)==row['sha256'],row['path']
    c=read(root,'claims.json');assert c['source_revision']==REV and c['source_version']=='1.0.471-beta' and c['public_patch_base']==BASE
    assert c['characterization_test_passed'] is True and c['passing_characterization_tests']==1
    assert c['recovery_scenario']=='FAILED_UNSUPPORTED_COLD_RESUME'
    for key in ['cold_recovery_success','new_current_source_cut_constructed','stale_authority_reused','activation_proven','strong_get_tested','native_runtime_fix_implemented','azure_cause_proven','data_loss_proven','physical_qualification','production_qualified','deployed','ci_tested','public_engine_merge_tested']:assert c[key] is False,key
    assert c['acknowledged_values_reconciled_on_source']==c['ledger_acknowledged_records']==1
    assert c['unknown2_resolved_on_source_but_not_new_ack'] is True and c['target_native_unknown_not_zero'] is True
    assert c['target_intent_retained'] is True and c['target_sparse_index']==2
    assert {r['name'] for r in c['runs']}==set(RUNS)
    for report in c['runs']:
        name=report['name'];revision,exit_code,reason=RUNS[name];d=root/'runs'/name
        a=read(d,'inputs.json');r=read(d,'result.json');assert all(r[k]==v for k,v in a.items())
        assert r['source_revision']==report['source_revision']==revision and r['dirty_diff_sha256']==sha(b'')
        assert r['exit_code']==report['exit_code']==exit_code and r['guard_stop_reason']==report['guard_stop_reason']==reason
        assert r['pause_episodes']==0 and r['external_fixture'] is None
        assert r['command']==['cargo','test','--release','--locked','-p','bicdb-core','--config','profile.release.package.bicdb-core.opt-level=0','--config','profile.release.package.bicdb-core.debug=0','--lib',TEST,'--features','tls-replication','--','--nocapture']
        assert r['environment']=={'CARGO_TARGET_DIR':'/tmp/bicdb-azure-artifact-target','CARGO_BUILD_JOBS':'1','CARGO_INCREMENTAL':'0','RUST_TEST_THREADS':'2'}
        samples=[json.loads(s) for s in gzip.decompress((d/'resources.jsonl.gz').read_bytes()).decode().splitlines()]
        assert samples and min(s['available_bytes'] for s in samples)==r['minimum_available_bytes']
        assert all(not s['paused'] and s['available_bytes']>=12*2**30 for s in samples)
        output=(d/'stdout').read_text();stderr=gzip.decompress((d/'stderr.gz').read_bytes()).decode()
        assert f'Compiling bicdb-core v1.0.471-beta ({TREE}/crates/bicdb-core)' in stderr
        if name== 'coherent-interrupted-normal-cold':
            assert 'error[E0308]' in stderr and 'Option<Arc<Record>>' in stderr and 'found `Option<Record>`' in stderr
            assert 'running 1 test' not in output and not re.search(r'^test \S+ \.\.\.',output,re.M)
            assert all(s['disk_free_bytes']>=8*2**30 for s in samples)
        elif name.endswith('-v2'):
            assert min(s['disk_free_bytes'] for s in samples)<8*2**30 and r['end_unix']-r['start_unix']<1800
            assert 'running 1 test' not in output and 'test result:' not in output
        else:
            assert all(s['disk_free_bytes']>=8*2**30 for s in samples)
            assert re.findall(r'^test (\S+) \.\.\. ok$',output,re.M)==['distribution_range_recovery::recovery_order_tests::interrupted_normal_repair::'+TEST]
            assert 'test result: ok. 1 passed; 0 failed; 0 ignored;' in output
            lines=[line.split(' ',1)[1] for line in stderr.splitlines() if line.startswith('INTERRUPTED_NORMAL_COLD_RESULT ')]
            assert len(lines)==1 and json.loads(lines[0])==read(root,'projections/cold-result.json')
    report=read(root,'projections/cold-result.json');before=read(root,'projections/before-close.json')
    assert report['characterization_test_passed'] is True and report['recovery_scenario']==c['recovery_scenario']
    for key in ['activation_proven','intent_cleared','new_current_source_cut_constructed','new_genesis_created_after_reopen','physical_qualification','published_image_base_present','stale_authority_reused','strong_get_tested']:assert report[key] is False,key
    assert report['unknown2_resolved_on_source_but_not_new_ack'] is True and report['original_catalogues_unchanged'] is True
    assert report['fresh_metadata_term']>before['scope']['metadata_term'] and report['fresh_metadata_read_index']>before['scope']['metadata_read_index']
    assert report['source_actual_native']==before['source_native']==2
    assert report['fresh_data_fence_observations']==1 and report['required_data_quorum']==2
    assert 'no distinct current-data-voter epoch-fence majority' in report['data_quorum_error']
    assert 'no retained recovery source freeze' in report['runtime_session_error']
    assert 'durable native image installation is incomplete' in report['target_fence_error'] and report['target_ready_error']==report['target_fence_error']
    assert before['stage']=='AFTER_CLEAR_BEFORE_ANY_ROW_COPY'
    native=report['target_actual_native'];assert native==before['target_native']
    assert native['native_applied_index'] is None and native['native_base_index'] is None and native['native_receipt_sha256'] is None
    assert native['factory_initialization_complete'] is False and native['invalidated'] is True
    assert report['target_local_db_sequence']==before['target_local_db_sequence']==0
    assert report['target_raw_last']==0 and report['target_sparse_index']==2
    assert report['original_target_open']==before['target_open'] and report['current_target_open']!=report['original_target_open']
    assert report['target_birth']==before['target_birth'] and report['target_birth_sha256']==before['target_birth_sha256'] and report['target_uid']==before['target_uid']
    source_before=report['source_identity_before_close'];source_after=report['source_identity_after_close'];assert source_before==before['source_identity']
    for k in ['birth','items_local_uid','native_range_index','db_sequence_observed']:assert source_before[k]==source_after[k]
    assert source_before['db_open']!=source_after['db_open'] and source_after['actual_native_visibility_bound']==2
    assert report['source_logical_history_sha256']==before['source_logical_history_sha256']==source_before['logical_history_sha256']
    cat=before['catalogue']['catalog'];intent=cat['image_install_intents']['1'];sparse=cat['sparse_committed_decisions']['1']['2']
    assert cat['ranges']['1']['last_index']==cat['ranges']['1']['resolved_through']==0
    assert sparse['checksum_sha256']==report['sparse_sha256']==before['sparse_sha256']
    assert sparse['command']['index']==intent['source_range_index']==2 and sparse['native_birth_id']==report['target_birth']
    assert intent['attempt_id']==report['image_attempt']==before['image_attempt']
    assert intent['target_namespace_ids']==[['items',report['target_uid']]]
    assert before['history_stage_id']==report['stage_id'] and before['history_stage_sha256']==report['stage_sha256']
    ledger=[json.loads(line) for line in (root/'projections/controller-acknowledged.jsonl').read_text().splitlines()]
    assert len(ledger)==1 and ledger[0]['acknowledged_key']=='interrupted-normal-ack-1'
    ack=(root/'projections/ack-record.canonical.json').read_bytes();assert sha(ack)==ledger[0]['record_sha256']==before['ack_record_sha256']
    assert json.loads(ack)=={'id':'interrupted-normal-ack-1','vector':None,'metadata':{},'timestamp':None,'payload':None}
    commands=read(root,'projections/source-retained-commands.json');assert commands['projection_is_additional_database_read'] is False
    entries=commands['actual_retained_entries'];assert set(entries)=={'1','2'} and all(e['state']=='applied' for e in entries.values())
    assert entries['1']['command']['mutations'][0]['record']==json.loads(ack)
    assert entries['2']['command']==sparse['command'] and entries['2']['command']['mutations'][0]['record_id']=='interrupted-normal-uncertain-2'
    exe=c['test_executable'];assert exe['sha256']=='1e054519a4e62110b9770338eae6fe4cc2e7f107983936896ea78d1032936aa8' and exe['bytes']==136517008
    assert exe['mutable_shared_test_executable'] is True and exe['recorded_mtime_within_successful_run'] is True and exe['release_binary'] is False
    custody=c['private_custody'];assert custody['manifest_sha256']=='6f958adcf53821fbea5d7159b22c1a2695fbf7956b9d7bf8c0a2c1a9043e111a'
    assert custody['files']==107 and custody['bytes']==1055754 and custody['classification']=='FAILED_UNSUPPORTED_COLD_RESUME'
    assert custody['database_opened_for_copy'] is False and custody['source_destination_hash_parity'] is True
    assert custody['directory_mode']=='0700' and custody['file_mode']=='0600' and custody['source_revision']==REV
    final=gzip.decompress((root/'engine.patch.gz').read_bytes());prior=gzip.decompress((root/'prior-5c65.patch.gz').read_bytes())
    assert sha(final)==c['patch_sha256'] and sha(prior)==c['prior_source']['patch_sha256'] and c['prior_source']['source_revision']==PRIOR
    if repo:reconstruct(root,repo,[(final,'source-files.json'),(prior,'prior-source-files.json')])
    if private_dir:
        p=Path(private_dir);m=p/'MANIFEST.json';assert sha(m.read_bytes())==custody['manifest_sha256'];records=read(p,'MANIFEST.json')['files']
        assert len(records)==custody['files'] and sum(r['bytes'] for r in records)==custody['bytes']
        assert {f.relative_to(p).as_posix() for f in p.rglob('*') if f.is_file()}=={'MANIFEST.json',*[r['path'] for r in records]}
        for row in records:
            f=p/row['path'];b=f.read_bytes();assert len(b)==row['bytes'] and sha(b)==row['sha256'] and f.stat().st_mode&0o777==0o600
        assert all(d.stat().st_mode&0o777==0o700 for d in [p,*[d for d in p.rglob('*') if d.is_dir()]])
    return {'source_revision':REV,'characterization_passed':True,'recovery_scenario':'FAILED_UNSUPPORTED_COLD_RESUME','rust_tests_rerun':False,'source_reconstructed':bool(repo),'private_custody_verified':bool(private_dir),'production_qualified':False}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',nargs='?',default=Path(__file__).parent);p.add_argument('--repo');p.add_argument('--private-dir');a=p.parse_args();print(json.dumps(verify(a.directory,a.repo,a.private_dir),sort_keys=True))
