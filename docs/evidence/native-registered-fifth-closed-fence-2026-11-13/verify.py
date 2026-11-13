#!/usr/bin/env python3
"""Offline exact-source failed-cohort receipt; optional private byte verification."""
import argparse,gzip,hashlib,json,pathlib,re,zipfile
ROOT=pathlib.Path(__file__).resolve().parent
SOURCE='da56e489a34c1776e97a4ea089c1f250215cf8dc'
CORRECTION='550c23d0458ab7a4e10af94d1cc625a7eb69bb89'
FOREIGN='cluster error: native range image: native recovery birth/bootstrap/local member binding mismatch'
PREFIX='distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::'
CASES={
'native_registered_fifth_actual_closed_handoff_fence_without_initial_writer_baseline':'Required',
'native_registered_fifth_closed_fence_actual_foreign_database_refused':'Foreign',
'native_registered_fifth_closed_fence_actual_durable_invalidation_is_not_unknown_absence':'ActualInvalidation',
'native_registered_fifth_closed_fence_actual_sink_fsync_then_original_cancel_retains_refusal':'SinkCanceled',
'native_registered_fifth_closed_fence_actual_receiver_stop_refused':'Stopped',
'native_registered_fifth_closed_fence_actual_store_slot_restore_refused':'SlotAba',
'native_registered_fifth_closed_fence_actual_sink_io_unknown_exact_live_refsync':'SinkIoUnknown',
'native_registered_fifth_closed_fence_captured_original_caller_cancel_refused':'Canceled',
'native_registered_fifth_closed_fence_generic_ingress_stays_strict':'Ingress',
'native_registered_fifth_closed_fence_original_empty_hook_aba_refused':'HookAba'}
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def terminals(raw):
    return [json.loads(l[l.index('{'):]) for l in raw.decode().splitlines() if 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in l]
def derive(raw):
    text=raw.decode();lines=text.splitlines()
    i=next(i for i,l in enumerate(lines) if '[command]/usr/bin/git log -1 --format=%H' in l)
    assert lines[i+1].split()[-1]==SOURCE,'actual checkout source'
    tests=re.findall(r'test ('+re.escape(PREFIX)+r'\S+) \.\.\. (ok|FAILED)',text)
    assert len(tests)==10 and len({n for n,_ in tests})==10
    assert {n.split('::')[-1] for n,_ in tests}==set(CASES),'actual ten executed names'
    assert sum(r=='ok' for _,r in tests)==9
    assert [(CASES[n.split('::')[-1]],r) for n,r in tests if r=='FAILED']==[('Foreign','FAILED')]
    assert 'test result: FAILED. 9 passed; 1 failed; 0 ignored; 0 measured; 980 filtered out; finished in 17.87s' in text
    ts=terminals(raw);assert len(ts)==10
    facts=[]
    for t in ts:
        assert t['source']==SOURCE and t['package_version']=='1.0.491-beta'
        assert t['initial_ranges']==[] and t['actual_original_writer_guards_retained']==4
        assert not t['source_image_capture_exercised'] and not t['old_closed_data_quorum_exercised']
        assert t['local_image_source_session_wire_not_exercised'] and t['no_registered_row_or_new_data_quorum_claim']
        assert t['custody_error'] is None and t['evidence_errors_so_far']==[]
        case='Required' if t['phase']=='required_fence_acceptance' else ('Foreign' if t['primary_error'] else t['reports'][2]['control'])
        assert case in CASES.values()
        assert t['required_baseline_succeeded']==(case!='Foreign')
        assert t['primary_error']==(FOREIGN if case=='Foreign' else None)
        acks=t['ack_ledger'];assert len(acks)==2
        for n,a in enumerate(acks,1):
            assert a['actual_api_inserted'] is True and a['actual_api_error'] is None
            assert a['actual_full_record']['metadata']==a['value']
            assert a['value']=={'nested':[None,17,{'full':'payload'}],'object':{},'registered_fifth_source':n}
        fences=t['reports'][:2];assert [r['range'] for r in fences]==[1,2]
        for n,r in enumerate(fences,1):
            assert r['actual_reply_ok'] and r['actual_error'] is None and r['actual_target']=='n4'
            assert r['actual_source']['node_id']=='n'+str(n) and not r['actual_source']['plaintext_loopback']
            assert r['range_epoch']==2 and r['topology_generation']==5
            assert r['original_role_and_caller_live_at_reply'] and r['original_parent_remaining_ms']>0 and r['original_role_remaining_ms']>0
            assert not r['rows_copied'] and not r['serving_granted']
            o=r['actual_success_observation'];assert o['scope']==r['scope'] and o['node_id']=='n4' and o['is_active_recovery']
            assert o['native']['native_applied_index'] is None and o['native']['invalidated'] is True
            assert o['image_history']['native_point'] is None
            assert o['native_database_incarnation']==r['after']['open'] and o['store_open_id']==t['store_slot']
            assert 'CommittedHandoff' in r['scope']['purpose']
            for state in [r['before'],r['after']]:
                assert state['writer_installed'] is False and state['B']==0
                assert all(x['native_applied_index'] is None for x in state['points'].values())
            assert {k:v for k,v in r['before'].items() if k!='raw'}=={k:v for k,v in r['after'].items() if k!='raw'}
        if case=='Foreign':assert len(t['reports'])==2 # failed before appended control/afterstate proof
        elif case!='Required':
            c=t['reports'][2];assert c['reached_genuine_valid_capability'] and c['all_earlier_required_real_tls_fences_and_original_source_acks_passed']
        facts.append({'case':case,'terminal':t})
    assert {x['case'] for x in facts}==set(CASES.values())
    return {'source':SOURCE,'run':43351805110,'core_job':131644446840,'workflow':'failure',
        'runtime':{'passed':9,'failed':1,'ignored':0,'filtered':980,'seconds':17.87},
        'tests':[{'name':n,'case':CASES[n.split('::')[-1]],'result':r} for n,r in tests],
        'fixtures':sorted(facts,key=lambda x:x['case']),
        'correction':{'source':CORRECTION,'executed_in_this_run':False},
        'physical_qualification':False,'engine_replay':False,'rows_transferred':False,'automatic_relocation':False}
def verify(root=ROOT,private_zip=None):
    manifest=load(root/'MANIFEST.json')
    for x in manifest['files']:
        b=(root/x['path']).read_bytes();assert len(b)==x['bytes'] and sha(b)==x['sha256'],x['path']
    for label,job,conclusion in [('core',131644446840,'failure'),('compile',131644404794,'success'),('formatting',131644404499,'success')]:
        b=(root/(label+'.log.gz')).read_bytes();raw=gzip.decompress(b);idx=load(root/(label+'-index.json'))
        assert idx['source_revision']==SOURCE and idx['run_id']==43351805110 and idx['job_id']==job and idx['job_conclusion']==conclusion
        assert idx['raw_sha256']==sha(raw) and idx['raw_bytes']==len(raw)
        assert (idx.get('compressed_sha256') or idx.get('gzip_sha256'))==sha(b)
    raw=gzip.decompress((root/'core.log.gz').read_bytes())
    actual=derive(raw);assert actual==json.loads(gzip.decompress((root/'derived.json.gz').read_bytes())),'full raw-to-derived parity'
    assert load(root/'results.json')=={k:v for k,v in actual.items() if k!='fixtures'}
    compile=gzip.decompress((root/'compile.log.gz').read_bytes()).decode()
    assert 'cargo check --workspace --all-targets --locked' in compile and 'cargo check -p bicdb-workflow --all-targets --locked' in compile
    assert len(re.findall(r'Finished.*`dev` profile',compile))==2
    receipt=load(root/'custody-receipt.json')
    assert len(receipt['fixtures'])==10 and receipt['source']==SOURCE and receipt['engine_opened_or_replayed'] is False
    assert all(x['files']==96 and x['missing']==[] and x['guards_held'] for x in receipt['fixtures'])
    assert sum(x['files'] for x in receipt['fixtures'])==960 and sum(x['bytes'] for x in receipt['fixtures'])==29152656
    controls=json.loads(gzip.decompress((root/'controls.json.gz').read_bytes()));assert len(controls)==9
    foreign=next(x for x in controls if x['case']=='Foreign')
    assert foreign['intent']['reached_genuine_valid_capability'] and foreign['intent']['outcome']=='before_actual_control'
    assert foreign['cleanup']['primary_failure_preserved']==FOREIGN and foreign['control_result'] is None
    assert all(x['cleanup']['errors']==[] for x in controls)
    for c in controls:
        t=next(f['terminal'] for f in actual['fixtures'] if f['case']==c['case'])
        if c['case']!='Foreign':assert c['control_result']==t['reports']
        assert c['intent']['case']==c['case'] and c['intent']['scope']['range_id']==1
    if private_zip:
        zbytes=pathlib.Path(private_zip).read_bytes();assert len(zbytes)==2390349 and sha(zbytes)=='57732a282f49f5e0204d81c06521ac4cdaed96abf7bd07c39d6e5d9f08899664'
        with zipfile.ZipFile(private_zip) as z:
            assert len(z.namelist())==1042 and z.read('source-revision.txt').decode().strip()==SOURCE
            for f in receipt['fixtures']:
                mb=z.read(f['manifest']);assert sha(mb)==f['manifest_sha256'];m=json.loads(mb)
                prefix=f['manifest'].rsplit('/',1)[0]+'/custody/'
                for x in m['files']:
                    b=z.read(prefix+x['path']);assert len(b)==x['bytes'] and sha(b)==x['sha256']
            for f in actual['fixtures']:
                target=next(n for n in z.namelist() if n.endswith('/terminal.json') and json.loads(z.read(n))==f['terminal'])
                assert target
    print('PASS: exact failed9/1, ten names/fixtures, twenty actual fences and ACKs; no engine/replay')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--private-zip');a=p.parse_args();verify(private_zip=a.private_zip)
