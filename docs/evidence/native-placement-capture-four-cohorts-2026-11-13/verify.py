#!/usr/bin/env python3
"""Offline source-scoped raw/fixture verification; never opens a native database."""
import argparse,gzip,hashlib,json,pathlib,re,subprocess,zipfile
ROOT=pathlib.Path(__file__).resolve().parent
EXPECTED={'212': {'artifact': 14006816760,
         'core_job': 131661983398,
         'counts': [2, 0, 0, 994, 2.61],
         'run': 43357200474,
         'source': '21259881268b1ae84b0c87b2211d519eaaf06703',
         'tests': {'db::range_image_native::rf3_fixture::native_historical_tls::native_strong_point_tests::joint_old_closure_tests::native_joint_actual_old_capture_finished_publish_cancel_does_not_renew_or_end_session': 'ok',
                   'db::range_image_native::rf3_fixture::native_historical_tls::native_strong_point_tests::joint_old_closure_tests::native_joint_actual_old_capture_initial_request_cancel_at_wait_refused': 'ok'},
         'verified_bytes': 0,
         'verified_files': 0,
         'workflow': 'success',
         'zip_bytes': 42178,
         'zip_entries': 4,
         'zip_sha256': '7db80961e77a4300e5a63ca1557790e9c9b3fd78c5b812302a8d176cfa3bca52'},
 '5de': {'artifact': 14006394476,
         'core_job': 131663347398,
         'counts': [13, 0, 0, 984, 30.34],
         'run': 43357622068,
         'source': '5de01e7e8916f3da9ea377484755ef12662f22ca',
         'tests': {'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_durable_invalidation_is_not_unknown_absence': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_foreign_database_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_nonleader_rpc_initiator_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_receiver_stop_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_sink_fsync_then_original_cancel_retains_refusal': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_sink_io_unknown_exact_live_refsync': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_store_slot_restore_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_captured_original_caller_cancel_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_generic_ingress_stays_strict': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_original_empty_hook_aba_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::native_registered_fifth_actual_closed_handoff_fence_without_initial_writer_baseline': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::placement_cases::native_registered_fifth_placement_actual_metadata_leader_can_initiate': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::placement_cases::native_registered_fifth_placement_actual_planner_retiree_is_not_initiator': 'ok'},
         'verified_bytes': 37891198,
         'verified_files': 1248,
         'workflow': 'success',
         'zip_bytes': 3109287,
         'zip_entries': 1352,
         'zip_sha256': 'e94c09e4c187f99469dcb67ec0f5ea1541cd6138b847123ca8c89aa497475268'},
 '751': {'artifact': 14004684180,
         'core_job': 131651839472,
         'counts': [10, 0, 0, 984, 16.89],
         'run': 43354081271,
         'source': '751fc28f45c849e0bdb9085744e123409cb0b580',
         'tests': {'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_durable_invalidation_is_not_unknown_absence': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_foreign_database_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_receiver_stop_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_sink_fsync_then_original_cancel_retains_refusal': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_sink_io_unknown_exact_live_refsync': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_actual_store_slot_restore_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_captured_original_caller_cancel_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_generic_ingress_stays_strict': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::closed_fence_controls::native_registered_fifth_closed_fence_original_empty_hook_aba_refused': 'ok',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::native_registered_fifth_actual_closed_handoff_fence_without_initial_writer_baseline': 'ok'},
         'verified_bytes': 29152656,
         'verified_files': 960,
         'workflow': 'success',
         'zip_bytes': 2446828,
         'zip_entries': 1043,
         'zip_sha256': 'c7408c407847b886a8f83d230c5f8f220d7fb2d50482f78b860b27246ad7ccd3'},
 '80a': {'artifact': 14006341679,
         'core_job': 131658467851,
         'counts': [0, 2, 0, 994, 4.56],
         'run': 43356111432,
         'source': '80aabf0fa5f25d8da652825e1c7927d67ce77934',
         'tests': {'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::placement_cases::native_registered_fifth_placement_actual_metadata_leader_can_initiate': 'FAILED',
                   'distribution_native_host::provisional_listener::tests::learner_enrollment_tests::promotion_tests::registered_image_baseline::placement_cases::native_registered_fifth_placement_actual_planner_retiree_is_not_initiator': 'FAILED'},
         'verified_bytes': 5821300,
         'verified_files': 192,
         'workflow': 'failure',
         'zip_bytes': 435073,
         'zip_entries': 209,
         'zip_sha256': '6bdebf5c8f0f580200ed7eee5d78ba065f2a02cf0bbd07ee9453bfc7a376cc00'}}

def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def zload(p):return json.loads(gzip.decompress(p.read_bytes()))
def case(t):return t.get('assignment_mode','OriginalLeader')+'/'+('Required' if t['phase']=='required_fence_acceptance' else t['reports'][2]['control'])
def derive(raw,tag):
 e=EXPECTED[tag];text=raw.decode();lines=text.splitlines()
 i=next(i for i,l in enumerate(lines) if '[command]/usr/bin/git log -1 --format=%H' in l)
 assert lines[i+1].split()[-1]==e['source'],'actual executed checkout source'
 actual_tests=re.findall(r'test ((?:distribution_native_host|db::)\S+) \.\.\. (ok|FAILED)',text)
 assert len(actual_tests)==len(set(n for n,_ in actual_tests))==sum(e['counts'][:2]),'actual name uniqueness'
 tests=dict(actual_tests)
 assert tests==e['tests'],'exact actual executed names and results'
 summary=re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; 0 measured; (\d+) filtered out; finished in ([0-9.]+)s',text)
 assert len(summary)==1 and [*[int(x) for x in summary[0][:4]],float(summary[0][4])]==e['counts'],'whole cohort outcome'
 ts=[json.loads(l[l.index('{'):]) for l in lines if 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in l]
 if tag=='212':
  assert not ts
  return {'source':e['source'],'tests':tests,'counts':e['counts'],'fixtures':[],
   'source_session_wire_proved':False,'remote_image_install_proved':False,'physical_qualified':False}
 assert len(ts)==sum(e['counts'][:2]);facts=[]
 for t in ts:
  assert t['source']==e['source'] and t['package_version']=='1.0.492-beta'
  assert t['initial_ranges']==[] and t['actual_original_writer_guards_retained']==4
  assert t['source_image_capture_exercised'] is False and t['old_closed_data_quorum_exercised'] is False
  assert t['local_image_source_session_wire_not_exercised'] and t['no_registered_row_or_new_data_quorum_claim']
  assert t['custody_error'] is None and t['evidence_errors_so_far']==[]
  acks=t['ack_ledger'];assert len(acks)==2
  for r,a in zip(t['reports'][:2],acks):
   n=r['range']
   assert a['actual_api_inserted'] is True and a['actual_api_error'] is None
   assert a['actual_full_record']['metadata']==a['value']=={'nested':[None,17,{'full':'payload'}],'object':{},'registered_fifth_source':n}
  mode=t.get('assignment_mode','OriginalLeader');fences=t['reports'][:2]
  assert len(fences)==2 and {r['range'] for r in fences}=={1,2}
  expected_successes=(1 if mode=='AutomaticReplicaPlan' else 0) if tag=='80a' else 2
  assert sum(r['actual_reply_ok'] for r in fences)==expected_successes
  assert t['required_baseline_succeeded'] is (tag!='80a')
  if tag=='80a':assert 'REQUIRED genuine registered fifth CommittedHandoff fences unavailable' in t['primary_error']
  else:assert t['primary_error'] is None
  for r in fences:
   assert r['actual_target']=='n4' and r['range_epoch']==2 and r['topology_generation']==5
   assert not r['actual_source']['plaintext_loopback']
   assert r['original_role_and_caller_live_at_reply'] and r['original_parent_remaining_ms']>0 and r['original_role_remaining_ms']>0
   assert not r['rows_copied'] and not r['serving_granted']
   assert 'CommittedHandoff' in r['scope']['purpose']
   for s in [r['before'],r['after']]:
    assert s['writer_installed'] is False and s['B']==0
    assert all(p['native_applied_index'] is None for p in s['points'].values())
   if r['actual_reply_ok']:
    assert r['actual_error'] is None
    o=r['actual_success_observation'];assert o['scope']==r['scope'] and o['node_id']=='n4' and o['is_active_recovery']
    assert o['native']['native_applied_index'] is None and o['native']['invalidated'] is True
    assert o['image_history']['native_point'] is None and o['store_open_id']==t['store_slot']
    assert o['native_database_incarnation']==r['after']['open']
    assert {k:v for k,v in r['before'].items() if k!='raw'}=={k:v for k,v in r['after'].items() if k!='raw'}
   else:
    assert r['actual_error']=='cluster error: cluster node n4 rejected RPC cluster_data_operation_failed: cluster error: native host bootstrap: assigned CLOSED fence differs from actual Pending target Learner/plan/birth'
    assert r['before']==r['after']
  if mode=='MetadataLeader':assert all(r['actual_source']['node_id']=='n0' for r in fences)
  if mode=='AutomaticReplicaPlan':
   moves=t['actual_automatic_plan']['replica_moves'];assert len(moves)==2
   for m,r in zip(moves,fences):
    assert m['target']=='n4' and m['source']==r['retiring_replica'] and m['range_id']==r['range'] and m['expected_epoch']==1 and m['reason']=='load_balance'
   assert any(r['retiring_replica']!=r['actual_source']['node_id'] for r in fences)
  if len(t['reports'])==3:
   c=t['reports'][2];assert c['reached_genuine_valid_capability'] and c['all_earlier_required_real_tls_fences_and_original_source_acks_passed']
   assert c['error']
   if c['control']=='Foreign':
    f=c['actual_foreign_database'];assert f['actual_initial_foreign_database_unchanged']
    assert f['before']==f['after'] and f['before']['writer_installed'] is True
    assert f['before']['birth']!=c['before']['birth'] and f['before']['open']!=c['before']['open']
    assert c['before']==c['after']
  facts.append(t)
 assert len({case(t) for t in facts})==len(facts)
 return {'source':e['source'],'tests':tests,'counts':e['counts'],'fixtures':sorted(facts,key=case),
  'source_session_wire_proved':False,'remote_image_install_proved':False,'physical_qualified':False}
def verify(root=ROOT,private_dir=None,repo=None):
 for f in load(root/'MANIFEST.json')['files']:
  b=(root/f['path']).read_bytes();assert len(b)==f['bytes'] and sha(b)==f['sha256'],f['path']
 assert load(root/'identity.json')==EXPECTED,'fixed source/run/test identity'
 totals={'tests':0,'passed':0,'failed':0,'fixtures':0,'fences':0,'acks':0,'custody_files':0}
 for tag,e in EXPECTED.items():
  d=root/tag;workflow=load(d/'workflow.json')
  assert workflow['id']==e['run'] and workflow['head_sha']==e['source'] and workflow['status']=='completed' and workflow['conclusion']==e['workflow']
  jobs=load(d/'jobs.json');assert {j['label'] for j in jobs}=={'core','compile','format'}
  for job in jobs:
   raw=gzip.decompress((d/(job['label']+'.log.gz')).read_bytes());assert sha(raw)==job['raw_sha256'] and len(raw)==job['raw_bytes']
   assert job['status']=='completed' and job['conclusion']==('failure' if tag=='80a' and job['label']=='core' else 'success')
   if job['label']=='compile':
    s=raw.decode();assert 'cargo check --workspace --all-targets --locked' in s and 'cargo check -p bicdb-workflow --all-targets --locked' in s
    assert len(re.findall(r'Finished.*`dev` profile',s))==2
  actual=derive(gzip.decompress((d/'core.log.gz').read_bytes()),tag)
  assert zload(d/'derived.json.gz')==actual,'all raw-to-derived facts'
  observations=zload(d/'artifact-observations.json.gz');receipt=load(d/'custody-receipt.json')
  assert receipt['source']==e['source'] and receipt['run']==e['run'] and receipt['artifact_id']==e['artifact'] and receipt['zip_sha256']==e['zip_sha256']
  assert receipt['engine_opened_or_replayed'] is False
  for key in ['zip_bytes','zip_entries','verified_files','verified_bytes']:assert receipt[key]==e[key]
  assert sum(m['files'] for m in receipt['manifests'])==receipt['verified_files'] and sum(m['bytes'] for m in receipt['manifests'])==receipt['verified_bytes']
  assert len(observations)==len(receipt['manifests'])==len(actual['fixtures'])
  assert sorted([o['terminal'] for o in observations],key=case)==actual['fixtures'],'artifact-to-fullraw terminal equality'
  for o in observations:
   m=next(m for m in receipt['manifests'] if m['path']==o['manifest']);assert m['sha256']==o['manifest_sha256'] and m['files']==96 and m['source']==e['source']
   assert m['status']=='observed_stable_with_live_actual_owners' and m['engine_owners_alive'] and m['atomic_powerloss_snapshot_claimed'] is False
   if 'closed-fence-controls.json' in o:
    assert o['closed-fence-controls.json']==o['terminal']['reports']
    intent=o['closed-fence-control-intent.json'];assert intent['reached_genuine_valid_capability'] and intent['outcome']=='before_actual_control'
    assert intent['case']==o['terminal']['reports'][2]['control']
   assert o['cleanup.json']['errors']==[]
  if private_dir:
   path=next(pathlib.Path(private_dir).glob('cohort-four-'+tag+'-artifact*.zip'));b=path.read_bytes();assert sha(b)==e['zip_sha256'] and len(b)==receipt['zip_bytes']
   with zipfile.ZipFile(path) as z:
    assert len(z.namelist())==receipt['zip_entries'] and z.read('source-revision.txt').decode().strip()==e['source']
    for o in observations:
     m=json.loads(z.read(o['manifest']));assert sha(z.read(o['manifest']))==o['manifest_sha256']
     base=o['manifest'].rsplit('/',1)[0];assert json.loads(z.read(base+'/ack-ledger.json'))==o['terminal']['ack_ledger']
     assert json.loads(z.read(base+'/terminal.json'))==o['terminal']
     assert m['all_five_metadata_service_db_native_journal_guards_held'] and m['engine_owners_alive'] and not m['listeners_drained'] and not m['engine_reopened'] and not m['native_replay_or_checkpoint']
     for f in m['files']:
      v=z.read(base+'/custody/'+f['path']);assert len(v)==f['bytes'] and sha(v)==f['sha256']
  if repo:subprocess.run(['git','-C',repo,'merge-base','--is-ancestor','e37c666660c2ce599fff516c1de9351706154b53',e['source']],check=True)
  if repo and tag!='751':assert subprocess.check_output(['git','-C',repo,'show','--format=fuller',e['source']])==(d/'source-change.patch').read_bytes()
  totals['tests']+=sum(e['counts'][:2]);totals['passed']+=e['counts'][0];totals['failed']+=e['counts'][1];totals['fixtures']+=len(actual['fixtures']);totals['fences']+=2*len(actual['fixtures']);totals['acks']+=2*len(actual['fixtures']);totals['custody_files']+=receipt['verified_files']
 assert totals=={'tests':27,'passed':25,'failed':2,'fixtures':25,'fences':50,'acks':50,'custody_files':2400}
 return totals
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--private-dir');p.add_argument('--repo');a=p.parse_args();print('PASS',verify(private_dir=a.private_dir,repo=a.repo))
