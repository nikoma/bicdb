#!/usr/bin/env python3
"""Verify only public event-API/history/catalogue observations, never open a DB."""
import hashlib,json,pathlib,re,sys,zipfile
if not __debug__:raise SystemExit('Run without Python -O; assertions are required')
SOURCE='e1ecfeab71d3f1e104530e9d6aa3bf37971abd65'
CASES={'TargetCatalogueFloor','TargetCatalogueFloorMissingBase','TargetCatalogueFloorDirSyncRetry','TargetCatalogueFloorCanceled'}
def sha(b):return hashlib.sha256(b).hexdigest()
def verify(path):
 results={};ack_count=0;closed_count=0;frame_count=0
 with zipfile.ZipFile(path)as archive:
  names=archive.namelist()
  assert archive.read('source-revision.txt').decode().strip()==SOURCE
  selected=re.findall(r'^(\S+): test$',archive.read('selected-tests.log').decode(),re.M)
  outcomes=re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',archive.read('tests.log').decode(),re.M)
  assert len(selected)==len(set(selected))==len(outcomes)==4
  assert {n for n,_ in outcomes}==set(selected) and [v for _,v in outcomes].count('FAILED')==2
  assert 'test result: FAILED. 2 passed; 2 failed;'in archive.read('tests.log').decode()
  for entry in names:
   if not entry.endswith('/execution-identity.json'):continue
   prefix=entry.rsplit('/',1)[0]+'/'
   read=lambda name:json.loads(archive.read(prefix+name))
   identity=read('execution-identity.json');case=identity['source_case']
   assert case in CASES and case not in results and identity['source_commit']==SOURCE and identity['package_version']=='1.0.494-beta'
   row=read('registered-image-before-final-assertions.json');record=row['expected_acknowledged_record']
   assert record==row['acknowledged_record_observed_locally'] and row['record_read_error']is None and row['after_error']is None
   assert record in row['after']['rows']['__bic_cluster_events'] and row['after']['rows']['items']==[]
   app=read('ack-ledger.json');assert len(app)==2;ack_count+=len(app)
   for ack in app:
    assert ack['operation']=='ClusterEventClient.append'and ack['collection']=='items'
    assert ack['outcome']=='returned'and ack['actual_api_inserted']and ack['actual_api_error']is None and not ack['effect_unknown_on_error']
    assert sha(json.dumps(ack['value'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())==ack['payload_json_sha256']
   chosen=[a for a in app if a['actual_full_record']==record];assert len(chosen)==1;chosen=chosen[0]
   journal=read('source-frame-ledger.json');frames=[v for v in journal if v['operation']=='source_image_frame'];frame_count+=len(frames)
   assert all(f['outcome']=='returned'and f['exact_error']is None for f in frames)
   manifests=[f['actual_reply']['payload']['Manifest']for f in frames if 'Manifest'in f['actual_reply']['payload']];assert len(manifests)==1;mf=manifests[0]
   for f in frames:
    assert f['original_parent_live_at_reply']and f['original_own_live_at_reply']
    assert f['intent']==f['actual_reply']['request']and f['actual_reply']['manifest_sha256']==mf['checksum_sha256']
   point=[mf['range_index'],mf['h0'],mf['h']]
   close=[f['actual_reply']for f in journal if f['operation']=='remote_old_close'];assert len(close)==len(mf['actual_old_closures'])==3;closed_count+=len(close)
   for c in close:
    assert c['constraints']['native_point']==point and c['closed']['range_index']==point[0]and c['closed']['logical_history_sha256']==point[2]
   history=next(f['actual_reply']['payload']['History']['history']for f in frames if 'History'in f['actual_reply']['payload'])
   assert history['source_range_index']==point[0]and history['logical_history_sha256']==point[2]
   last=next(v for v in history['entries']if v['index']==point[0]);assert last['outcome']['kind']=='applied'
   assert all(c['constraints']['commands'][str(point[0])]==last['outcome']['command_checksum_sha256']for c in close)
   old=read('remote-old-ledger.json');assert journal[:len(old)]==old
   collection=next(v for v in old if v['operation']=='remote_old_fresh_all_closed_collection')
   assert collection['native_before']==collection['native_after']and collection['native_receipts_unchanged']and collection['original_mutation_requests_expired']
   assert len(collection['native_before'])==3 and collection['all_closed_and_irreversible_constraints_required']
   cleanup=read('cleanup.json');assert cleanup['errors']==[] and read('evidence-errors.json')['errors']==[]
   result={'source_case':case,'source_APP_returned_ACKs':2,'APP_failed':0,'APP_uncertain':0,'logical_collection':'items','physical_collection':'__bic_cluster_events','physical_items_empty':True,'selected_record_id':chosen['record_id'],'selected_payload_sha256':chosen['payload_json_sha256'],'selected_ACK_record_observed_before_base':True,'actual_source_frames':len(frames),'mixed_journal_entries':len(journal),'actual_OLD_closes':3,'native_OLD_records_unchanged':True}
   terminal=read('terminal.json')
   if case=='TargetCatalogueFloorMissingBase':
    d=read('registered-catalogue-floor-missing-base.json')
    assert d['before']==d['after']and d['owner_before']==d['owner_after']and d['image_before']==d['image_after']
    assert 'requires original actual verified native base'in d['exact_error']
    assert terminal['required_baseline_succeeded']and terminal['primary_error']is None
    result.update(classification='passed',catalogue_adopted=False,native_base_issued=False,exact_error=d['exact_error'])
   elif case=='TargetCatalogueFloor':
    d=read('registered-catalogue-floor-before-assertions.json');base=d['native_after']['base']
    assert d['native_before']==d['native_after']and base==d['native_after']['archive']and d['image_before']==d['image_after']
    assert d['first_progress']==d['second_progress']and d['catalogue_floor_adopted']and not d['serving']
    key=str(mf['capture_range']['id']);old=d['old_catalogue'];new=d['new_catalogue'];floor=new['ranges'][key];binding=new['native_image_bases'][key]
    assert all(floor[k]==point[0]for k in ['last_index','resolved_through','compacted_through'])and floor['entries']=={}
    assert binding['range_index']==base['range_index']==point[0]and binding['native_receipt_sha256']==base['checksum_sha256']
    assert binding['logical_genesis_sha256']==base['logical_genesis_sha256']==point[1]and binding['logical_history_sha256']==base['logical_history_sha256']==point[2]
    expected=json.loads(json.dumps(old));expected['ranges'][key]=floor;expected['native_image_bases']=new['native_image_bases'];assert expected==new
    assert d['owner_after']['catalogue_floor']['verified']and d['owner_before']['actual_owner']==d['owner_after']['actual_owner']
    for k in ['B','birth','open','namespace_ids','schema','protected_catalogue_uuid','points','slots','rows','writer_installed']:assert d['before'][k]==d['after'][k]
    assert not d['after']['writer_installed']and record in d['after']['rows']['__bic_cluster_events']
    assert terminal['required_baseline_succeeded']and terminal['primary_error']is None
    result.update(classification='passed',native_base_xid=base['native_page_xid'],final_image_xid=base['registered_final_image']['native_page_xid'],catalogue_adopted=True,serving=False,selected_full_ACK_payload_observed_after_adoption=True,only_selected_catalogue_floor_and_binding_changed=True,ordinary_API_refusal=d['ordinary_after_adoption_error'])
   else:
    assert cleanup['primary_failure_preserved']=='called `Result::unwrap()` on an `Err` value: QueryTimedOut'
    assert not terminal['required_baseline_succeeded']
    # Absence of these observations means the attempted mutation is UNKNOWN,
    # never a proof that its WAL/base/catalogue was absent or rolled back.
    assert not any(n.startswith(prefix+'registered-catalogue-floor-')for n in names)
    assert next(iter(frames[-1]['actual_reply']['payload']))=='Finished'
    result.update(classification='failed',failed_stage='PublishNativeBase prerequisite before fault arming; catalogue_tests.rs:102',exact_error='QueryTimedOut',named_fault_control='not tested',native_base_attempt_outcome='inconclusive; post-error owner/native-base observation missing',catalogue_adoption_observed=False,selected_ACK_post_timeout_visibility='not tested')
   results[case]=result
 assert set(results)==CASES and ack_count==8 and closed_count==12 and frame_count==71
 return {'source_revision':SOURCE,'cases':results,'executed_passed':2,'executed_failed':2,'source_APP_returned_ACKs':ack_count,'actual_OLD_closes':closed_count,'actual_source_frames':frame_count,'named_fault_controls_not_tested':2,'internal_native_setup_outcomes_inconclusive':2,'database_opened':False,'production_qualified':False}
if __name__=='__main__':print(json.dumps(verify(pathlib.Path(__file__).resolve().parent/'artifacts/catalogue4-public.zip'),indent=2))
