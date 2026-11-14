#!/usr/bin/env python3
"""Cross-check pinned source/target reports; never opens retained databases."""
import hashlib,json,pathlib,re,zipfile
if not __debug__:raise SystemExit('Run without Python -O; assertions are required')
ROOT=pathlib.Path(__file__).resolve().parent
SOURCE='d6908095e0d347e563ef68801f49e52dba35452d'
SHA='c619403d673014598039e968723c63d058e2165d0f56e718ca93b11ebc3684f5'
PATH=ROOT/'artifacts/rows5-public.zip'
assert hashlib.sha256(PATH.read_bytes()).hexdigest()==SHA
CASES={'TargetImageRows','TargetImageRowsPreWalCanceled','TargetImageRowsPostWalCanceled','TargetImageRowsWithoutAudit','TargetImageRowsApplyFailed'}
def encoded(v):return json.dumps(v,ensure_ascii=False,separators=(',',':')).encode()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'))
def token(namespace,key):
 n,k=namespace.encode(),key.encode()
 return int.from_bytes(hashlib.sha256(b'bicdb-distribution-key-v1\0'+len(n).to_bytes(8,'big')+n+len(k).to_bytes(8,'big')+k).digest()[:8],'big')
results={};ack_count=0;source_frames=0;source_closed=0;target_retries=0;wal_checks={}
with zipfile.ZipFile(PATH) as z:
 assert z.read('source-revision.txt').decode().strip()==SOURCE
 assert re.search(r'test result: ok\. 5 passed; 0 failed;',z.read('tests.log').decode())
 assert not any('/custody/' in n for n in z.namelist())
 for identity_name in z.namelist():
  if not identity_name.endswith('/execution-identity.json'):continue
  prefix=identity_name.rsplit('/',1)[0]+'/'
  load=lambda n:json.loads(z.read(prefix+n))
  identity=load('execution-identity.json');case=identity['source_case'];assert case in CASES and case not in results
  assert identity['source_commit']==SOURCE and identity['package_version']=='1.0.494-beta'
  terminal=load('terminal.json');assert terminal['source']==SOURCE and terminal['required_baseline_succeeded'] and terminal['primary_error'] is None and terminal['custody_error'] is None
  assert load('cleanup.json')['errors']==[] and load('evidence-errors.json')['errors']==[]
  acks=load('ack-ledger.json');assert len(acks)==2
  assert all(a['outcome']=='returned' and a['actual_api_inserted'] and not a['effect_unknown_on_error'] and a['actual_api_error'] is None for a in acks);ack_count+=len(acks)
  old=load('remote-old-ledger.json');close=[v for v in old if v['operation']=='remote_old_close'];assert len(close)==3
  assert all(v['outcome']=='returned_actual_closure' and v['exact_error'] is None for v in close);source_closed+=len(close)
  collection=next(v for v in old if v['operation']=='remote_old_fresh_all_closed_collection')
  assert collection['outcome']=='returned_actual_all_old_certificate' and collection['exact_error'] is None
  assert len(collection['old_members'])==3 and set(collection['old_members'])==set(collection['native_before'])==set(collection['native_after'])
  assert collection['native_before']==collection['native_after'] and collection['native_receipts_unchanged']
  assert collection['original_mutation_requests_expired'] and collection['all_closed_and_irreversible_constraints_required']
  manifest=load('registered-proposal-1.json')['record_after_first']['source']
  ledger_all=load('source-frame-ledger.json');assert ledger_all[:len(old)]==old
  assert {v['operation'] for v in ledger_all} <= {'source_seed_selection','remote_old_close','remote_old_fresh_all_closed_collection','source_image_frame'}
  ledger=[v for v in ledger_all if v['operation']=='source_image_frame'];source_frames+=len(ledger)
  assert len(ledger)>0
  for event in ledger:
   reply=event['actual_reply'];assert reply is not None and event['original_parent_live_at_reply'] and event['original_own_live_at_reply']
   assert reply['request']==event['intent'] and reply['manifest_sha256']==manifest['checksum_sha256']
  assert sum('Manifest' in e['actual_reply']['payload'] for e in ledger)==1
  if case=='TargetImageRowsWithoutAudit':
   d=load('registered-image-missing-audit.json');owner=d['actual_owner']
   assert 'full stored history not audited' in d['error'] and d['no_row_wal'] and d['no_image_or_serving']
   assert d['before']['B']==0 and not d['before']['writer_installed'] and not owner['history']['audited']
   assert owner['rows'] is None
   results[case]={'classification':'PASSED_ACTUAL_MISSING_STORED_AUDIT_REFUSAL','exact_error':d['error'],'target_row_wal_admitted':False,'seam_reached':True}
   continue
  if case=='TargetImageRowsApplyFailed':
   d=load('registered-image-inner-durable-apply-failure.json');pending=d['retained']['rows']['pending'];progress=d['retained']['rows']['progress']
   assert d['durable_logical_wal_proven_at_actual_inner_boundary'] and not d['apply_complete'] and not d['row_progress_complete']
   assert pending['logical_outcome']=='wal_durable_pending_apply' and pending['logical_sequence']==1
   assert pending['intent_commit_returned']==pending['intent']['native_xid'] and pending['intent']['logical_txid']>0
   assert not pending['actual_transaction_retained'] and pending['progress_attempt'] is None
   assert progress['records']==0 and progress['last_actual_target_b'] is None and not progress['complete']
   wal=load('registered-image-wal-retention.json');assert wal['phase']=='inner_apply_failed_before_retention_assertion' and wal['actual_active_pins']==1 and wal['actual_owner']==d['retained'] and wal['no_cold_or_serving_claim']
   wal_checks[case]={'actual_pending_apply_retention_leases':1,'explicit_checkpoint_exercised':False,'retained_original_logical_txid':pending['intent']['logical_txid'],'fenced_retry_keeps_same_owner':True}
   assert 'commit.materialize' in d['fenced_error'] and d['fresh_retry_error']==d['fenced_error'] and d['no_reopen_no_new_transaction_no_native_floor'];target_retries+=1
   results[case]={'classification':'PASSED_ACTUAL_INNER_WAL_DURABLE_APPLY_FAILURE','seam_reached':True,'exact_error':d['exact_error'],'fresh_retry_error':d['fresh_retry_error'],'retained_logical_txid':pending['intent']['logical_txid'],'retained_native_intent_xid':pending['intent']['native_xid'],'known_logical_sequence':pending['logical_sequence'],'apply_complete':False,'row_progress_records':0,'original_logical_transaction_consumed':True,'no_replacement_transaction':True,'commit_outcome_uncertain_to_caller':True}
   continue
  final=load('registered-image-history-and-rows.json');observation=load('registered-image-before-final-assertions.json')
  for key in ('before','after','actual_owner','rows','actual_source_seal','fault_outcomes'):assert final[key]==observation[key]
  assert observation['record_read_error'] is None and observation['after_error'] is None and observation['owner_error'] is None
  assert observation['acknowledged_record_observed_locally']==final['acknowledged_record_verified_locally']==observation['expected_acknowledged_record']
  before,after,rows,owner,seal=final['before'],final['after'],final['rows'],final['actual_owner'],final['actual_source_seal'];h=owner['history'];rh=owner['rows']['progress']
  assert before['B']==0 and after['B']==1 and rows['target_actual_visibility_b']==1 and not before['writer_installed'] and not after['writer_installed']
  for key in ('raw','namespace_ids','schema','birth','open','slots','protected_catalogue_uuid'):assert before[key]==after[key]
  assert before['points']['2']==after['points']['2'] and before['points']['2']['range_id']==2
  for point in after['points'].values():
   for field in ('native_base_index','native_applied_index','pending_command_index','materialized_pending_index','native_receipt_sha256'):assert field in point and point[field] is None
  assert owner['native_base'] is None and owner['serving'] is False
  assert owner['actual_original_host_retained'] and (owner['no_floor_or_serving'] and owner['final_image'] is None and owner['verification'] is None) and owner['history_pending'] is None and owner['rows']['pending'] is None
  assert h['audited'] and h['cursor']['complete'] and h['verified']==h['audit_next']==2
  assert h['range_index']==manifest['range_index']==1 and h['h0']==manifest['h0'] and h['h']==h['audit_head']==manifest['h']
  assert h['manifest_sha256']==manifest['checksum_sha256'] and h['attempt']==manifest['target_attempt'] and h['session']==manifest['handoff_session']
  assert rh['complete'] and rh['history_generation']==h['generation'] and rh['manifest_sha256']==manifest['checksum_sha256'] and rh['intent_sha256']==h['intent_sha256']
  assert rows['complete'] and rows['cursor']==rh['cursor'] and rows['cursor']['step']==3
  assert rows['records']==rh['records']==seal['record_count']==1 and rows['bytes']==rh['bytes']==seal['serialized_record_bytes']==177
  assert rh['last_actual_target_b']==1 and rows['content_sha256']==seal['image_root_sha256']
  history_steps={};row_steps={};finishes=[]
  for e in ledger:
   kind,body=next(iter(e['actual_reply']['payload'].items()))
   if kind=='History':assert history_steps.setdefault(canonical(body['previous_cursor']),body)==body
   elif kind=='Rows':assert row_steps.setdefault(canonical(body['previous_cursor']),body)==body
   elif kind=='Finished':finishes.append(body)
  assert finishes==[seal]
  entries=[entry for body in history_steps.values() for entry in body['history']['entries']]
  assert [e['index'] for e in entries]==[0,1] and entries[0]['commitment_sha256']==manifest['h0'] and entries[-1]['commitment_sha256']==manifest['h']
  for previous,current in zip(entries,entries[1:]):
   assert current['previous_sha256']==previous['commitment_sha256'];outcome={'kind':current['outcome']['kind']}
   if outcome['kind']=='applied':outcome['command_checksum_sha256']=current['outcome']['command_checksum_sha256']
   assert hashlib.sha256(encoded(['bicdb.native.logical-history.link.v1',current['range_id'],current['index'],current['previous_sha256'],outcome])).hexdigest()==current['commitment_sha256']
  records={}
  for step in row_steps.values():
   batch=step.get('batch')
   if batch:
    records.setdefault(batch['collection'],[]).extend(batch['records'])
    for record in batch['records']:
     value=token(batch['collection'],record['id']);bounds=manifest['capture_range'];assert value>=bounds['start_token'] and (bounds['end_token'] is None or value<bounds['end_token'])
  assert all(v==records.get(k,[]) for k,v in after['rows'].items())
  matching=[a for a in acks if a['range']==manifest['capture_range']['id']];assert len(matching)==1 and matching[0]['actual_full_record']==final['acknowledged_record_verified_locally']
  faults=final['fault_outcomes'];expected_fault=case!='TargetImageRows';assert len(faults)==int(expected_fault)
  if faults:
   f=faults[0];p=f['retained']['rows']['pending'];assert f['original_intent']==p['intent'] and f['original_txid']==p['intent']['logical_txid']
   assert f['completed']['rows']['pending'] is None and 'canceling statement due to user request' in f['exact_error']
   after_wal=case=='TargetImageRowsPostWalCanceled';assert f['known_durable_case']==after_wal
   assert p['logical_outcome']==('applied_known' if after_wal else 'not_attempted') and p['actual_transaction_retained']==(not after_wal)
   if after_wal:assert p['logical_sequence']==1
   wal=load('registered-image-wal-retention.json');wr=f['wal_retention'];assert wal['phase']=='committed_progress_before_release_assertion' and wal['actual_active_pins']==0 and wal['actual_owner']==f['completed'] and wal['original_txid']==f['original_txid'] and wal['no_cold_or_serving_claim']
   assert all(wr[k]==v for k,v in wal['wal_retention'].items()) and wr['active_before_retry']==1 and wr['active_after_committed_progress']==0 and wr['checkpoint_exercised']==after_wal
   if after_wal:assert wr['retained_bytes']==568 and re.fullmatch('[a-f0-9]{64}',wr['retained_sha256']) and wr['after_release_cut_bytes']==0
   wal_checks[case]={'retained_original_logical_txid':f['original_txid'],**wr,'raw_before_checkpoint_WAL_hash_independently_recomputed':False}
   target_retries+=1
  assert not final['strong_read_or_serving_proven'] and not final['cold_recovery_or_physical_qualification'] and not observation['other_range_initially_populated']
  results[case]={'classification':'PASSED_ACTUAL_COMPLETE_HISTORY_AND_ROWS','seam_reached':True,'target_B':[0,1],'records':1,'serialized_record_bytes':177,'full_source_ACK_record_matches_target':True,'history_chain_independently_hashed':True,'native_floor_fields_remain_unknown':True,'empty_other_range_preserved':True,'target_retry_completed_with_original_intent':bool(faults),'retained_original_logical_txid':faults[0]['original_txid'] if faults else None}
assert set(results)==CASES and ack_count==10 and source_closed==15 and target_retries==3
assert len(wal_checks)==3
report={'source_revision':SOURCE,'projection_sha256':SHA,'classification':'PASSED_FIVE_FOCUSED_ROWS_AND_SAME_OPEN_WAL_RETENTION_CONTROLS_ONLY','cases':results,'same_open_wal_retention_controls':wal_checks,'source_APP_operations':{'acknowledged':10,'failed':0,'uncertain':0,'retried':0},'remote_old_close_successes':source_closed,'source_frame_calls_with_actual_replies':source_frames,'target_control_outcomes':{'canceled_requests_reconciled':2,'apply_commit_unknown_retained_fenced':1,'missing_audit_refusals':1,'fresh_target_retries':3,'retry_completed':2,'retry_remained_fenced':1},'limitations':['Actual same-open ServerPaged checkpoint/retention control; no cold replay claim',
    'Pre-checkpoint WAL SHA is cross-checked among actual API reports; original raw pre-cut bytes are not separately archived',
    'Same-process/retained-owner fault controls on shared-host CI; no cold replay or independent physical failure test','Only first selected range copied; other target range remained empty/unknown','Three completed copies each reconciled one full acknowledged record/177 bytes and stored history; other source range ACK not claimed copied','Recorded stream digest/seal parity and actual target reports verified; independent target physical scan/final image belongs to separate tests','Native floor, catalogue adoption, Ready/Serving/Strong-read routing/controller/throughput/production qualification remain open','Native pre/post comparison is complete decoded records/xids/checksums, not raw page-value hashes']}
# The aggregate portable verifier consumes report; default validation is read-only.
if __name__ == '__main__':
 print(json.dumps({'cases':len(results),'source_APP_operations':report['source_APP_operations'],'target_control_outcomes':report['target_control_outcomes']}))
