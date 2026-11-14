#!/usr/bin/env python3
"""Read-only checks of actual logical ledgers and complete synthetic RPC frames."""
import collections,hashlib,json
SOURCE="21669a25e0d95ff5b7a9ec8d367d5895c6d8e8fc"
def sha(data):return hashlib.sha256(data).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def reconcile(z,expected,expected_inventory):
 names=z.namelist();root=next(n.rsplit('/',1)[0] for n in names if n.endswith('/two-range-stall-result.json'))
 selected=[x[:-6] for x in z.read('selected-tests.log').decode().splitlines() if x.endswith(': test')]
 inventory=[x[:-6] for x in z.read('whole-inventory.log').decode().splitlines() if x.endswith(': test')]
 assert selected==[expected] and len(inventory)==83 and sorted(inventory)==expected_inventory
 assert z.read('source-revision.txt').decode().strip()==SOURCE
 assert [x for x in z.read('tests.log').decode().splitlines() if x.startswith('test result:')]==['test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1170 filtered out; finished in 12.29s']
 def data(name):return json.loads(z.read(root+'/'+name))
 result=data('two-range-stall-result.json');ledger=data('two-range-stall-external-ledger.json');acks=data('ack-ledger.json');terminal=data('terminal.json');index=data('original-sdk-rpc-index.json');before=data('original-sdk-rpc-before-app.json');journal=data('source-frame-ledger.json')
 assert terminal['required_baseline_succeeded'] and terminal['evidence_errors_so_far']==[]
 assert result['target_rows_installed'] is False and result['second_range_moved_after_joined'] is False and result['disk_stall_tested'] is False and result['automatic_controller_or_physical_qualification'] is False
 assert len(acks)==2 and all(a['actual_api_inserted'] is True and a['actual_api_error'] is None and a['outcome']=='returned' and not a['effect_unknown_on_error'] for a in acks)
 for a in acks:assert sha(encoded(a['value']))==a['payload_json_sha256']
 assert len(ledger)==5 and [a['operation'] for a in ledger]==['ClusterEventClient.get_strong','ClusterEventClient.get_strong','ClusterEventClient.append','ClusterEventClient.get_strong','ClusterEventClient.get_strong']
 assert all(a['outcome']=='returned' and a['exact_error'] is None and a['debug_error'] is None and not a['uncertain_write_effect'] for a in ledger)
 assert sum(a['acknowledged'] for a in ledger)==1 and ledger[2]['actual_result'] is True
 for a in ledger:
  assert a['configured_sdk_request_ms']==2000 and a['manual_shard_hint'] is False and a['manual_sdk_refresh'] is False
  assert sha(encoded(a['expected_full_value']))==a['payload_json_sha256']
  if a['operation'].endswith('get_strong'):assert a['actual_strong_present'] and a['actual_result']==a['expected_full_value']
 for a in ledger[1:4]:assert a['hold_at_invocation']['currently_held'] and a['hold_at_return']['currently_held']
 assert result['actual_held_before']['currently_held'] and result['actual_held_after']['currently_held'], 'Actual selected response must remain held before and after the three calls'
 assert result['actual_after_release']['released'] and result['actual_after_release']['handler_resumed'] and not result['actual_after_release']['original_request_expired']
 assert result['actual_frame_validated_before_reconciliation'] and result['actual_frame_error'] is None
 assert result['new_write_reconciliation']==ledger[2]['expected_full_value'] and result['reconciliation_error'] is None
 prefix=result['pre_worker_source_journal_data_prefix'];full=result['actual_request_journal'];assert len(prefix)==5 and len(full)==6 and full[:5]==prefix and journal==full
 assert result['original_source_journal_prefix_preserved'] and result['actual_rows_entry_appended']
 assert full[4]['intent']['operation']=='Manifest' and list(full[5]['intent']['operation'])==['Rows']
 assert all(x['outcome'].startswith('returned') for x in full)
 row_reply=result['actual_rows_reply'];held=result['actual_held_before']['actual_rows_reply'];mf=result['actual_manifest']
 assert row_reply['request']==held['request']==full[-1]['intent'] and row_reply['checksum_sha256']==held['reply_checksum']
 assert row_reply['manifest_sha256']==mf['checksum_sha256']==held['manifest_sha256']
 assert row_reply['request']['source_session']==mf['source_session'] and row_reply['request']['target_attempt']==mf['target_attempt']
 exported=row_reply['payload']['Rows'];batch=exported['batch'];assert batch['collection']=='__bic_cluster_events' and len(batch['records'])==1
 assert batch['records'][0]['id']==acks[0]['record_id'] and batch['records'][0]['metadata']==acks[0]['value'] and batch['range_id']==1
 source_batch={'physical_collection':batch['collection'],'SDK_logical_namespace':'items','actual_range_id':batch['range_id'],'actual_batch_epoch':batch['range_epoch'],'records':len(batch['records']),'record_id':batch['records'][0]['id'],'payload_sha256':sha(encoded(batch['records'][0]['metadata'])),'snapshot_sequence':batch['snapshot_commit_sequence'],'Rows_stream_complete':exported['complete'],'scope':'one actual selected batch exported before hold; no Finish/full-range receive or target installation'}
 obs=index['actual_observer'];assert obs['attached_before_first_source_append'] and obs['configured_original_sdk_request_timeout_ms']==2000 and obs['actual_record_count']==28 and obs['complete_record_decode_and_contiguous_sequence'] and obs['evidence_error'] is None
 assert before['actual_observer']['actual_record_count']==0 and before['capture_phase']=='before_first_source_append_invocation'
 records=[]
 for n,item in enumerate(obs['records'],1):
  assert item['journal_sequence']==n and item['file_byte_count_stable'] and item['decode_error'] is None and item['raw_frame_evidence_error'] is None
  payload=z.read(root+'/original-sdk-rpc/'+item['file']);assert len(payload)==item['bytes']==item['file_bytes_before_read']==item['file_bytes_after_read'] and sha(payload)==item['sha256']
  record=json.loads(payload);assert record['journal_sequence']==n and record['error_chain']==[]
  if record['raw_frame'] is not None:
   frame=bytes(record['raw_frame']);assert len(frame)==int.from_bytes(frame[:4],'big')+4 and sha(frame)==item['raw_frame_header_and_hash']['sha256'];record['decoded_frame']=json.loads(frame[4:])
  records.append(record)
 attempts=collections.defaultdict(list)
 for record in records:attempts[record['attempt_id']].append(record)
 assert len(attempts)==7
 expected_values={a['key']:a['value'] for a in acks};expected_values[ledger[2]['key']]=ledger[2]['expected_full_value']
 wire=[]
 for attempt,rows in attempts.items():
  assert [a['phase'] for a in rows]==['invoked','request_frame_before_dispatch','response_frame_before_decode','validated_transport_result_ok']
  req=rows[1]['decoded_frame'];resp=rows[2]['decoded_frame'];assert req['request_id']==resp['request_id'] and req['destination_node_id']==resp['source_node_id']==rows[1]['destination'] and req['caller_node_id']=='n0' and req['protocol_version']==resp['protocol_version']==30
  assert 'Ok' in resp['response'];body=req['request'];kind=next(iter(body));detail=body[kind];returned=resp['response']['Ok'];key=detail['operation']['key'] if kind=='ApplicationEvent' else detail['key']
  if kind=='ApplicationEvent':
   assert detail['operation']['operation']=='append' and detail['operation']['value']==expected_values[key]
   assert returned['ApplicationEvent']=={'result':'appended','inserted':True}
   op='append';route=None;audit=None
  else:
   assert kind=='NativeEventGet' and detail['mode']=='strong'
   actual=returned['NativeEventGet'];assert actual['business_request_id']==detail['business_request_id'] and actual['challenge']==detail['challenge']
   value=actual['result']['value'];assert value['presence']=='present' and value['value']==expected_values[key]
   route=actual['result']['route'];audit=actual['result']['audit'];assert route['range_id']==audit['range_id']==detail['range_id']==2 and route['range_epoch']==audit['range_epoch']==detail['range_epoch']==1 and route['leader']=='n2'
   op='strong'
  wire.append({'attempt_id':attempt,'wire_request_id':req['request_id'],'operation':op,'key':key,'actual_destination':req['destination_node_id'],'actual_range':detail['range_id'],'actual_epoch':detail['range_epoch'],'actual_sdk_cached_topology_generation':rows[1]['topology_generation'],'actual_strong_response_route':route,'actual_strong_audit':audit,'wire_response_ok':True,'payload_sha256':sha(encoded(expected_values[key]))})
 wire.sort(key=lambda x:x['wire_request_id']);assert [a['operation'] for a in wire]==['append','append','strong','strong','append','strong','strong']
 assert [a['actual_range'] for a in wire]==[1,2,2,2,2,2,2] and all(a['actual_sdk_cached_topology_generation']==4 for a in wire)
 assert all(a['actual_strong_response_route']['generation']==5 for a in wire if a['operation']=='strong')
 return {'source_APP_ACKs':3,'full_Strong_values':4,'held_calls':3,'wire_records':len(records),'primary_wire_attempts':len(attempts),'recorded_retry_or_refresh':0,'source_batch':source_batch,'wire':wire,'fixture_root':root}
