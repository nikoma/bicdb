import json,zipfile,pathlib,collections,hashlib
root=pathlib.Path(__file__).resolve().parent
with zipfile.ZipFile(root/'14002265636.zip') as z:
 names=z.namelist();journal=z.read(next(n for n in names if n.endswith('journal.jsonl')));rows=[json.loads(x) for x in journal.splitlines()]
 terminal=json.loads(z.read(next(n for n in names if n.endswith('terminal.json'))));log=z.read('tests.log').decode()
assert len(rows)==1062 and [r['journal_sequence'] for r in rows]==list(range(1,1063))
bykind=collections.defaultdict(list)
for r in rows:bykind[r['kind']].append(r)
identity=bykind['execution_identity'][0];topology=bykind['bootstrap_reply'][0]['topology'];attempts={}
for r in bykind['actual_rpc']:
 a=r['actual'];item=attempts.setdefault(a['attempt_id'],{'attempt_id':a['attempt_id'],'operation':a['operation'],'logical_context':r['logical_context'],'phases':[],'request':None,'response':None})
 assert item['operation']==a['operation'] and item['logical_context']==r['logical_context']
 item['phases'].append({k:v for k,v in a.items() if k!='raw_frame'})
 if a['phase'] in ('request_frame_before_dispatch','response_frame_before_decode'):
  raw=bytes(a['raw_frame']);assert int.from_bytes(raw[:4],'big')==len(raw)-4
  frame=json.loads(raw[4:]);key='request' if a['phase']=='request_frame_before_dispatch' else 'response';assert item[key] is None;item[key]=frame
for a in attempts.values():
 assert a['request'] is not None
 if a['response']:
  q,p=a['request'],a['response'];assert q['request_id']==p['request_id'] and q['destination_node_id']==p['source_node_id'];assert p['cluster_id']==q['cluster_id']==topology['cluster_id'];assert p['protocol_version']==q['protocol_version']==29
  assert p['actual_native_source']==topology['nodes'][p['source_node_id']]['native_store_birth_v1']
phase_lines=[l for l in log.splitlines() if l.startswith('NATIVE_HOT_DATA_PHASE ')]
worker_rows=[json.loads(l.split(' ',1)[1]) for l in log.splitlines() if l.startswith('NATIVE_HOT_DATA_MAINTENANCE ')]
assert len(phase_lines)==len(worker_rows)==15
receipt={'source':identity['source'],'run_id':43345593406,'core_job':131624117250,'compile_consumer_job':131624086315,'classification':'FAILED_REQUIRED_ONE_PEER_RECOVERY_AFTER_GENUINE_PREREQUISITES','runtime':{'passed':0,'failed':1,'ignored':0,'filtered':980,'seconds':21.74},'compile_and_isolated_consumer':'SUCCESS','artifact':{'id':14002265636,'bytes':258094,'sha256':hashlib.sha256((root/'14002265636.zip').read_bytes()).hexdigest(),'journal_bytes':len(journal),'journal_sha256':hashlib.sha256(journal).hexdigest()},'execution':identity,'journal_kind_counts':dict(collections.Counter(r['kind'] for r in rows)),'logical_intents':bykind['logical_intent'],'logical_public_replies':bykind['logical_public_reply'],'native_cut':bykind['actual_native_cut'][0],'old_ack_correlation':bykind['old_ack_native_command_correlation'][0],'actual_origin_exit':bykind['actual_origin_exit'][0],'actual_admitted_command':bykind['actual_admitted_command_correlation'][0],'loss_prerequisites':{k:bykind[k][0] for k in ['health_gate_start_before_peer_stop','actual_peer_supervisor_stop','actual_peer_data_metadata_listener_stop','actual_stopped_peer_status_diagnostic','peer_connect_probe_result','loss_and_surviving_mq_prerequisites_before_hold_release','actual_controller_scheduling_hold_released']},'health_rounds':bykind['health_round'],'post_gate':bykind['actual_post_gate_origin_state'][0],'terminal':terminal,'cleanup':bykind['test_terminal_after_owned_stop_attempts'][0],'worker_phase_lines':phase_lines,'worker_attempts':worker_rows,'attempts':list(attempts.values()),'limits':{'initial_bootstrap_raw_frame_covered':False,'controller_internal_rpcs_complete_wire_ledger':False,'native_db_custody':False,'binary_custody':False,'three_hosts_one_core_process_not_cli_or_vm':True,'not_twenty_seconds_continuous_outage':True,'no_Azure_causal_claim':True}}
stored=json.loads((root/'receipt.json').read_text())
assert stored==receipt, 'derived receipt differs from original complete journal/logs'

def semantic(r):
 assert r['source']=='ae58458836a070975f1643333c104d0bddce7cc2'
 assert r['classification']=='FAILED_REQUIRED_ONE_PEER_RECOVERY_AFTER_GENUINE_PREREQUISITES'
 assert r['runtime']=={'passed':0,'failed':1,'ignored':0,'filtered':980,'seconds':21.74}
 assert r['execution']['test_executable_sha256']=='7723c4faa066ecd15442d34fe5f7c31da0178373d9163634958fac9ab382df67'
 x=r['actual_origin_exit']['probe']
 assert x['core_result']=='commit_outcome_unknown' and x['unknown_sequence'] is None and x['actual_transaction_id']==x['unknown_transaction_id']==11
 assert x['fired'] and x['release_callback_ok'] and x['release_callback_returned'] and x['normal_app_handler_exit'] and x['app_span_ended'] and not x['diagnostic_integrity_error']
 assert not r['actual_origin_exit']['actual_api_user_ack']
 cut=r['native_cut'];assert cut['origin']['native_applied_index']==1 and cut['origin']['pending_command_index']==2 and not cut['origin']['invalidated']
 peers=cut['peer_native_materialization'];assert [p['node'] for p in peers]==['n1','n2']
 for p in peers:
  v=p['verified_actual_native'];assert v['native_R']==v['typed_local_B']==2
  assert v['full_record']['metadata']==r['actual_admitted_command']['full_payload']
  assert v['old_ack_full_record']['metadata']==r['old_ack_correlation']['full_payload']
 assert peers[0]['verified_actual_native']['H']==peers[1]['verified_actual_native']['H']
 loss=r['loss_prerequisites'];assert loss['actual_peer_supervisor_stop']['ok'] and loss['actual_peer_data_metadata_listener_stop']['ok']
 assert loss['peer_connect_probe_result']['connection_refused'] and loss['peer_connect_probe_result']['error_kind']=='ConnectionRefused'
 assert not loss['actual_stopped_peer_status_diagnostic']['status']['receiver_listening']
 assert loss['loss_and_surviving_mq_prerequisites_before_hold_release']['health_elapsed_ns']<loss['actual_controller_scheduling_hold_released']['health_elapsed_ns']<40_000_000
 mq=[x['outcome'] for x in r['logical_public_replies'] if x['outcome']['classification']=='ACTUAL_FRESH_HOST_METADATA_QQ'];assert len(mq)==1 and mq[0]['read_index']==1 and mq[0]['term']==1
 intents=r['logical_intents'];out=r['logical_public_replies'];assert len(intents)==len(out)==12
 assert [x['context']['diagnostic_business_id'] for x in intents]==[x['diagnostic_business_id'] for x in out]
 classes=collections.Counter(x['outcome']['classification'] for x in out)
 assert classes=={'USER_ACK':1,'STRONG_PRESENT':1,'NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN':3,'NO_SUCCESSFUL_READ_NO_ABSENCE':6,'ACTUAL_FRESH_HOST_METADATA_QQ':1}
 assert len(r['attempts'])==277
 assert collections.Counter(a['operation'] for a in r['attempts'])=={'event_append':15,'native_event_get':49,'metadata_snapshot_discovery':162,'topology_discovery':50,'other_rpc':1}
 app=[a for a in r['attempts'] if a['operation']=='event_append'];assert sum('Ok' in a['response']['response'] for a in app)==1
 assert 'commit outcome unknown' in app[1]['response']['response']['Err']['message']
 assert all('recovery is not complete' in a['response']['response']['Err']['message'] for a in app[2:])
 strong=[a['response']['response']['Ok']['NativeEventGet']['result'] for a in r['attempts'] if a['operation']=='native_event_get' and a['response']]
 assert len(strong)==46 and collections.Counter(x['kind'] for x in strong)=={'value':1,'unavailable':45}
 assert strong[0]['value']=={'presence':'present','value':r['old_ack_correlation']['full_payload']}
 assert len(r['worker_attempts'])==15 and all(w['phase']=='Fence' and w['position']==0 and not w['completed'] and w['remaining_ms']>4900 and 'Connection refused' in w['error'] for w in r['worker_attempts'])
 assert len(r['health_rounds'])==2 and r['health_rounds'][-1]['receipt_elapsed_ns']==15_333_326_103
 assert all(not x['ever_next_user_ack'] and not x['old_exact'] and not x['committed_exact'] and not x['next_exact'] for x in r['health_rounds'])
 t=r['terminal'];assert not t['required_one_peer_loss_repair_and_next_ack'] and not t['ever_next_user_ack'] and t['no_manual_maintenance_fallback']
 assert t['last_health_receipt_elapsed_ns']==15_333_326_103 and t['terminal_elapsed_ns']==15_338_946_979
 assert t['origin_final_prefix']==r['post_gate']['prefix']==cut['origin']
 assert r['cleanup']['cleanup_errors']==[] and r['limits']['native_db_custody'] is False

semantic(stored)
manifest=json.loads((root/'manifest.json').read_text())
for item in manifest['files']:
 p=root/item['path'];assert p.is_file() and p.stat().st_size==item['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'], item['path']
import sys,copy
if '--tamper-tests' in sys.argv:
 cases=[]
 def case(name,edit):
  changed=copy.deepcopy(stored);edit(changed)
  try:semantic(changed)
  except (AssertionError,KeyError,IndexError,TypeError):cases.append(name);return
  raise AssertionError('semantic tamper accepted: '+name)
 case('retroactive fault ACK',lambda x:x['actual_origin_exit'].__setitem__('actual_api_user_ack',True))
 case('drop original Unknown',lambda x:x['actual_origin_exit']['probe'].__setitem__('core_result','ok'))
 case('missing native peer proof',lambda x:x['native_cut']['peer_native_materialization'].pop())
 case('timeout is peer loss',lambda x:x['loss_prerequisites']['peer_connect_probe_result'].__setitem__('error_kind','TimedOut'))
 case('missing real attempt',lambda x:x['attempts'].pop())
 case('manual recovery',lambda x:x['terminal'].__setitem__('no_manual_maintenance_fallback',False))
 case('false absence',lambda x:next(a for a in x['attempts'] if a['operation']=='native_event_get')['response']['response']['Ok']['NativeEventGet']['result']['value'].__setitem__('presence','absent'))
 case('pretend required gate passed',lambda x:x['terminal'].__setitem__('required_one_peer_loss_repair_and_next_ack',True))
 print('PASS: '+str(len(cases))+' semantic tamper rejections')
print('PASS: complete original artifact/raw-source manifest, 1062 rows, 277 exact attempts, FAILED required gate; no engine replay')
