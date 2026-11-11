#!/usr/bin/env python3
"""Verify public projection completeness/counts/latencies; not private wire authenticity."""
import argparse,collections,gzip,hashlib,json,pathlib,importlib.util,itertools,sys,stat
EXPECTED={
 'first-attempt/cloud-smoke','first-attempt/smoke-reconciliation','first-attempt/baseline-4','first-attempt/finally-reconciliation-0',
 'light-attempt/two-target-fixture','light-attempt/light-steady-4','light-attempt/two-target-finally-reconciliation','light-attempt/light-finally-reconciliation-0','light-attempt/light-finally-reconciliation-1'}
ENGINE='840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d'
CORE_SOURCE='d49a391b3c6157f90f29e9f2f5214143c60f9f4b'
WORKER='b8cc2607220e32f705a23fd6b7d4f41c22298cd1b0ec92cc8e83847783ef8d2c'
CLIENT='a1ab418d1031ec409ce108aeb547cf5a2ceebff46f41811d991f7182259f66fb'
def check(test,message):
 if not test:raise ValueError(message)
def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def distribution(values):
 v=sorted(values)
 def at(p):return v[max(0,(len(v)*p+99)//100-1)]if v else None
 return {'samples':len(v),'p50_ns':at(50),'p95_ns':at(95),'p99_ns':at(99),'max_ns':v[-1]if v else None}
def phase(path):
 metadata=json.loads((path/'projection.json').read_text());summary=json.loads((path/'summary.json').read_text())
 check(digest(path/'records.jsonl.gz')==metadata['projection_sha256'],'projection digest differs')
 counts=collections.Counter();outcomes=collections.Counter();latencies=collections.defaultdict(list);rpc_latencies=collections.defaultdict(list);errors=collections.Counter();unknown_responses=set();database_acks=set();observation_triggers=set();observation_completed=set();observation_gaps=set();fenced=False;run_completed=None;traffic_started=None;first_gap_time=None;logged_controller_errors=[];rpc_failures=set();worker_ready={};operations={};terminals={};rpc_starts={};rpc_results={};ack_rows={};receipts=set();exact_reads=set();read_outcomes={};required=None;attempted=set();terminal_targets=set();verified=set();previous='0'*64
 with gzip.open(path/'records.jsonl.gz','rt')as stream:
  for index,line in enumerate(stream):
   r=json.loads(line);kind=r['kind'];check(r['sequence']==index,'noncontiguous record sequence');check(r['raw_previous_sha256']==previous,'projected raw digest linkage differs');previous=r['raw_record_sha256'];check(len(previous)==64,'invalid source digest');counts[kind]+=1
   if kind=='worker_ready':worker_ready[r['worker']]=r['ready'];check(not r['ready']['transport']['dev_localhost_plaintext'],'plaintext worker in mTLS experiment')
   if kind=='traffic_window_started':traffic_started=r
   if kind=='watermark_observation_trigger':
    check(r['observation_id']not in observation_triggers,'duplicate observer trigger');observation_triggers.add(r['observation_id'])
   if kind=='watermark_observation_completed':
    check(r['observation_id']in observation_triggers and r['observation_id']not in observation_completed,'missing/duplicate observer completion');observation_completed.add(r['observation_id'])
   if kind=='observation_coverage_gap':
    if first_gap_time is None:first_gap_time=r['controller_unix_ns']
    if r.get('observation_id'):check(r['observation_id']in observation_triggers,'observer gap lacks trigger');observation_gaps.add(r['observation_id'])
   if kind in ['controller_resource_guard','controller_budget_guard','client_transport_fenced','client_channel_fenced','diagnostic_capture_fenced','reconciliation_incomplete']or(kind=='worker_stderr_final_status'and r.get('error')):fenced=True
   if kind=='run_failed':logged_controller_errors.extend(r.get('errors',[]))
   if kind=='run_completed':
    run_completed=r;fenced=fenced or bool(r.get('dispatch_fenced'));logged_controller_errors.extend(r.get('controller_errors',[]))
   check(r['projection_is_raw_wire_capture']is False,'projection falsely claims raw wire capture')
   if kind=='reconciliation_inputs_loaded':required={v['target_id']for v in r['targets']};check(len(required)==r['unique_write_keys'],'duplicate required targets')
   if kind=='operation_started':
    logical=r['logical_id'];check(logical not in operations,'duplicate logical start');operations[logical]={k:r.get(k)for k in ['key','namespace','expected_payload_sha256','source_ack_evidence','reconciliation_target_id']}
    if r.get('reconciliation_target_id'):attempted.add(r['reconciliation_target_id'])
   if kind=='rpc_started':
    check(r['logical_id']in operations,'RPC lacks logical attempt');check(r['rpc_id']not in rpc_starts,'duplicate RPC start');rpc_starts[r['rpc_id']]={'sequence':r['sequence'],'logical_id':r['logical_id'],'request':r['request'],'destination':r['destination_node_id']}
    if r.get('purpose')=='application':
     op=operations[r['logical_id']];mutation=r['request']['ApplicationEvent']['operation'];check(mutation['key']==op['key']and mutation['namespace']==op['namespace'],'retry changed stable key/namespace')
     if mutation['operation']=='append':check(mutation['original_value_canonical_sha256']==op['expected_payload_sha256'],'retry changed payload hash')
   if kind=='rpc_completed':
    check(r['rpc_id']in rpc_starts,'RPC result lacks start');check(r['rpc_id']not in rpc_results,'duplicate RPC result');rpc_results[r['rpc_id']]=True;wire=r['wire'];start=rpc_starts[r['rpc_id']]
    check(wire['wire_request_id']==start['sequence']+1 and wire['rpc_id']==r['rpc_id'],'wire/request ID mismatch')
    if wire.get('transport_success'):
     request=wire['request_envelope'];response=wire['response_envelope'];check(request['request']==start['request']and request['destination_node_id']==start['destination'],'wire dispatch differs from durable RPC start');check(r['worker']in worker_ready and request['caller_node_id']==wire['caller_node_id']==worker_ready[r['worker']]['caller_node_id'],'wire caller differs from admitted worker');pins=worker_ready[r['worker']]['topology']['nodes'].get(request['destination_node_id'],{});check(wire.get('tls_peer_certificate_sha256')in [pins.get('tls_certificate_sha256'),pins.get('pending_tls_certificate_sha256')]and wire.get('tls_peer_certificate_sha256')is not None,'successful response lacks recorded membership TLS pin');check(response['request_id']==request['request_id']and response['cluster_id']==request['cluster_id']and response['source_node_id']==request['destination_node_id']and response['protocol_version']==request['protocol_version'],'successful envelope identity mismatch')
    if r.get('purpose')=='application' and wire.get('transport_success'):
     op=operations[r['logical_id']];mutation=wire['request_envelope']['request']['ApplicationEvent']['operation'];check(mutation['key']==op['key']and mutation['namespace']==op['namespace'],'successful wire request changed logical key');check(wire['caller_node_id']=='n0','application gateway changed')
     if mutation['operation']=='append' and wire['response_envelope']['response'].get('Ok',{}).get('ApplicationEvent',{}).get('result')=='appended':database_acks.add(r['logical_id'])
    status='transport_error'if not wire.get('transport_success')else 'remote_error'if 'Err'in wire.get('response_envelope',{}).get('response',{})else'ok'
    key=str(r.get('purpose'))+':'+str(r.get('operation'))+':'+status
    rpc_latencies[key+':controller'].append(r['controller_rpc_elapsed_ns']);rpc_latencies[key+':member_wire'].append(wire['elapsed_ns'])
   if kind=='rpc_failure':
    check(r['rpc_id']in rpc_starts,'RPC failure lacks start');rpc_failures.add(r['rpc_id']);errors[(r.get('purpose'),r['error']['category'],r['error'].get('code'))]+=1
    if r['error'].get('response_unknown'):unknown_responses.add(r['rpc_id'])
   if kind=='ack_observed':
    check(r['logical_id']in operations and r['logical_id']in database_acks,'ACK lacks prior same-operation validated Appended RPC');ack_rows[r['sequence']]=r
   if kind=='ack_durable_receipt':
    a=ack_rows.get(r['receipt']['sequence']);check(a is not None and a['logical_id']==r['logical_id']and a['raw_record_sha256']==r['receipt']['record_sha256'],'ACK durability linkage differs');receipts.add(r['logical_id'])
   if kind=='operation_terminal':
    logical=r['logical_id'];check(logical in operations and logical not in terminals,'missing/duplicate logical terminal');terminals[logical]=r;outcomes[r['outcome']]+=1;latencies[r['operation']+':'+r['outcome']].append(r['logical_elapsed_ns'])
    if r['outcome']=='acknowledged':check(logical in receipts,'ACK terminal lacks durable receipt')
    if r.get('reconciliation_target_id'):
     terminal_targets.add(r['reconciliation_target_id'])
     if r['outcome']=='exact_match':verified.add(r['reconciliation_target_id'])
    if r['operation']=='get'and r.get('response')is not None:
     response=r['response'];actual=response['original_value_canonical_sha256'];expected=r['expected_payload_sha256'];observed='exact_match'if actual==expected else'absent'if response['original_value_is_null']else'mismatch';check(observed==r['outcome'],'Get outcome differs from returned-value hash/nullness')
     key=(r['namespace'],r['key'],expected);read_outcomes[key]=observed
     if observed=='exact_match':exact_reads.add(key)
 check(sum(counts.values())==metadata['projected_records']and dict(counts)==metadata['record_counts'],'record count/sampling mismatch')
 check(dict(outcomes)==metadata['terminal_outcomes']==summary['outcomes'],'terminal totals differ')
 check(len(operations)==summary['logical_attempts'],'logical attempt count differs')
 pending=observation_triggers-observation_completed-observation_gaps
 check(pending==set(summary['pending_observation_ids']),'observer pending set differs')
 check(counts['observation_coverage_gap']==summary['observation_coverage_gaps'],'observer gap count differs')
 unfinished_rpcs=set(rpc_starts)-set(rpc_results)-rpc_failures;unterminated=set(operations)-set(terminals)
 coverage=(required is None or required==attempted==terminal_targets)
 telemetry=not(fenced or counts['observation_coverage_gap']or pending or unfinished_rpcs or unterminated or logged_controller_errors or not coverage)
 planned=bool(telemetry and run_completed and run_completed.get('termination_reason')in ['traffic_deadline','operation_ceiling','reconciliation_inputs_complete']and operations)
 passed=bool(planned and all(name in ['acknowledged','exact_match']for name in outcomes))
 check(summary['telemetry_complete']==telemetry and summary['planned_experiment_complete']==planned and summary['workload_passed']==passed,'summary completeness/workload flags differ from trace semantics')
 computed={key:distribution(values)for key,values in latencies.items()};check(computed==summary['raw_latency_distributions'],'logical percentiles differ')
 if required is not None:
  report=summary['reconciliation']
  for field,actual in [('required_target_ids',required),('attempted_target_ids',attempted),('terminal_target_ids',terminal_targets),('verified_target_ids',verified),('unattempted_target_ids',required-attempted),('unverified_target_ids',required-verified)]:check(set(report[field])==actual,'reconciliation target-set mismatch: '+field)
 ack_keys={(r['namespace'],r['key'],r['expected_payload_sha256'])for r in terminals.values()if r['outcome']=='acknowledged'}
 uncertain={(r['namespace'],r['key'],r['expected_payload_sha256'])for r in terminals.values()if r['outcome']=='uncertain_write'}
 return {'record_count':sum(counts.values()),'individual_rpc_calls':len(rpc_starts),'individual_rpc_results':len(rpc_results),'rpc_calls_with_unknown_response':len(unknown_responses),'acknowledged_logical_ids_with_unknown_rpc_history':len({rpc_starts[rpc]['logical_id']for rpc in unknown_responses if rpc in rpc_starts}&{logical for logical,r in terminals.items()if r['outcome']=='acknowledged'}),'retry_backoffs':counts['retry_backoff'],'observation_coverage_gaps':counts['observation_coverage_gap'],'logical_attempts':len(operations),'requested_seconds':traffic_started['requested_seconds']if traffic_started else None,'seconds_before_first_observer_gap':(first_gap_time-traffic_started['controller_unix_ns'])/1e9 if first_gap_time and traffic_started else None,'reconciliation_counts':{'required':len(required),'attempted':len(attempted),'terminal':len(terminal_targets),'verified':len(verified),'unattempted':len(required-attempted),'prior_ack':sum(op.get('source_ack_evidence')=='durable_client_ack'for op in operations.values()),'prior_ack_unverified':sum(op.get('source_ack_evidence')=='durable_client_ack'and terminals.get(logical,{}).get('outcome')!='exact_match'for logical,op in operations.items())}if required is not None else None,'outcomes':dict(outcomes),'logical_latency_ns':computed,'rpc_latency_ns':{key:distribution(values)for key,values in rpc_latencies.items()},'rpc_failures':[{'purpose':k[0],'category':k[1],'code':k[2],'count':n}for k,n in sorted(errors.items(),key=lambda p:str(p[0]))],'ack_keys':ack_keys,'uncertain_keys':uncertain,'exact_reads':exact_reads,'read_outcomes':read_outcomes,'telemetry_complete':summary['telemetry_complete'],'planned_experiment_complete':summary['planned_experiment_complete'],'workload_passed':summary['workload_passed']}
def verify(root,check_published=True):
 check(all(not p.is_symlink()for p in root.rglob('*')),'symlink inside evidence bundle');manifest=json.loads((root/'manifest.json').read_text());listed=manifest['files'];actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()and p!=root/'manifest.json'}
 check(actual==set(listed),'manifest does not cover exact artifact file set')
 check({'README.md','claims.json','artifact-identities.json','local-readiness-receipt.json','durable-private-custody.json','executed/trace_client.py','executed/verify-evidence.py'}<=set(listed),'critical artifact omitted')
 for name,record in listed.items():
  path=root/name;check(not path.is_symlink()and path.stat().st_size==record['bytes']and digest(path)==record['sha256'],'manifest mismatch: '+name)
 found={str(p.parent.relative_to(root))for p in root.glob('*/*/projection.json')};check(found==EXPECTED,'critical phase set missing/extra')
 identity=json.loads((root/'artifact-identities.json').read_text());check(digest(root/'executed/trace_client.py')==CLIENT,'retained client source digest differs');check(identity['engine_sha256']==ENGINE and identity['runtime_source']==CORE_SOURCE and identity['worker_sha256']==WORKER and identity['client_sha256']==CLIENT,'runtime/source pin differs')
 claims=json.loads((root/'claims.json').read_text())
 for flag in ['four_to_five_cluster_join_executed','data_movement_to_fifth_node_demonstrated','fault_test_executed','sustained_capacity_gain_demonstrated','strong_read_proof','production_qualified']:check(claims[flag]is False,'unsupported qualification/capability claim: '+flag)
 reports={name:phase(root/name)for name in sorted(EXPECTED)}
 known=set().union(*(r['ack_keys']for r in reports.values()));latest=[r for name,r in reports.items()if name.startswith('light-attempt/')and'reconciliation'in name];matched=set().union(*(r['exact_reads']for r in latest));missing=known-matched
 uncertain=reports['light-attempt/light-steady-4']['uncertain_keys'];latest_outcomes={k:v for r in latest for k,v in r['read_outcomes'].items()};resolved=collections.Counter(latest_outcomes.get(k,'unobserved')for k in uncertain)
 check(len(known)==3261==claims['all_known_ack_keys_observed_exact_after_graceful_restart']and not missing,'known ACK coverage differs');check(dict(resolved)=={'exact_match':7,'absent':1},'uncertain-write outcomes differ')
 first=reports['first-attempt/baseline-4'];after_first=reports['first-attempt/finally-reconciliation-0'];light=reports['light-attempt/light-steady-4'];fixture=reports['light-attempt/two-target-fixture'];dedicated=reports['light-attempt/two-target-finally-reconciliation'];recon=reports['light-attempt/light-finally-reconciliation-0'];smoke=reports['light-attempt/light-finally-reconciliation-1']
 def count_error(report,purpose,category):return sum(r['count']for r in report['rpc_failures']if r['purpose']==purpose and r['category']==category)
 expected_first={'requested_baseline_seconds':first['requested_seconds'],'observed_until_dispatch_fence_seconds':first['seconds_before_first_observer_gap'],'logical_attempts':first['logical_attempts'],'acknowledged':first['outcomes']['acknowledged'],'exact_gets':first['outcomes']['exact_match'],'application_transport_rpc_failures':count_error(first,'application','wire_transport_or_validation_error'),'application_remote_rpc_failures':count_error(first,'application','server_remote_error'),'observer_transport_rpc_failures':count_error(first,'watermark','wire_transport_or_validation_error'),'observer_coverage_gaps':first['observation_coverage_gaps'],'planned_experiment_complete':first['planned_experiment_complete'],'subsequent_all_ack_values_matched':after_first['outcomes']['exact_match'],'acknowledged_logical_ids_with_earlier_unknown_rpc_history':first['acknowledged_logical_ids_with_unknown_rpc_history']}
 expected_light={'fixture_acknowledged':fixture['outcomes']['acknowledged'],'fixture_gets_exact':fixture['outcomes']['exact_match'],'steady_four_requested_seconds':light['requested_seconds'],'steady_four_logical_attempts':light['logical_attempts'],'steady_four_acknowledged':light['outcomes']['acknowledged'],'steady_four_uncertain_write':light['outcomes']['uncertain_write'],'available_client_telemetry_complete':light['telemetry_complete'],'steady_four_workload_passed':light['workload_passed'],'dedicated_two_target_gets_exact':dedicated['outcomes']['exact_match'],'latest_main_reconciliation_required':recon['reconciliation_counts']['required'],'latest_main_reconciliation_attempted':recon['reconciliation_counts']['attempted'],'latest_main_reconciliation_exact':recon['outcomes']['exact_match'],'latest_main_reconciliation_absent_uncertain':recon['outcomes']['absent'],'latest_main_reconciliation_ack_targets':recon['reconciliation_counts']['prior_ack'],'latest_main_reconciliation_ack_targets_unverified':recon['reconciliation_counts']['prior_ack_unverified'],'latest_smoke_targets_exact':smoke['outcomes']['exact_match']}
 for group,expected in [('first_attempt',expected_first),('second_light_attempt',expected_light)]:
  for key,value in expected.items():check(claims[group][key]==value,'numeric/phase claim differs: '+group+'.'+key)
 for cohort in ['first-attempt','light-attempt']:
  with gzip.open(root/cohort/'operations.jsonl.gz','rt')as stream:check(not any(r.get('kind')=='join_started'and r.get('node')==4 for r in map(json.loads,stream)),'fifth-node join was attempted')
 check(claims['first_attempt']['fifth_node_join_attempted']is False and claims['second_light_attempt']['fifth_node_join_attempted']is False,'phase join claim differs')
 check(claims['uncertain_writes']['present']==resolved['exact_match']and claims['uncertain_writes']['none_observed']==resolved['absent']and claims['uncertain_writes']['none_establishes_rollback']is False,'uncertain outcome claim differs')
 check(not reports['first-attempt/baseline-4']['telemetry_complete'],'first incomplete baseline incorrectly promoted')
 check(reports['light-attempt/light-steady-4']['telemetry_complete']and not reports['light-attempt/light-steady-4']['workload_passed'],'second DB failure incorrectly promoted')
 for r in reports.values():
  for k in ['ack_keys','uncertain_keys','exact_reads','read_outcomes']:r.pop(k)
 result={'projection_verification_passed':True,'known_ack_keys':len(known),'known_ack_keys_observed_exact_after_restart':len(known&matched),'uncertain_write_observations':dict(resolved),'phases':reports,'private_verbatim_wire_authenticity_independently_verified':False,'strong_read_proof':False,'four_to_five_cluster_join_executed':False,'fault_test_executed':False,'capacity_qualified':False,'production_qualified':False}
 published=root/'verification-results.json'
 if check_published and published.exists():check(json.loads(published.read_text())==result,'published verification result differs from recomputation')
 return result
def _verify_private(root,private):
 source=private/'client-v3/trace_client.py';check(digest(source)==CLIENT,'private frozen client digest differs')
 spec=importlib.util.spec_from_file_location('private_frozen_client',source);checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
 projector_path=root/'executed/project_evidence.py';spec=importlib.util.spec_from_file_location('frozen_projection',projector_path);projector=importlib.util.module_from_spec(spec);spec.loader.exec_module(projector)
 replacements=json.loads((private/'operations/public-replacements.private.json').read_text());details={}
 for name in sorted(EXPECTED):
  cohort,phase_name=name.split('/');folder='8186dd18b83f4195b1319a85a1ee3498'if cohort=='first-attempt'else'light-30cf5a05bde64ced88bd576e8b9950ea';raw=private/'experiment'/folder/phase_name/'trace.jsonl';metadata=json.loads((root/name/'projection.json').read_text());check(digest(raw)==metadata['raw_journal_sha256'],'private raw journal digest differs: '+name)
  validated=checker.validate_trace(raw,allow_incomplete=True);count=0
  with gzip.open(root/name/'records.jsonl.gz','rt')as stream:
   for original,projected_line in itertools.zip_longest(checker.read_journal(raw),stream):
    check(original is not None and projected_line is not None,'private/public record count differs: '+name)
    expected=projector.project(original,replacements);expected['raw_record_sha256']=expected.pop('record_sha256');expected['raw_previous_sha256']=expected.pop('previous_sha256');expected['projection_is_raw_wire_capture']=False
    check(expected==json.loads(projected_line),'canonical private/public record differs: '+name+':'+str(count));count+=1
  details[name]={'records_compared':count,'raw_journal_sha256':metadata['raw_journal_sha256'],'frozen_raw_validation':validated}
 return {'private_originals_canonical_comparison_passed':True,'private_raw_hashchain_body_linkage_canonical_projection_verified':True,'private_verbatim_wire_authenticity_independently_verified':False,'production_qualified':False,'phases':details}
def readonly_private_comparison(root,private,callback):
 def snapshot(path):return {str(p.relative_to(path)):(stat.S_IFMT(p.lstat().st_mode),stat.S_IMODE(p.lstat().st_mode),p.stat().st_size if p.is_file()else None)for p in path.rglob('*')}
 before_private=snapshot(private);before_public=snapshot(root);previous=sys.dont_write_bytecode;sys.dont_write_bytecode=True
 try:result=callback()
 finally:
  sys.dont_write_bytecode=previous
  check(snapshot(private)==before_private and snapshot(root)==before_public,'private comparison changed evidence file-set/modes/sizes')
 result['private_and_public_file_sets_modes_sizes_unchanged']=True
 return result

def verify_private(root,private):
 custody=json.loads((root/'durable-private-custody.json').read_text());check(digest(private/'manifest.json')==custody['manifest_sha256'],'private canonical custody manifest differs')
 return readonly_private_comparison(root,private,lambda:_verify_private(root,private))
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('root',type=pathlib.Path,nargs='?',default=pathlib.Path(__file__).resolve().parents[1]/'docs/evidence/azure-instrumented-replay-2026-11-11');p.add_argument('--private-dir',type=pathlib.Path);a=p.parse_args();result=verify(a.root)
 if a.private_dir:result['private_canonical_comparison']=verify_private(a.root,a.private_dir)
 print(json.dumps(result,sort_keys=True))
