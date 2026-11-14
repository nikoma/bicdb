#!/usr/bin/env python3
"""Reconcile the four-case passive first-APP cohort; no DB custody is read.
Exit zero checks evidence consistency, not the test result or HA qualification.
"""
import argparse
import collections
import hashlib
import importlib.util
import json
import pathlib
import re
import zipfile

ORIGIN_SHA='4e963ef81ec1beb408b60c241543e701e2a73e87ddf06d0f11b7aed25c613255'
ANSI=re.compile(r'\x1b\[[0-9;]*m')

def require(value,why):
    if not value: raise ValueError(why)

def sha(data): return hashlib.sha256(data).hexdigest()

def verify(archive,expected_zip,expected_source,origin_path):
    require(sha(archive.read_bytes())==expected_zip,'projection ZIP differs')
    require(sha(origin_path.read_bytes())==ORIGIN_SHA,'origin frame decoder differs')
    spec=importlib.util.spec_from_file_location('origin_verifier',origin_path)
    origin=importlib.util.module_from_spec(spec);spec.loader.exec_module(origin)
    with zipfile.ZipFile(archive) as z:
        names=z.namelist()
        require(len(names)==len(set(names)),'duplicate ZIP paths')
        require(sum(i.file_size for i in z.infolist())<=256*1024*1024,'oversized projection')
        require(all(not n.startswith('/') and '..' not in pathlib.PurePosixPath(n).parts and '/custody/' not in n for n in names),'unsafe/custody member')
        require(z.read('source-revision.txt').decode().strip()==expected_source,'source differs')
        selected=[line[:-6] for line in ANSI.sub('',z.read('selected-tests.log').decode()).splitlines() if line.endswith(': test')]
        logs=ANSI.sub('',z.read('tests.log').decode())
        executed=re.findall(r'^test ([^\s]+) \.\.\. (ok|FAILED|ignored)$',logs,re.M)
        require(len(selected)==4 and len(executed)==4 and set(selected)=={name for name,_ in executed} and all('::first_app_observed_cohort::' in name for name in selected),'not exactly four selected/executed cohort cases')
        cases=[]
        for journal_name in names:
            if not journal_name.endswith('/first-app-rpc.jsonl'): continue
            prefix=journal_name.rsplit('/',1)[0]+'/'
            identity=json.loads(z.read(prefix+'execution-identity.json'))
            require(identity['source_commit']==expected_source,'actual fixture source differs')
            matching=[name for name in selected if name==identity['actual_rust_test_thread']]
            require(len(matching)==1,'actual Rust fixture identity differs from selected cohort')
            case_test=matching[0];actual_result=dict(executed)[case_test]
            expected_cases={
                'native_registered_fifth_remote_transfer_unique_runtime_owner_preserves_data':'TargetTransfer',
                'native_registered_fifth_remote_transfer_actual_callback_arc_blocks_then_fresh_retry':'TargetTransferBusy',
                'native_registered_fifth_remote_transfer_actual_cancellation_before_rotation_preserves_n':'TargetTransferCanceled',
                'native_registered_fifth_remote_transfer_actual_hook_aba_before_rotation_preserves_n':'TargetTransferHookRevoked',
            }
            require(identity['source_case']==expected_cases[case_test.rsplit('::',1)[1]],'actual cohort SourceCase differs')
            mode=json.loads(z.read(prefix+'first-app-mode.json'))
            require(mode['observer_only_cohort'] is True and mode['ordinary_strong_precondition'] is False,'cohort substituted Strong warmup')
            require(mode['cohort_adds_no_deadline_or_warmup'] is True and mode['app_per_call_timeout_ms']==2000 and mode['initial_open_absolute_budget_ms']==20000,'cohort changed original deadlines')
            require(mode['actual_rust_test_thread']==case_test and mode['configured_rust_test_threads']=='4','actual cohort identity/configuration differs')
            traffic_first=True
            events=[json.loads(line) for line in z.read(journal_name).decode().splitlines() if line.strip()]
            require([e['journal_sequence'] for e in events]==list(range(1,len(events)+1)),'journal missing/reordered sequence')
            intents={};replies={};attempts=collections.defaultdict(list)
            for event in events:
                logical=(event.get('context') or {}).get('logical_id')
                if event['kind']=='intent':
                    require(logical and logical not in intents,'duplicate/missing logical intent')
                    intents[logical]=event
                elif event['kind']=='api_reply':
                    require(logical and logical not in replies,'duplicate/missing API reply')
                    replies[logical]=event
                elif event['kind']=='actual_rpc':
                    rpc=dict(event['rpc']);frame=rpc.pop('raw_frame',None)
                    rpc['raw_frame']=origin.frame_projection(frame)
                    rpc['source_journal_sequence']=event['journal_sequence']
                    attempts[(logical,rpc['attempt_id'])].append(rpc)
                else: raise ValueError('unknown first-APP journal event')
            require(set(replies)<=set(intents),'API reply without intent')
            operations=[];ordinals=collections.Counter();counts=collections.Counter()
            for logical,intent in intents.items():
                context=intent['context'];reply=replies.get(logical);operation=context['operation']
                is_append=operation=='ClusterEventClient.append'
                if is_append:
                    value=context['value'];payload_sha=sha(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode())
                    require(payload_sha==context['payload_json_sha256'],'expected APP full-payload hash differs')
                    require(context['record_id']==context['collection']+'/'+context['key'],'record ID differs')
                key=(operation,context['collection'],context['key']);ordinals[key]+=1
                if reply is None: classification='intent_without_reply'
                elif reply.get('actual_api_error') is not None: classification='uncertain' if is_append else 'failed_strong_call'
                elif is_append: classification='acknowledged_new_insert' if reply['actual_api_value'] is True else 'returned_existing'
                elif reply['actual_api_value'] is None: classification='successful_strong_absence'
                else: classification='successful_strong_present'
                counts[classification]+=1
                related=[{'attempt_id':attempt,'phases':phases} for (lid,attempt),phases in attempts.items() if lid==logical]
                wire_app_replies=[]
                for attempt in related:
                    requests=[phase['raw_frame']['decoded'] for phase in attempt['phases'] if phase.get('phase')=='request_frame_before_dispatch' and phase.get('raw_frame') and isinstance(phase['raw_frame'].get('decoded'),dict)]
                    responses=[phase['raw_frame']['decoded'] for phase in attempt['phases'] if phase.get('phase')=='response_frame_before_decode' and phase.get('raw_frame') and isinstance(phase['raw_frame'].get('decoded'),dict)]
                    for request in requests:
                        kind=request.get('request');app=kind.get('ApplicationEvent') if isinstance(kind,dict) else None
                        if not app: continue
                        actual=app['operation']
                        require(actual['namespace']==context['collection'] and actual['key']==context['key'],'wire request record differs from logical context')
                        if is_append:
                            require(actual['operation']=='append' and actual['value']==context['value'],'wire APP full payload differs')
                            require(app['range_id']==context['range'] and app['range_epoch']==context['range_epoch'],'wire APP range/epoch differs')
                        for response in responses:
                            require(response['request_id']==request['request_id'] and response['cluster_id']==request['cluster_id'],'wire response is not bound to actual request')
                            require(response['source_node_id']==request['destination_node_id'],'wire response source differs from actual destination')
                            value=response.get('response',{}).get('Ok',{}).get('ApplicationEvent')
                            if value is not None: wire_app_replies.append(value)
                if classification=='acknowledged_new_insert':
                    require(any(value.get('inserted') is True and value.get('result')=='appended' for value in wire_app_replies),'API ACK lacks retained exact wire APP success')
                operations.append({'logical_id':logical,'intent_sequence':intent['journal_sequence'],'intent_elapsed_ns':intent['elapsed_ns'],'intent_unix_ns':intent['unix_ns'],'api_duration_ns':reply['elapsed_ns']-intent['elapsed_ns'] if reply else None,'context':context,'actual_reply':reply,
                    'classification':classification,'same_key_logical_ordinal':ordinals[key],'actual_wire_application_replies':wire_app_replies,
                    'additional_recorded_rpc_attempts_not_necessarily_retries':max(0,len(related)-1),'all_rpc_attempts':related,
                    'any_recorded_may_have_dispatched':any(p.get('may_have_dispatched') for a in related for p in a['phases']),
                    'no_recorded_dispatch_does_not_establish_rollback':True})
            ledger=json.loads(z.read(prefix+'ack-ledger.json')) if prefix+'ack-ledger.json' in names else []
            for ack in ledger:
                require(ack['logical_id'] in intents,'logical ACK ledger entry lacks immutable intent')
                reply=replies.get(ack['logical_id'])
                if reply:
                    require(ack['actual_api_error']==reply['actual_api_error'],'logical ACK API error differs from raw journal')
                    require(ack['actual_api_inserted']==reply['actual_api_value'],'logical ACK return differs from raw journal')
                if 'actual_full_record' in ack:
                    require(ack['actual_full_record']['metadata']==ack['value'],'acknowledged full record payload differs')
            snapshots={n.rsplit('/',1)[1]:json.loads(z.read(n)) for n in names if n.startswith(prefix) and re.fullmatch(r'first-app-(pre-probe|pre-append|after-append-[0-9]+)\.json',n.rsplit('/',1)[1])}
            for name,snapshot in snapshots.items():
                require(len(snapshot['nodes'])==4,'snapshot lacks four actual nodes: '+name)
                require(snapshot['cached_readiness_is_not_admission'] is True,'snapshot claims cached admission')
            if traffic_first:
                require(all(op['context']['operation']=='ClusterEventClient.append' for op in operations),'traffic-first diagnostic included Strong warmup')
            appends=[op for op in operations if op['context']['operation']=='ClusterEventClient.append']
            require(all(op['same_key_logical_ordinal']==1 for op in appends),'APP helper was retried')
            transfers={n.rsplit('/',1)[1]:json.loads(z.read(n)) for n in names if n.startswith(prefix+'registered-transfer-') and n.endswith('.json')}
            for name,transfer in transfers.items():
                require(transfer['before']==transfer['after'],'ownership transfer changed target native/user state: '+name)
            if actual_result=='ok':
                require(len(appends)==2 and all(op['classification']=='acknowledged_new_insert' for op in appends),'passed fixture lacks both original real APP ACKs')
                require(len(transfers)==1,'passed diagnostic lacks first-range actual ownership transfer boundary')
            unmatched=[{'context_logical_id':logical,'attempt_id':attempt,'phases':phases} for (logical,attempt),phases in attempts.items() if logical not in intents]
            cases.append({'directory':prefix,'actual_identity':identity,'observer_mode':mode,'actual_test_result':actual_result,'counts':dict(counts),'operations':operations,'logical_ack_ledger':ledger,
                'all_actual_snapshot_observations':snapshots,'actual_ownership_transfer_boundaries':transfers,'unmatched_context_rpc_attempts':unmatched,
                'terminal':json.loads(z.read(prefix+'terminal.json')) if prefix+'terminal.json' in names else None})
        require(len(cases)<=4,'unexpected extra cohort fixture journal')
        recorded={case['actual_identity']['actual_rust_test_thread'] for case in cases}
        missing=[name for name in selected if name not in recorded]
        all_identities=[json.loads(z.read(name)) for name in names if name.endswith('/execution-identity.json')]
        no_journal=[]
        for name in names:
            if not name.endswith('/execution-identity.json'):continue
            identity=json.loads(z.read(name))
            if identity['actual_rust_test_thread'] in recorded:continue
            prefix=name.rsplit('/',1)[0]+'/'
            no_journal.append({'actual_identity':identity,'actual_test_result':dict(executed).get(identity['actual_rust_test_thread']),
                'first_app_not_observed':True,'terminal':json.loads(z.read(prefix+'terminal.json')) if prefix+'terminal.json' in names else None,
                'logical_ack_ledger':json.loads(z.read(prefix+'ack-ledger.json')) if prefix+'ack-ledger.json' in names else []})
        require(all(i['source_commit']==expected_source for i in all_identities),'fixture identity has different actual source')
        require(len({case['observer_mode']['actual_process_id'] for case in cases})<=1,'cohort fixtures did not share one actual process')
    return {'source_commit':expected_source,'projection_sha256':expected_zip,'selected_tests':selected,'actual_test_results':executed,
        'cases':cases,'all_retained_actual_identities':all_identities,'missing_first_app_journals':missing,'fixtures_without_first_app_journal':no_journal,'overlap':overlap(cases),'evidence_consistency_checked':True,'qualification':False,
        'limitations':['Actual RPC caller context is correlation; each attempt retains its own raw identity.',
        'Readiness probes precede writes; this run does not establish Strong-after-ACK read consistency.',
        'The four passive-observer counterparts do not replace the historical unobserved6d failed evidence.',
        'Both source APP ACKs precede one first-range ownership transfer; the source-only loop returns once target owns A.',
        'No DB replay, hardware fault, operational RPO/RTO, scaling or production qualification.']}

def overlap(cases):
    operations=[]
    fixtures=[]
    for case in cases:
        name=case['actual_identity']['actual_rust_test_thread']
        start=case['observer_mode']['fixture_initialization_started_unix_ns']
        replies=[op['actual_reply']['unix_ns'] for op in case['operations'] if op['actual_reply']]
        if replies: fixtures.append({'case':name,'start_unix_ns':start,'end_unix_ns':max(replies)})
        for op in case['operations']:
            if op['actual_reply']:
                require(op['actual_reply']['unix_ns']>=op['intent_unix_ns'],'recorded clock inversion; overlap cannot be established')
                operations.append({'case':name,'logical_id':op['logical_id'],'record_id':op['context']['record_id'],
                    'start_unix_ns':op['intent_unix_ns'],'end_unix_ns':op['actual_reply']['unix_ns']})
    pairs=[]
    for i,left in enumerate(operations):
        for right in operations[i+1:]:
            if left['case']==right['case']: continue
            common=min(left['end_unix_ns'],right['end_unix_ns'])-max(left['start_unix_ns'],right['start_unix_ns'])
            if common>0:pairs.append({'left':left['logical_id'],'right':right['logical_id'],'overlap_ns':common})
    endpoints=sorted([(op['start_unix_ns'],1,op['case']) for op in operations]+[(op['end_unix_ns'],-1,op['case']) for op in operations])
    active=collections.Counter();maximum=0
    for _time,change,case in endpoints:
        active[case]+=change;maximum=max(maximum,sum(count>0 for count in active.values()))
    all_fixture_overlap=(min(f['end_unix_ns'] for f in fixtures)-max(f['start_unix_ns'] for f in fixtures)) if len(fixtures)==4 else None
    return {'recorded_api_intent_reply_intervals':operations,'pairwise_cross_fixture_api_intersections':pairs,
        'maximum_distinct_fixtures_with_overlapping_recorded_api_intervals':maximum,
        'fixture_start_through_last_api_reply_intervals':fixtures,
        'common_all_four_fixture_interval_ns':max(0,all_fixture_overlap) if all_fixture_overlap is not None else None,
        'limits':['API intervals include observer IO; they bound actual ordinary calls, not precise engine execution or socket-write completion.',
            'Actual Unix timestamps and actual process identities establish these observations; configured threads alone do not.',
            'No shared-resource cause, deadlock, rollback, availability or performance qualification is inferred.']}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('projection',type=pathlib.Path);p.add_argument('--expected-zip-sha256',required=True);p.add_argument('--expected-source',required=True);p.add_argument('--origin-verifier',type=pathlib.Path,default=pathlib.Path(__file__).with_name('verify-native-origin-evidence.py'));p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
    result=verify(a.projection,a.expected_zip_sha256,a.expected_source,a.origin_verifier);a.output.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({'source':result['source_commit'],'actual_test_results':result['actual_test_results'],'counts':[case['counts'] for case in result['cases']],'qualification':False}))
if __name__=='__main__': main()
