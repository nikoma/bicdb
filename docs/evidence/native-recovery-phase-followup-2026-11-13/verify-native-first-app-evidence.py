#!/usr/bin/env python3
"""Reconcile a public first-APP diagnostic projection; no DB custody is read.
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
        require(len(selected)==1 and len(executed)==1 and selected[0]==executed[0][0],'not exactly one selected/executed diagnostic')
        cases=[]
        for journal_name in names:
            if not journal_name.endswith('/first-app-rpc.jsonl'): continue
            prefix=journal_name.rsplit('/',1)[0]+'/'
            identity=json.loads(z.read(prefix+'execution-identity.json'))
            require(identity['source_commit']==expected_source,'actual fixture source differs')
            require(selected[0].endswith(identity['actual_rust_test_thread']),'actual Rust test identity differs')
            traffic_first=selected[0].endswith('native_registered_fifth_remote_transfer_observed_traffic_first_diagnostic')
            readiness_qualified=selected[0].endswith('native_registered_fifth_remote_transfer_after_ordinary_strong_absent_diagnostic')
            require(traffic_first or readiness_qualified,'unknown diagnostic selector')
            require(identity['source_case']==('TargetTransferObservedTrafficFirstDiagnostic' if traffic_first else 'TargetTransferReadinessDiagnostic'),'actual fixture case differs from selected diagnostic')
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
                        app=request.get('request',{}).get('ApplicationEvent')
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
                operations.append({'logical_id':logical,'intent_sequence':intent['journal_sequence'],'intent_elapsed_ns':intent['elapsed_ns'],'api_duration_ns':reply['elapsed_ns']-intent['elapsed_ns'] if reply else None,'context':context,'actual_reply':reply,
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
            snapshots={n.rsplit('/',1)[1]:json.loads(z.read(n)) for n in names if n.startswith(prefix+'first-app-') and n.endswith('.json')}
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
            if executed[0][1]=='ok':
                require(len(appends)==2 and all(op['classification']=='acknowledged_new_insert' for op in appends),'passed fixture lacks both original real APP ACKs')
                require(len(transfers)==1,'passed diagnostic lacks first-range actual ownership transfer boundary')
            unmatched=[{'context_logical_id':logical,'attempt_id':attempt,'phases':phases} for (logical,attempt),phases in attempts.items() if logical not in intents]
            cases.append({'directory':prefix,'actual_identity':identity,'counts':dict(counts),'operations':operations,'logical_ack_ledger':ledger,
                'all_actual_snapshot_observations':snapshots,'actual_ownership_transfer_boundaries':transfers,'unmatched_context_rpc_attempts':unmatched,
                'terminal':json.loads(z.read(prefix+'terminal.json')) if prefix+'terminal.json' in names else None})
        require(len(cases)==1,'expected one diagnostic fixture journal')
    return {'source_commit':expected_source,'projection_sha256':expected_zip,'selected_tests':selected,'actual_test_results':executed,
        'cases':cases,'evidence_consistency_checked':True,'qualification':False,
        'limitations':['Actual RPC caller context is correlation; each attempt retains its own raw identity.',
        'Readiness probes precede writes; this run does not establish Strong-after-ACK read consistency.',
        'Original four traffic-first cases and failed6d campaign are not rerun or replaced.',
        'Both source APP ACKs precede one first-range ownership transfer; the source-only loop returns once target owns A.',
        'No DB replay, hardware fault, operational RPO/RTO, scaling or production qualification.']}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('projection',type=pathlib.Path);p.add_argument('--expected-zip-sha256',required=True);p.add_argument('--expected-source',required=True);p.add_argument('--origin-verifier',type=pathlib.Path,default=pathlib.Path('/tmp/verify-native-origin-evidence.py'));p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
    result=verify(a.projection,a.expected_zip_sha256,a.expected_source,a.origin_verifier);a.output.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n');print(json.dumps({'source':result['source_commit'],'actual_test_results':result['actual_test_results'],'counts':[case['counts'] for case in result['cases']],'qualification':False}))
if __name__=='__main__': main()
