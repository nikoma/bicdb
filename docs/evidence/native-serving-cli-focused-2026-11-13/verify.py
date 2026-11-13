#!/usr/bin/env python3
"""Offline exact bytes AND independently rederived journal/log semantics. No engine/network."""
import collections
import gzip
import hashlib
import json
import pathlib
import re

CLI_SOURCE = '5cec7687196fe6b113805485672e8730c8295a56'
SERVING_SOURCE = 'ca8383ef36190c6c69cc8853cd51dd7803c7cc52'
ROOT = pathlib.Path(__file__).resolve().parent


def sha(data):
    return hashlib.sha256(data).hexdigest()


def same(a,b):
    # JSON distinguishes booleans from numbers; Python container == does not.
    return json.dumps(a,sort_keys=True,separators=(',',':'),ensure_ascii=False)==json.dumps(b,sort_keys=True,separators=(',',':'),ensure_ascii=False)


def frame(value):
    raw = bytes(value)
    assert len(raw) >= 4 and int.from_bytes(raw[:4], 'big') == len(raw) - 4, 'complete exact frame length'
    return json.loads(raw[4:])


def projection(rows, item):
    """Pure derived DATA, deliberately separate from validation (also used by tamper controls)."""
    intents = {x['diagnostic_operation_id']: x['request'] for x in rows if x['kind'] == 'logical_request_intent'}
    logical = []
    for x in rows:
        if x['kind'] != 'logical_public_reply':
            continue
        outcome = x['outcome']; request = intents[x['diagnostic_operation_id']]
        entry = dict(id=x['diagnostic_operation_id'], request=request, operation=request.get('public_operation'), key=request.get('key'),
                     phase=request.get('phase'), classification=outcome['classification'], presence=outcome.get('presence'),
                     error_chain=outcome.get('error_chain'), reply={k: v for k, v in outcome.items() if k != 'snapshot'})
        if 'snapshot' in outcome:
            entry.update(topology_generation=outcome['snapshot']['topology']['generation'], member_count=len(outcome['snapshot']['topology']['nodes']))
        logical.append(entry)
    phases = [x for x in rows if x['kind'] == 'actual_rpc_phase']
    grouped = collections.defaultdict(list)
    for x in phases:
        grouped[(x['observer_instance_id'], x['actual']['attempt_id'])].append(x)
    physical = []
    for (observer, attempt), events in grouped.items():
        a = events[-1]['actual']
        physical.append(dict(observer_instance_id=observer, attempt_id=attempt,
            diagnostic_operation_id=events[0]['logical_context']['diagnostic_operation_id'],
            observer_name=events[0]['observer_name'], operation=a['operation'], caller=a['caller'],
            destination=a['destination'], address=a['address'], topology_generation=a['topology_generation'],
            request_id=a['request_id'], phase_order=[e['actual']['phase'] for e in events],
            frames=[dict(phase=e['actual']['phase'], sha256=sha(bytes(e['actual']['raw_frame']))) for e in events if e['actual']['raw_frame'] is not None],
            errors=[dict(phase=e['actual']['phase'], error_chain=e['actual']['error_chain']) for e in events if e['actual']['error_chain']],
            final_bytes_written=a['bytes_written'], final_may_have_dispatched=a['may_have_dispatched']))
    return dict(file=item['original_artifact_path'], bytes=item['raw_bytes'], sha256=item['raw_sha256'],
        test=rows[0]['test'], records=len(rows), counts=dict(collections.Counter(x['kind'] for x in rows)),
        attempt_count=len(grouped), phase_counts=dict(collections.Counter(x['actual']['phase'] for x in phases)),
        operations=dict(collections.Counter(x['actual']['operation'] for x in phases)),
        wire_frames=sum(x['actual']['raw_frame'] is not None for x in phases), logical=logical,
        sink_sequence_complete=[x['sink_sequence'] for x in rows] == list(range(1, len(rows)+1)),
        fixture_finished=rows[-1], physical_attempts=physical)


def verify_journal(rows, item, index):
    assert [x['sink_sequence'] for x in rows] == list(range(1, len(rows)+1)), 'exact sink sequence'
    assert all(a['sink_unix_ns'] <= b['sink_unix_ns'] for a, b in zip(rows, rows[1:])), 'sink chronology'
    assert rows[0]['kind'] == 'journal_started' and rows[0]['source'] == CLI_SOURCE
    assert rows[-1]['kind'] == 'fixture_case_finished' and not rows[-1]['trace_fenced']
    assert not rows[0]['bootstrap_raw_frame_covered'] and rows[0]['no_protocol_or_serving_authority']
    active = None; intents = {}; replies = {}; groups = collections.defaultdict(list); topologies = []
    observer_sequences = collections.defaultdict(list)
    for row in rows[1:-1]:
        kind = row['kind']
        if kind == 'logical_request_intent':
            ident = row['diagnostic_operation_id']
            assert active is None and ident not in intents, 'unique nonoverlapping logical intent'
            assert row['context_is_protocol_business_id'] is False
            active = ident; intents[ident] = row['request']
        elif kind == 'logical_public_reply':
            ident = row['diagnostic_operation_id']
            assert ident == active and ident not in replies, 'exact paired reply order'
            replies[ident] = row; active = None
            if 'snapshot' in row['outcome']:
                snapshot = row['outcome']['snapshot']; topology = snapshot['topology']; topologies.append(topology)
                assert row['outcome']['classification'] == 'ACTUAL_AUTHENTICATED_BOOTSTRAP_DATA_ONLY'
                assert topology['cluster_id'] == 'native-operator-cli' and not row['outcome']['bootstrap_raw_frame_covered']
        elif kind == 'actual_rpc_phase':
            assert active is not None and same(row['logical_context'],dict(diagnostic_operation_id=active, request=intents[active])), 'physical exact logical context'
            a = row['actual']; observer_sequences[row['observer_instance_id']].append(a['journal_sequence'])
            groups[(row['observer_instance_id'], a['attempt_id'])].append(row)
        else:
            raise AssertionError('unexpected raw journal record kind')
    if index==0:
        assert any(x.get('key')=='before-initial-data-quorum' for x in intents.values()), 'original uncertain intent cannot be dropped'
    assert active is None and intents.keys() == replies.keys() and len(intents) == [11, 8, 10][index], 'all29 actual intent/reply pairs'
    for sequence in observer_sequences.values():
        assert sequence == list(range(1, len(sequence)+1)), 'observer exact phase sequence'
    assert len(groups) == [127, 6, 8][index], 'all physical attempts retained'
    assert len(rows)==[533,42,54][index], 'every original phase/intent/reply record retained'
    successful = collections.defaultdict(list); errors = collections.defaultdict(list); dispatched = collections.defaultdict(bool)
    for (observer, attempt), events in groups.items():
        aa = [e['actual'] for e in events]; logical = events[0]['logical_context']; ident = logical['diagnostic_operation_id']; intent = intents[ident]
        assert aa[0]['phase'] == 'invoked'
        assert all(a['attempt_id'] == attempt and e['observer_instance_id'] == observer and e['observer_name'] == events[0]['observer_name'] and same(e['logical_context'],logical) for e, a in zip(events, aa))
        assert all(a['operation'] == aa[0]['operation'] and a['caller'] == aa[0]['caller'] and a['destination'] == aa[0]['destination'] for a in aa)
        assert all(a['elapsed_ns'] <= b['elapsed_ns'] and a['phase_unix_ns'] <= b['phase_unix_ns'] for a, b in zip(aa, aa[1:])), 'attempt monotone times'
        phases = [a['phase'] for a in aa]
        assert phases in [
            ['invoked','request_frame_before_dispatch','response_frame_before_decode','validated_transport_result_ok'],
            ['invoked','request_frame_before_dispatch','response_frame_before_decode','validated_transport_result_error'],
            ['invoked','request_frame_before_dispatch','exchange_error_before_cancellation_override','validated_transport_result_error','return_boundary_error_after_evidence'],
        ], 'exact ordered actual attempt phases'
        requests = [a for a in aa if a['phase'] == 'request_frame_before_dispatch']; responses = [a for a in aa if a['phase'] == 'response_frame_before_decode']
        assert len(requests) == 1 and len(responses) <= 1
        req = requests[0]; envelope = frame(req['raw_frame'])
        assert envelope['protocol_version'] == 29 and envelope['cluster_id'] == 'native-operator-cli'
        assert envelope['caller_node_id'] == req['caller'] and envelope['destination_node_id'] == req['destination'] and envelope['request_id'] == req['request_id'], 'request identity'
        known = next(t for t in topologies if t['generation'] == req['topology_generation'] and req['destination'] in t['nodes'])
        assert req['address'] == known['nodes'][req['destination']]['address'], 'actual route address/generation'
        if intent['public_operation']=='replica_receiver_refusal_probe':
            assert req['destination']==intent['destination']
        if operation_marker := envelope['request'] if isinstance(envelope['request'],dict) else None:
            selected=operation_marker.get('ApplicationEvent',operation_marker.get('NativeEventGet'))
            if selected is not None:
                descriptor=next(x for x in known['ranges'].values() if x['id']==selected['range_id'])
                assert selected['range_epoch']==descriptor['epoch']
                if intent['public_operation']!='replica_receiver_refusal_probe':
                    assert req['destination']==descriptor['leader'], 'recorded DATA leader route'
                else:
                    assert selected['range_id']==intent['range_id'] and selected['range_epoch']==intent['range_epoch']
        for a in aa[1:]:
            assert (a['request_id'], a['address'], a['topology_generation']) == (req['request_id'], req['address'], req['topology_generation']), 'phase route/id continuity'
            assert isinstance(a['error_chain'], list) and all(isinstance(e, str) and e for e in a['error_chain'])
            dispatched[ident] |= a['may_have_dispatched']
            if a['error_chain']:
                errors[ident].append(a['error_chain'])
        request = envelope['request']; operation = req['operation']
        if operation.startswith('event_'):
            app = request['ApplicationEvent']; op = app['operation']
            if intent['public_operation'] == 'replica_receiver_refusal_probe':
                assert same(op,intent['operation']) and app['range_id'] == intent['range_id'] and app['range_epoch'] == intent['range_epoch']
            else:
                assert intent['public_operation'] == 'append' and same(op,{k: intent[k] for k in ('namespace', 'key', 'value')} | {'operation': 'append'}), 'append exact payload/context'
        elif operation == 'native_event_get':
            get = request['NativeEventGet']; assert get['mode'] == 'strong'
            expected = intent['operation'] if intent['public_operation'] == 'replica_receiver_refusal_probe' else intent
            assert get['namespace'] == expected['namespace'] and get['key'] == expected['key'], 'Strong exact key/context'
            assert 0 < get['budget_ms'] <= intent.get('request_timeout_ms', 5000)
        else:
            assert operation == 'metadata_snapshot_discovery' and request == 'FetchBootstrapSnapshot', 'only actual recorded discovery lane'
        if responses:
            response = responses[0]; returned = frame(response['raw_frame'])
            assert returned['protocol_version'] == envelope['protocol_version'] and returned['cluster_id'] == envelope['cluster_id']
            assert returned['request_id'] == envelope['request_id'] and returned['source_node_id'] == req['destination'], 'actual response identity'
            assert same(returned['actual_native_source'],known['nodes'][req['destination']]['native_store_birth_v1']), 'actual response native source'
            assert response['bytes_written'] == len(bytes(req['raw_frame'])) and response['may_have_dispatched']
            body = returned['response']; assert set(body) in ({'Ok'}, {'Err'})
            terminals = [a for a in aa if a['phase'].startswith('validated_transport_result_')]
            assert len(terminals) == 1
            terminal = terminals[0]
            if 'Err' in body:
                err = body['Err']; expected = f"cluster error: cluster node {req['destination']} rejected RPC {err['code']}: {err['message']}"
                assert terminal['phase'] == 'validated_transport_result_error' and terminal['error_chain'] == [expected], 'full original remote error context'
            else:
                assert terminal['phase'] == 'validated_transport_result_ok' and not terminal['error_chain']
                result = body['Ok']
                if operation == 'native_event_get':
                    actual = result['NativeEventGet']; get = request['NativeEventGet']
                    assert actual['business_request_id'] == get['business_request_id'] and actual['challenge'] == get['challenge']
                    if actual['result']['kind'] == 'value':
                        value = actual['result']; route = value['route']; audit = value['audit']
                        assert (route['range_id'], route['range_epoch'], route['leader']) == (get['range_id'], get['range_epoch'], req['destination'])
                        assert audit['business_request_id'] == get['business_request_id'] and audit['challenge'] == get['challenge']
                        assert (audit['range_id'], audit['range_epoch']) == (get['range_id'], get['range_epoch'])
                        assert audit['range_index'] > 0 and audit['target_local_visibility_db_sequence'] <= audit['snapshot_sequence']
                if operation != 'metadata_snapshot_discovery':
                    successful[ident].append(result)
        else:
            assert 'exchange_error_before_cancellation_override' in phases and 'return_boundary_error_after_evidence' in phases
            assert aa[-1]['error_chain'] == ['canceling statement due to query timeout'], 'exact recorded cancellation override'
    requests_by_observer=collections.defaultdict(list)
    for (observer,_),events in groups.items():
        requests_by_observer[observer].append(next(e['actual']['request_id'] for e in events if e['actual']['phase']=='request_frame_before_dispatch'))
    for ids in requests_by_observer.values():
        assert ids==list(range(1,len(ids)+1)), 'every original observer request ID retained once'
    ack_values = {}; classifications = []
    for ident, intent in intents.items():
        row = replies[ident]; outcome = row['outcome']; cls = outcome['classification']; classifications.append(cls)
        assert row['any_actual_rpc_may_have_dispatched'] == dispatched[ident], 'actual dispatch classification'
        operation = intent['public_operation']
        if operation.startswith('authenticated_bootstrap'):
            assert not successful[ident] and not dispatched[ident] and not outcome['bootstrap_raw_frame_covered']
            continue
        assert row['durable_outcome_inferred_from_error'] is False
        if operation == 'append':
            assert intent['payload_sha256'] == sha(json.dumps(intent['value'], separators=(',', ':'), ensure_ascii=False).encode()), 'full intent payload hash'
            acks = [x['ApplicationEvent'] for x in successful[ident] if 'ApplicationEvent' in x]
            if cls == 'USER_ACK':
                assert same(acks,[{'result': 'appended', 'inserted': outcome['inserted']}]) and outcome['inserted'] is True, 'ACK backed by exact actual successful append reply'
                ack_values[intent['key']] = intent['value']
            else:
                assert cls == 'NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN' and not acks and outcome['error_chain'] in errors[ident], 'uncertain remains uncertain; no retroactive ACK'
        elif operation == 'get_strong':
            values = [x['NativeEventGet']['result']['value'] for x in successful[ident] if 'NativeEventGet' in x and x['NativeEventGet']['result']['kind'] == 'value']
            if cls == 'STRONG_SUCCESS':
                assert len(values) == 1 and same(values[0],{k: v for k, v in outcome.items() if k in ('presence', 'value')}), 'actual full Strong value/presence'
                key = intent['key']
                if key in ack_values:
                    assert outcome['presence'] == 'present' and 'value' in outcome and same(outcome['value'],ack_values[key]), 'entire earlier ACK including null/empty survives'
                else:
                    assert key == 'genuine-unwritten-key' and outcome == {'classification':'STRONG_SUCCESS', 'presence':'absent'}, 'genuine absence only'
            else:
                assert cls == 'NO_SUCCESSFUL_READ_NO_ABSENCE' and not values and outcome['error_chain'] in errors[ident], 'failed read is never absence'
        else:
            assert operation == 'replica_receiver_refusal_probe' and cls == 'ACTUAL_RECEIVER_REFUSAL'
            structured = [x['NativeEventGet']['result'] for x in successful[ident] if 'NativeEventGet' in x]
            if structured:
                assert len(structured) == 1 and structured[0]['kind'] == 'unavailable'
                assert outcome['error_chain'] == ['cluster error: strong_point_read_unavailable: '+structured[0]['detail']]
            else:
                assert outcome['error_chain'] in errors[ident]
    assert [classifications.count(x) for x in ('USER_ACK','STRONG_SUCCESS','ACTUAL_RECEIVER_REFUSAL','NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN','NO_SUCCESSFUL_READ_NO_ABSENCE')] == [[3,5,0,1,1],[1,2,3,0,0],[2,3,3,0,0]][index], 'all actual classifications'
    assert {x.get('key') for x in intents.values() if x.get('key')} >= ([{'before-initial-data-quorum','not-written-before-open','external-first-ack','external-json-null','external-empty-object','genuine-unwritten-key'}, {'ack-before-fifth-replica-receiver'}, {'ack-before-fifth-registry-refresh','fresh-ack-after-fifth-registry-addition'}][index])
    return projection(rows, item)


def plain_lines(text):
    return [re.sub(r'^\S+Z ', '', re.sub(r'\x1b\[[0-9;]*m', '', line)) for line in text.splitlines()]


def executed_tests(text):
    lines = plain_lines(text); starts = [(i, re.match(r'^test ([\w:]+) \.\.\. ?(.*)$', l)) for i,l in enumerate(lines)]
    starts = [(i,m) for i,m in starts if m]
    names = []
    for pos,(i,m) in enumerate(starts):
        end = starts[pos+1][0] if pos+1 < len(starts) else len(lines)
        assert m[2] != 'FAILED', 'no failed per-test result may be counted'
        assert m[2] == 'ok' or 'ok' in lines[i+1:end], 'each actual libtest case terminates ok'
        names.append(m[1])
    assert len(names) == len(set(names)), 'unique actual executed names'
    return names


def verify(root=ROOT):
    manifest=json.loads((root/'MANIFEST.json').read_text())
    for item in manifest['files']:
        data=(root/item['path']).read_bytes();assert len(data)==item['bytes'] and sha(data)==item['sha256'],item['path']
    result=json.loads((root/'results.json').read_text())
    assert (result['serving']['source'], result['cli']['source']) == (SERVING_SOURCE, CLI_SOURCE)
    assert (result['serving']['passed'],result['serving']['failed'],result['cli']['passed'],result['cli']['failed']) == (9,0,6,0)
    logs={}
    for label in ['serving-core','serving-compile','cli-core','cli-compile']:
        raw=gzip.decompress((root/(label+'.log.gz')).read_bytes());idx=json.loads((root/(label+'-index.json')).read_text())
        assert len(raw)==idx['raw_bytes'] and sha(raw)==idx['raw_sha256'] and idx['job_conclusion']=='success'
        compressed=(root/(label+'.log.gz')).read_bytes()
        assert len(compressed)==idx['gzip_bytes'] and sha(compressed)==idx['gzip_sha256']
        source=SERVING_SOURCE if label.startswith('serving') else CLI_SOURCE
        assert idx['source_revision']==source and source in plain_lines(raw.decode()), 'exact executed checkout source line'
        logs[label]=raw.decode()
    for label,count in [('serving-core',9),('cli-core',6)]:
        names=executed_tests(logs[label]);assert len(names)==count
        data=result['serving' if label.startswith('serving') else 'cli']
        assert data['executed_tests']==names
        assert re.search(r'test result: ok\. '+str(count)+r' passed; 0 failed; 0 ignored;',logs[label])
    serving_raw=[line for line in logs['serving-core'].splitlines() if re.search(r'Z test [\w:]+ \.\.\. ok$',line)]
    assert result['serving']['tests']==serving_raw
    assert 'ACTUAL_SERVING_SCHEMA_REFUSAL reached=true exact_schema_error=true parent_live_at_semantic_refusal=true' in logs['serving-core']
    listed=[x.removesuffix(': test') for x in (root/'cli-selected-tests.txt').read_text().splitlines()]
    assert listed==result['cli']['executed_tests']
    ledger=[]
    for line in logs['cli-core'].splitlines():
        m=re.search(r'NATIVE_INITIAL_DATA_EXTERNAL_LEDGER (\{.*\})',line)
        if m:ledger.append(json.loads(m[1]))
    assert same(ledger,json.loads((root/'cli-external-ledger.json').read_text()))
    identities=[x['execution_identity'] for x in ledger if 'execution_identity' in x]
    assert len(identities)==3
    for identity in identities:
        assert identity['source']==CLI_SOURCE and identity['test_package_version']==result['cli']['package_version']
        assert identity['binary_sha256']==result['cli']['binary_sha256'] and identity['binary_bytes']==result['cli']['binary_bytes']
    derived=[]; all_intents={}; all_replies={}
    for index,item in enumerate(result['cli']['journals']):
        raw=gzip.decompress((root/item['file']).read_bytes());assert len(raw)==item['raw_bytes'] and sha(raw)==item['raw_sha256']
        rows=[json.loads(line) for line in raw.splitlines()]
        assert rows[0]['test'] in result['cli']['executed_tests']
        derived.append(verify_journal(rows,item,index))
        for row in rows:
            if row['kind']=='logical_request_intent':
                ident=row['diagnostic_operation_id'];assert ident not in all_intents
                all_intents[ident]=row['request']
            elif row['kind']=='logical_public_reply':all_replies[row['diagnostic_operation_id']]=row['outcome']
    assert same(derived,json.loads((root/'cli-journal-summary.json').read_text())), 'entire stored summary rederived from raw journals'
    for entry in ledger:
        ident=entry.get('logical_operation_id')
        if ident is None:continue
        assert ident in all_intents and ident in all_replies, 'external ledger actual logical ID'
        intent=all_intents[ident];outcome=all_replies[ident]
        if 'key' in entry:assert entry['key']==intent['key']
        if 'namespace' in entry:assert entry['namespace']==intent['namespace']
        if 'payload_sha256' in entry:assert entry['payload_sha256']==intent['payload_sha256']
        if intent['public_operation']=='append' and 'value' in entry:
            assert same(entry['value'],intent['value']), 'external ledger full APP intent payload'
        if 'acknowledged' in entry:
            if entry['acknowledged']:
                assert outcome['classification']=='USER_ACK' and entry['error'] is None
            else:
                assert outcome['classification']=='NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN' and outcome['error_chain']==[entry['error']]
        if entry['stage'].startswith('strong') or entry['stage'] in ('strict_old_ack_read','strict_fresh_ack_read_reply'):
            assert outcome['classification']=='STRONG_SUCCESS'
            if 'present' in entry:assert entry['present'] is (outcome['presence']=='present')
            if outcome['presence']=='present':assert same(entry['value'],outcome['value']), 'external ledger full Strong payload'
            else:assert 'value' not in outcome, 'external null report is not presence'
    terminal=next(x for x in ledger if x['stage']=='strict_registry_addition_terminal')
    assert terminal['healthy_triple_receipt_ns']==932375073 and terminal['healthy_triple_receipt_ns']<terminal['health_limit_ns']
    assert terminal['old_ack_read'] and terminal['fresh_ack'] and terminal['fresh_ack_read']
    assert terminal['fifth_data_serving'] is False and terminal['data_move']=='not_attempted'
    assert sum(len(x['logical']) for x in derived)==29
    assert [x['wire_frames'] for x in derived]==[253,12,16]
    assert result['prior_failed_history']['fourcase_fail']==1 and result['prior_failed_history']['fourcase_pass']==3
    assert result['prior_failed_history']['scope_pass']==8 and result['prior_failed_history']['scope_fail']==1
    assert not result['qualified'] and not result['deployed'] and result['fifth_data_move']=='NOT_ATTEMPTED', 'focused results cannot self-qualify or claim fifth DATA move'
    assert not result['source_engine_opened_or_replayed_for_receipt']
    return derived


if __name__=='__main__':
    verify()
    print('PASS exact9/6 raw-log names/sources; all29 raw logical pairs; all141 physical attempts and281 complete frame records; full ACK/Strong/null/absence/unknown/error context; failed gates retained')
