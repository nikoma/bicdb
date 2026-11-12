#!/usr/bin/env python3
"""Offline bounded receipt checks, with optional exact private ZIP parity.
These verify retained bytes and semantics; they do not replay the engine or
independently authenticate that CI executed the recorded source.
"""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import zipfile

HERE = Path(__file__).resolve().parent
SOURCE = 'aac6192c2b034e1f2e4c88a16fd84bcd94e0bf6b'

def digest(b):
    return hashlib.sha256(b).hexdigest()

def require(condition, reason):
    if not condition:
        raise ValueError(reason)

def validate(r, e):
    require(r['source'] == e['source'] == SOURCE, 'source')
    require(r['actual_counts']['registry']['passed'] == 4 and r['actual_counts']['registry']['failed'] == 1, 'registry failed receipt')
    require(r['actual_counts']['schema']['passed'] == 1 and r['actual_counts']['schema']['failed'] == 0, 'schema count')
    require(r['actual_counts']['cli']['passed'] == 5 and r['actual_counts']['cli']['failed'] == 1, 'whole CLI remains failed')
    require(len(r['checks']) == 6 and all(c['source_revision'] == SOURCE for c in r['checks']), 'six source-bound jobs')
    require(all(c['job_conclusion'] == 'success' for c in r['checks'][3:]), 'compile/consumer status')
    identity = r['execution_identity']
    require(identity['source'] == SOURCE and identity['binary_bytes'] == 751406136 and identity['binary_sha256'] == '35c3b103e1a54d6aee1c10ec9bbc597a995429ecc96879a44c1ebc14363361df', 'actual CLI binary')
    records = e['records']
    require(len(records) == 54 and [x['sink_sequence'] for x in records] == list(range(1, 55)), 'all54 original ordered records')
    require(records[0]['source'] == SOURCE and not records[0]['bootstrap_raw_frame_covered'], 'bootstrap gap remains explicit')
    require(records[-1]['kind'] == 'fixture_case_finished' and not records[-1]['trace_fenced'], 'complete unfenced journal')
    intents = {x['diagnostic_operation_id']: x for x in records if x['kind'] == 'logical_request_intent'}
    replies = {x['diagnostic_operation_id']: x for x in records if x['kind'] == 'logical_public_reply'}
    require(len(intents) == len(replies) == 10 and intents.keys() == replies.keys(), 'logical operation parity')
    rpc = [x for x in records if x['kind'] == 'actual_rpc_phase']
    require(len(rpc) == 32, '32 actual phases')
    requests = {}; responses = {}; gateway_ids = set()
    for x in rpc:
        a = x['actual']; context = x['logical_context']['diagnostic_operation_id']
        require(context in intents, 'actual phase context')
        if a['error_chain']:
            require(intents[context]['request']['public_operation'] == 'replica_receiver_refusal_probe' and a['phase'] == 'validated_transport_result_error' and a['error_chain'] == replies[context]['outcome']['error_chain'], 'only exact expected receiver refusal errors')
        frame = a.get('decoded_frame')
        if frame is None:
            continue
        require(a['raw_frame_bytes'] > 4 and len(a['raw_frame_sha256']) == 64, 'framed byte identity')
        key = (x['observer_instance_id'], a['attempt_id'])
        require(frame['request_id'] == a['request_id'] and frame['protocol_version'] == 28, 'request identity/version')
        if a['phase'] == 'request_frame_before_dispatch':
            requests[key] = (x, frame)
        elif a['phase'] == 'response_frame_before_decode':
            responses[key] = (x, frame)
    require(len(requests) == len(responses) == 8 and requests.keys() == responses.keys(), '8 actual paired wire attempts')
    for key, (x, request) in requests.items():
        reply, response = responses[key]
        require(request['request_id'] == response['request_id'] and request['destination_node_id'] == response['source_node_id'], 'peer/request parity')
        require(response['actual_native_source']['writer_enforcement_version'] == 1, 'actual source birth')
        intent = intents[x['logical_context']['diagnostic_operation_id']]['request']
        if intent['public_operation'] != 'replica_receiver_refusal_probe':
            gateway_ids.add(x['observer_instance_id'])
            require('Ok' in response['response'], 'healthy original gateway response')
        if 'NativeEventGet' in request['request']:
            req = request['request']['NativeEventGet']; val = response['response']['Ok']['NativeEventGet']
            require(all(req[k] == val[k] for k in ['business_request_id', 'challenge']), 'Strong business/challenge parity')
            if intent['public_operation'] != 'replica_receiver_refusal_probe':
                result = val['result']
                require(result['kind'] == 'value' and result['value']['presence'] == 'present', 'no false absence')
                require(result['audit']['first_metadata_context_id'] != result['audit']['final_metadata_context_id'], 'distinct final MQ')
                expected = next(i['request']['value'] for i in intents.values() if i['request'].get('public_operation') == 'append' and i['request'].get('key') == intent['key'])
                require(result['value']['value'] == expected, 'full ACK value parity')
    require(len(gateway_ids) == 1, 'SAME gateway observer across join')
    ledger = e['strict_stdout_ledger']
    terminal = next(x for x in ledger if x['stage'] == 'strict_registry_addition_terminal')
    require(terminal['rounds'] == 1 and all(terminal[k] for k in ['old_ack_read','fresh_ack','fresh_ack_read']), 'healthy triple')
    require(terminal['healthy_triple_receipt_ns'] == 1015797325 < terminal['health_limit_ns'] == 10000000000, 'original10s receipt budget')
    require(not terminal['observed_false_absence'] and not terminal['observed_wrong_value'] and not terminal['fifth_data_serving'] and terminal['data_move'] == 'not_attempted', 'scope')
    bindings = [x for x in ledger if x['stage'] == 'readonly_retained_writer_binding']
    require(len(bindings) == 8, 'all4 before/after bindings')
    for node in ['n0','n1','n2','n3']:
        pair = [x for x in bindings if x['node'] == node]
        before = next(x['binding'] for x in pair if x['phase'] == 'before_fifth')
        after = next(x['binding'] for x in pair if x['phase'] != 'before_fifth')
        for k in ['persisted_policy_registration_generation','persisted_policy_coordinator_id','persisted_policy_database_open_incarnation','persisted_policy_range_store_open_id','persisted_policy_checksum_sha256']:
            require(before[k] == after[k], 'original writer binding changed')
        extension = after['actual_verified_registry_extension']
        require(not extension['publication_pending'] and extension['verified_extension']['sequence'] == 1, 'all4 published extension')
    require(e['failed_command']['action'] == 'elect' and e['failed_status']['error'].endswith('owned metadata member changed/busy') and e['failed_stderr_bytes'] == 0, 'exact failed command')
    require(len(e['failed_children_waits']) == 5 and all(x['wait_ok'] and not x['before_kill']['exited'] for x in e['failed_children_waits']), 'five living children then waited')
    states = e['stopped_metadata_states']
    require(len(states) == 5 and all(x['current_term'] == 2 and x['commit_index'] == 5 and x['leader_id'] == 'n4' for x in states), 'later stopped metadata state')
    require(next(x for x in states if x['root'] == 'db4')['role'] == 'leader', 'automatic later leadership')
    c = r['failed_fixture_custody']
    require(c['database_files'] == 96 and c['database_logical_bytes'] == 1505406 and c['directory_entries'] == 40 and c['diagnostic_files'] == 32 and c['complete'], 'custody counts')
    require(not c['engine_open_or_replay'] and not c['data_ack_fixture_roots_retained'], 'no false reconciliation')

def private_parity(r, e, directory):
    for artifact in r['artifacts']:
        p = directory / artifact['private_filename']
        require(p.stat().st_size == artifact['bytes'] and digest(p.read_bytes()) == artifact['sha256'], 'ZIP parity')
    with zipfile.ZipFile(directory/'13981575889.zip') as z:
        raw = z.read(e['strict_journal_path'])
        require(len(raw) == e['strict_journal_bytes'] and digest(raw) == e['strict_journal_sha256'], 'original strict journal parity')
        originals = [json.loads(l) for l in raw.splitlines()]
        projected_records = copy.deepcopy(originals)
        for record in projected_records:
            if record['kind'] == 'actual_rpc_phase':
                frame = record['actual'].pop('raw_frame')
                if frame is not None:
                    b = bytes(frame)
                    require(int.from_bytes(b[:4], 'big') == len(b)-4, 'original frame length header')
                    record['actual'].update(raw_frame_sha256=digest(b), raw_frame_bytes=len(b), decoded_frame=json.loads(b[4:]))
            if record['kind'] == 'logical_public_reply' and 'snapshot' in record['outcome']:
                snapshot = record['outcome'].pop('snapshot')
                record['outcome']['snapshot_topology'] = snapshot['topology']
        require(projected_records == e['records'], 'all54 exact allowlisted original record projections')
        for original, projected in zip(originals, e['records']):
            if original['kind'] == 'actual_rpc_phase' and original['actual']['raw_frame'] is not None:
                b = bytes(original['actual']['raw_frame']); a = projected['actual']
                require(digest(b) == a['raw_frame_sha256'] and json.loads(b[4:]) == a['decoded_frame'], 'actual raw frame parity')
        c = r['failed_fixture_custody']; manifest = z.read(c['diagnostics_manifest_path'])
        require(digest(manifest) == c['diagnostics_manifest_sha256'], 'diagnostic manifest')
        base = c['diagnostics_manifest_path'].rsplit('/',1)[0]
        for f in json.loads(manifest)['files']:
            b=z.read(base+'/'+f['path']);require(len(b)==f['bytes'] and digest(b)==f['sha256'], 'diagnostic file parity')
    with zipfile.ZipFile(directory/'13981795545.zip') as z:
        c=r['failed_fixture_custody']; raw=z.read(c['database_manifest_path']);require(digest(raw)==c['database_manifest_sha256'], 'DB manifest')
        base=c['database_manifest_path'].rsplit('/',1)[0]
        for f in json.loads(raw)['entries']:
            if f['type']=='regular_file':
                b=z.read(base+'/database-snapshot/'+f['path']);require(len(b)==f['copied_bytes']==f['source']['logical_bytes'] and digest(b)==f['copied_sha256'], 'DB file parity')

def main():
    p=argparse.ArgumentParser();p.add_argument('--private-dir',type=Path);p.add_argument('--tamper-tests',action='store_true');a=p.parse_args()
    for line in (HERE/'SHA256SUMS').read_text().splitlines():
        expected,name=line.split('  ',1);require(digest((HERE/name).read_bytes())==expected, 'portable file hash')
    r=json.loads((HERE/'receipt.json').read_text());e=json.loads((HERE/'events.json').read_text());validate(r,e)
    if a.private_dir: private_parity(r,e,a.private_dir)
    if a.tamper_tests:
        changes=[lambda r,e:r['actual_counts']['cli'].update(failed=0),lambda r,e:next(x for x in e['strict_stdout_ledger'] if x['stage']=='strict_registry_addition_terminal').update(healthy_triple_receipt_ns=10000000001),lambda r,e:e['records'].pop(),lambda r,e:next(x for x in e['strict_stdout_ledger'] if x['stage']=='readonly_retained_writer_binding' and x['phase']!='before_fifth')['binding'].update(persisted_policy_registration_generation='changed')]
        for change in changes:
            rr,ee=copy.deepcopy(r),copy.deepcopy(e);change(rr,ee)
            try:validate(rr,ee)
            except (ValueError,KeyError,StopIteration):pass
            else:raise ValueError('semantic tamper accepted')
        print('4 semantic tamper cases PASS')
    print('first-aac receipt PASS'+('; private ZIP/file parity PASS' if a.private_dir else '; portable checks only'))

if __name__=='__main__':main()
