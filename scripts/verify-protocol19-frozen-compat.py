#!/usr/bin/env python3
"""Verify retained wire evidence only. Starts no server and changes no database."""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import uuid

PIN = '840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d'

def require(ok, message):
    if not ok:
        raise ValueError(message)

def frame(path):
    raw = path.read_bytes()
    require(len(raw) >= 4 and struct.unpack('>I', raw[:4])[0] == len(raw) - 4,
            'invalid frame length: ' + str(path))
    return json.loads(raw[4:])

def verify(directory):
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*')
              if p.is_file() and p.name != 'manifest.json'}
    require(set(manifest) == actual, 'export manifest coverage differs')
    for name, digest in manifest.items():
        path = Path(name)
        require(not path.is_absolute() and '..' not in path.parts, 'unsafe artifact path')
        target = directory / path
        require(target.is_file() and not target.is_symlink(), 'nonregular artifact')
        require(hashlib.sha256(target.read_bytes()).hexdigest() == digest,
                'artifact hash mismatch: ' + name)
    summary = json.loads((directory / 'summary.json').read_text())
    require(summary['old_binary_sha256'] == PIN and summary['old_runtime_version'] == '1.0.463-beta',
            'wrong old artifact')
    for key in ['tls_authentication_exercised', 'new_protocol20_server_exercised',
                'new_native_genesis_exercised', 'handoff_session_drop_executed',
                'application_write_health_established', 'multinode_consensus_qualification',
                'physical_failure_or_production_qualification', 'new_engine_build_performed',
                'shared_cargo_slot_used']:
        require(summary[key] is False, 'unsupported capability claim: ' + key)
    field_drop_witnesses = 0
    stable_state_cuts = 0
    changed_state_cuts = 0
    for name in summary['cases']:
        case = directory / name
        report = json.loads((case / 'report.json').read_text())
        require(report['binary_sha256'] == PIN and report['outcome'] == 'PASS', 'case artifact/outcome differs')
        require(report['runner_sha256'] == hashlib.sha256((case / 'executed-runner.py').read_bytes()).hexdigest(),
                'executed source bytes differ')
        calls = json.loads((case / 'calls.json').read_text())
        require([c['label'] for c in calls] == ['01-v19-ping', '02-v20-append', '03-v19-same-append', '04-v19-ping-after'],
                'missing paired protocol calls')
        for row in calls:
            request = frame(case / (row['label'] + '.request.bin'))
            response = frame(case / (row['label'] + '.response.bin'))
            require(request == row['request'] and response == row['response'], 'wire/JSON projection differs')
            require(row['protocol'] == request['protocol_version'], 'protocol projection differs')
            for suffix, key in [('request', 'request_sha256'), ('response', 'response_sha256')]:
                require(hashlib.sha256((case / (row['label'] + '.' + suffix + '.bin')).read_bytes()).hexdigest() == row[key], 'wire digest projection differs')
            require(response['request_id'] == request['request_id'], 'response correlation differs')
            require(response['source_node_id'] == request['destination_node_id'] == 'n1', 'wrong node')
            require(response['cluster_id'] == request['cluster_id'], 'wrong cluster')
        for row in [calls[0], calls[3]]:
            require(row['protocol'] == row['request']['protocol_version'] == 19 and row['response']['response'] == {'Ok': 'Pong'}, 'Ping control failed')
        old, refused = calls[2], calls[1]
        require(refused['request']['protocol_version'] == 20 and old['request']['protocol_version'] == 19,
                'protocol pair differs')
        require(refused['request']['request'] == old['request']['request'], 'Append bodies differ')
        error = refused['response']['response']
        require('Err' in error and 'unsupported cluster data protocol 20; expected 19' in error['Err']['message'],
                'no actual old-version header refusal')
        incoming = old['request']['request']['MetadataAppend']['request']['snapshot']['topology']
        domain = incoming['range_image_bootstrap_id']
        require(uuid.UUID(domain).int != 0, 'invalid synthetic test domain')
        before = (case / 'before-v20.metadata.raw.json').read_bytes()
        after = (case / 'after-v20.metadata.raw.json').read_bytes()
        stable = before == after
        require(report['metadata_bytes_identical_across_v20'] is stable,
                'state-byte stability report differs')
        require(report['concurrent_metadata_change_observed'] is (not stable),
                'state-change observation differs')
        stable_state_cuts += int(stable)
        changed_state_cuts += int(not stable)
        baseline = json.loads(before)['state']
        request_body = old['request']['request']['MetadataAppend']['request']
        require(baseline['role'] == 'leader' and baseline['commit_index'] > 0, 'no actual metadata leader runtime')
        require(request_body['term'] == baseline['current_term'] and request_body['prev_log_index'] == baseline['base_index'] and request_body['prev_log_term'] == baseline['base_term'], 'Append not derived from actual committed state')
        require(report['before_v20_sha256'] == hashlib.sha256(before).hexdigest()
                and report['after_v20_sha256'] == hashlib.sha256(after).hexdigest(), 'state digest projection differs')
        acknowledged = old['response']['response'].get('Ok', {}).get('MetadataAppend', {}).get('success') is True
        expected_case = summary['case_expectations'][name]
        require(expected_case['metadata_bytes_stable'] is stable, 'case stability expectation differs')
        if acknowledged:
            require(expected_case['domain_drop'] == report['v19_append_ack_and_domain_drop'] == 'PASS',
                    'ACK case classification differs')
            retained = json.loads((case / 'after-v19.metadata.raw.json').read_bytes())['state']['committed_topology']
            require('range_image_bootstrap_id' not in retained, 'domain retained; omission not established')
            expected = dict(incoming)
            expected.pop('range_image_bootstrap_id')
            require(retained == expected == baseline['committed_topology'], 'unexpected committed topology changes across synthetic Append')
            require(report['v19_domain_retained'] is False, 'report disagrees with retained state')
            field_drop_witnesses += 1
        else:
            require(expected_case['domain_drop'] == report['v19_append_ack_and_domain_drop'] == 'NOT_TESTED_PRECONDITION_OR_CONCURRENT_CHANGE',
                    'non-ACK cannot establish domain omission')
            require(report['v19_non_ack_response'] == old['response'], 'non-ACK report differs')
        teardown = report['teardown']
        require(calls[-1]['ended_unix_ns'] <= teardown['stop_requested_unix_ns'] <= teardown['reaped_unix_ns'], 'teardown timing differs')
        require(report['pid'] == teardown['pid'] and report['pgid'] == teardown['pgid'], 'wrong owned process identity')
        require(teardown['exit_code'] == 0 and teardown['proc_pid_absent'] is True
                and teardown['rpc_connection_result'] == teardown['sql_connection_result'] == 111,
                'owned process/ports teardown not recorded')
    require(field_drop_witnesses >= 1 and stable_state_cuts >= 1, 'missing positive field-drop witness')
    return {'retained_cases_verified': len(summary['cases']),
            'actual_field_drop_witnesses': field_drop_witnesses,
            'stable_state_cuts': stable_state_cuts, 'changed_state_cuts': changed_state_cuts,
            'old19_ack_drops_native_domain': True, 'old19_refuses_header20': True,
            'tls_or_physical_qualification': False, 'new20_server_tested': False}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.directory), sort_keys=True))
    except (ValueError, KeyError, OSError, TypeError) as error:
        parser.exit(1, 'compatibility evidence verification failed: ' + str(error) + '\n')
