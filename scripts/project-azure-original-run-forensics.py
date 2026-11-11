#!/usr/bin/env python3
"""Read-only projections from original retained Azure evidence; no BicDB open."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import tarfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--run-dir', type=Path, required=True)
parser.add_argument('--output-dir', type=Path, required=True)
args = parser.parse_args()
base, output = args.run_dir, args.output_dir
output.mkdir(parents=True, exist_ok=True)
inputs = {}

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def read(relative):
    raw = (base / relative).read_bytes()
    inputs[relative] = sha(raw)
    return json.loads(raw)

def member(archive, name):
    info = archive.getmember(name)
    assert info.isfile() and info.size < 64 * 1024 * 1024
    return archive.extractfile(info).read()

def save(name, value):
    (output / name).write_text(json.dumps(value, indent=2) + '\n')

def token(collection, record_id):
    c, k = collection.encode(), record_id.encode()
    return int.from_bytes(hashlib.sha256(b'bicdb-distribution-key-v1\0'
        + len(c).to_bytes(8, 'big') + c + len(k).to_bytes(8, 'big') + k).digest()[:8], 'big')

failed = read('failed-growth-workload.json')
points = read('post-failure-diagnosis/original-unverified-point-reads.private.json')
observations = read('post-failure-diagnosis/convergence-observations.json')
diagnosis = read('post-failure-diagnosis/summary.json')
checks = read('post-failure-diagnosis/all-ack-ledger-checks.private.json')
receipt = read('diagnostic-databases.private/receipt.json')
focus = [{'label': label, 'key': key, 'record_id': failed['namespace'] + '/' + key,
          'record_id_sha256': sha((failed['namespace'] + '/' + key).encode())}
         for label, key in zip(['A', 'B'], failed['unverified_writes'])]
assert len(focus) == 2
snapshots = [('baseline_four_checkpoint', read('growth/topology-4-1794375806545507994.json')),
             ('mid_growth_local_observation', read('mid-growth-status.private.json')['status']),
             ('diagnostic_300s_last_local_observation', read('post-failure-diagnosis/latest-topology.private.json'))]
result = []
for item in focus:
    rows = []
    for label, snapshot in snapshots:
        topology = snapshot['topology']
        key_token = token('__bic_cluster_events', item['record_id'])
        ranges = [r for r in topology['ranges'].values()
                  if r['start_token'] <= key_token and (r['end_token'] is None or key_token < r['end_token'])]
        assert len(ranges) == 1
        descriptor = ranges[0]
        rows.append({'snapshot': label, 'topology_generation': topology['generation'],
            'metrics_generated_at_ms': snapshot.get('metrics', {}).get('generated_at_ms'),
            'range_id': descriptor['id'], 'range_epoch': descriptor['epoch'], 'leader': descriptor['leader'],
            'replicas': descriptor['replicas']})
    matches = [point for point in points if point['key'] == item['key']]
    assert len(matches) == 5 and all(point['matches_expected'] for point in matches)
    result.append({'label': item['label'], 'record_id_sha256': item['record_id_sha256'],
        'range_id': rows[0]['range_id'], 'snapshots': rows, 'original_archived_nodes': [],
        'original_per_key_read_result': 'NOT RETAINED: error, absent value and mismatch were collapsed',
        'original_request_destinations_topology_generations_retries_timestamps': 'NOT RETAINED',
        'later_routed_reads': [{'client_node': p['node'], 'started_unix_ns': p['started_unix_ns'],
            'completed_unix_ns': p['completed_unix_ns'], 'exact_expected_value': True} for p in matches],
        'first_retained_success_completed_unix_ns': min(p['completed_unix_ns'] for p in matches),
        'visibility_restoration_cause': 'UNKNOWN: first retained success was after restart and 300-second observation; intermediate events not correlated'})

logs = []
log_inputs = []
for entry in sorted(receipt, key=lambda row: row['node']):
    node = entry['node']
    archive_path = base / 'diagnostic-databases.private' / f'node-{node}.tar.gz'
    assert sha(archive_path.read_bytes()) == entry['sha256'] == entry['remote_sha256']
    with tarfile.open(archive_path) as archive:
        catalog_raw = member(archive, 'db/cluster-range-write-log.json')
        topology_raw = member(archive, 'db/cluster-topology.json')
        log_raw = member(archive, 'server.log')
    envelope, persisted = json.loads(catalog_raw), json.loads(topology_raw)
    catalog, topology = envelope['catalog'], persisted['topology']
    assert sha(json.dumps(catalog, ensure_ascii=False, separators=(',', ':')).encode()) == envelope['checksum_sha256']
    assert sha(json.dumps(topology, ensure_ascii=False, separators=(',', ':')).encode()) == persisted['checksum_sha256']
    by_range = {r['id']: r for r in topology['ranges'].values()}
    log_text = log_raw.decode(errors='replace')
    log_inputs.append({'node': node, 'original_archive_sha256': entry['sha256'],
        'catalog_sha256': sha(catalog_raw), 'persisted_topology_sha256': sha(topology_raw),
        'server_log_sha256': sha(log_raw), 'server_log_bytes': len(log_raw),
        'original_key_string_occurrences': sum(log_text.count(item['key']) for item in focus)})
    for item, row in zip(focus, result):
        range_id = row['range_id']
        log = catalog['ranges'].get(str(range_id))
        commands = []
        if log:
            for entry_log in log['entries'].values():
                command = entry_log['command']
                for mutation in command['mutations']:
                    if mutation['collection'] != '__bic_cluster_events' or mutation['record_id'] != item['record_id']:
                        continue
                    record = mutation['record']
                    expected = {'run': failed['run'], 'payload': 'x' * failed['payload_bytes']}
                    assert record is not None and record['metadata'] == expected
                    commands.append({'command_id_sha256': sha(command['command_id'].encode()),
                        'command_checksum_sha256': command['checksum_sha256'], 'range_epoch': command['range_epoch'],
                        'index': command['index'], 'declared_leader': command['leader_node_id'],
                        'state': entry_log['state'], 'exact_expected_payload': True,
                        'resolved_watermark_covers_command': command['index'] <= log['resolved_through']})
        descriptor = by_range[range_id]
        row['original_archived_nodes'].append({'node': node, 'topology_generation': topology['generation'],
            'persisted_range_epoch': descriptor['epoch'], 'persisted_range_leader': descriptor['leader'],
            'persisted_replicas': descriptor['replicas'], 'catalog_range': None if log is None else {
                name: log[name] for name in ['current_epoch', 'last_index', 'resolved_through', 'compacted_through']},
            'matching_retained_commands': commands,
            'scope': 'persisted local state after owned sequential shutdown; not original read-time topology'})
    for number, line in enumerate(log_text.splitlines(), 1):
        for fragment in line.split(' | '):
            match = re.search(r'\br(11|41): (.+)', fragment)
            if not match:
                continue
            message = match[2]
            message = re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
                lambda found: 'command-sha256:' + sha(found[0].encode()), message)
            assert not re.search(r'(?:\d{1,3}\.){3}\d{1,3}', message)
            logs.append({'node': node, 'line_number': number, 'range_id': int(match[1]),
                'message': message, 'wall_clock_timestamp': 'NOT RETAINED on this fragment',
                'causal_link_to_original_failed_Get': 'NOT ESTABLISHED'})

save('two-ack-range-forensics.json', {'runtime_source': 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb',
    'focus': result, 'original_cold_log_inputs': log_inputs, 'untimestamped_range_log_fragments': logs})

first = observations[0]['started_unix_ns']
fields = ['topology_generation', 'nodes_live', 'nodes_suspect', 'nodes_dead', 'replicas_total',
          'active_relocations', 'failed_relocations', 'under_replicated_ranges', 'unavailable_ranges']
series = [{'since_first_sample_seconds': (r['started_unix_ns'] - first) / 1e9,
    'started_unix_ns': r['started_unix_ns'], 'completed_unix_ns': r['completed_unix_ns'],
    'metrics': {name: r['metrics'][name] for name in fields},
    'metadata_consensus': {name: r['metadata_consensus'][name]
        for name in ['current_term', 'leader_id', 'commit_index', 'topology_generation']}}
    for r in observations]
summary = {name: {'first': series[0]['metrics'][name], 'last': series[-1]['metrics'][name],
    'min': min(r['metrics'][name] for r in series), 'max': max(r['metrics'][name] for r in series),
    'distinct_values': len({r['metrics'][name] for r in series})} for name in fields}
final_gen = series[-1]['metrics']['topology_generation']
last_gen_first = next(r for r in series if r['metrics']['topology_generation'] == final_gen)
latest = snapshots[-1][1]['topology']
save('diagnostic-progress.json', {'samples': len(series), 'series': series, 'metric_summary': summary,
    'sampled_span_seconds': (series[-1]['completed_unix_ns'] - first) / 1e9,
    'configured_policy_bound_seconds': 300, 'gate_passed': diagnosis['recovery_converged'],
    'observed_metadata_terms': sorted({r['metadata_consensus']['current_term'] for r in series}),
    'observed_metadata_leaders': sorted({r['metadata_consensus']['leader_id'] for r in series}),
    'commit_index_first': series[0]['metadata_consensus']['commit_index'],
    'commit_index_last': series[-1]['metadata_consensus']['commit_index'],
    'final_generation_first_observed_seconds': last_gen_first['since_first_sample_seconds'],
    'final_generation_observation_plateau_seconds': (series[-1]['completed_unix_ns'] - last_gen_first['started_unix_ns']) / 1e9,
    'boundary_relocation_phase_counts': dict(collections.Counter(r['phase'] for r in latest['relocations'].values())),
    'boundary_failed_relocations': [{name: move[name] for name in ['id', 'range_id', 'source', 'target',
        'expected_epoch', 'learner_epoch', 'phase', 'resume_phase', 'created_at_ms', 'updated_at_ms', 'last_error']}
        for move in latest['relocations'].values() if move['phase'] == 'failed'],
    'per_relocation_poll_history': 'NOT RETAINED; metric aggregates do not establish a specific move oscillated',
    'actual_os_node_liveness_continuously_monitored': False})

error_counts = collections.Counter(failed['error_samples'])
save('failure-sample-classification.json', {'failed_logical_operation_count': failed['failed_operations'],
    'is_http_status_404': False, 'retained_error_sample_count': len(failed['error_samples']),
    'unretained_failed_operation_bodies': failed['failed_operations'] - len(failed['error_samples']),
    'sample_error_counts': dict(error_counts), 'sample_category_counts': {
        'connection_reset': sum(count for error, count in error_counts.items() if 'Connection reset' in error),
        'resource_temporarily_unavailable': sum(count for error, count in error_counts.items() if 'Resource temporarily unavailable' in error),
        'range_leader_recovery_incomplete': sum(count for error, count in error_counts.items() if 'leader recovery is not complete' in error),
        'no_event_member_reachable': sum(count for error, count in error_counts.items() if 'no event cluster member is reachable' in error)},
    'sample_operation_ids_types_timestamps_retry_paths_topology_generations': 'NOT RETAINED',
    'all_404_failed_operations_individually_classified': False,
    'failed_call_latencies_and_individual_successful_call_latencies': 'NOT RETAINED',
    'later_native_ledger_reads': [{'phase': r['phase'], 'checked': json.loads(r['response']['stdout'])['checked'],
        'unverified': json.loads(r['response']['stdout'])['unverified'], 'command_exit_code': r['response']['code'],
        'seconds': (r['completed_unix_ns'] - r['started_unix_ns']) / 1e9} for r in checks]})
save('input-provenance.json', {'original_run_inputs_sha256': inputs,
    'projector_sha256': sha(Path(__file__).read_bytes()),
    'method': 'Read-only original controller files/tar members; no extraction, engine open, replay, VM operations, or new workload.',
    'raw_originals_preserved': True})
print(json.dumps({'focus_ranges': [r['range_id'] for r in result], 'diagnostic_samples': len(series),
    'failed_logical_operations': failed['failed_operations'], 'retained_error_samples': len(failed['error_samples'])}))
