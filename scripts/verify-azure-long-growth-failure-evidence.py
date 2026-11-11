#!/usr/bin/env python3
"""Check retained Azure growth-failure evidence; this is not a cluster test."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path


def verify(directory):
    directory = Path(directory)
    def read(name):
        return json.loads((directory / name).read_text())
    def require(condition, message):
        if not condition:
            raise ValueError(message)
    manifest = read('manifest.json')
    require(manifest['format'] == 1, 'unsupported evidence format')
    actual_files = {str(path.relative_to(directory)) for path in directory.rglob('*')
                    if path.is_file() and path != directory / 'manifest.json'}
    require(set(manifest['files']) == actual_files, 'manifest must cover every retained artifact exactly')
    for name, expected in manifest['files'].items():
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'manifest path escapes evidence directory')
        path = directory / name
        require(path.is_file() and not path.is_symlink(), f'missing regular artifact: {name}')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == expected, f'artifact digest mismatch: {name}')
    report = read('native-report.json')
    failed = read('failed-growth-workload.json')
    require(not report['complete'] and report['qualification_status'] == 'not_qualified', 'failed run promoted to completion/qualification')
    require(report['source_commit'] == 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb', 'runtime source changed')
    require(report['binary_sha256'] == '15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028', 'runtime artifact changed')
    require(report['client_placement'] == 'first_data_node' and not report['physical_failure_domains_verified'], 'unsupported client/host isolation claim')
    require(len(report['machines']) == 5 and len({v['machine_hash'] for v in report['machines']}) == 5, 'five distinct guest identities not established')
    require(report['requested_storage_mode'] == 'server_paged' and report['requested_buffer_pool_bytes'] == 268435456 and report['node_capacity_bytes'] == 100000000000, 'explicit storage configuration changed')
    require(report['fsync'] and report['replication_factor'] == 3 and report['ranges'] == 64, 'durability/topology configuration changed')
    require([v['phase'] for v in report['results']] == ['steady-4'], 'unexpected completed native phases')
    baseline = report['results'][0]
    require((baseline['successful_operations'], baseline['failed_operations'], baseline['acknowledged_writes']) == (7592, 0, 3804), 'baseline counters changed')
    require((failed['successful_operations'], failed['failed_operations'], failed['acknowledged_writes']) == (2746, 404, 1223), 'growth counters changed')
    require(not failed['verification_passed'] and len(failed['unverified_writes']) == 2, 'original ACK verification failure missing')
    require(len(failed['error_samples']) == 20, 'bounded error samples changed')
    require(failed == next(json.loads(line) for line in report['error'].splitlines() if line.startswith('{')), 'failed result differs from original command output')
    require(not any(v['kind'].startswith('process_kill') for v in report['timeline']), 'later fault phase unexpectedly executed')
    for phase, result in [('steady-4', baseline), ('growth-4-to-5', failed)]:
        ledger = read(phase + '-ledger.json')
        require(ledger['namespace'] == failed['namespace'] and ledger['run'] == result['run'], f'{phase}: ledger identity mismatch')
        require(len(ledger['keys']) == len(set(ledger['keys'])) == result['acknowledged_writes'], f'{phase}: ACK ledger count/uniqueness mismatch')
        if phase == 'growth-4-to-5':
            require(set(failed['unverified_writes']) <= set(ledger['keys']), 'unverified IDs lack acknowledged-ledger evidence')
        samples = json.loads(gzip.decompress((directory / (phase + '-resources.json.gz')).read_bytes()))
        require(samples and all(v['nodes'] for v in samples), f'{phase}: resource observations absent')
    receipt = read('pre-diagnosis-cold-export-receipt.json')
    require(len(receipt) == 5 and {v['node'] for v in receipt} == set(range(5)), 'missing pre-restart cold archive')
    require(all(v['status'] == 'exported' and v['sha256'] == v['remote_sha256'] and v['owned_engine_stopped_before_archive'] for v in receipt), 'cold archive stop/digest verification failed')
    diagnosis = read('diagnosis-summary.json')
    require(diagnosis['original_failure_preserved'] and not diagnosis['new_writes'], 'diagnosis changed workload contract')
    require('application/event' in diagnosis['new_writes_field_scope'], 'diagnostic write scope missing')
    points = read('diagnosis-point-reads.json')
    require({(v['node'], v['key']) for v in points} == {(node, key) for node in range(5) for key in failed['unverified_writes']}, 'two-key diagnosis does not cover five routed member contexts')
    expected_value = {'run': failed['run'], 'payload': 'x' * failed['payload_bytes']}
    require(len(points) == 10 and all(v['response']['code'] == 0 and json.loads(v['response']['stdout'])['value'] == expected_value and v['matches_expected'] for v in points), 'originally unverified ACK values do not match')
    checks = read('diagnosis-ledger-checks.json')
    require([v['phase'] for v in checks] == ['steady-4', 'growth-4-to-5'], 'full-ledger diagnosis incomplete')
    actual_passed = True
    for check, count in zip(checks, (3804, 1223)):
        response = check.get('response', {})
        try:
            result = json.loads(response.get('stdout', ''))
        except json.JSONDecodeError:
            result = None
        if result and 'checked' in result:
            require(result['checked'] == count and 0 <= result['unverified'] <= count, 'diagnostic ledger counts invalid')
            require(len(result['unverified_key_samples']) <= min(20, result['unverified']), 'diagnostic error-sample limit invalid')
            ledger_keys = set(read(check['phase'] + '-ledger.json')['keys'])
            require(set(result['unverified_key_samples']) <= ledger_keys, 'diagnostic sample lacks acknowledged-ledger evidence')
        actual_passed &= bool(response.get('code') == 0 and result and result.get('checked') == count and result.get('unverified') == 0 and result.get('verification_passed'))
    baseline_check = json.loads(checks[0]['response']['stdout'])
    require(checks[0]['response']['code'] == 1 and baseline_check['unverified'] == 111 and len(baseline_check['unverified_key_samples']) == 20, 'baseline diagnostic failure missing')
    growth_check = json.loads(checks[1]['response']['stdout'])
    require(checks[1]['response']['code'] == 1 and growth_check['unverified'] == 18 and len(growth_check['unverified_key_samples']) == 18, 'growth diagnostic failure missing')
    require(not set(growth_check['unverified_key_samples']) & set(failed['unverified_writes']), 'later growth IDs unexpectedly overlap original two')
    classification = read('raw-classification-validity.json')
    raw_calls = read('raw-classification-results.json')
    require(classification['missed_final_prelaunch_completion_check'] and not classification['scope_valid_for_engine_availability'] and not classification['scope_valid_for_original_failure_classification'], 'cleanup-confounded classification promoted to valid evidence')
    require(len(raw_calls) == 20 and classification['all_calls_cleanup_confounded'], 'cleanup classification scope changed')
    require(all(call['started_unix_ns'] >= checks[1]['completed_unix_ns'] and call['completed_unix_ns'] <= diagnosis['completed_unix_ns'] for call in raw_calls), 'cleanup-confounded timestamp bounds mismatch')
    require(read('final-persisted-topology-summaries.json')['cleanup_confounded'], 'sequential-shutdown topology misrepresented as live status')
    claims = read('claims.json')
    require(claims['all_known_acks_verified_after_diagnostic_restart'] == actual_passed, 'diagnostic ACK claim differs from results')
    require(not claims['production_qualified'] and not claims['capacity_gain_demonstrated'] and not claims['completed_before_after_comparison'], 'unsupported qualification or capacity claim')
    require(claims['diagnostic_known_acks_matched'] == 4898 and claims['diagnostic_combined_predicate_unverified'] == 129 and not claims['cleanup_confounded_classification_used_for_availability_claims'], 'diagnostic counts or scope changed')
    require(claims['native_failure_records'] == {'failed_operation_count': 404, 'retained_error_samples': 20, 'all_failed_attempts_logged': False}, 'failure-record limit missing')
    for launch_name, field, source_name in [('native-launch.json', 'harness_sha256', 'sources/scale-cluster.executed.py'), ('observer-launch.json', 'observer_sha256', 'sources/observer.executed.py'), ('diagnosis-launch.json', 'helper_sha256', 'sources/diagnosis.executed.py'), ('export-launch.json', 'helper_sha256', 'sources/export.executed.py'), ('post-recovery-export-launch.json', 'helper_sha256', 'sources/export-post-recovery.executed.py'), ('raw-classification-launch.json', 'helper_sha256', 'sources/raw-classification.executed.py')]:
        require(read(launch_name)[field] == hashlib.sha256((directory / source_name).read_bytes()).hexdigest(), f'executed helper provenance mismatch: {source_name}')
    builder_digest = hashlib.sha256((directory / 'sources/build-bundle.executed.py').read_bytes()).hexdigest()
    require(read('build-bundle-launch.json')['helper_sha256'] == read('derivation.json')['builder_sha256'] == builder_digest, 'derived bundle builder provenance mismatch')
    return {'verified': True, 'runtime_version': '1.0.461-beta', 'native_trial_passed': False, 'known_acknowledgments': 5027, 'all_known_acks_verified_after_diagnostic_restart': actual_passed, 'production_qualified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'docs/evidence/azure-long-growth-failure-2026-11-11')
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.evidence_dir), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        parser.exit(1, f'evidence verification failed: {error}\n')


if __name__ == '__main__':
    main()
