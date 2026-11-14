#!/usr/bin/env python3
"""Offline logical verification; optional raw custody verification never opens an engine."""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import re
import zipfile

HERE = pathlib.Path(__file__).resolve().parent


def sha(data):
    return hashlib.sha256(data).hexdigest()


def verify_run(record, raw=None):
    folder = HERE / record['key']
    projection = json.loads((folder / 'projection-manifest.json').read_text())
    captured = json.loads((folder / 'capture-reconciliation.json').read_text())
    github = json.loads((folder / 'github-run.json').read_text())
    assert github['databaseId'] == record['run'] and github['headSha'] == record['source']
    assert github['status'] == 'completed' and github['conclusion'] == 'failure'
    assert next(j for j in github['jobs'] if j['databaseId'] == record['core_job'])['conclusion'] == 'failure'
    assert next(j for j in github['jobs'] if j['name'] == 'Community-only workspace compiles, tests included')['conclusion'] == 'success'
    artifacts = json.loads((folder / 'github-artifacts.json').read_text())['artifacts']
    assert len(artifacts) == 1 and artifacts[0]['id'] == record['artifact'] and artifacts[0]['size_in_bytes'] == record['raw_bytes']
    assert captured['source'] == projection['source_commit'] == record['source']
    assert captured['raw_sha256'] == projection['original_zip_sha256'] == record['raw_sha256']
    assert not record['production_qualified'] and not captured['qualification']
    assert projection['original_archive_unchanged'] and not projection['projection_fixture_db_custody_present']
    spec = importlib.util.spec_from_file_location('project_' + record['key'], folder / 'project-logical.py')
    projector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(projector)
    assert sha((folder / 'project-logical.py').read_bytes()) == projection['projector_sha256']
    archive_path = folder / 'logical-evidence.zip'
    assert sha(archive_path.read_bytes()) == projection['projection_zip_sha256'] == record['projection_sha256']
    assert archive_path.stat().st_size == record['projection_bytes']
    expected = {m['path']: m for m in projection['included_members']}
    assert len(expected) == record['projection_members'] and len(projection['omitted_members']) == record['omitted_members']
    result = {'run': record['run'], 'source': record['source'], 'qualification': False}
    with zipfile.ZipFile(archive_path) as z:
        names = z.namelist()
        assert len(names) == len(set(names)) and set(names) == set(expected)
        for name in names:
            assert projector.allowed(name) and '/custody/' not in name and not name.endswith('/custody-manifest.json')
            data = z.read(name)
            assert len(data) == expected[name]['bytes'] and sha(data) == expected[name]['sha256']
            assert not projector.PRIVATE_PEM.search(data)
        assert z.read('source-revision.txt').decode().strip() == record['source']
        selected = [v[:-6] for v in z.read('selected-tests.log').decode().splitlines() if v.endswith(': test')]
        log = z.read('tests.log').decode()
        executed = dict(re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$', log, re.M))
        assert len(selected) == len(set(selected)) == len(executed) == record['selected'] == 10
        assert set(selected) == set(executed)
        assert sum(v == 'ok' for v in executed.values()) == record['passed']
        assert sum(v == 'FAILED' for v in executed.values()) == record['failed']
        assert f"test result: FAILED. {record['passed']} passed; {record['failed']} failed;" in log
        def get(root, name):
            return json.loads(z.read(root + name))
        summary_cases = {c['case']: c for c in captured['cases']}
        app_count = 0
        cases = set()
        committed_timeouts = 0
        for name in names:
            if not name.endswith('/execution-identity.json'):
                continue
            root = name.rsplit('/', 1)[0] + '/'
            identity = get(root, 'execution-identity.json')
            case = identity['source_case']
            assert case not in cases
            cases.add(case)
            assert identity['source_commit'] == record['source'] and identity['package_version'] == record['source_version']
            outcome = executed[identity['actual_rust_test_thread']]
            assert summary_cases[case]['test_classification'] == ('passed' if outcome == 'ok' else 'failed')
            cleanup = get(root, 'cleanup.json')
            assert cleanup['errors'] == [] and get(root, 'evidence-errors.json')['errors'] == []
            assert (cleanup['primary_failure_preserved'] is None) == (outcome == 'ok')
            app = get(root, 'ack-ledger.json')
            assert len(app) == 2
            for ack in app:
                assert ack['operation'] == 'ClusterEventClient.append' and ack['outcome'] == 'returned' and ack['actual_api_error'] is None
                assert sha(json.dumps(ack['value'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()) == ack['payload_json_sha256']
            app_count += len(app)
            ledger = get(root, 'source-frame-ledger.json')
            frames = [v for v in ledger if v.get('operation') == 'source_image_frame']
            assert len(frames) == summary_cases[case]['actual_source_frames']
            assert all(v['outcome'] == 'returned' and v['exact_error'] is None for v in frames)
            mf = next(v['actual_reply']['payload']['Manifest'] for v in frames if 'Manifest' in v['actual_reply']['payload'])
            anchor = [mf['range_index'], mf['h0'], mf['h']]
            closed = [v['actual_reply'] for v in ledger if v.get('operation') == 'remote_old_close']
            assert len(closed) == 3 and all(v['constraints']['native_point'] == anchor for v in closed)
            collection = next(v for v in get(root, 'remote-old-ledger.json') if v['operation'] == 'remote_old_fresh_all_closed_collection')
            assert collection['native_before'] == collection['native_after'] and collection['original_mutation_requests_expired']
            if case in ('TargetNativeBaseAbortSyncFailed', 'TargetNativeBaseCommitSyncUnknown'):
                report = get(root, 'registered-native-base-abort-recovery-outcome.json')
                before, first, after = [report[k] for k in ('before', 'after_first_failure', 'after_fresh_request')]
                a, b = [snap['image']['value']['native_base'] for snap in (first, after)]
                assert a == b and b['native_attempts_issued'] == 1 and b['retired'] == []
                assert report['actual_io_fault']['fired'] == 1 and report['actual_io_fault']['fault_disarmed_before_fresh_request']
                assert report['first_health_status']['wal_failure'] == report['after_health_status']['wal_failure']
                assert 'WriteFailed' in report['fresh_error_debug']
                for snap in (first, after):
                    assert snap['owner']['diagnostic_error'] == report['first_health_status']['core_recovery_error']
                if record['key'] == '817':
                    for snap in (before, first, after):
                        assert snap['memory_custody']['diagnostic_error'] is None
                        assert snap['memory_custody']['value'] == before['memory_custody']['value']
                    custody = before['memory_custody']['value']
                    assert custody['lease_present'] and custody['lease_has_exact_original_preflight']
                    assert custody['actual_owner'] == custody['attempt'] == custody['source_target_attempt']
                    assert custody['memory_only'] and not custody['health_checked'] and not custody['native_storage_read'] and not custody['admission_or_readiness_granted']
                    assert outcome == 'ok'
                else:
                    assert outcome == 'FAILED' and 'diagnostic_error' in cleanup['primary_failure_preserved']
            if 'RoleExpiry' in case:
                report = get(root, 'registered-native-base-real-role-expiry-outcome.json')
                first, after = [report[k] for k in ('after_first_expiry', 'after_fresh_request')]
                a, b = [snap['image']['value']['native_base'] for snap in (first, after)]
                assert report['first_exact_error'] == 'canceling statement due to query timeout' and report['retry_exact_error'] is None
                assert report['parent_budget_ms_unchanged'] == 5000
                assert len(report['actual_cut_events']) == 2 and report['actual_cut_events'][1]['original_role']['expired_by_clock']
                assert all(not e['fake_cancellation_or_deadline_renewal'] and e['original_parent']['original_check_error'] is None for e in report['actual_cut_events'])
                assert after['native']['value']['base'] == after['native']['value']['attempt_archive'] == b['receipt']
                assert b['verified'] and b['actual_commit_returned'] == b['receipt']['native_page_xid']
                if case.endswith('AfterCommit'):
                    assert report['issued_native_xid'] == b['receipt']['native_page_xid'] and b['native_attempts_issued'] == 1 and b['retired'] == []
                else:
                    assert b['receipt']['native_page_xid'] > report['issued_native_xid'] and b['native_attempts_issued'] == 2 and len(b['retired']) == 1
                    retired = b['retired'][0]
                    assert retired['receipt'] == a['receipt'] and retired['canonical_bytes'] == a['attempted_canonical_bytes']
                    assert retired['same_open_terminal_status'] == 'Aborted' and retired['core_page_wal_healthy_at_retirement']
                    assert retired['puts_completed'] == (1 if case.endswith('AfterPut') else 0)
            if case == 'TargetNativeBaseAbortBudget':
                report = get(root, 'registered-native-base-abort-recovery-outcome.json')
                assert len(report['actual_aborted_requests']) == 9
                first, after = [report[k]['image']['value']['native_base'] for k in ('before_exhausted_request', 'after_exhausted_request')]
                assert first == after and after['native_attempts_issued'] == 9 and len(after['retired']) == 8
                assert 'proven-abort retry budget exhausted' in report['exact_budget_error']
            if root + 'registered-native-image-critical-outcome.json' in names:
                critical = get(root, 'registered-native-image-critical-outcome.json')
                assert 1 <= len(critical) <= 16
                for i, call in enumerate(critical):
                    assert call['call_index'] == i and call['parent_budget_ms_unchanged'] == 5000
                    assert call['actual_manifest_sha256'] == mf['checksum_sha256'] and not call['retry_or_deadline_change']
                    assert call['request_order_unchanged'] == ['Role', 'OWN', 'Finished', 'actual_host']
                    assert call['before']['memory_custody'] == call['after']['memory_custody']
                    if outcome == 'FAILED' and call['operation'] == 'PublishNativeBase':
                        assert record['key'] == '817' and call['exact_primary_error'] == 'canceling statement due to query timeout'
                        snap = call['after']
                        base = snap['image']['value']['native_base']
                        native = snap['native']['value']
                        assert native['base'] == native['attempt_archive'] == base['receipt']
                        xid = base['receipt']['native_page_xid']
                        assert xid == base['actual_commit_returned'] == 21 and native['native_tx_status_api_observations'] == {'21': 'Committed'}
                        assert not base['verified'] and base['native_attempts_issued'] == 1 and base['retired'] == []
                        windows = {v['stage']: v['observation'] for v in call['request_windows']}
                        pre, post = [windows[k] for k in ('before_actual_host_invoke', 'after_actual_host_invoke')]
                        assert all(pre[k]['original_check_error'] is None for k in ('role', 'own', 'source_original_token', 'parent'))
                        assert post['role']['expired_by_clock'] and post['role']['original_check_error'] is not None
                        assert all(post[k]['original_check_error'] is None and not post[k]['expired_by_clock'] for k in ('own_original_token', 'source_original_token', 'parent'))
                        committed_timeouts += 1
        assert len(cases) == 10 and app_count == record['app_returned_event_operations'] == 20
        assert committed_timeouts == (2 if record['key'] == '817' else 0)
    if raw is not None:
        assert raw.stat().st_size == record['raw_bytes'] and sha(raw.read_bytes()) == record['raw_sha256']
        files = total = 0
        with zipfile.ZipFile(raw) as original:
            all_members = projection['included_members'] + projection['omitted_members']
            assert set(original.namelist()) == {m['path'] for m in all_members}
            for member in all_members:
                data = original.read(member['path'])
                assert len(data) == member['bytes'] and sha(data) == member['sha256']
            for name in original.namelist():
                if not name.endswith('/custody-manifest.json'):
                    continue
                root = name.rsplit('/', 1)[0] + '/'
                custody = json.loads(original.read(name))
                assert custody['status'] == 'observed_stable_with_live_actual_owners'
                assert custody['all_five_metadata_service_db_native_journal_guards_held'] and custody['engine_owners_alive']
                assert not custody['engine_reopened'] and not custody['listeners_drained'] and not custody['atomic_powerloss_snapshot_claimed']
                count = 0
                for entry in custody['files']:
                    data = original.read(root + 'custody/' + entry['path'])
                    assert len(data) == entry['bytes'] and sha(data) == entry['sha256']
                    files += 1
                    count += len(data)
                assert count == custody['total_bytes']
                total += count
        assert files == record['custody']['custody_files_verified'] and total == record['custody']['custody_bytes_verified']
        result['raw_custody_verified'] = {'files': files, 'bytes': total}
    else:
        result['raw_custody_verified'] = False
    result.update(selected=10, passed=record['passed'], failed=record['failed'], application_returned_event_operations=app_count, actual_committed_role_expiry_observations=committed_timeouts)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-c1e', type=pathlib.Path)
    parser.add_argument('--raw-817', type=pathlib.Path)
    args = parser.parse_args()
    sums = json.loads((HERE / 'SHA256SUMS.json').read_text())
    actual = {p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name != 'SHA256SUMS.json' and '__pycache__' not in p.parts}
    assert actual == set(sums)
    for name, expected in sums.items():
        assert sha((HERE / name).read_bytes()) == expected, name
    records = json.loads((HERE / 'runs.json').read_text())['runs']
    print(json.dumps({'verification': 'passed', 'runs': [verify_run(r, args.raw_c1e if r['key'] == 'c1e' else args.raw_817) for r in records]}, sort_keys=True))


if __name__ == '__main__':
    main()
