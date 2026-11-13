#!/usr/bin/env python3
"""Bounded offline packet/ledger verifier. No engine open, Cargo or network."""
import argparse, collections, copy, gzip, hashlib, json, pathlib, re, subprocess, zipfile
ROOT = pathlib.Path(__file__).resolve().parent
SOURCE = '8334a99e666970263c2d5a4192b5c9ce61d26677'
FIXED = '60c47ae29d84371f6815cdedf1fb0b8ffa866d75'

def audit(rows, terminal, log):
    assert not any((r['kind'] in ('supported_metadata_maintenance', 'supported_registry_maintenance') for r in rows)), 'manual tick before success'
    assert len(rows) == 339 and [r['journal_sequence'] for r in rows] == list(range(1, 340))
    kinds = collections.Counter((r['kind'] for r in rows))
    assert kinds['actual_rpc'] == 310 and kinds['logical_intent'] == kinds['logical_public_reply'] == 9
    assert not any((kinds[k] for k in ('supported_metadata_maintenance', 'supported_registry_maintenance')))
    bykind = lambda kind: next((r for r in rows if r['kind'] == kind))
    identity = bykind('execution_identity')
    assert identity['source'] == SOURCE and identity['portable_native_db_custody'] is False
    intents = {r['context']['diagnostic_business_id']: r for r in rows if r['kind'] == 'logical_intent'}
    bykey = {r['context']['key']: r['context']['payload'] for r in intents.values() if r['context']['operation'] == 'append'}
    api = []
    for r in rows:
        if r['kind'] != 'logical_public_reply':
            continue
        c = intents[r['diagnostic_business_id']]['context']
        out = r['outcome']
        assert out['classification'] != 'STRONG_ABSENT'
        if out['classification'] in ('USER_ACK', 'STRONG_PRESENT'):
            assert out['full_payload'] == bykey[c['key']]
        api.append({'intent_sequence': intents[r['diagnostic_business_id']]['journal_sequence'], 'reply_sequence': r['journal_sequence'], 'operation': c['operation'], 'event_id': c['event_id'], 'diagnostic_business_id': r['diagnostic_business_id'], 'wire_business_id_is_separate': True, 'payload_sha256': c['payload_sha256'], 'configured_rpc_or_attempt_timeout_ms': c['configured_rpc_or_attempt_timeout_ms'], 'intent_unix_ns': intents[r['diagnostic_business_id']]['unix_ns'], 'reply_unix_ns': r['unix_ns'], 'elapsed_ns': r['api_receipt_elapsed_ns'], 'outcome': out})
    assert collections.Counter((a['outcome']['classification'] for a in api)) == {'USER_ACK': 2, 'STRONG_PRESENT': 4, 'NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN': 1, 'NO_SUCCESSFUL_READ_NO_ABSENCE': 2}
    old = bykind('old_ack_native_command_correlation')
    cut = bykind('actual_native_cut')
    exit = bykind('actual_origin_exit')
    cmd = cut['actual_command']
    probe = exit['probe']
    assert probe == terminal['first_actual_origin_error']
    assert all((probe[k] is True for k in ('fired', 'release_callback_ok', 'release_callback_returned', 'normal_app_handler_exit', 'app_span_ended')))
    assert probe['unknown_sequence'] is None and probe['unknown_transaction_id'] == probe['actual_transaction_id'] == 11 and (probe['actual_ticket_command_id'] == cmd['command_id'])
    assert probe['core_result'] == 'commit_outcome_unknown' and (not probe['panicked']) and (not probe['diagnostic_integrity_error'])
    assert cmd['index'] == 2 and cut['raw_state'] == 'quorum_committed' and (cut['native_proof_not_inferred_from_raw_counters'] is True)
    assert cut['origin']['native_applied_index'] == 1 and cut['origin']['pending_command_index'] == 2 and (not cut['origin']['invalidated'])
    peers = cut['peer_native_materialization']
    assert {p['node'] for p in peers} == {'n1', 'n2'}
    for p in peers:
        f = p['verified_actual_native']
        assert f['native_R'] == 2 and f['typed_local_B'] == 2 and (f['H0'] == old['native_before']['H0'])
        assert f['full_record']['metadata'] == bykey[f['full_record']['id'].split('/', 1)[1]]
        assert f['old_ack_full_record']['metadata'] == old['full_payload']
    assert peers[0]['verified_actual_native']['H'] == peers[1]['verified_actual_native']['H']
    requests = {}
    replies = {}
    rpc = []
    strong = []
    snapshots = []
    for r in rows:
        if r['kind'] != 'actual_rpc':
            continue
        a = r['actual']
        aid = a['attempt_id']
        phase = a['phase']
        raw = a['raw_frame']
        if phase not in ('request_frame_before_dispatch', 'response_frame_before_decode'):
            continue
        frame = bytes(raw)
        assert int.from_bytes(frame[:4], 'big') == len(frame) - 4
        f = json.loads(frame[4:])
        assert f['protocol_version'] == 29 and f['request_id'] == a['request_id']
        dest = requests if phase == 'request_frame_before_dispatch' else replies
        assert aid not in dest
        dest[aid] = (r, f)
    assert len(requests) == 77 and len(replies) == 76
    for aid, (r, q) in requests.items():
        a = r['actual']
        response = replies.get(aid)
        reply = response[1] if response else None
        entry = {'attempt_id': aid, 'operation': a['operation'], 'diagnostic_business_id': r['logical_context']['diagnostic_business_id'], 'event_id': r['logical_context']['event_id'], 'request_id': q['request_id'], 'destination': q['destination_node_id'], 'address': a['address'], 'topology_generation': a['topology_generation'], 'request': q['request'], 'request_frame_sha256': hashlib.sha256(bytes(a['raw_frame'])).hexdigest(), 'request_unix_ns': r['unix_ns'], 'has_complete_response': response is not None}
        if response:
            rr = response[0]
            assert reply['request_id'] == q['request_id'] and reply['source_node_id'] == q['destination_node_id']
            committed = bykind('bootstrap_reply')['topology']
            source = reply['source_node_id']
            assert reply['actual_native_source'] == committed['nodes'][source]['native_store_birth_v1']
            assert a['topology_generation'] == 1 and a['address'] == committed['nodes'][source]['address']
            if a['operation'] == 'event_append':
                operation = q['request']['ApplicationEvent']['operation']
                context = r['logical_context']
                assert operation['namespace'] == 'items' and operation['key'] == context['key'] and (operation['value'] == context['payload'])
            entry['source'] = reply['source_node_id']
            entry['actual_native_source'] = reply['actual_native_source']
            entry['reply'] = reply['response']
            entry['response_unix_ns'] = rr['unix_ns']
            entry['response_frame_sha256'] = hashlib.sha256(bytes(rr['actual']['raw_frame'])).hexdigest()
            resp = reply['response']
            ok = resp.get('Ok', {})
            if a['operation'] == 'metadata_snapshot_discovery':
                assert set(ok) == {'BootstrapSnapshot'}
                c = ok['BootstrapSnapshot']['topology']
                assert c['generation'] == 1
                assert c == bykind('bootstrap_reply')['topology']
                source = reply['source_node_id']
                assert reply['actual_native_source'] == c['nodes'][source]['native_store_birth_v1']
                snapshots.append({'attempt_id': aid, 'request_id': q['request_id'], 'source': source, 'generation': c['generation'], 'actual_native_source': reply['actual_native_source']})
            if a['operation'] == 'native_event_get' and 'NativeEventGet' in ok:
                read_request = q['request']['NativeEventGet']
                read_reply = ok['NativeEventGet']
                assert read_reply['business_request_id'] == read_request['business_request_id'] and read_reply['challenge'] == read_request['challenge']
                v = read_reply['result']
                if v['kind'] == 'value':
                    assert v['audit']['business_request_id'] == read_request['business_request_id'] and v['audit']['challenge'] == read_request['challenge']
                    assert v['value']['presence'] == 'present' and v['value']['value'] == bykey[r['logical_context']['key']]
                    assert v['audit']['first_metadata_context_id'] != v['audit']['final_metadata_context_id']
                    strong.append({'event_id': r['logical_context']['event_id'], 'request_id': q['request_id'], 'source': reply['source_node_id'], 'full_value': v['value']['value'], 'audit': v['audit']})
        rpc.append(entry)
    assert len(snapshots) == 51 and len(strong) == 4
    assert [v['audit']['range_index'] for v in strong] == [1, 2, 2, 3]
    assert [v['audit']['target_local_visibility_db_sequence'] for v in strong] == [1, 2, 2, 3]
    assert all((v['audit']['snapshot_sequence'] == v['audit']['target_local_visibility_db_sequence'] and v['source'] == 'n0' for v in strong))
    assert strong[1]['audit']['logical_history_sha256'] == strong[2]['audit']['logical_history_sha256'] == peers[0]['verified_actual_native']['H']
    assert collections.Counter((e['operation'] for e in rpc)) == {'event_append': 6, 'native_event_get': 20, 'metadata_snapshot_discovery': 51}
    phases = [re.search('NATIVE_HOT_DATA_PHASE node=(\\S+) target=(\\S+) phase=(\\S+) command=(\\S+) nonce=(\\S+) remaining_ms=(\\d+)', l).groups() for l in log.splitlines() if 'NATIVE_HOT_DATA_PHASE ' in l]
    assert len(phases) == 18 and collections.Counter((p[2] for p in phases)) == {p: 3 for p in ('Fence', 'Certify', 'Apply', 'Prepare', 'Complete', 'Activate')}
    assert all((p[0] == 'n0' and p[3] == cmd['command_id'] for p in phases))
    completed = [json.loads(l.split('NATIVE_HOT_DATA_MAINTENANCE ', 1)[1]) for l in log.splitlines() if 'NATIVE_HOT_DATA_MAINTENANCE ' in l]
    assert len(completed) == 1 and completed[0]['completed'] is True and (completed[0]['phase'] == 'Publish') and (completed[0]['remaining_ms'] == 560)
    assert {p[4] for p in phases} == {completed[0]['recovery_nonce']}
    health = bykind('health_round')
    assert all((health[k] is True for k in ('old_exact', 'committed_exact', 'next_user_ack', 'ever_next_user_ack', 'next_exact')))
    assert health['round'] == 1 and health['receipt_elapsed_ns'] == 1523718116 and (terminal['health_elapsed_ns'] == 1523989554)
    assert terminal['bounded_autonomous_repair_and_next_ack'] is True and terminal['actual_public_api_user_ack'] is False
    cleanup = bykind('test_terminal_after_owned_stop_attempts')
    assert cleanup['cleanup_errors'] == [] and cleanup['first_error'] is None and (cleanup['portable_native_db_custody'] is False)
    return dict(identity=identity, journal_rows=len(rows), kinds=dict(kinds), api=api, first_origin_unknown=probe, old_ack_native_command=old, native_cut=cut, actual_rpc_attempts=rpc, successful_snapshot_observations=snapshots, successful_strong=strong, phase_call_trace=[list(p) for p in phases], completed_publication=completed[0], health=health, terminal=terminal, cleanup=cleanup, manual_maintenance_records_before_success=0, health_start_approx_from_receipt_unix_ns=health['unix_ns'] - health['receipt_elapsed_ns'], fault_armed_unix_ns=bykind('fault_armed')['unix_ns'], handler_exit_observed_unix_ns=exit['unix_ns'], healthy_receipt_unix_ns=health['unix_ns'])

def read_packet():
    manifest = json.loads((ROOT / 'manifest.json').read_text())
    assert manifest['format'] == 1 and manifest['source_baseline'] == SOURCE and (manifest['source_controls'] == FIXED)
    assert set(manifest['files']) == {str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and p.name != 'manifest.json' and ('__pycache__' not in p.parts)}
    assert len(manifest['files']) <= 128 and sum((f['bytes'] for f in manifest['files'].values())) <= 32 * 1024 * 1024
    for name, fact in manifest['files'].items():
        path = ROOT / name
        assert not path.is_symlink() and path.stat().st_size == fact['bytes'] and (fact['bytes'] <= 8 * 1024 * 1024)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == fact['sha256'], name
    archives = [('baseline-8334.zip', SOURCE, '7a56b40fd316fcbf5f3b94f43159bf123672fbc8b1ef57c7f8fb9992bc241120'), ('corrected-controls-60c.zip', FIXED, '519d375efcd908753662f31acb2ac2f102e1ea212f6d6f5deacf27edf93ef369'), ('first-controls-failed-8334.zip', SOURCE, 'ff2dfb87b589b4a79ce7665ee193b39f58a74f7009d82be3aa259860703b33e0')]
    for name, source, digest in archives:
        path = ROOT / 'artifacts' / name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        with zipfile.ZipFile(path) as z:
            assert z.read('source-revision.txt').decode().strip() == source
            assert sum((i.file_size for i in z.infolist())) < 8 * 1024 * 1024
            for i in z.infolist():
                rel = pathlib.PurePosixPath(i.filename)
                assert not rel.is_absolute() and '..' not in rel.parts and (not i.is_dir())
                assert i.filename in ('rustc.txt', 'source-revision.txt', 'selected-tests.log', 'tests.log') or (name == 'baseline-8334.zip' and i.filename.startswith('fault-state/native-origin-pre-wal-') and i.filename.endswith(('/journal.jsonl', '/terminal.json')))
                data = z.read(i)
                assert b'-----BEGIN PRIVATE KEY-----' not in data and b'-----BEGIN RSA PRIVATE KEY-----' not in data and (b'-----BEGIN EC PRIVATE KEY-----' not in data)
            if name == 'baseline-8334.zip':
                rows = [json.loads(l) for l in z.read(next((n for n in z.namelist() if n.endswith('/journal.jsonl')))).splitlines()]
                terminal = json.loads(z.read(next((n for n in z.namelist() if n.endswith('/terminal.json')))))
                log = z.read('tests.log').decode()
    indices = {}
    for path in (ROOT / 'logs').glob('*-index.json'):
        index = json.loads(path.read_text())
        indices[index['job_id']] = index
        digest = hashlib.sha256()
        total = 0
        with gzip.open(ROOT / 'logs' / index['compressed_file'], 'rb') as f:
            while (block := f.read(65536)):
                total += len(block)
                assert total <= 16 * 1024 * 1024
                digest.update(block)
        assert total == index['raw_bytes'] and digest.hexdigest() == index['raw_sha256']
        assert hashlib.sha256((ROOT / 'logs' / index['compressed_file']).read_bytes()).hexdigest() == index['compressed_sha256']
        assert index['authoritative_source_verified'] is True
    assert len(indices) == 10
    for job in (131587991318, 131588000052, 131588026088, 131588036795):
        i = indices[job]
        assert i['source_revision'] == 'a96bbf958b8f366d41bbe31b2bc776f1fb6bcbe4' and i['job_conclusion'] == 'failure' and (not i['test_summaries']) and (not i['test_observations'])
        assert len(i['failure_context']) == 1 and 'E0599' in '\n'.join(i['failure_context'][0]['lines'])
        if job in (131588026088, 131588036795):
            assert all((any((s['name'] == name and s['conclusion'] == 'skipped' for s in i['steps'])) for name in ('Record source and list selected Core library tests', 'Execute selected Core library tests')))
    first = indices[131590289083]
    baseline = indices[131590303747]
    fixed = indices[131595098465]
    assert first['source_revision'] == baseline['source_revision'] == SOURCE and fixed['source_revision'] == FIXED
    assert first['job_conclusion'] == 'failure' and len(first['failure_context']) == 1 and ('left: Null' in '\n'.join(first['failure_context'][0]['lines']))
    assert '5 passed; 1 failed; 0 ignored;' in first['test_summaries'][0]
    assert baseline['job_conclusion'] == fixed['job_conclusion'] == 'success'
    assert '2 passed; 0 failed; 0 ignored;' in baseline['test_summaries'][0] and '6 passed; 0 failed; 0 ignored;' in fixed['test_summaries'][0]
    assert len(first['test_observations']) == len(fixed['test_observations']) == 6 and len(baseline['test_observations']) == 2
    assert all((x['result'] == 'ok' for x in baseline['test_observations'] + fixed['test_observations']))
    assert {x['test'] for x in first['test_observations']} == {x['test'] for x in fixed['test_observations']}
    assert len({x['test'] for x in first['test_observations'] + baseline['test_observations'] + fixed['test_observations']}) == 8
    for job, source in ((131590268686, SOURCE), (131590257278, SOURCE), (131595062512, FIXED)):
        i = indices[job]
        assert i['source_revision'] == source and i['job_conclusion'] == 'success'
        assert all((any((s['name'] == name and s['conclusion'] == 'success' for s in i['steps'])) for name in ('cargo check --workspace --all-targets', 'Workflow consumer compiles without Core defaults')))
    return (rows, terminal, log)

def source_check(repo):
    parity = json.loads((ROOT / 'source-parity.json').read_text())
    assert parity['baseline_source'] == SOURCE and parity['corrected_controls_source'] == FIXED and (parity['production_bytes_equal'] is True)
    for name, fact in parity['source_files'].items():
        data = (ROOT / 'sources' / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == fact['sha256']
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\x00' + data).hexdigest() == fact['git_blob']
        if repo:
            assert subprocess.check_output(['git', 'show', fact['commit'] + ':' + fact['path']], cwd=repo) == data
    patch = (ROOT / 'sources/corrected-control.patch').read_bytes()
    assert hashlib.sha256(patch).hexdigest() == parity['patch_sha256']
    if repo:
        assert subprocess.check_output(['git', 'diff', '--name-only', SOURCE, FIXED], cwd=repo, text=True).splitlines() == [parity['sole_changed_tracked_path']]
        assert subprocess.check_output(['git', 'diff', SOURCE, FIXED, '--', parity['sole_changed_tracked_path']], cwd=repo) == patch
        path = parity['source_files']['origin-fault-unchanged.rs']['path']
        for source in (parity['prior_fault_baseline_source'], SOURCE, FIXED):
            assert subprocess.check_output(['git', 'show', source + ':' + path], cwd=repo) == (ROOT / 'sources/origin-fault-unchanged.rs').read_bytes()
    return 'PASS' if repo else 'NOT_RUN (use --repo for repository-wide production byte parity)'

def tamper_tests(rows, terminal, log):

    def one(kind, items):
        return next((r for r in items if r['kind'] == kind))

    def false_absence(items, term):
        for row in items:
            if row['kind'] != 'actual_rpc':
                continue
            a = row['actual']
            if a['phase'] != 'response_frame_before_decode':
                continue
            frame = json.loads(bytes(a['raw_frame'])[4:])
            g = frame.get('response', {}).get('Ok', {}).get('NativeEventGet', {}).get('result', {})
            if g.get('kind') == 'value':
                g['value'] = {'presence': 'absent'}
                data = json.dumps(frame, separators=(',', ':')).encode()
                a['raw_frame'] = list(len(data).to_bytes(4, 'big') + data)
                return
        raise AssertionError('tamper fixture has no actual Strong value')

    def missing_attempt(items, term):
        items.pop(next((i for i, r in enumerate(items) if r['kind'] == 'actual_rpc' and r['actual']['phase'] == 'request_frame_before_dispatch')))

    def snapshot_error(items, term):
        for row in items:
            if row['kind'] == 'actual_rpc' and row['actual']['operation'] == 'metadata_snapshot_discovery' and (row['actual']['phase'] == 'response_frame_before_decode'):
                a = row['actual']
                frame = json.loads(bytes(a['raw_frame'])[4:])
                frame['response'] = {'Err': {'code': 'test_only_error', 'message': 'no successful snapshot'}}
                data = json.dumps(frame, separators=(',', ':')).encode()
                a['raw_frame'] = list(len(data).to_bytes(4, 'big') + data)
                return
    cases = [('dropped-first-Unknown', lambda r, t: one('actual_origin_exit', r)['probe'].__setitem__('core_result', 'aborted')), ('retroactive-user-ACK', lambda r, t: next((x for x in r if x['kind'] == 'logical_public_reply' and x['outcome']['classification'] == 'NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN'))['outcome'].__setitem__('classification', 'USER_ACK')), ('manual-tick-before-success', lambda r, t: r.append({'kind': 'supported_registry_maintenance', 'journal_sequence': 340})), ('false-absence-wire-reply', false_absence), ('missing-physical-attempt', missing_attempt), ('incorrect-executed-source', lambda r, t: one('execution_identity', r).__setitem__('source', '0' * 40)), ('error-is-not-successful-snapshot', snapshot_error), ('wrong-actual-native-peer-prefix', lambda r, t: one('actual_native_cut', r)['peer_native_materialization'][0]['verified_actual_native'].__setitem__('native_R', 0))]
    for name, mutate in cases:
        r = copy.deepcopy(rows)
        t = copy.deepcopy(terminal)
        mutate(r, t)
        try:
            audit(r, t, log)
        except (AssertionError, KeyError):
            pass
        else:
            raise AssertionError('semantic tamper accepted: ' + name)
    return len(cases)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=pathlib.Path)
    parser.add_argument('--tamper-tests', action='store_true')
    args = parser.parse_args()
    rows, terminal, log = read_packet()
    verified = audit(rows, terminal, log)
    expected = json.loads((ROOT / 'receipt.json').read_text())
    assert verified == expected['verified_baseline']
    assert expected['baseline_source'] == SOURCE and expected['corrected_controls_source'] == FIXED
    assert (expected['runtime_test_executions'], expected['passed_executions'], expected['preserved_failed_executions'], expected['distinct_tests']) == (14, 13, 1, 8)
    cohorts = expected['cohorts']
    assert len(cohorts) == 4
    assert cohorts[0]['source'] == 'a96bbf958b8f366d41bbe31b2bc776f1fb6bcbe4' and cohorts[0]['selected_test_executions'] == 0 and (cohorts[0]['classification'] == 'FAILED_COMPILATION_BEFORE_TEST_SELECTION')
    assert [(c['source'], c['run'], c['core_job'], c['passed'], c['failed'], c['filtered'], c['seconds']) for c in cohorts[1:]] == [(SOURCE, 43335418131, 131590289083, 5, 1, 965, 5.23), (SOURCE, 43335421681, 131590303747, 2, 0, 969, 14.38), (FIXED, 43336846793, 131595098465, 6, 0, 965, 6.74)]
    assert cohorts[1]['failed_case_remaining_assertions'] == 'NOT_EXECUTED'
    assert expected['classification'] == 'PASSED_SCOPED_COMPONENT_WITH_PRESERVED_EARLIER_FAILURES_NOT_PRODUCTION_QUALIFICATION'
    source = source_check(args.repo)
    n = tamper_tests(rows, terminal, log) if args.tamper_tests else 0
    print(json.dumps({'packet': 'PASS', 'raw_logs': 10, 'actual_tests_distinct': 8, 'runtime_executions': 14, 'passed_executions': 13, 'preserved_failed_executions': 1, 'journal_rows': 339, 'physical_attempts': 77, 'successful_snapshots': 51, 'source_repository_verification': source, 'semantic_tamper_rejections': n, 'engine_open_or_network_or_build': False}))
if __name__ == '__main__':
    main()
