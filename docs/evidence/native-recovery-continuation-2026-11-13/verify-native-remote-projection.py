#!/usr/bin/env python3
"""Check projected source/OLD/NoDomain test evidence without opening DB roots.

Checks retained logical API/authenticated-payload observations and their
cross-bindings. It does not recreate native grants, validate a TLS handshake,
cold-replay a DB, install a target, or qualify serving/HA/scale-out.
"""
import argparse
import collections
import hashlib
import json
import pathlib
import re
import zipfile

ANSI = re.compile(r'\x1b\[[0-9;]*m')
SHA = re.compile(r'[0-9a-f]{64}')
MAX_TOTAL = 256 * 1024 * 1024


def require(value, reason):
    if not value:
        raise ValueError(reason)


def encoded(value, sort=True):
    return json.dumps(value, ensure_ascii=False, sort_keys=sort, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def token(namespace, key):
    namespace, key = namespace.encode(), key.encode()
    return int.from_bytes(hashlib.sha256(b'bicdb-distribution-key-v1\0'
        + len(namespace).to_bytes(8, 'big') + namespace
        + len(key).to_bytes(8, 'big') + key).digest()[:8], 'big')


def known_digest(value, where):
    require(isinstance(value, str) and SHA.fullmatch(value), 'invalid digest at ' + where)


def check_old(row):
    reply = row.get('actual_reply')
    if reply is None:
        require(bool(row.get('exact_error')), 'OLD operation lacks both actual reply and exact error')
        return
    request = row['request']
    require(reply['request'] == request, 'OLD reply does not bind exact request')
    require(reply['node_id'] == row['recipient'], 'OLD recipient differs from actual reply')
    known_digest(request['scope_sha256'], 'OLD request scope')
    require(reply['constraints']['scope_sha256'] == request['scope_sha256'], 'OLD constraints scope differs')
    closed = reply.get('closed')
    if request['operation'] == 'Constrain':
        require(closed is None, 'constraint-only observation became a closed vote')
    if closed is not None:
        require(closed['scope_sha256'] == request['scope_sha256'], 'OLD closed scope differs')
        require(closed['node_id'] == row['recipient'], 'OLD closed node differs')
        for field in ('logical_genesis_sha256', 'logical_history_sha256', 'native_receipt_sha256'):
            known_digest(closed[field], 'OLD closed ' + field)
    if row.get('operation') == 'remote_old_close_retry':
        require(bool(row.get('original_error')) and bool(row.get('readonly_exact_error')),
                'retained native retry lost original/readonly uncertain outcomes')
        require(row['native_after_original_error'] == row['native_after_retry'],
                'exact fresh retry changed retained native OLD record')
        require(row.get('native_receipt_is_not_completed_closure') is True,
                'native OLD receipt was relabelled as completed closure')
        require(closed is not None and closed['native_receipt_sha256']
                == row['native_after_retry']['checksum_sha256'], 'retry closure differs from retained native record')


def check_frames(rows, acknowledgments):
    manifests, result = {}, []
    frames = [row for row in rows if row.get('operation') == 'source_image_frame']
    grouped = collections.defaultdict(list)
    for index, row in enumerate(frames):
        request = row['intent']
        known_digest(request['manifest_sha256'], 'source frame intent manifest')
        if row.get('actual_reply') is None:
            require(bool(row.get('exact_error')), 'failed source frame lost exact error')
            continue
        reply = row['actual_reply']
        require(row['outcome'] == 'returned' and row.get('exact_error') is None,
                'returned source reply has contradictory outcome')
        require(reply['request'] == request, 'source frame reply differs from exact request')
        require(reply['manifest_sha256'] == request['manifest_sha256'], 'source frame manifest mismatch')
        require(reply['source_id'] == row['source'] and reply['target_id'] == row['target'],
                'source frame source/target identity differs')
        require(row.get('original_parent_live_at_reply') is True and row.get('original_own_live_at_reply') is True,
                'successful source frame lost original bounded caller/OWN')
        payload = reply['payload']
        require(isinstance(payload, dict) and len(payload) == 1, 'unknown source payload shape')
        kind, body = next(iter(payload.items()))
        grouped[request['manifest_sha256']].append((index, row, kind, body))
        if kind == 'Manifest':
            require(body['checksum_sha256'] == request['manifest_sha256'], 'manifest body digest identity differs')
            require(body['source_session'] == request['source_session'] and body['target_attempt'] == request['target_attempt'],
                    'manifest source session/target attempt differs')
            require(body['source_id'] == reply['source_id'] and body['target_id'] == reply['target_id']
                    and body['source_birth'] == reply['source_birth'], 'manifest native source/target differs')
            previous = manifests.setdefault(body['checksum_sha256'], body)
            require(previous == body, 'same manifest digest returned different complete manifest')
    for manifest_sha, stream in grouped.items():
        require(manifest_sha in manifests, 'returned steps have no retained actual Manifest')
        manifest = manifests[manifest_sha]
        for field in ('h0', 'h', 'old_quorum_sha256', 'old_config_sha256', 'new_config_sha256',
                      'portable_catalogue_sha256', 'schema_sha256', 'source_native_receipt'):
            known_digest(manifest[field], 'manifest ' + field)
        require(set(manifest['actual_old_closures']) == set(manifest['old_members']),
                'source manifest lacks ALL actual OLD closure members; a majority/constraints cannot substitute')
        require(len({closed['scope_sha256'] for closed in manifest['actual_old_closures'].values()}) == 1,
                'manifest OLD closures do not share the same exact retained scope')
        for node, closed in manifest['actual_old_closures'].items():
            known_digest(closed['scope_sha256'], 'manifest OLD scope')
            known_digest(closed['native_receipt_sha256'], 'manifest OLD native receipt')
            require(node == closed['node_id'] and node in manifest['old_members'], 'manifest OLD member mismatch')
            require(closed['range_index'] == manifest['range_index']
                    and closed['logical_genesis_sha256'] == manifest['h0']
                    and closed['logical_history_sha256'] == manifest['h'], 'manifest OLD native point mismatch')
        unique_rows, history, finishes = {}, [], []
        for index, row, kind, body in stream:
            require(row['intent']['source_session'] == manifest['source_session']
                    and row['intent']['target_attempt'] == manifest['target_attempt'], 'step lost retained session/attempt')
            if kind == 'History':
                current = body['history']
                require(current['logical_genesis_sha256'] == manifest['h0']
                        and current['logical_history_sha256'] == manifest['h']
                        and current['source_range_index'] == manifest['range_index']
                        and current['range_id'] == manifest['capture_range']['id']
                        and current['source_native_receipt_sha256'] == manifest['source_native_receipt'],
                        'actual source history differs from manifest native cut')
                history.extend(current['entries'])
            elif kind == 'Rows':
                cursor = encoded(row['intent']['operation'])
                previous = unique_rows.setdefault(cursor, body)
                require(previous == body, 'actual same-cursor row retry changed immutable full step')
            elif kind == 'Finished':
                finishes.append(body)
        records = [record for step in unique_rows.values() for record in (step.get('batch') or {}).get('records', [])]
        if history:
            require([entry['index'] for entry in history] == list(range(manifest['range_index'] + 1)),
                    'history is not exact contiguous genesis-to-cut')
            require(history[0]['commitment_sha256'] == manifest['h0'], 'genesis differs from manifest')
            for index, entry in enumerate(history[1:], 1):
                require(entry['previous_sha256'] == history[index - 1]['commitment_sha256'], 'history predecessor changed')
                outcome = {'kind': entry['outcome']['kind']}
                if outcome['kind'] == 'applied':
                    outcome['command_checksum_sha256'] = entry['outcome']['command_checksum_sha256']
                link = ['bicdb.native.logical-history.link.v1', entry['range_id'], entry['index'], entry['previous_sha256'], outcome]
                require(digest(encoded(link, sort=False)) == entry['commitment_sha256'], 'canonical native history link hash differs')
            require(history[-1]['commitment_sha256'] == manifest['h'], 'history does not finish at manifest H')
        for step in unique_rows.values():
            batch = step.get('batch')
            if not batch:
                continue
            for record in batch['records']:
                route = token(batch['collection'], record['id'])
                bounds = manifest['capture_range']
                require(route >= bounds['start_token'] and (bounds['end_token'] is None or route < bounds['end_token']),
                        'returned record lies outside manifest physical range')
        matching_ack = [ack for ack in acknowledgments if ack['range'] == manifest['capture_range']['id']]
        if finishes:
            require(history and matching_ack, 'finished stream lacks history or external ACK record')
            require(all(ack['actual_full_record'] in records for ack in matching_ack), 'complete externally ACKed record did not cross source stream')
            for seal in finishes:
                require(seal['record_count'] == len(records) and seal['history_entries'] == len(history), 'seal record/history count differs')
                require(seal['range_index'] == manifest['range_index'] and seal['logical_history_sha256'] == manifest['h']
                        and seal['logical_genesis_sha256'] == manifest['h0'] and seal['source_id'] == manifest['source_id'],
                        'seal native point/source differs')
        result.append({'manifest_sha256': manifest_sha, 'range_id': manifest['capture_range']['id'],
            'native_index': manifest['range_index'], 'h0': manifest['h0'], 'h': manifest['h'],
            'source': manifest['source_id'], 'target': manifest['target_id'],
            'successful_frame_observations': len(stream), 'history_entries_verified': len(history),
            'unique_full_records': records, 'same_cursor_row_retry_observations': sum(kind == 'Rows' for _, _, kind, _ in stream) - len(unique_rows),
            'complete_seals': len(finishes), 'actual_old_closure_scopes': {node: closed['scope_sha256'] for node, closed in manifest['actual_old_closures'].items()}})
    return result


def verify(path, expected_sha, expected_source, expected_tests):
    require(digest(path.read_bytes()) == expected_sha, 'projection ZIP differs from pinned digest')
    with zipfile.ZipFile(path) as zipped:
        infos = zipped.infolist()
        require(sum(i.file_size for i in infos) <= MAX_TOTAL, 'projection exceeds bounded uncompressed size')
        require(not any('/custody/' in i.filename or i.filename.endswith('/custody-manifest.json') for i in infos),
                'projection contains excluded fixture custody')
        require(zipped.read('source-revision.txt').decode().strip() == expected_source, 'source revision differs')
        selected = [line[:-6] for line in ANSI.sub('', zipped.read('selected-tests.log').decode()).splitlines() if line.endswith(': test')]
        text = ANSI.sub('', zipped.read('tests.log').decode())
        actual = dict(re.findall(r'^test (\S+) \.\.\. (ok|FAILED|ignored)$', text, re.M))
        require(len(selected) == expected_tests and set(actual) == set(selected), 'selected/executed test identities differ')
        summaries = re.findall(r'^test result: (.+)$', text, re.M)
        require(summaries and all(value == 'ok' for value in actual.values()), 'a selected test did not pass')
        prefixes = sorted({i.filename.rsplit('/', 1)[0] for i in infos if i.filename.startswith('fault-state/')})
        cases = []
        for prefix in prefixes:
            files = {i.filename.rsplit('/', 1)[-1]: json.loads(zipped.read(i)) for i in infos
                     if i.filename.startswith(prefix + '/') and i.filename.endswith('.json')}
            terminal = files['terminal.json']
            require(terminal['source'] == expected_source and terminal['primary_error'] is None, 'case source/primary error differs')
            require(files['evidence-errors.json']['errors'] == [] and files['cleanup.json']['errors'] == [], 'case evidence or cleanup errors')
            acknowledgments = files['ack-ledger.json']
            for ack in acknowledgments:
                require(ack['actual_api_error'] is None and ack['outcome'] == 'returned', 'external ACK ledger has failed API')
                require(digest(encoded(ack['value'])) == ack['payload_json_sha256'], 'ACK complete intended payload hash differs')
                require(ack['record_id'] == ack['collection'] + '/' + ack['key']
                        and ack['actual_full_record']['id'] == ack['record_id']
                        and ack['actual_full_record']['metadata'] == ack['value'], 'ACK full record identity/payload differs')
            ledger_names = [n for n in ('source-frame-ledger.json', 'remote-old-ledger.json') if n in files]
            for name in ledger_names:
                for row in files[name]:
                    if row.get('operation', '').startswith('remote_old_'):
                        check_old(row)
            source_rows = files.get('source-frame-ledger.json', [])
            streams = check_frames(source_rows, acknowledgments)
            comparisons = []
            for name, body in files.items():
                if name.startswith('no-domain-'):
                    require(body['before'] == body['after'], 'NoDomain changed complete target snapshot')
                    require(body['no_installing_created'] and body['no_rows_installed'] and body['no_R0_or_serving_grant'], 'NoDomain claimed target installation/serving')
                    require(body['after']['writer_installed'] is False and all(not rows for rows in body['after']['rows'].values()), 'NoDomain target contains rows/writer')
                    require(body['after']['B'] == 0 and all(point['native_base_index'] is None
                        and point['native_applied_index'] is None and point['pending_command_index'] is None
                        and point['materialized_pending_index'] is None for point in body['after']['points'].values()),
                        'NoDomain target acquired a native R0/applied/pending point')
                    if name.endswith('-member.json'):
                        require(body['metadata_term_after'] > body['metadata_term_before'], 'metadata-term revocation did not change actual term')
                    if name.endswith('-schema.json'):
                        require(body['schema_generation_after'] > body['schema_generation_before']
                            and body['schema_case_is_generation_invalidation_not_DDL'] is True,
                            'schema-generation revocation was not observed as its stated non-DDL event')
                    if name.endswith('-point.json'):
                        require(body['reached_full_no_domain'] and body['result'] == 'point_observed' and body['exact_error'] is None, 'positive NoDomain point was not actually reached')
                    else:
                        require(body['result'] == 'revoked_before_installing' and bool(body['exact_error']), 'NoDomain revocation not actually observed')
                    comparisons.append({'member': name, 'full_target_before_after_equal': True,
                        'result': body['result'], 'reached_full_no_domain': body['reached_full_no_domain'], 'exact_error': body['exact_error']})
            if 'source-constraints-result.json' in files:
                result = files['source-constraints-result.json']
                require(result['target_before'] == result['target_after'] and result['known_pending_qualified'] is False
                        and result['source_image_captured'] is False and result['serving_granted'] is False, 'constraint-only case changed target or granted authority')
            if 'old-sink-control-result.json' in files:
                result = files['old-sink-control-result.json']
                before, after = dict(result['before']), dict(result['after'])
                before.pop('raw'); after.pop('raw')
                if result['revoke_hook']:
                    before.pop('writer_installed'); after.pop('writer_installed')
                require(before == after and result['native_old_close_present'] is False and not result['rollback_claimed'],
                        'pre-WAL OLD failure changed native data/claimed rollback')
            if 'source-frames-result.json' in files:
                result = files['source-frames-result.json']
                require(result['actual_frame_count'] == sum(row.get('operation') == 'source_image_frame' for row in source_rows), 'source frame count dropped attempts')
                require(result['target_rows_installed'] is False and result['serving_granted'] is False, 'source stream claimed target rows/Serving')
                if 'target_after' in result:
                    require(result['target_before'] == result['target_after'], 'fresh source-stream target before/after snapshot differs')
            cases.append({'case_directory': prefix, 'external_ack_records': acknowledgments,
                'source_streams': streams, 'all_logical_old_and_source_attempts': {name: files[name] for name in ledger_names},
                'independent_NoDomain_before_after_checks': comparisons,
                'source_target_unchanged_scope': 'CI assertion only where result retains target_before without target_after; independent equality only where both complete snapshots exist',
                'all_reports': files})
    return {'format_version': 1, 'source_commit': expected_source, 'projection_sha256': expected_sha,
        'selected_tests': selected, 'actual_test_results': actual, 'test_summaries': summaries,
        'case_count': len(cases), 'cases': cases,
        'limitations': ['No explicit per-directory Rust-test-name identity is retained; selected group execution and case ledgers are separate facts.',
            'Logical API/source authenticated-payload observations are retained; raw TLS handshake bytes are not.',
            'Native scopes are cross-checked as recorded commitments; this does not recreate opaque native authorities.',
            'Source streaming or NoDomain never proves target installation, Serving, cold replay, hardware HA or capacity gain.',
            'Lower-policy archive with no case ledgers proves selected tests ran, not independent native-state reconciliation.']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('projection', type=pathlib.Path)
    p.add_argument('--expected-sha256', required=True)
    p.add_argument('--expected-source', required=True)
    p.add_argument('--expect-tests', required=True, type=int)
    p.add_argument('--report', required=True, type=pathlib.Path)
    a = p.parse_args()
    r = verify(a.projection, a.expected_sha256, a.expected_source, a.expect_tests)
    r['verifier_sha256'] = digest(pathlib.Path(__file__).read_bytes())
    a.report.write_text(json.dumps(r, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k: r[k] for k in ('source_commit', 'projection_sha256', 'case_count', 'test_summaries')}))


if __name__ == '__main__':
    main()
