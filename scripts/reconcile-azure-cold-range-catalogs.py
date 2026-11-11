#!/usr/bin/env python3
"""Read-only cold-archive catalog reconciliation; never opens BicDB."""
import collections
import argparse
import gzip
import hashlib
import json
import pathlib
import tarfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--archive-dir', type=pathlib.Path, required=True)
parser.add_argument('--output-dir', type=pathlib.Path, required=True)
parser.add_argument('--focus-record-id-sha256', action='append', default=[])
args = parser.parse_args()
ROOT = args.archive_dir
OUT = args.output_dir
OUT.mkdir(parents=True, exist_ok=True)
MAX_MEMBER = 64 * 1024 * 1024

def sha(data):
    return hashlib.sha256(data).hexdigest()

def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()

def member(archive, name):
    info = archive.getmember(name)
    assert info.isfile() and info.size <= MAX_MEMBER
    return archive.extractfile(info).read()

receipt = json.loads((ROOT / 'receipt.json').read_text())
assert len(receipt) == 5
with tarfile.open(ROOT / 'node-0.tar.gz') as archive:
    ledgers = {phase: json.loads(member(archive, phase + '-ledger.json'))
               for phase in ['steady-4', 'growth-4-to-5']}
expected = {}
phase_keys = {}
ledger_inputs = {}
for phase, ledger in ledgers.items():
    assert len(ledger['keys']) == len(set(ledger['keys']))
    phase_keys[phase] = []
    for key in ledger['keys']:
        record_id = ledger['namespace'] + '/' + key
        assert record_id not in expected
        expected[record_id] = {'run': ledger['run'], 'payload': 'x' * ledger['payload_bytes']}
        phase_keys[phase].append(record_id)
    with tarfile.open(ROOT / 'node-0.tar.gz') as archive:
        ledger_inputs[phase] = sha(member(archive, phase + '-ledger.json'))
observations = {key: [] for key in expected}
nodes = []
for entry in sorted(receipt, key=lambda item: item['node']):
    node = entry['node']
    path = ROOT / f'node-{node}.tar.gz'
    assert entry['status'] == 'exported' and entry['owned_engine_stopped_before_archive']
    digest = sha(path.read_bytes())
    assert digest == entry['sha256'] == entry['remote_sha256']
    with tarfile.open(path) as archive:
        raw = member(archive, 'db/cluster-range-write-log.json')
    envelope = json.loads(raw)
    catalog = envelope['catalog']
    assert sha(compact(catalog)) == envelope['checksum_sha256']
    states = collections.Counter()
    mismatches = 0
    count = 0
    for range_id, log in catalog['ranges'].items():
        assert log['compacted_through'] <= log['resolved_through'] <= log['last_index']
        for index, item in log['entries'].items():
            command = item['command']
            assert int(index) == command['index'] > log['compacted_through']
            assert str(command['range_id']) == range_id
            assert command['cluster_id'] == catalog['cluster_id']
            payload = {key: value for key, value in command.items() if key != 'checksum_sha256'}
            assert sha(compact(payload)) == command['checksum_sha256']
            states[item['state']] += 1
            count += 1
            for mutation in command['mutations']:
                key = mutation['record_id']
                if mutation['collection'] != '__bic_cluster_events' or key not in expected:
                    continue
                record = mutation['record']
                exact = (record is not None and record['id'] == key
                         and record['metadata'] == expected[key])
                mismatches += int(not exact)
                observations[key].append({'node': node, 'state': item['state'], 'exact': exact,
                    'observed_metadata_sha256': sha(compact(record['metadata'])) if record else None,
                    'command': command['checksum_sha256'],
                    'resolved_through_covers_index': command['index'] <= log['resolved_through']})
    nodes.append({'node': node, 'original_archive_sha256': digest,
        'original_catalog_sha256': sha(raw), 'catalog_bytes': len(raw),
        'catalog_checksum_valid': True, 'command_checksums_validated': count,
        'range_count': len(catalog['ranges']), 'entry_states': dict(states),
        'matching_ack_mutation_occurrences': sum(sum(x['node'] == node and x['exact'] for x in rows) for rows in observations.values()),
        'ack_identity_payload_mismatch_occurrences': mismatches,
        'watermark_and_entry_index_checks_passed': True})

def summarize(keys):
    exact = certified = applied = resolved = 0
    missing = []
    hist = collections.Counter()
    same_command_hist = collections.Counter()
    for key in keys:
        rows = [row for row in observations[key] if row['exact']]
        certified_rows = [row for row in rows if row['state'] in ['quorum_committed', 'applied']]
        exact += bool(rows)
        certified += bool(certified_rows)
        applied += any(row['state'] == 'applied' for row in rows)
        resolved += any(row['resolved_through_covers_index'] for row in certified_rows)
        hist[len({row['node'] for row in certified_rows})] += 1
        groups = collections.defaultdict(set)
        for row in certified_rows:
            groups[row['command']].add(row['node'])
        same_command_hist[max(map(len, groups.values()), default=0)] += 1
        if not rows:
            missing.append(sha(key.encode()))
    return {'ack_count': len(keys), 'with_exact_payload_in_any_retained_command': exact,
        'with_exact_payload_in_any_applied_or_quorum_committed_command': certified,
        'with_exact_payload_in_any_applied_command': applied,
        'with_exact_payload_in_certified_command_at_or_below_resolved_watermark': resolved,
        'catalog_count_with_matching_certified_command_histogram': dict(sorted(hist.items())),
        'max_catalog_count_for_same_exact_certified_command_histogram': dict(sorted(same_command_hist.items())),
        'no_retained_matching_command_record_id_sha256': missing}

summary = {'runtime_source': 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb',
    'runtime_version': '1.0.461-beta',
    'method': 'Read tar archive members only; no extraction, engine open, replay, VM access or mutation.',
    'original_pre_diagnosis_cold_archives': True,
    'archive_receipt_sha256': sha((ROOT / 'receipt.json').read_bytes()),
    'original_ledger_member_sha256': ledger_inputs,
    'nodes': nodes,
    'phases': {phase: summarize(keys) for phase, keys in phase_keys.items()},
    'all_known_acknowledgments': summarize(list(expected)),
    'original_two_unverified': [],
    'row_visibility_verified': False,
    'quorum_certificate_signatures_independently_verified': False,
    'physical_host_independence_verified': False,
    'production_qualified': False,
    'limitations': [
        'Applied/quorum_committed are durable local catalog states, not independently verified voter signatures or end-to-end row reads.',
        'Catalog checksums are unkeyed integrity checks, not authenticity attestations.',
        'Absent catalog history can result from snapshot-installed prefixes, relocation cleanup or compaction; it does not prove absent rows or lost commits.',
        'Matching entries in several guest catalogs do not establish independent physical hosts.',
        'Reconciliation covers retained immutable-event ACK identities/payloads only, not pages, indexes, FTS, queues, audit sinks or production crash safety.'
    ]}
for record_id in expected:
    if sha(record_id.encode()) not in args.focus_record_id_sha256:
        continue
    summary['original_two_unverified'].append({'record_id_sha256': sha(record_id.encode()),
        'summary': summarize([record_id]),
        'matching_observations': [{k: v for k, v in row.items() if k != 'command'}
                                  for row in observations[record_id]]})
assert len(summary['original_two_unverified']) == len(set(args.focus_record_id_sha256))
details = [{'record_id_sha256': sha(key.encode()),
            'expected_metadata_sha256': sha(compact(expected[key])),
            'matching_catalog_observations': observations[key]}
           for key in sorted(expected)]
detail_bytes = (json.dumps(details, indent=2) + '\n').encode()
(OUT / 'per-ack-catalog-observations.json.gz').write_bytes(gzip.compress(detail_bytes, mtime=0))
summary['per_ack_observations_sha256'] = sha((OUT / 'per-ack-catalog-observations.json.gz').read_bytes())
(OUT / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({'acknowledgments_reconciled': len(expected),
    'with_exact_payload_in_any_applied_command': summary['all_known_acknowledgments']['with_exact_payload_in_any_applied_command'],
    'archives_read': len(nodes), 'engine_opened': False}))
