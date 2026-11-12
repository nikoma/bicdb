"""Reconcile saved HA rowsets against ACK phases derived from the fsynced ledger.

No BicDB process or database is opened. Snapshot-declared ACK counts are
checked against the ledger's independently recorded before/successor/after
phases; they never determine which acknowledged values must survive.
"""
import argparse
import collections
import hashlib
import json
import pathlib
import re
import sqlite3
import time

p = argparse.ArgumentParser()
p.add_argument('root', type=pathlib.Path)
a = p.parse_args()


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def phase_key(phase):
    match = re.fullmatch(r'round([0-9]+)-(before|successor|after)', phase)
    assert match, ('unknown ledger phase', phase)
    return int(match[1]), {'before': 0, 'successor': 1, 'after': 2}[match[2]]


result = json.loads((a.root / 'result.json').read_text())
assert result['status'] == 'passed' and result['storage_mode'] == 'server_paged'
assert not result['physical_power_loss'] and not result['hard_vm_reset']
assert not result['experimental_commit_durability']
assert sorted(case['voters'] for case in result['cases']) == [3, 5]
out = dict(source=result['source'], binary_sha256=result['binary_sha256'],
           result_sha256=sha(a.root / 'result.json'), engine_opened=False,
           expectation_source='ACK ledger phases; snapshot counters only cross-checked',
           cases=[], checked_unix=time.time())
for case in result['cases']:
    n = case['voters']
    rounds = {3: 3, 5: 2}[n]
    assert case['status'] == 'passed' and case['processes_stopped']
    assert case['required_voting_standbys'] == n // 2
    assert [stage['round'] for stage in case['rounds']] == list(range(rounds))
    ledger = pathlib.Path(case['ack_ledger'])
    assert sha(ledger) == case['ack_ledger_sha256']
    with sqlite3.connect(ledger.as_uri() + '?mode=ro', uri=True) as db:
        ack = list(db.execute('SELECT id,value,phase FROM ack ORDER BY id'))
        attempts = {identity: (value, phase, status, sqlstate)
                    for identity, value, phase, status, sqlstate in
                    db.execute('SELECT id,value,phase,status,sqlstate FROM attempts')}
    phases = collections.Counter(phase for _, _, phase in ack)
    expected_phases = {f'round{round_id}-{name}': count
                       for round_id in range(rounds)
                       for name, count in [('before', 10), ('successor', 1), ('after', 5)]}
    assert dict(phases) == expected_phases, ('incomplete acknowledged phase coverage', phases)
    assert len(ack) == case['acknowledged'] == rounds * 16
    for identity, value, phase in ack:
        assert attempts[identity][:3] == (value, phase, 'acknowledged')
    refused = {identity for identity, (_, _, status, sqlstate) in attempts.items()
               if status == 'error' and sqlstate == '25006'}
    checked = 0
    for stage in case['rounds']:
        assert stage['successor']['node'] == stage['new_primary']
        assert {snapshot['node'] for snapshot in stage['nodes']} == {f'n{i}' for i in range(n)}
        assert len(stage['nodes']) == n
        snapshots = [(stage['successor'], 1), *((snapshot, 2) for snapshot in stage['nodes'])]
        for snapshot, phase_number in snapshots:
            boundary = (stage['round'], phase_number)
            expected = {identity: value for identity, value, phase in ack if phase_key(phase) <= boundary}
            assert snapshot['acknowledged'] == len(expected), 'snapshot counter cannot weaken ledger expectations'
            path = pathlib.Path(snapshot['rows_file'])
            assert sha(path) == snapshot['rows_sha256']
            pairs = json.loads(path.read_text())
            rows = dict(pairs)
            assert len(rows) == len(pairs) == snapshot['observed']
            assert all(rows.get(identity) == value for identity, value in expected.items())
            assert -1 not in rows and not refused.intersection(rows)
            # Error replies can leave an uncertain committed write. Such a row
            # is allowed when the ledger records the matching attempted value
            # in an already reached phase; it is never relabeled as an ACK.
            for identity, value in rows.items():
                attempted_value, attempted_phase, _, _ = attempts[identity]
                assert value == attempted_value and phase_key(attempted_phase) <= boundary
            checked += 1
    assert checked == rounds * (n + 1)
    formats = []
    for node in sorted(ledger.parent.glob('n*/format.json')):
        data = json.loads(node.read_text())
        assert data['storage_mode'] == 'server_paged' and 'server_paged_storage' in data['feature_flags']
        formats.append(dict(path=str(node), sha256=sha(node)))
    assert {pathlib.Path(item['path']).parent.name for item in formats} == {f'n{i}' for i in range(n)}
    out['cases'].append(dict(voters=n, acknowledged=len(ack), known_readonly_refusals=len(refused),
                             snapshots_verified=checked, ledger_phase_counts=expected_phases,
                             paged_formats=formats))
out['status'] = 'passed'
output = a.root / 'independent-artifact-verification-v2.json'
assert not output.exists()
output.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(dict(status=out['status'], cases=[{key: value for key, value in case.items()
                                                 if key not in ['paged_formats', 'ledger_phase_counts']}
                                                for case in out['cases']])), flush=True)
