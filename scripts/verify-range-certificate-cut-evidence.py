#!/usr/bin/env python3
"""Validate retained red characterization; this does not run a database or HA test."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
base = root / 'docs/evidence/range-certificate-ack-cut-2026-11-11'
manifest = json.loads((base / 'manifest.json').read_text())
for name, digest in manifest.items():
    assert Path(name).name == name
    assert hashlib.sha256((base / name).read_bytes()).hexdigest() == digest, name
build = json.loads((base / 'build.json').read_text())
assert hashlib.sha256((root / 'scripts/experiments/range-certificate-cut.rs').read_bytes()).hexdigest() == build['source_sha256']
assert build['source_commit'] == 'd49a391b3c6157f90f29e9f2f5214143c60f9f4b'
assert build['libraries']['bicdb_core'] == 'dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356'
result = json.loads((base / 'red.json').read_text())
assert result['exit_code'] == 101
observed = next(json.loads(line) for line in (base / 'red.stdout').read_text().splitlines() if line.startswith('{'))
assert observed['client_commit_returned_success'] is True
assert observed['only_unavailable_node'] == 'n0' and observed['surviving_voters'] == 2
assert [p['entry']['state'] for p in observed['pre_loss_probes']] == ['applied', 'committed', 'prepared']
commands = [p['entry']['command'] for p in observed['pre_loss_probes']]
assert commands[0] == commands[1] == commands[2]
assert commands[0]['mutations'][0]['record']['metadata'] == observed['expected_payload']
assert next(iter(observed['authority_after_loss']['ranges'].values()))['leader'] == 'n1'
assert all('ambiguous provisional decision' in error for error in observed['three_recovery_attempts'])
assert len(observed['three_recovery_attempts']) == 3
failed = [(r['phase'], r['destination']) for r in observed['replication_phase_calls'] if r['injected_failure']]
assert failed == [('commit', 'n2'), ('certify', 'n1')]
summary = json.loads((base / 'summary.json').read_text())
assert summary['findings']['one_leader_loss_ack_recoverability'] == 'FAILED'
assert summary['findings']['data_loss'] == 'INCONCLUSIVE'
assert summary['findings']['physical_ha_qualification'] == 'NOT_TESTED'
print('Retained actual-core ACK-recovery invariant red verified; data loss not demonstrated; no physical HA qualification.')
