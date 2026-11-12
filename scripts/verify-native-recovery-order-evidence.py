#!/usr/bin/env python3
"""Read-only integrity/scope checks; never executes the database or certifies HA."""
from pathlib import Path
import hashlib
import json
import sys

DEFAULT = Path(__file__).resolve().parents[1] / 'docs/evidence/native-recovery-order-2026-11-11'

def need(condition, message):
    if not condition:
        raise ValueError(message)

def verify(root=DEFAULT):
    root = Path(root).resolve()
    def read(name):
        path = (root / name).resolve()
        need(path.is_relative_to(root), 'evidence path escapes bundle')
        return json.loads(path.read_text())
    manifest = read('MANIFEST.json')
    files = manifest['files']
    need(len(files) == len({x['path'] for x in files}), 'duplicate manifest entry')
    need({p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name != 'MANIFEST.json'} == {x['path'] for x in files}, 'manifest file set differs')
    for entry in files:
        path = (root / entry['path']).resolve()
        need(path.is_relative_to(root) and not path.is_symlink(), 'unsafe evidence path')
        raw = path.read_bytes()
        need(len(raw) == entry['bytes'] and hashlib.sha256(raw).hexdigest() == entry['sha256'], 'evidence digest differs: ' + entry['path'])
    findings = read('findings.json')
    for field in ['current_main_causal_claim', 'ci_tested', 'physical_qualification', 'production_qualified', 'activation_tested', 'strong_get_tested', 'irretrievable_loss_proven']:
        need(findings[field] is False, 'unsupported qualification/attribution: ' + field)
    expected = [('9eace333c198e28e5416fb2943760e6286d5be59', 'FAIL', 101), ('021ba5c42d9acd025760cb9496ff9b03f98d5f18', 'PASS', 0), ('7d2f84a50d4aeaeee211ec4d54120f5c539a8ba4', 'PASS', 0), ('e68fa21fc4bfc52b53294e73dde57e42eb36876f', 'FAIL', 101)]
    cohorts = findings['cohorts']
    need(len(cohorts) == 4, 'four exact source cohorts required')
    for cohort, (source, outcome, code) in zip(cohorts, expected):
        need((cohort['source'], cohort['outcome'], cohort['exit_code']) == (source, outcome, code), 'baseline result/source rewritten')
        if 'run' in cohort:
            run = read(cohort['run'])
            need(run['source_revision'] == source and run['exit_code'] == code, 'run source/exit mismatch')
            need(run['dirty_diff_sha256'] == hashlib.sha256(b'').hexdigest() and run['guard_stop_reason'] is None, 'dirty or interrupted run')
    summary = read(cohorts[2]['summary'])
    names = [name for run in summary['runs'] for name in run['tests']]
    need(len(names) == len(set(names)) == summary['distinct_tests'] == 22, 'repair suite distinct count mismatch')
    for run in summary['runs']:
        need(run['result']['source_revision'] == expected[2][0] and run['result']['exit_code'] == 0, 'repair suite not exact passing source')
        need(run['passed'] == len(run['tests']) and run['result']['guard_stop_reason'] is None, 'repair suite incomplete')
    large = cohorts[3]
    need(large['earliest_operation'] == 'fence_epoch(n0)' and large['later_compacted_gap_exercised'] is False, 'later boundary falsely claimed')
    need(large['raw_entry_counts'] == {'applied': 1024, 'quorum_committed': 1}, 'irreversible obligations truncated/relabelled')
    pre = read('large-witness/large-prefix-preconditions.json')
    need(pre['acknowledged_commits'] == 1025 and pre['source_native']['native_applied_index'] == 1025 and pre['source_native']['pending_command_index'] == 1026, 'actual source precondition mismatch')
    refusal = read('large-witness/first-recovery-refusal.json')
    need(refusal['operation'] == 'fence_epoch' and refusal['recipient'] == 'n0' and 'malformed fenced image-history obligations' in refusal['error'], 'first refusal not preserved')
    catalog = read('large-witness/post-exit-catalog-summary.json')
    for node in ['n0', 'n2']:
        need(catalog[node]['retained_state_counts'] == large['raw_entry_counts'] and catalog[node]['retained_entries'] == 1025 and catalog[node]['envelope_checksum_independently_matches'] is True, 'post-exit catalog facts differ')
    need(catalog['n1']['catalog_file_present'] is False, 'unreached catalog observation changed')
    ledger = [json.loads(line) for line in (root / 'large-witness/controller-acknowledged-1025.jsonl').read_text().splitlines()]
    need([entry['index'] for entry in ledger] == list(range(1, 1026)), 'ACK ledger is incomplete, duplicated or includes unknown tail')
    custody = read('large-witness/custody-receipt.json')
    need(custody['files'] == 91 and custody['bytes'] == 152842336 and custody['manifest_sha256'] == '7426449ea05b610832b99ab6673fa552b0be5f409e0c6cf9da10b285b5062cf2', 'custody receipt changed')
    return {'result': 'VERIFIED_SCOPED_FAILURES_AND_CONTROLS', 'cohorts': 4, 'repair_tests': 22, 'large_first_refusal': 'fence-history-obligation-bound', 'qualification': False}

if __name__ == '__main__':
    print(json.dumps(verify(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT), sort_keys=True))
