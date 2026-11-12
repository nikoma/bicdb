#!/usr/bin/env python3
"""Read-only validation of retained local component evidence; never qualification."""
import argparse
import hashlib
import json
import re
from pathlib import Path

SOURCE = '33ffcf88a470a7f267838561d1b8f04d9b1f5820'
DEFAULT = Path(__file__).resolve().parents[1] / 'docs/evidence/native-rf3-current-repair-2026-11-11'


def verify(root=DEFAULT):
    root = Path(root)
    manifest = json.loads((root / 'manifest.json').read_text())
    actual_paths = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p.name != 'manifest.json'}
    assert set(manifest) == actual_paths, 'manifest must cover every retained file'
    for rel, digest in manifest.items():
        path = Path(rel)
        assert not path.is_absolute() and '..' not in path.parts, 'unsafe manifest path'
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == digest, f'changed evidence: {rel}'
    summary = json.loads((root / 'summary.json').read_text())
    assert summary['source_revision'] == SOURCE
    assert summary['scope'] == 'LOCAL_COMPONENT_TESTS_ONLY'
    assert summary['private_candidate'] is True and summary['ci_for_this_candidate'] == 'NOT_RUN'
    assert summary['qualification'] == {
        'physically_tested': False, 'production_qualified': False, 'deployed': False,
        'sustained_scale_out_proven': False, 'rpo': None, 'rto': None,
        'p50': None, 'p95': None, 'p99': None,
    }, 'local results cannot be promoted to physical qualification'
    names = []
    for phase in summary['phases'].values():
        result = json.loads((root / phase['result']).read_text())
        assert result['source_revision'] == SOURCE and result['exit_code'] == 0
        assert result['dirty_diff_sha256'] == hashlib.sha256(b'').hexdigest()
        assert result['guard_stop_reason'] is None and result['pause_episodes'] == 0
        prefix = phase['result'].removesuffix('-result.json')
        observed = re.findall(r'^test (\S+) \.\.\. ok$', (root / (prefix + '.stdout')).read_text(), re.M)
        assert observed == phase['tests'] and len(observed) == phase['count']
        names.extend(observed)
    assert len(names) == len(set(names)) == summary['test_count'] == 10
    stderr = (root / 'runs/final-normal-repair.stderr').read_text()
    assert f'Compiling bicdb-core v1.0.471-beta (/tmp/bicdb-native-current-repair/crates/bicdb-core)' in stderr, 'actual intended compile required'
    measured = json.loads(next(line.split('RF3_CURRENT_REPAIR_EVIDENCE ', 1)[1] for line in stderr.splitlines() if line.startswith('RF3_CURRENT_REPAIR_EVIDENCE ')))
    assert measured == summary['actual_repair_observation']
    assert [measured[k] for k in ['source_range_index', 'source_db_sequence', 'target_before_db_sequence', 'installed_target_local_db_sequence', 'cold_target_db_sequence']] == [1, 5, 20, 21, 21]
    assert measured['native_ready_after_adoption'] == 'REFUSED_RETAINED_IMAGE_INTENT'
    assert measured['physical_qualification'] is False and measured['other_range_imports_are_rf3_user_acks'] is False
    assert measured['selected_native_r0_unchanged_by_diagnostic_other_range_imports'] is True
    assert measured['source_data_leader'] != measured['metadata_leader']
    assert len(summary['failed_attempts']) == 2
    for attempt in summary['failed_attempts']:
        assert json.loads((root / attempt['result']).read_text())['exit_code'] == 101, 'retain unsuccessful attempts'
    for path, digest in summary['source_snapshots'].items():
        assert hashlib.sha256((root / 'source' / path).read_bytes()).hexdigest() == digest
    assert summary['test_binary']['retained_as_operator_binary'] is False
    return {'status': 'PASS', 'scope': summary['scope'], 'source_revision': SOURCE, 'distinct_tests': 10, 'physical_qualification': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root', type=Path, default=DEFAULT)
    print(json.dumps(verify(parser.parse_args().evidence_root), indent=2))
