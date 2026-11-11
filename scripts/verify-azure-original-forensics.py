#!/usr/bin/env python3
"""Check portable original-run forensics; optional private inputs are static reads."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def check(condition, message):
    if not condition:
        raise ValueError(message)

def verify(directory, run_dir=None):
    directory = Path(directory)
    read = lambda name: json.loads((directory / name).read_text())
    manifest = read('manifest.json')
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*')
              if p.is_file() and p.name != 'manifest.json'}
    check(set(manifest['files']) == actual, 'manifest coverage mismatch')
    for name, expected in manifest['files'].items():
        path = directory / name
        check(not Path(name).is_absolute() and '..' not in Path(name).parts
              and path.is_file() and not path.is_symlink(), 'unsafe artifact path')
        check(sha(path.read_bytes()) == expected, 'artifact digest mismatch: ' + name)
    focus = read('two-ack-range-forensics.json')
    check(focus['runtime_source'] == 'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb', 'runtime changed')
    check([x['range_id'] for x in focus['focus']] == [41, 11], 'focus range mismatch')
    for row, index, leader in zip(focus['focus'], [74, 55], ['n2', 'n0']):
        commands = [c for n in row['original_archived_nodes'] for c in n['matching_retained_commands']]
        check(len(commands) == 4 and all(c['index'] == index and c['range_epoch'] == 11
            and c['declared_leader'] == leader and c['state'] == 'applied'
            and c['exact_expected_payload'] and c['resolved_watermark_covers_command'] for c in commands),
            'cold command projection mismatch')
        check(len(row['later_routed_reads']) == 5 and all(r['exact_expected_value'] for r in row['later_routed_reads']),
              'routed observation mismatch')
        check(row['original_per_key_read_result'].startswith('NOT RETAINED'), 'original response invented')
    progress = read('diagnostic-progress.json')
    check(progress['samples'] == len(progress['series']) == 68 and not progress['gate_passed'], 'policy result mismatch')
    check(progress['metric_summary']['active_relocations']['min'] == 48
          and progress['metric_summary']['active_relocations']['last'] == 61
          and progress['metric_summary']['failed_relocations']['last'] == 3, 'policy metrics mismatch')
    check(progress['observed_metadata_terms'] == [13] and progress['observed_metadata_leaders'] == ['n2'], 'metadata trace mismatch')
    failure = read('failure-sample-classification.json')
    check(failure['failed_logical_operation_count'] == 404 and not failure['is_http_status_404']
          and failure['retained_error_sample_count'] == sum(failure['sample_category_counts'].values()) == 20
          and failure['unretained_failed_operation_bodies'] == 384
          and not failure['all_404_failed_operations_individually_classified'], 'operation sample scope mismatch')
    statuses = read('finding-statuses.json')
    check(all(row['status'] in ['passed', 'failed', 'inconclusive', 'not_tested'] for row in statuses), 'invalid finding status')
    source = Path(__file__).with_name('project-azure-original-run-forensics.py')
    check(sha(source.read_bytes()) == read('input-provenance.json')['projector_sha256'], 'projector changed')
    if run_dir is not None:
        with tempfile.TemporaryDirectory(prefix='bicdb-original-forensics-') as temp:
            subprocess.run([sys.executable, str(source), '--run-dir', str(run_dir), '--output-dir', temp],
                           check=True, capture_output=True, text=True, timeout=60)
            for name in ['two-ack-range-forensics.json', 'diagnostic-progress.json',
                         'failure-sample-classification.json', 'input-provenance.json']:
                check(read(name) == json.loads((Path(temp) / name).read_text()), 'private-input recomputation mismatch: ' + name)
    return {'verified': True, 'original_run_passed': False, 'focus_ranges': [41, 11],
            'private_inputs_read': run_dir is not None, 'root_cause_proven': False, 'production_qualified': False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'docs/evidence/azure-original-run-forensics-2026-11-11')
    parser.add_argument('--run-dir', type=Path, help='Optional original private run directory; omitted means public artifacts only.')
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.evidence_dir, args.run_dir), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, 'original forensic verification failed: ' + str(error) + '\n')

if __name__ == '__main__':
    main()
