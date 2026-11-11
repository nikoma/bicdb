#!/usr/bin/env python3
"""Verify portable catalog observations; private archives are read only if supplied."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

def sha(data):
    return hashlib.sha256(data).hexdigest()

def require(condition, message):
    if not condition:
        raise ValueError(message)

def verify(directory, ledger_dir, archive_dir=None):
    directory, ledger_dir = Path(directory), Path(ledger_dir)
    manifest = json.loads((directory / 'manifest.json').read_text())
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*')
              if p.is_file() and p.name != 'manifest.json'}
    require(set(manifest['files']) == actual, 'manifest coverage mismatch')
    for name, digest in manifest['files'].items():
        path = directory / name
        require(not Path(name).is_absolute() and '..' not in Path(name).parts
                and path.is_file() and not path.is_symlink(), 'unsafe artifact path')
        require(sha(path.read_bytes()) == digest, 'artifact checksum mismatch: ' + name)
    summary = json.loads((directory / 'summary.json').read_text())
    details_path = directory / 'per-ack-catalog-observations.json.gz'
    require(sha(details_path.read_bytes()) == summary['per_ack_observations_sha256'], 'detail digest mismatch')
    details = json.loads(gzip.decompress(details_path.read_bytes()))
    by_id = {row['record_id_sha256']: row for row in details}
    require(len(by_id) == len(details) == 5027, 'ACK observation count/uniqueness mismatch')
    all_expected = set()
    for phase, count in [('steady-4', 3804), ('growth-4-to-5', 1223)]:
        ledger = json.loads((ledger_dir / (phase + '-ledger.json')).read_text())
        require(len(ledger['keys']) == len(set(ledger['keys'])) == count, 'ledger count mismatch')
        expected_hash = sha(json.dumps({'run': ledger['run'], 'payload': 'x' * ledger['payload_bytes']},
            ensure_ascii=False, separators=(',', ':')).encode())
        phase_ids = {sha((ledger['namespace'] + '/' + key).encode()) for key in ledger['keys']}
        require(not all_expected.intersection(phase_ids), 'overlapping phase identities')
        all_expected.update(phase_ids)
        for identity in phase_ids:
            require(identity in by_id, 'known ACK identity missing')
            row = by_id[identity]
            require(row['expected_metadata_sha256'] == expected_hash, 'expected payload hash mismatch')
            good = [x for x in row['matching_catalog_observations']
                    if x['exact'] and x['observed_metadata_sha256'] == expected_hash]
            require(any(x['state'] == 'applied' and x['resolved_through_covers_index'] for x in good),
                    'ACK lacks matching Applied/resolved catalog observation')
        require(summary['phases'][phase]['ack_count'] == count, 'phase summary count mismatch')
    require(set(by_id) == all_expected, 'unexpected ACK identity')
    require(summary['all_known_acknowledgments']['ack_count'] == 5027
            and summary['all_known_acknowledgments']['with_exact_payload_in_any_applied_command'] == 5027
            and summary['all_known_acknowledgments']['with_exact_payload_in_certified_command_at_or_below_resolved_watermark'] == 5027,
            'summary exceeds observed Applied/resolved coverage')
    require(not summary['row_visibility_verified'] and not summary['quorum_certificate_signatures_independently_verified']
            and not summary['physical_host_independence_verified'] and not summary['production_qualified'], 'unsupported qualification claim')
    require(len(summary['nodes']) == 5 and all(n['catalog_checksum_valid'] and n['watermark_and_entry_index_checks_passed']
            and n['ack_identity_payload_mismatch_occurrences'] == 0 for n in summary['nodes']), 'catalog integrity summary inconsistent')
    script = Path(__file__).with_name('reconcile-azure-cold-range-catalogs.py')
    require(sha(script.read_bytes()) == manifest['reconciliation_script_sha256'], 'reconciliation helper changed')
    if archive_dir is not None:
        with tempfile.TemporaryDirectory(prefix='bicdb-cold-catalog-recheck-') as temporary:
            argv = [sys.executable, str(script), '--archive-dir', str(archive_dir), '--output-dir', temporary]
            for focus in summary['original_two_unverified']:
                argv.extend(['--focus-record-id-sha256', focus['record_id_sha256']])
            subprocess.run(argv, check=True, capture_output=True, text=True, timeout=60)
            recomputed = json.loads((Path(temporary) / 'summary.json').read_text())
            require(recomputed == summary, 'private-archive recomputation differs from exported summary')
            require(json.loads(gzip.decompress((Path(temporary) / details_path.name).read_bytes())) == details,
                    'private-archive per-ACK recomputation differs')
    return {'verified': True, 'acknowledgments': 5027, 'private_archives_read': archive_dir is not None,
            'actual_row_visibility_verified': False, 'production_qualified': False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[1]
    parser.add_argument('--evidence-dir', type=Path, default=root / 'docs/evidence/azure-cold-catalog-reconciliation-2026-11-11')
    parser.add_argument('--ledger-dir', type=Path, default=root / 'docs/evidence/azure-long-growth-failure-2026-11-11')
    parser.add_argument('--archive-dir', type=Path, help='Optional original private archive directory; omitted means no private archive reads.')
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.evidence_dir, args.ledger_dir, args.archive_dir), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, 'cold catalog evidence verification failed: ' + str(error) + '\n')

if __name__ == '__main__':
    main()
