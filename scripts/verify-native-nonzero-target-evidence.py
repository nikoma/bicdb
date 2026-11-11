#!/usr/bin/env python3
"""Read-only replay of retained local component evidence; never qualification."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re

SOURCE = '8a7ddf6ead2bce6e3a41e04fdf68af1a7df3cf57'
BINARY = 'b11c62e01f769a25d78f0e0927dd784f874c9433427397a9d89daba3705a5747'
FAILURES = [
    '8bc0e8d85641af640a571ac3386796a93229c5e2',
    '96fdbe6ffb324a5cbb10490f121cb7376c64eaa5',
    'c8fd9076ee8ac192ccfa69db6547b55682706ced',
    'ff96c52da9d25ae685abec23cb3425481ed5d046',
]

def require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)

def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def verify(root: Path) -> dict:
    manifest = json.loads((root/'manifest.json').read_text())['files']
    for relative, expected in manifest.items():
        path = root/relative
        require(path.is_relative_to(root) and path.is_file(), f'missing evidence: {relative}')
        require(sha(path.read_bytes()) == expected, f'evidence hash differs: {relative}')
    s = json.loads((root/'summary.json').read_text())
    require(s['source_commit'] == SOURCE and s['source_version'] == '1.0.466-beta', 'source identity changed')
    require(s['status'] == 'PASS' and s['distinct_tests'] == 34, 'component result changed')
    for flag in ['ci_tested','separate_processes_tested','mtls_tested','physical_scale_certified','production_qualified','deployed','operator_binary_produced']:
        require(s[flag] is False, f'unsupported maturity upgrade: {flag}')
    require(all(value is None for value in s['metrics'].values()), 'unsupported measured metric')
    require(len(s['open_gates']) >= 7, 'open qualification gates erased')
    require(s['actual_native_copy_range_indices'] == [0,1,1025], 'actual copy scope changed')
    require(s['history_retention_control'] == {'source_range_index':1025,'raw_command_payload_compacted_through':1,'retained_raw_irrevocable_commands':1024,'history_entries':1026,'max_history_entries_per_step':256}, 'retention control changed')
    distinct = set()
    for phase, count in zip(s['phases'],[10,16,7,1], strict=True):
        receipt = json.loads((root/phase['receipt']).read_text())
        output = gzip.decompress((root/phase['log']).read_bytes()).decode()
        require(receipt['source_commit'] == SOURCE and receipt['exit_code'] == 0 and receipt['resource_stop_reason'] is None, 'successful source/run mismatch')
        names = re.findall(r'^test (\S+) \.\.\. ok$', output, re.M)
        require(len(names) == count and phase['tests'] == count, 'passing test count changed')
        require(f'test result: ok. {count} passed; 0 failed;' in output, 'successful result missing')
        require(not distinct.intersection(names), 'test counted twice')
        distinct.update(names)
    require(len(distinct) == 34, 'distinct count differs')
    require(any('actual_native_r1025_install' in name for name in distinct), 'retention test missing')
    require(any('actual_native_r1_install_uses_target_local_visibility' in name for name in distinct), 'target MVCC test missing')
    compile_log = gzip.decompress((root/'attempt-5-image-install.log.gz').read_bytes()).decode()
    require('Compiling bicdb-core v1.0.466-beta (/tmp/bicdb-native-range-image-install/crates/bicdb-core)' in compile_log, 'actual candidate compilation missing')
    identity = json.loads((root/s['test_binary_identity']).read_text())
    require(identity['source_commit'] == SOURCE and identity['binary_sha256'] == BINARY and identity['binary_bytes'] == 122254096, 'same executable identity changed')
    require(identity['prior_compile_receipt_sha256'] == sha((root/'attempt-5-image-install.result.json').read_bytes()), 'same-executable compilation link differs')
    require(len(s['retained_failed_attempts']) == 4, 'failures erased')
    for phase, source in zip(s['retained_failed_attempts'],FAILURES,strict=True):
        receipt = json.loads((root/phase['receipt']).read_text())
        output = gzip.decompress((root/phase['log']).read_bytes()).decode()
        require(receipt['source_commit'] == source and receipt['exit_code'] == 101 and receipt['resource_stop_reason'] is None, 'failed run relabeled')
        require('error' in output, 'failed evidence missing')
    for index in [2,4]:
        failed = gzip.decompress((root/f'attempt-{index}-image-install.log.gz').read_bytes()).decode()
        require('test result: FAILED. 8 passed; 1 failed;' in failed, 'failed assertions hidden')
    inputs = json.loads((root/'source-inputs.json').read_text())
    require(inputs['source_commit'] == SOURCE and len(inputs['files']) >= 25, 'source snapshot identity missing')
    for relative, info in inputs['files'].items():
        data = gzip.decompress((root/info['snapshot']).read_bytes())
        require(sha(data) == info['sha256'] and len(data) == info['bytes'], f'source snapshot differs: {relative}')
    return {'status':'PASS','source_commit':SOURCE,'distinct_tests':34,'physical_scale_certified':False,'production_qualified':False,'scope':'single-host local components; RF1 source R0/R1/R1025'}

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, default=Path(__file__).resolve().parents[1]/'docs/evidence/native-nonzero-target-install-2026-11-11')
    args = parser.parse_args()
    print(json.dumps(verify(args.evidence.resolve()),sort_keys=True))

if __name__ == '__main__':
    main()
