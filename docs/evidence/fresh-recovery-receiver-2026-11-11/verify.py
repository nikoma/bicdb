#!/usr/bin/env python3
"""Verify retained local controls and exact source; does not rerun Rust tests."""
import argparse, gzip, hashlib, io, json, re, tarfile
from pathlib import Path


def sha(data): return hashlib.sha256(data).hexdigest()

def verify(root):
    root = Path(root)
    manifest = json.loads((root / 'MANIFEST.json').read_text())
    assert not any(p.is_symlink() for p in root.rglob('*')), 'symlinks are not retained evidence'
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p != root/'MANIFEST.json'}
    expected = {r['path'] for r in manifest}
    assert len(expected) == len(manifest) and actual == expected, 'exact manifest coverage'
    for row in manifest:
        p = root / row['path']
        assert not p.is_symlink() and p.stat().st_size == row['bytes'] and sha(p.read_bytes()) == row['sha256'], row['path']
    claims = json.loads((root / 'claims.json').read_text())
    assert claims['unique_tests'] == 44 and claims['test_executions'] == 45
    assert all(claims[k] is False for k in ['ci_tested','independent_machine_tested','production_qualified','deployed','later_source_apply_or_rpc_collection_tested'])
    assert sha(gzip.decompress((root/'compiled-change.patch.gz').read_bytes())) == claims['compiled_change_sha256']
    source = json.loads((root/'source-files.json').read_text())
    entries = {}
    with tarfile.open(fileobj=io.BytesIO((root/'source.tar.gz').read_bytes()), mode='r:gz') as archive:
        for member in archive:
            assert member.isfile() and member.name not in entries and not member.name.startswith('/') and '..' not in Path(member.name).parts
            data = archive.extractfile(member).read()
            entries[member.name] = {'path': member.name, 'bytes': len(data), 'sha256': sha(data)}
    assert source == [entries[k] for k in sorted(entries)], 'source archive exact membership/digest'
    for row in json.loads((root/'compiled-core-source.json').read_text()):
        assert entries[row['path']] == row, 'compiled source snapshot'
    unique, executions = set(), 0
    for row in claims['runs']:
        run = root/'runs'/row['run']
        result = json.loads((run/'result.json').read_text())
        inputs = json.loads((run/'inputs.json').read_text())
        assert all(result[k] == v for k,v in inputs.items())
        assert result['source_revision'] == claims['compiled_source_revision']
        assert result['dirty_diff_sha256'] == claims['compiled_change_sha256']
        assert result['exit_code'] == 0 and result['guard_stop_reason'] is None and result['pause_episodes'] == 0
        samples = [json.loads(line) for line in (run/'resources.jsonl').read_text().splitlines()]
        assert samples and result['minimum_available_bytes'] == min(s['available_bytes'] for s in samples)
        assert all(not s['paused'] and s['available_bytes'] >= 12*2**30 for s in samples)
        text = (run/'stdout').read_text()
        ids = re.findall(r'^test (\S+) \.\.\. ok$', text, re.M)
        summary = re.search(r'test result: ok\. (\d+) passed; 0 failed; 0 ignored;', text)
        assert summary and int(summary[1]) == len(ids) == row['executions'] and ids == row['test_ids']
        unique.update(ids); executions += len(ids)
    first = gzip.decompress((root/'runs/coherent-recovery-first-v1/stderr.gz').read_bytes()).decode()
    assert 'Compiling bicdb-core v1.0.466-beta (/tmp/bicdb-range-recovery-authority/crates/bicdb-core)' in first
    assert sorted(unique) == claims['test_ids'] and len(unique) == 44 and executions == 45
    return {'retained_results_verified': True, 'source_archive_verified': True, 'unique_local_tests': 44,
            'executions': 45, 'production_qualified': False, 'rust_tests_rerun': False}

if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('directory', nargs='?', default=Path(__file__).parent)
    print(json.dumps(verify(p.parse_args().directory), sort_keys=True))
