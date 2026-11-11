"""Verify portable build records, optionally checking retained executable bytes."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path)
args = parser.parse_args()
for name, expected in json.loads((ROOT / 'portable-sha256s.json').read_text()).items():
    assert Path(name).name == name
    assert digest(ROOT / name) == expected, name
artifact = json.loads((ROOT / 'artifact.json').read_text())
manifest = json.loads((ROOT / 'CAPABILITIES.json').read_text())
result = json.loads((ROOT / 'result.json').read_text())
assert artifact['source_revision'] == result['source_commit'] == manifest['artifact']['source_revision']
assert artifact['version'] == 'bicdb ' + manifest['artifact']['version']
assert artifact['exercised_binary']['sha256'] == manifest['artifact']['binary_sha256']
assert artifact['capability_manifest_sha256'] == digest(ROOT / 'CAPABILITIES.json')
assert result['exit_code'] == 0 and result['resource_stop_reason'] is None
assert result['resource_pause_count'] == 0 and result['min_available_bytes'] >= 10 * 1024**3
assert artifact['qualified_multi_range_admission_check']['exit_code'] == 2
assert artifact['qualified_multi_range_admission_check']['result']['allowed'] is False
assert artifact['production_qualified'] is False and artifact['published_release'] is False
for claims in manifest['modes'].values():
    for claim in claims.values():
        for level in ('tested', 'qualified', 'deployed'):
            assert claim[level] == {'status': 'not_established', 'evidence': []}
if args.binary:
    assert digest(args.binary) == artifact['exercised_binary']['sha256']
    assert args.binary.stat().st_size == artifact['exercised_binary']['bytes']
print(json.dumps({'verified': True, 'binary_checked': bool(args.binary),
                  'source_revision': artifact['source_revision'],
                  'version': manifest['artifact']['version'], 'production_qualified': False}))
