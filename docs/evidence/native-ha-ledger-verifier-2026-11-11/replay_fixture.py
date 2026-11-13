"""Replay the frozen artifact verifier and controls without running BicDB."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile

p = argparse.ArgumentParser()
p.add_argument('output', type=Path, help='a fresh destination for the rebound fixture')
a = p.parse_args()
base = Path(__file__).resolve().parent
output = a.output.resolve()
output.mkdir(exist_ok=False)
manifest = json.loads((base / 'fixture-manifest.json').read_text())
with tarfile.open(base / 'fixture.tar.gz', 'r:gz') as archive:
    extracted = set()
    for member in archive.getmembers():
        path = Path(member.name)
        assert member.isfile() and path.parts[0] == 'data'
        assert not path.is_absolute() and '..' not in path.parts
        relative = Path(*path.parts[1:])
        expected = manifest['files'][str(relative)]
        payload = archive.extractfile(member).read()
        assert len(payload) == expected['bytes']
        assert hashlib.sha256(payload).hexdigest() == expected['sha256']
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        extracted.add(str(relative))
assert extracted == set(manifest['files'])
original = (output / 'result.json').read_bytes()
(output / 'original-result.json').write_bytes(original)
result = json.loads(original)
assert result['source'] == manifest['source']
original_root = Path(manifest['original_root'])


def rebound(path):
    return str(output / Path(path).relative_to(original_root))


for case in result['cases']:
    case['ack_ledger'] = rebound(case['ack_ledger'])
    for stage in case['rounds']:
        for snapshot in [stage['successor'], *stage['nodes']]:
            snapshot['rows_file'] = rebound(snapshot['rows_file'])
(output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
subprocess.run([sys.executable, str(base / 'verify_native_ha_ack_artifacts_v2.py'), str(output)], check=True)
subprocess.run([sys.executable, str(base / 'control_native_ha_artifact_verifier.py'),
                '--artifacts', str(output), '--verifier', str(base / 'verify_native_ha_ack_artifacts_v2.py'),
                '--report', str(output / 'replayed-controls.json')], check=True)
