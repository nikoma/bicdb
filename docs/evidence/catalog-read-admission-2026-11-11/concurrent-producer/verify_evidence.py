"""Verify retained evidence integrity and source-scoped execution counts."""
import gzip
import hashlib
import json
from pathlib import Path
import re

root = Path(__file__).resolve().parent
manifest = json.loads((root / 'manifest.json').read_text())
for name, expected in manifest['files'].items():
    assert Path(name).name == name
    data = (root / name).read_bytes()
    assert len(data) == expected['bytes'], name
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name

for batch in manifest['batches']:
    spec = json.loads((root / batch['spec']).read_text())
    receipt = json.loads((root / batch['receipt']).read_text())
    summary = json.loads((root / batch['summary']).read_text())
    assert re.fullmatch('[0-9a-f]{40}', batch['source'])
    assert spec['source'] == receipt['source'] == summary['source'] == batch['source']
    assert receipt['status'] == summary['status'] == batch['status']
    log = gzip.decompress((root / batch['log_gz']).read_bytes())
    assert hashlib.sha256(log).hexdigest() == batch['raw_log_sha256']
    counts = re.findall(
        rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log
    )
    totals = [sum(int(row[index]) for row in counts) for index in range(3)]
    assert totals == [batch['passed'], batch['failed'], batch['ignored']], batch['label']
    if batch['status'] == 'focused_passed':
        assert len(receipt['steps']) == len(spec['commands'])
        assert all(step['exit'] == 0 for step in receipt['steps'])
        assert totals[1] == 0
    else:
        assert any(step['exit'] != 0 for step in receipt['steps'])
    print(batch['source'][:8], batch['status'], 'passed/failed/ignored', totals)

print('Integrity and recorded counts verified; broader acceptance remains unproven.')
