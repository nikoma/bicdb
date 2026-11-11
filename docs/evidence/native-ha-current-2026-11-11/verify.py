#!/usr/bin/env python3
"""Verify retained test/log identity; this is not a production certifier."""
import gzip
import hashlib
import json
from pathlib import Path
import re


def main():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / 'sha256s.json').read_text())
    for relative, expected in manifest.items():
        path = root / relative
        assert path.is_file() and not path.is_symlink(), relative
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, relative
    result = json.loads((root / 'results.json').read_text())
    assert result['classification'] == 'source_regression_single_host'
    assert not result['production_qualified']
    assert not result['deployment_verified']
    assert not result['physical_scale_certified']
    actual_total = 0
    for run in result['runs']:
        assert run['source_commit'] == result['source_commit']
        assert run['exit_code'] == 0, run['label']
        text = gzip.decompress((root / run['raw_log']).read_bytes()).decode()
        actual = [list(map(int, match)) for match in re.findall(
            r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', text)]
        assert actual == run['test_result_counts'], run['label']
        actual_total += sum(row[0] for row in actual)
        assert all(row[1:] == [0, 0, 0, 0] for row in actual), run['label']
    assert actual_total == result['total_passed']
    print(json.dumps({'verified': True, 'source_commit': result['source_commit'],
                      'total_passed': actual_total, 'production_qualified': False,
                      'physical_scale_certified': False}))


if __name__ == '__main__':
    main()
