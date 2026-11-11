#!/usr/bin/env python3
"""Check retained positive characterizations AND intentionally failed strong target."""
import argparse
import hashlib
import json
from pathlib import Path
import re

def require(condition, message):
    if not condition:
        raise ValueError(message)

def verify(directory):
    directory = Path(directory)
    read = lambda name: json.loads((directory / name).read_text())
    manifest = read('manifest.json')
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*')
              if p.is_file() and p.name != 'manifest.json'}
    require(set(manifest['files']) == actual, 'manifest coverage mismatch')
    for name, digest in manifest['files'].items():
        path = directory / name
        require(not Path(name).is_absolute() and '..' not in Path(name).parts
                and path.is_file() and not path.is_symlink(), 'unsafe artifact path')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, 'artifact checksum mismatch: ' + name)
    summary, build, green, red = [read(name) for name in ['summary.json','build.json','characterization.json','target-red.json']]
    require(summary['runtime_source'] == build['source_commit'] == 'd49a391b3c6157f90f29e9f2f5214143c60f9f4b', 'baseline source changed')
    require(summary['runtime_version'] == '1.0.463-beta' and summary['core_rlib_sha256'] == build['libraries']['bicdb_core']
            == 'dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356', 'linked core changed')
    fixture = Path(__file__).parent / 'experiments/strong-read-characterization.rs'
    require(hashlib.sha256(fixture.read_bytes()).hexdigest() == summary['source_fixture_sha256'] == build['source_sha256'], 'executed fixture/source mismatch')
    require(build['exit_code'] == 0 and not build['engine_recompiled'], 'unexpected engine rebuild or link failure')
    require(min(row['available_bytes'] for row in build['memory_samples']) >= 12 * 1024**3, 'memory headroom violated')
    require(summary['binary_sha256'] == green['binary_sha256'] == red['binary_sha256'], 'test artifacts differ')
    require(green['exit_code'] == 0 and summary['characterization_test_count'] == 2, 'legacy characterization result changed')
    require(red['exit_code'] == 101 and red['expected_current_failure'] and not red['feature_implemented']
            and summary['explicit_strong_healthy_positive_exit_code'] == 101, 'unimplemented positive target promoted to pass')
    require(not summary['explicit_strong_get_implemented'] and not summary['production_qualified']
            and not summary['physical_ha_qualification'] and not summary['original_Azure_cause_proven'], 'unsupported qualification claim')
    observations = [json.loads(match) for match in re.findall(r'(\{"case":.*\})', (directory/'characterization.stdout').read_text())]
    by_case = {row['case']:row for row in observations}
    require(by_case['old_recovered_minority']['legacy_absent'] and by_case['old_recovered_minority']['new_quorum_commit_completed'], 'minority characterization absent')
    caught = by_case['new_leader_catchup']
    require(caught['legacy_absent_before'] and caught['exact_value_after'] and caught['authority_epoch'] == 2
            and caught['retained_command_epoch'] == 1 and caught['resolved_through'] == 1, 'catch-up/healthy epoch distinction absent')
    require('unknown variant `strong_get`' in (directory/'target-red.stderr').read_text(), 'strong unsupported evidence absent')
    for name, provenance in read('provenance.json').items():
        require(hashlib.sha256((directory/name).read_bytes()).hexdigest() == provenance['exported_sha256'], 'export provenance mismatch')
    return {'evidence_verified':True, 'legacy_characterizations_passed':2,
            'strong_healthy_target_passed':False, 'strong_healthy_target_exit_code':101,
            'production_qualified':False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir',type=Path,default=Path(__file__).resolve().parents[1]/'docs/evidence/strong-read-characterization-2026-11-11')
    args=parser.parse_args()
    try:
        print(json.dumps(verify(args.evidence_dir),sort_keys=True))
    except (OSError,ValueError,KeyError,TypeError) as error:
        parser.exit(1,'characterization evidence check failed: '+str(error)+'\n')

if __name__=='__main__':
    main()
