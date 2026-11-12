"""Cross-version control without modifying either production source or fixture bytes."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import time

ROOT = Path('/root/bicdb-durability-s3')
OUT = ROOT / 'diagnostics/native-decimal-predicate-upgrade-20261112'
OUT.mkdir(parents=True, exist_ok=False)
record = dict(status='checking_actual_helper_builds', application_qualified=False,
              source_modified=False, old_fixture_modified=False, started_unix=time.time())


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save():
    tmp = OUT / 'result.tmp'
    tmp.write_text(json.dumps(record, indent=2) + '\n')
    tmp.replace(OUT / 'result.json')


def inventory(path):
    return {str(p.relative_to(path)): dict(bytes=p.stat().st_size, sha256=sha(p))
            for p in sorted(path.rglob('*')) if p.is_file()}


def read_build(label, source):
    unit = 'bicdb-' + label
    state = dict(line.split('=', 1) for line in subprocess.check_output(
        ['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'Result'], text=True).splitlines())
    assert state['ActiveState'] not in ['active', 'activating', 'deactivating', 'reloading']
    assert state['Result'] == 'success', state
    receipt = ROOT / ('report-' + label + '.json')
    build = json.loads(receipt.read_text())
    assert build['source'] == source and build['status'] == 'focused_passed'
    assert len(build['steps']) == 1 and build['steps'][0]['exit'] == 0
    binary = Path(build['binary'])
    assert binary.parent == ROOT / 'qualification-artifacts-debug' / source
    assert binary.name == 'decimal_predicate_upgrade_probe'
    assert sha(binary) == build['binary_sha256']
    return binary, dict(source=source, binary=str(binary), binary_sha256=build['binary_sha256'],
                        receipt=str(receipt), receipt_sha256=sha(receipt), scope='helper build only')


def run(binary, action, path, label):
    environment = dict(os.environ, BICDB_COMMIT_DURABILITY='all-logs', BICDB_SYNC_OUTBOX='off')
    for flag in ['BICDB_PAGE_WAL_UNLOCKED_SYNC', 'BICDB_PAGE_DELTAS']:
        environment.pop(flag, None)
    result = subprocess.run([str(binary), action, str(path)], env=environment,
                            capture_output=True, text=True, timeout=120)
    (OUT / (label + '.stdout')).write_text(result.stdout)
    (OUT / (label + '.stderr')).write_text(result.stderr)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert len(rows) == 1, result.stdout
    return rows[0]


save()
old, old_identity = read_build('decimal-native-probe-old-v2', '1e6ea47c58dd1f3783640ee8747037855c3ad5b4')
new, new_identity = read_build('decimal-native-probe-candidate777-v2', 'b616aa25a2f468ad3647140dbb836086009dd9e8')
record.update(old=old_identity, candidate=new_identity, producer_sha256=sha(__file__))
before = run(old, 'create', OUT / 'old-db', 'old-creator')
assert before['status'] == 'observed_and_closed' and before['rows_before'] == before['rows_after'] == 1
assert len(before['definitions']) == 1 and before['definitions'][0]['predicate']['value_type'] == 'decimal'
record['old_observation'] = before
frozen = inventory(OUT / 'old-db')
record['closed_old_files'] = frozen
record['status'] = 'copying_closed_real_old_generation'
save()
shutil.copytree(OUT / 'old-db', OUT / 'candidate-copy')
assert inventory(OUT / 'candidate-copy') == frozen
after = run(new, 'verify', OUT / 'candidate-copy', 'candidate-reader')
record['candidate_observation'] = after
assert inventory(OUT / 'old-db') == frozen, 'old fixture changed'
assert sha(old) == old_identity['binary_sha256'] and sha(new) == new_identity['binary_sha256']
gap = (after['status'] == 'observed_and_closed'
       and after['predicate_matches_acknowledged_row'] is True
       and 'acknowledged' not in after['index_ids']
       and any(not report['valid'] and report['missing_entries'] > 0 for report in after['verification']))
record.update(status='upgrade_gap_reproduced' if gap else 'observations_collected',
              source_candidate='777e82cf829cbee94b881054a29b686a271a75b6',
              source_old='f43583da4c34209e810c5e6a8f5925831b2949a1',
              helper_source_only_adds_example=True, old_fixture_unchanged=True,
              processes_stopped=True, finished_unix=time.time())
save()
print(json.dumps(record), flush=True)
