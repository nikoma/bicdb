"""Exercise semantic controls on copies of completed native HA artifacts.

The source ledgers, rowsets and native databases are never modified or opened.
Recomputed hashes ensure a refusal tests ledger reconciliation, not corruption.
"""
import argparse
import hashlib
import json
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument('--artifacts', type=pathlib.Path, required=True)
p.add_argument('--verifier', type=pathlib.Path, required=True)
p.add_argument('--report', type=pathlib.Path, required=True)
a = p.parse_args()
assert not a.report.exists()
original = json.loads((a.artifacts / 'result.json').read_text())
assert original['status'] == 'passed'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


report = dict(source=original['source'], verifier=str(a.verifier), verifier_sha256=sha(a.verifier),
              original_result_sha256=sha(a.artifacts / 'result.json'),
              status='running', started_unix=time.time(), engine_opened=False, cases=[])


def save():
    a.report.write_text(json.dumps(report, indent=2) + '\n')


def fixture(root):
    data = json.loads(json.dumps(original))
    for case in data['cases']:
        old_ledger = pathlib.Path(case['ack_ledger'])
        assert sha(old_ledger) == case['ack_ledger_sha256']
        folder = root / ('voters-' + str(case['voters']))
        folder.mkdir()
        ledger = folder / 'acknowledged.sqlite'
        shutil.copy2(old_ledger, ledger)
        case['ack_ledger'] = str(ledger)
        for node in old_ledger.parent.glob('n*/format.json'):
            target = folder / node.parent.name / 'format.json'
            target.parent.mkdir()
            shutil.copy2(node, target)
        for stage in case['rounds']:
            for snapshot in [stage['successor'], *stage['nodes']]:
                old_path = pathlib.Path(snapshot['rows_file'])
                assert sha(old_path) == snapshot['rows_sha256']
                path = folder / old_path.name
                shutil.copy2(old_path, path)
                snapshot['rows_file'] = str(path)
    return data


def rewrite(snapshot, rows):
    path = pathlib.Path(snapshot['rows_file'])
    path.write_text(json.dumps(sorted(rows.items()), indent=2) + '\n')
    snapshot.update(rows_sha256=sha(path), observed=len(rows))


save()
try:
    for control, expected in [('unchanged', 0), ('lost_ack_with_zero_declared_expectations', 1),
                              ('changed_ack_value', 1), ('readonly_refusal_present', 1),
                              ('uncertain_error_write_present', 0)]:
        with tempfile.TemporaryDirectory(prefix='ha-verifier-control-') as directory:
            root = pathlib.Path(directory)
            data = fixture(root)
            case = data['cases'][0]
            with sqlite3.connect(case['ack_ledger']) as db:
                acknowledged_id, acknowledged_value = db.execute('SELECT id,value FROM ack ORDER BY id LIMIT 1').fetchone()
                refused_id, refused_value = db.execute("SELECT id,value FROM attempts WHERE status='error' AND sqlstate='25006' ORDER BY id LIMIT 1").fetchone()
                if control == 'uncertain_error_write_present':
                    uncertain_id = db.execute('SELECT max(id)+100 FROM attempts').fetchone()[0]
                    db.execute('INSERT INTO attempts VALUES(?,?,?,?,?)',
                               (uncertain_id, 'uncertain-value', 'round0-after', 'error', '57014'))
                    db.commit()
            case['ack_ledger_sha256'] = sha(pathlib.Path(case['ack_ledger']))
            if control == 'lost_ack_with_zero_declared_expectations':
                for stage in case['rounds']:
                    for snapshot in [stage['successor'], *stage['nodes']]:
                        rows = dict(json.loads(pathlib.Path(snapshot['rows_file']).read_text()))
                        assert acknowledged_id in rows
                        del rows[acknowledged_id]
                        snapshot['acknowledged'] = 0
                        rewrite(snapshot, rows)
            elif control != 'unchanged':
                snapshot = case['rounds'][0]['nodes'][0]
                rows = dict(json.loads(pathlib.Path(snapshot['rows_file']).read_text()))
                if control == 'changed_ack_value':
                    rows[acknowledged_id] = acknowledged_value + ':changed'
                elif control == 'readonly_refusal_present':
                    assert refused_id not in rows
                    rows[refused_id] = refused_value
                elif control == 'uncertain_error_write_present':
                    rows[uncertain_id] = 'uncertain-value'
                rewrite(snapshot, rows)
            (root / 'result.json').write_text(json.dumps(data, indent=2) + '\n')
            result = subprocess.run([sys.executable, str(a.verifier), str(root)], capture_output=True, text=True)
            item = dict(control=control, expected='accept' if expected == 0 else 'reject',
                        exit=result.returncode, stdout=result.stdout[-2000:], stderr=result.stderr[-2000:])
            item['matched_expectation'] = (result.returncode == 0) == (expected == 0)
            report['cases'].append(item)
            save()
    report.update(status='passed' if all(c['matched_expectation'] for c in report['cases']) else 'failed', finished_unix=time.time())
    save()
    print(json.dumps(report), flush=True)
    raise SystemExit(0 if report['status'] == 'passed' else 1)
except Exception as error:
    report.update(status='failed', error=repr(error), finished_unix=time.time())
    save()
    raise
