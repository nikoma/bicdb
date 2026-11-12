"""Original PG remaining-06 diagnostic on the private partition composition.

No test, assertion, bytecode, timeout or provider changes. This is a debug
application probe, not official qualification of a merged release.
"""
import argparse
import fcntl
import hashlib
import re
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path('/root/bicdb-durability-s3')
STATE = Path('/root/.local/state/bicdb/idempiere-reference')
args = argparse.ArgumentParser()
args.add_argument('--source', required=True)
args.add_argument('--build-label', required=True)
args.add_argument('--spec-sha256', required=True)
args.add_argument('--development-version', required=True)
args.add_argument('--run-suffix', required=True)
opts = args.parse_args()
SOURCE = opts.source
assert re.fullmatch('[a-f0-9]{40}', SOURCE)
assert re.fullmatch('[a-z0-9-]+', opts.build_label)
assert re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+-beta', opts.development_version)
assert re.fullmatch('[a-z0-9-]+', opts.run_suffix)
LABEL = 'private-partition-' + SOURCE[:8] + '-mtable95' + '-' + opts.run_suffix
BUILD_UNIT = 'bicdb-' + opts.build_label
OUTPUT = ROOT / ('report-' + LABEL + '.json')
assert not OUTPUT.exists()
record = dict(source=SOURCE, development_version=opts.development_version, status='waiting_debug_build',
              diagnostic_unmerged=True, application_qualified=False, release_published=False,
              deployed=False, assertions_bytecode_waits_unchanged=True, started_unix=time.time())


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save():
    tmp = OUTPUT.with_suffix('.tmp')
    tmp.write_text(json.dumps(record, indent=2) + '\n')
    tmp.replace(OUTPUT)


def files(path):
    return {str(p.relative_to(path)): [p.stat().st_size, p.stat().st_mtime_ns] for p in path.rglob('*') if p.is_file()}


def closed(path):
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            links = [proc/'cwd', proc/'exe', *list((proc/'fd').iterdir())]
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        for fd in links:
            try:
                value = os.readlink(fd)
            except OSError:
                continue
            assert value != str(path) and not value.startswith(str(path) + '/'), ('baseline_open', str(proc))


save()
try:
    while subprocess.check_output(['systemctl', 'show', BUILD_UNIT, '-p', 'ActiveState', '--value'], text=True).strip() in ['active', 'activating', 'reloading', 'deactivating']:
        time.sleep(5)
    build_path = ROOT / ('report-' + opts.build_label + '.json')
    build = json.loads(build_path.read_text())
    assert build['source'] == SOURCE and build['status'] == 'focused_passed'
    spec_path = ROOT/'host-control'/(opts.build_label+'.json')
    assert sha(spec_path) == opts.spec_sha256
    spec = json.loads(spec_path.read_text())
    assert spec['source'] == SOURCE and spec['label'] == opts.build_label
    assert len(build['steps']) == len(spec['commands']) and len(build['steps']) > 0
    assert all(step['exit'] == 0 for step in build['steps'])
    assert [step['command'] for step in build['steps']] == spec['commands']
    assert build['steps'][-1]['command'] == ['cargo', '+1.96.0', 'build', '--locked', '-j', '1', '-p', 'bicdb-pgwire', '--example', 'oracle_server']
    artifact = build
    assert Path(artifact['binary']).parent == ROOT/'qualification-artifacts-debug'/SOURCE
    assert sha(artifact['binary']) == artifact['binary_sha256']
    record.update(status='waiting_exclusive_application_lock')
    save()
    application_lock = (ROOT/'final-qualification.lock').open('a')
    fcntl.flock(application_lock, fcntl.LOCK_EX)
    record.update(exclusive_application_lock=str(ROOT/'final-qualification.lock'))
    prior = json.loads((STATE/'pg-provider/main475-e098c26e-full-qualification.json').read_text())
    assert prior['status'] == 'finished' and prior['source'] == 'e098c26e846181ce1f3e6e9cca7f98e12a2af05e'
    baseline_path = Path(prior['baseline_record'])
    baseline = json.loads(baseline_path.read_text())
    assert baseline['status'] == 'baseline_verified'
    assert baseline['counts_match'] and baseline['declared_column_type_attributes_match'] and baseline['deterministic_values_match']
    parent = Path(baseline['database_path']).resolve()
    closed(parent)
    before = files(parent)
    runner = STATE/'pg-provider/run_pg_batch.py'
    plan_path = STATE/'original-pristine-remaining-06-release-class-plan.json'
    plan = json.loads(plan_path.read_text())
    assert plan['expected_invocations'] == 95
    assert any(row['name'] == 'org.idempiere.test.base.MTableTest' for row in plan['classes'])
    frozen = {str(p): sha(p) for p in [runner, plan_path, baseline_path, build_path, Path(artifact['binary'])]}
    record.update(status='running_original_remaining06', binary=artifact['binary'], binary_sha256=artifact['binary_sha256'],
                  debug_build_receipt=str(build_path), baseline=str(baseline_path), baseline_sha256=sha(baseline_path),
                  protected_baseline_closed=True, original_classes=plan['classes'], expected_invocations=95,
                  inputs_sha256=frozen, sql_profile='postgres', commit_durability='all-logs', sync_outbox='off',
                  experimental_page_flags_enabled=False)
    save()
    environment = dict(os.environ, BICDB_PG_BASELINE_RECORD=str(baseline_path), BICDB_PG_BINARY=artifact['binary'],
                       BICDB_COMMIT_DURABILITY='all-logs', BICDB_SYNC_OUTBOX='off',
                       BICDB_COMPAT_QUERY_LOG=str(ROOT/('tc-step-'+LABEL+'-queries.log')))
    for key in ['BICDB_PAGE_WAL_UNLOCKED_SYNC', 'BICDB_PAGE_DELTAS', 'BICDB_QUERY_QUEUE_WAIT_TRACE']:
        environment.pop(key, None)
    with (ROOT/('tc-step-'+LABEL+'-driver.log')).open('xb') as log:
        code = subprocess.call([str(ROOT/'host-control/venv/bin/python3'), str(runner), 'remaining-06', LABEL],
                               env=environment, stdout=log, stderr=subprocess.STDOUT)
    run_path = STATE/'pg-provider/runs'/(LABEL+'-test-run.json')
    result = json.loads(run_path.read_text())
    assert result['processes_stopped'] and result['expected_test_counts_matched'] and result['selected_classes_all_reported']
    assert result['binary_sha256'] == artifact['binary_sha256']
    assert result['test_assertions_or_bytecode_changed'] is False
    assert result['tests'] == 95
    assert all(sha(p) == digest for p, digest in frozen.items())
    closed(parent)
    assert before == files(parent), 'original verified baseline changed'
    record.update(status='finished', exit=code, tests=result['tests'], failures=result['failures'],
                  errors=result['errors'], skipped=result['skipped'], original_tests_all_passed=result['tests_success'],
                  result=str(run_path), result_sha256=sha(run_path), baseline_unchanged=True,
                  processes_stopped=True, finished_unix=time.time())
    save()
except BaseException as error:
    record.update(status='failed', error=repr(error), failed_unix=time.time())
    save()
    raise
print(json.dumps(record), flush=True)
raise SystemExit(0 if record.get('original_tests_all_passed') else 1)
