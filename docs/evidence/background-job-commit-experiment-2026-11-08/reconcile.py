#!/usr/bin/env python3
"""Reconcile saved diagnostic run records; never executes application tests.

Usage: python3 reconcile.py /path/to/bgjob-experiment [output-directory]
Only the 70 full-batch runs are included; two pilots are excluded.
"""
import hashlib
import json
from pathlib import Path
import statistics
import sys
import xml.etree.ElementTree as ET

source = Path(sys.argv[1])
out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent
out.mkdir(parents=True, exist_ok=True)
expected = {(v, p): (10 if v == 'a2' else 5)
            for v in ('a', 'a2', 'b', 'c', 'd', 'e')
            for p in ('remaining-08', 'remaining-14')}
allowed = ['batch', 'variant', 'plan', 'rep', 'binary_sha256', 'source_commit',
           'server_experimental_gates', 'runner_sha256', 'driver_sha256',
           'analysis_sha256', 'baseline_record', 'jdbc_adapter_sha256',
           'jdbc_provider_sha256', 'runner_returncode', 'run_status', 'wall_s',
           'selected_runtime_class_origins_matched', 'expected_invocations',
           'tests', 'failures', 'background_jobs', 'observed_process_gates',
           'overall', 'host', 'disposable_database_removed']
rows = []
xml_checked = 0
for path in sorted((source / 'runs').glob('*.json')):
    record = json.loads(path.read_text())
    if (record['variant'], record['plan']) not in expected:
        continue
    assert record['diagnostic_only'] and not record['application_qualified']
    assert record['run_status'] == 'tests_failed'
    assert record['selected_runtime_class_origins_matched']
    assert record['disposable_database_removed']
    assert record['tests'] == record['expected_invocations']
    assert record['tests'] == (97 if record['plan'] == 'remaining-08' else 94)
    assert len(record['failures']) == 1
    assert record['failures'][0]['method'] == 'testBackgroundJob'
    assert len(record['background_jobs']) == 1
    assert record['background_jobs'][0]['test_outcome'] == 'failure'
    observed = record.get('observed_process_gates')
    if observed is not None:
        assert not observed['jvm']
        assert observed['database'] == {'BICDB_SYNC_OUTBOX': 'off', **record['server_experimental_gates']}
    else:
        assert record['batch'] == 'bgjob-a-remaining-08-r1'
    # The original runner records identify the retained JUnit files. Cross-check
    # rather than interpreting a harness exit code of zero as an application pass.
    original = json.loads(Path(record['run_record']).read_text())
    junit_files = list(Path(record['base']).rglob('TEST-*.xml'))
    assert junit_files, record['batch']
    totals = {k: 0 for k in ('tests', 'failures', 'errors', 'skipped')}
    failures = []
    for junit in junit_files:
        root = ET.parse(junit).getroot()
        for key in totals:
            totals[key] += int(root.attrib.get(key, 0))
        for tc in root.findall('testcase'):
            for failure in list(tc.findall('failure')) + list(tc.findall('error')):
                failures.append((tc.get('classname'), tc.get('name')))
    assert totals == {'tests': record['tests'], 'failures': 1, 'errors': 0, 'skipped': 0}, (record['batch'], totals)
    assert failures == [(record['failures'][0]['cls'], 'testBackgroundJob')]
    xml_checked += 1
    rows.append({**{k: record.get(k) for k in allowed},
                 'record_file': path.name,
                 'record_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                 'junit_crosscheck': totals})
assert len(rows) == 70
for key, n in expected.items():
    group = [r for r in rows if (r['variant'], r['plan']) == key]
    assert len(group) == n, (key, len(group))
    assert len({r['rep'] for r in group}) == n
    assert len({r['binary_sha256'] for r in group}) == 1

summary = []
fields = ('test_time_s', 'job_censored_after_s', 'window_statement_ms_sum',
          'window_commit_throughput_per_s')
for variant, plan in sorted(expected):
    group = [r for r in rows if (r['variant'], r['plan']) == (variant, plan)]
    jobs = [r['background_jobs'][0] for r in group]
    med = lambda vals: round(statistics.median(vals), 3)
    summary.append(dict(variant=variant, plan=plan, runs=len(group), passes=0,
                        source_commit=group[0]['source_commit'],
                        binary_sha256=group[0]['binary_sha256'],
                        medians={**{k: med([j[k] for j in jobs]) for k in fields},
                                 **{f'commit_{k}': med([j['window_commits'][k] for j in jobs])
                                    for k in ('n', 'p50', 'p95', 'p99', 'sum_ms')}}))
result = dict(format=1, diagnostic_only=True, application_qualified=False,
              production_promotion=False, full_batch_runs=len(rows),
              junit_records_crosschecked=xml_checked,
              target_tests_passed=0, target_tests_failed=len(rows),
              other_invocations_passed=sum(r['tests'] - 1 for r in rows),
              summary=summary)
(out / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
(out / 'runs.json').write_text(json.dumps(rows, indent=2) + '\n')
lines = ['| Variant | Batch | Runs | Passes | COMMIT p50/p95/p99 ms | COMMIT ms in job window | Total DB ms in job window |',
         '|---|---|---:|---:|---|---:|---:|']
for r in summary:
    m = r['medians']
    lines.append(f"| {r['variant'].upper()} | {r['plan']} | {r['runs']} | 0 | {m['commit_p50']}/{m['commit_p95']}/{m['commit_p99']} | {m['commit_sum_ms']} | {m['window_statement_ms_sum']} |")
(out / 'results-table.md').write_text('\n'.join(lines) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'summary'}))
