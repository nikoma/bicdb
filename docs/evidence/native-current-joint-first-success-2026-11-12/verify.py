#!/usr/bin/env python3
"""Offline log/artifact receipt checks, not a build or database verifier."""
import argparse
import copy
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parent
SOURCE = 'eedf23d58e236cf62d61366349ad4de3bd52373b'
RESULT = re.compile(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9.]+)s')

def require(condition, message):
    if not condition:
        raise ValueError(message)

def validate_summary(s, logs):
    require(s['source_revision'] == SOURCE and s['source_version'] == '1.0.489-beta' and s['wire_protocol'] == 28, 'source identity')
    expected = [(43319425823, 131535932600, 2, 955, 100.89), (43319432266, 131535951702, 1, 956, 98.32)]
    ids = []
    for cohort, (run, job, count, filtered, seconds) in zip(s['runtime_cohorts'], expected):
        require((cohort['run_id'], cohort['core_job_id']) == (run, job), 'job identity')
        text = logs[cohort['log']]
        matches = RESULT.findall(text)
        require(matches == [('ok', str(count), '0', '0', '0', str(filtered), str(seconds))], 'actual runtime result')
        r = cohort['result']
        require((r['status'], r['passed'], r['failed'], r['ignored'], r['filtered'], r['seconds']) == ('ok', count, 0, 0, filtered, seconds), 'reported runtime result')
        passed = re.findall(r'\btest (db::\S+) \.\.\. ok', text)
        require(passed == cohort['test_ids'] and len(passed) == count, 'full test IDs')
        ids.extend(passed)
    require(len(s['runtime_cohorts']) == 2 and len(ids) == len(set(ids)) == s['unique_passed_tests'] == 3 and sorted(ids) == s['unique_passed_test_ids'], 'unique test count')
    require(len(s['compile_checks']) == 2 and {c['job_id'] for c in s['compile_checks']} == {131535889066, 131535911651}, 'compile scope')
    for c in s['compile_checks']:
        require(c['workspace_all_targets'] == c['isolated_consumer_no_core_defaults'] == 'PASS', 'compile result')
    a = s['actual_anchor']
    require(a == {'node':'n3','physical_ids':['n0','n1','n2'],'protected_effective_ids':['n1','n2','n3'],'observer_ids':['n1','n2','n3'],'historical_R':3,'current_R_after_ACK4':4,'observed_anchor_local_B':2,'ready_grant':False}, 'historical anchor contract')
    for text in [logs['positive.log.gz'], logs['archive-control.log.gz']]:
        n3 = [line for line in text.splitlines() if 'ACTUAL_JOINT_ACTIVATED_ANCHOR_MEMBERS node=n3 ' in line]
        require(n3 and all('R=3 ' in line and 'B=2 ' in line and 'physical_members=[ClusterNodeId("n0"), ClusterNodeId("n1"), ClusterNodeId("n2")]' in line and 'protected_effective_members=[ClusterNodeId("n1"), ClusterNodeId("n2"), ClusterNodeId("n3")]' in line and 'actual_observer_members=[ClusterNodeId("n1"), ClusterNodeId("n2"), ClusterNodeId("n3")]' in line for line in n3), 'actual physical/effective member diagnostics')
    d = s['archive_control']
    require(d['exact_error'] == 'immutable joint activated serving archive is missing' and d['receiver_remaining_ms'] == 3890 and d['original_parent_live'] and not d['Strong_after_corruption_qualified'], 'archive refusal contract')
    require(d['actual_native_WAL_delete'] and d['current_pointer_unchanged'] and d['full_R4_H_B_UID_rows_slots_raw_3ACKs_unchanged'], 'actual archive fault facts')
    require(any('remaining_ms=Some(3890)' in line and d['exact_error'] in line for line in logs['archive-control.log.gz'].splitlines()), 'actual live receiver refusal')
    require('ACTUAL_CURRENT_JOINT_N3_ANCHOR_ARCHIVE_CONTROL all3Activate=true realACK4StrongPrerequisite=true currentR=4 anchorR=3 actualArchiveDeleted=true readonlyReplyRefused=true actualDataUnchanged=true' in logs['archive-control.log.gz'], 'actual fault terminal')
    sibling = s['sibling_failure']
    require(sibling['source_revision'] == SOURCE and sibling['run_id'] == 43319438217 and sibling['core_job_id'] == 131535993682, 'sibling source identity')
    require(RESULT.findall(logs[sibling['log']]) == [('FAILED','0','4','0','0','953','81.00')], 'sibling actual failure')
    require(sibling['result']['passed'] == 0 and sibling['result']['failed'] == 4 and sibling['NativeProbe_events_contexts_spans'] == 0 and not sibling['CurrentJoint_or_fault_cuts_reached'] and not sibling['observer_cost_measured'], 'sibling precondition limit')
    require('NATIVE_PROBE_' not in logs[sibling['log']], 'zero probe traces')
    for key in ['whole_candidate_qualified','operator_or_autonomous_4_to_5_qualified','physical_or_8GiB_qualified','deployed','engine_opened_or_replayed_for_receipt','full_source_reconstruction_included']:
        require(s[key] is False, 'qualification/custody limit: ' + key)
    require(all(a['workflow_files'] == 4 and a['database_custody_retained'] is False for a in s['success_artifacts']), 'successful artifact custody')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--repo')
    p.add_argument('--self-test', action='store_true')
    args = p.parse_args()
    for row in (ROOT/'MANIFEST.sha256').read_text().splitlines():
        digest, name = row.split('  ', 1)
        require(hashlib.sha256((ROOT/name).read_bytes()).hexdigest() == digest, 'manifest: ' + name)
    s = json.loads((ROOT/'result.json').read_text())
    logs = {}
    for name, metadata in s['raw_logs'].items():
        raw = gzip.decompress((ROOT/'raw'/name).read_bytes())
        require(len(raw) == metadata['raw_bytes'] and hashlib.sha256(raw).hexdigest() == metadata['raw_sha256'], 'complete raw: ' + name)
        text = raw.decode()
        require(text.lower().count('unexpected end of file') == metadata['eof_diagnostics'] and text.lower().count('broken pipe') == metadata['broken_pipe_diagnostics'], 'diagnostics: ' + name)
        require(not re.search(r'-----BEGIN [A-Z ]*PRIVATE KEY-----|\bgh[oprsu]_[A-Za-z0-9]{20,}\b', text), 'private material in log')
        logs[name] = text
    validate_summary(s, logs)
    for artifact in s['success_artifacts']:
        path = ROOT/artifact['path']
        require(hashlib.sha256(path.read_bytes()).hexdigest() == artifact['zip_sha256'], 'artifact hash')
        with zipfile.ZipFile(path) as z:
            require(set(z.namelist()) == {'rustc.txt','source-revision.txt','selected-tests.log','tests.log'}, 'artifact has no DB custody')
            require(z.read('source-revision.txt').decode().strip() == SOURCE, 'artifact source')
    if args.repo:
        def read(name): return subprocess.check_output(['git','-C',args.repo,'show',SOURCE+':'+name], text=True)
        require('version = "1.0.489-beta"' in read('crates/bicdb-core/Cargo.toml'), 'exact source version')
        source = read('crates/bicdb-core/src/db/range_image_native/serving_configuration.rs')
        require('immutable joint activated serving archive is missing' in source and 'actual.native_write_members()?' in source, 'source producer identity')
        for name in ['post_joint_no_data_tests.rs','native_activation_idempotence_tests.rs','activated_anchor_archive_tests.rs']:
            read('crates/bicdb-core/src/db/range_image_native/rf3_fixture/native_historical_tls/native_joint_old_closure_tests/complete_joint_tests/'+name)
    if args.self_test:
        cases = []
        def changed(mutator):
            bad = copy.deepcopy(s); mutator(bad); cases.append(bad)
        changed(lambda b: b.update(source_revision='0'*40))
        changed(lambda b: b.update(unique_passed_tests=4))
        changed(lambda b: b.update(whole_candidate_qualified=True))
        changed(lambda b: b['success_artifacts'][0].update(database_custody_retained=True))
        changed(lambda b: b['sibling_failure']['result'].update(failed=0))
        changed(lambda b: b['actual_anchor'].update(historical_R=4))
        for bad in cases:
            try: validate_summary(bad, logs)
            except ValueError: continue
            raise ValueError('semantic tamper accepted')
        print('6 semantic tamper controls PASS')
    print('Complete log/artifact receipt PASS; whole candidate remains NOT QUALIFIED')

if __name__ == '__main__':
    main()
