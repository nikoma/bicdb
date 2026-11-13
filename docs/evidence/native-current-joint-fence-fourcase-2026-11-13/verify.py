#!/usr/bin/env python3
"""Offline retained byte/result/profile consistency only; no engine/network."""
import gzip
import hashlib
import json
import pathlib
import re
from derive_profile import derive,SOURCE

ROOT=pathlib.Path(__file__).resolve().parent

def verify(root=ROOT):
    manifest=json.loads((root/'MANIFEST.json').read_text())
    for item in manifest['files']:
        data=(root/item['path']).read_bytes()
        assert len(data)==item['bytes'] and hashlib.sha256(data).hexdigest()==item['sha256'],item['path']
    result=json.loads((root/'results.json').read_text())
    assert result['source']==SOURCE and result['run']==43343984328
    assert result['workflow_conclusion']=='failure' and result['formatting'].startswith('FAIL')
    assert result['core']['pass']==4 and result['core']['fail']==0
    logs={}
    for label,job,conclusion in [('core',131618814672,'success'),('compile',131618780247,'success'),('formatting',131618780186,'failure')]:
        compressed=(root/(label+'.log.gz')).read_bytes();raw=gzip.decompress(compressed)
        idx=json.loads((root/(label+'-index.json')).read_text())
        assert idx['source_revision']==SOURCE and idx['run_id']==43343984328 and idx['job_id']==job
        assert idx['job_conclusion']==conclusion
        assert len(raw)==idx['raw_bytes'] and hashlib.sha256(raw).hexdigest()==idx['raw_sha256']
        assert len(compressed)==idx['gzip_bytes'] and hashlib.sha256(compressed).hexdigest()==idx['gzip_sha256']
        logs[label]=raw
    text=logs['core'].decode()
    assert 'test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 976 filtered out; finished in 67.70s' in text
    tests=[line for line in text.splitlines() if re.search(r'Z test [\w:]+ \.\.\. ok$',line)]
    assert len(tests)==len(set(tests))==4 and tests==result['core']['test_results']
    assert text.count('ACTUAL_ACTIVATION_SCOPE_POSTCAT_CUT reached=true')==4
    assert 'distribution_range_recovery.rs' in logs['formatting'].decode() and 'FENCE_TRACE_BEGIN' in logs['formatting'].decode()
    actual=derive(logs['core']);saved=json.loads(gzip.decompress((root/'profile.json.gz').read_bytes()))
    assert actual==saved,'all actual buffered DATA/spans/correlations must be rederived'
    assert actual['event_count']==1024 and len(actual['groups'])==32 and actual['all_dropped_zero']
    assert actual['phase_counts']=={'receiver':16,'collector-begin':4,'collector-record':12}
    records=[x for x in actual['groups'] if x['phase']=='collector-record']
    assert len(records)==12 and all(x['events'][-1]['stage']=='record-counted' for x in records)
    assert min(x['role_slack_at_last_marker_us'] for x in records)==1194109
    assert min(x['initial_deadlines']['caller_deadline_us']-x['initial_deadlines']['role_deadline_us'] for x in records)==770233
    current=[x for x in actual['groups'] if x['phase']=='receiver' and x['test'] is not None]
    assert len(current)==12 and {x['node'] for x in current}=={'n1','n2','n3'}
    assert len({x['test'] for x in current})==4
    assert result['prior_core']['pass']==3 and result['prior_core']['fail']==1
    assert not result['production_timing_fix'] and not result['old_cause_resolved'] and not result['qualified']
    assert not result['artifact']['native_database_custody_retained'] and not result['engine_opened_or_replayed']
    return actual

if __name__=='__main__':
    verify()
    print('PASS wholeCore4/0 and workflow-formatFAIL distinctions; full1024event/32buffer rederivation; exactbound/slack/stages; old3/1 unresolved')
