#!/usr/bin/env python3
"""Bounded offline source-separated CI receipt checks; never opens an engine."""
import argparse
import collections
import copy
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
HERE=Path(__file__).resolve().parent

def require(ok, why):
    if not ok: raise ValueError(why)

def digest(data): return hashlib.sha256(data).hexdigest()

def validate(r):
    expected={'registry':(5,'29bfd6c9af7652a8e40a97e46b23d4629777ece9'), 'routing':(3,'29bfd6c9af7652a8e40a97e46b23d4629777ece9'), 'expiry':(1,'ded3c412cf25c12f3c6fe524d22aac02310a1c0b'), 'owned_corrected':(4,'6e00ba519715ee0ca392357394f2b0a9749a1358')}
    require(len(r['runtime'])==4, 'four distinct runtime receipts')
    for v in r['runtime']:
        count,source=expected[v['label']]
        require(v['source_revision']==source and v['job_conclusion']=='success', 'actual source and terminal outcome')
        require(v['passed']==count==len(v['test_names']) and not v['failed'] and not v['ignored'], 'actual focused counts')
        require(len(v['test_summaries'])==1 and f'{count} passed; 0 failed;' in v['test_summaries'][0], 'raw summary agreement')
    require(len(r['compile_jobs'])==4, 'four successful compiler/consumer jobs')
    require(all(v['job_conclusion']==v['all_targets']==v['isolated_consumer_without_core_defaults']=='success' for v in r['compile_jobs']), 'compiler states')
    pending=r['corrected_owned_compile_at_freeze']
    require(pending['status']=='in_progress' and pending['conclusion'] is None and pending['job_id']==131504948931, 'corrected-head compiler pending checkpoint')
    c=r['cancellations']; require(len(c)==3 and all(v['job_conclusion']=='cancelled' and not v['test_summaries'] and v['not_a_suite_pass'] for v in c), 'cancellations remain cancellations')
    require(len(c[0]['selected_test_names'])==60 and len(c[0]['selected_without_completed_result'])==2 and c[0]['completed_individual_results'].get('ok')==57, 'partial broad progress')
    require(sum(c[0]['completed_individual_results'].values())==58, 'ignored helper separate')
    require(len(c[1]['selected_test_names'])==len(c[1]['selected_without_completed_result'])==4 and not c[1]['completed_individual_results'], 'blocked four controls had no result')
    steps={s['name']:s['conclusion'] for s in c[2]['steps']}
    require(steps['cargo check --workspace --all-targets']=='cancelled' and steps['Workflow consumer compiles without Core defaults']=='skipped', 'canceled compiler is not pass')
    require([v['nested_config_call_in_receiver_guard'] for v in r['source_file_fingerprints']]==[True,True,False], 'fixture source distinction')
    for v in r['runtime']+r['compile_jobs']+c:
        require(len(v['raw_sha256'])==64 and v['raw_bytes']>0, 'raw receipt identity')

def verify_raw(r, directory):
    for v in r['runtime']+r['compile_jobs']+r['cancellations']:
        sha=hashlib.sha256();size=0
        with gzip.open(directory/v['compressed_file'],'rb') as f:
            while b:=f.read(65536):
                size+=len(b);require(size<=8*1024*1024, 'raw log declared verification bound');sha.update(b)
        require(size==v['raw_bytes'] and sha.hexdigest()==v['raw_sha256'], 'exact raw log parity')

def main():
    p=argparse.ArgumentParser();p.add_argument('--raw-dir',type=Path);p.add_argument('--repo',type=Path);p.add_argument('--verify-private-zips',action='store_true');p.add_argument('--tamper-tests',action='store_true');a=p.parse_args()
    for line in (HERE/'SHA256SUMS').read_text().splitlines():
        sha,name=line.split('  ',1);require(digest((HERE/name).read_bytes())==sha, 'portable file hash')
    r=json.loads((HERE/'receipt.json').read_text());validate(r)
    if a.raw_dir: verify_raw(r,a.raw_dir)
    if a.repo:
        for v in r['source_file_fingerprints']:
            b=subprocess.check_output(['git','-C',str(a.repo),'show',v['source']+':'+v['path']]);require(digest(b)==v['sha256'], 'actual source file identity')
    if a.verify_private_zips:
        for v in r['archive_receipts']:
            p=Path(v['private_path']);require(p.stat().st_size==v['bytes'], 'ZIP length')
            with p.open('rb') as f:require(hashlib.file_digest(f,'sha256').hexdigest()==v['sha256'], 'ZIP digest')
    if a.tamper_tests:
        changes=[lambda r:r['runtime'][3].update(passed=0),lambda r:r['cancellations'][0].update(job_conclusion='success'),lambda r:r['compile_jobs'][0].update(all_targets='skipped'),lambda r:r['source_file_fingerprints'][2].update(nested_config_call_in_receiver_guard=True)]
        for change in changes:
            altered=copy.deepcopy(r);change(altered)
            try:validate(altered)
            except (ValueError,KeyError):pass
            else:raise ValueError('semantic tamper accepted')
        print('4 semantic tamper cases PASS')
    print('isolation receipt PASS; no broad-suite or publication claim')
if __name__=='__main__':main()
