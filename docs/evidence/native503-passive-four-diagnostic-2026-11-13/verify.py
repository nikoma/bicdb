#!/usr/bin/env python3
"""Portable retained-byte checker. No network, engine open, replay or grants."""
import argparse, copy, gzip, hashlib, json, pathlib, re, subprocess, zipfile

ROOT = pathlib.Path(__file__).resolve().parent
SOURCE = '2b8c2b2de400038729a226ff61b9b275df846d18'
DIAGNOSTIC = '7ab56348a8309d4d396b1af87467232bfed1ac96'
CORE = '3e65798272bcc9bd050c505556de481ac2b185a6'
RUN = 43537820765
ZIP_HASHES = {
    14090703744: '9e6ab7215572aaabf60dbc6fceaf6967f2d9512a40e0bb4f1134326f9424bda5',
    14090462497: 'aa0432fe3abd21a61865d9422c28bc80504c3fa47a90035fb49f522d27e88e34',
}
RAW_HASHES = {
    132271009934: '69c4d8a68cb23ec693b713101d9b91819ce0431804994c14157c4cdaf176ff15',
    132270985379: '6b456c75be78ade52be25a93739caeb49c54b6d54b71225ef3d1a6495516ee5e',
}
def need(condition, message):
    if not condition:
        raise ValueError(message)
def sha(data):
    return hashlib.sha256(data).hexdigest()
def load(name):
    return json.loads((ROOT/name).read_text())
def expected_tests():
    return load('dispatch-checkpoint.json')['expected_selected']
def snapshot_lines(lines):
    return [json.loads(line.split('NATIVE_PASSIVE_DIAGNOSTICS ', 1)[1])
            for line in lines if 'NATIVE_PASSIVE_DIAGNOSTICS ' in line]
def check_selection(requested, listed, starts, results):
    expected = expected_tests()
    need(len(expected)==4 and len(set(expected))==4, 'expected selection')
    need(requested==listed==starts==expected, 'exact requested/listed/runtime order')
    need(results==['ok']*4, 'four actual terminal successes')
def check_pressure(rows):
    need(0 < len(rows) <= 4100, 'pressure bound')
    need(rows[0]['event']=='started' and rows[-1]['event']=='completed', 'guard boundaries')
    need(set(x['event'] for x in rows)=={'started','sample','completed'}, 'no guard interruption')
    final=rows[-1]
    need(final['guard_returncode']==final['child_returncode']==0, 'guard/child success')
    need(final['omitted_sample_records']==0 and final['final_group_signal_before_leader_reap'], 'guard cleanup/sample bound')
    need(0 < sum(x['event']=='sample' for x in rows)<=4096, 'sample count')
def check_snapshots(snapshots):
    need([s['test'] for s in snapshots]==expected_tests(), 'four snapshot identities')
    busy=[]; terminals_total=errors_total=0; ledger=[]
    for s in snapshots:
        need(s['source']==s['workflow_sha_environment']==SOURCE, 'snapshot source')
        need(s['package_version']=='1.0.503-beta', 'snapshot version')
        need(s['global_capture_complete'] is False and s['background_quiescence_proven'] is False, 'global capture gaps')
        need(s['counters_are_snapshot_only'] and s['authority'] is False, 'observational scope')
        need(s['dropped_bound']==s['serialization_errors']==s['dropped_closed_at_snapshot']==0, 'snapshot other drops')
        records=s['records'];need(len(records)==s['stored_records'], 'retained record count')
        busy.append(s['dropped_busy'])
        need(s['retained_buffer_incomplete_at_snapshot']==(s['dropped_busy']!=0), 'buffer integrity field')
        terms={r['data']['acquisition_id']:r for r in records if r['stage']=='metadata_acquire_return'}
        need(all(r['data']['observation']['result_ok'] for r in terms.values()), 'captured acquire terminal results')
        terminals_total+=len(terms)
        for r in records:
            if r['stage']!='metadata_probe_actual_return':continue
            d=r['data'];outcome=d['observation']['observation']
            if outcome['result_ok']:continue
            errors_total+=1;term=terms.get(d['acquisition_id'])
            need(term is not None and r['capture_elapsed_us']>=term['capture_elapsed_us'], 'probe cancellation after matching success')
            chain=outcome['actual_error_chain']
            need(chain['complete'] and [x['text'] for x in chain['entries']]==['canceling statement due to user request'], 'recorded cancellation error identity')
        if 'normal_serving_current_damage' in s['test']:
            injections=[r for r in records if r['stage']=='normal_damage_actual_injection_reached']
            need(len(injections)==1 and injections[0]['data']['archive'] is False, 'actual current damage cut')
        if 'hot_recovery' in s['test']:
            hosts=[r['data'] for r in records if r['stage']=='initial_preparing_host_return']
            need(sorted(x['host_index'] for x in hosts)==[0,1,2] and all(x['result_ok'] for x in hosts), 'hot preparation reached')
        if 'drop_without_stop' not in s['test']:continue
        for mode in range(3):
            rs=[r for r in records if r['lifecycle_mode']==mode]
            def one(stage):
                found=[r['data'] for r in rs if r['stage']==stage]
                need(len(found)==1, 'one lifecycle stage '+stage)
                return found[0]
            intent=one('lifecycle_append_intent');ack=one('lifecycle_append_actual_return');strong=one('lifecycle_strong_actual_return')
            need(intent['configured_request_budget_ms']==2000 and intent['raw_SDK_frames_recorded'] is False, 'original SDK budget and capture gap')
            need(ack['acknowledged'] and ack['inserted'] and not ack['uncertain_write_effect'], 'APP actual success')
            need(ack['logical_id']==intent['logical_id']==strong['preceding_append_id'], 'APP/Strong diagnostic link')
            need(strong['success'] and strong['present'] and strong['full_value']==intent['expected_full_value'], 'Strong full value')
            value=strong['full_value'];digest=sha(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
            need(digest==intent['expected_payload_sha256'], 'Strong expected payload hash')
            one('lifecycle_mode_all_original_assertions_complete')
            branch=one('lifecycle_fault_or_drop_branch_entered')['branch']
            need(branch==['drop_without_stop','canceled_explicit_stop','durability_fence'][mode], 'actual lifecycle branch')
            reopens=[r['data'] for r in rs if r['stage']=='lifecycle_same_root_reopen_return']
            need(len(reopens)==(0 if mode==2 else 3) and all(x['result_ok'] for x in reopens), 'same-root native open results')
            ledger.append((mode,ack['logical_id'],strong['logical_id']))
    need(busy==[11,1,0,6] and sum(busy)==18, 'explicit busy drops')
    need(terminals_total==157 and errors_total==60, 'retained acquire/error counts')
    need(len(ledger)==3, 'three independent lifecycle fixture ledgers')
    return {'busy_drops':sum(busy),'captured_successful_acquisition_terminals':terminals_total,'post_success_cancellation_records':errors_total,'APP_acknowledged':3,'Strong_full_values':3,'same_root_reopens':6}
def parse_tree(data):
    entries={};offset=0
    while offset<len(data):
        space=data.index(b' ',offset);end=data.index(b'\0',space)
        entries[data[space+1:end].decode()]=data[end+1:end+21].hex();offset=end+21
    need(offset==len(data), 'Git tree byte boundary')
    return entries
def check_git_objects(repo):
    proof=load('core-tree-proof.json');objects={}
    for oid,kind in proof['object_types'].items():
        data=(ROOT/'git-objects'/(oid+'.bin')).read_bytes()
        need(hashlib.sha1(kind.encode()+b' '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid, 'raw Git object hash')
        objects[oid]=data
        if repo:
            actual=subprocess.check_output(['git','-C',str(repo),'cat-file',kind,oid])
            need(actual==data, 'repository Git object bytes')
    need(set(proof['commits'])=={SOURCE,DIAGNOSTIC}, 'Git commit identities')
    for commit,path in proof['commits'].items():
        need(objects[commit].splitlines()[0]==('tree '+path['root_tree']).encode(), 'commit root binding')
        need(parse_tree(objects[path['root_tree']])['crates']==path['crates_tree'], 'crates tree binding')
        need(parse_tree(objects[path['crates_tree']])['bicdb-core']==path['core_tree']==CORE, 'identical actual Core tree')
    if repo:
        paths=subprocess.check_output(['git','-C',str(repo),'diff','--name-only',DIAGNOSTIC,SOURCE],text=True).splitlines()
        need(paths==['.github/workflows/tests.yml','scripts/qualification/guard-build-memory.py'], 'only qualifier workflow and guard changed')
        for item in load('source-index.json')['files']:
            data=subprocess.check_output(['git','-C',str(repo),'show',item['source']+':'+item['path']])
            need(sha(data)==item['sha256'], 'repository source hash')
            need(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==item['blob'], 'repository blob hash')
def verify(repo=None):
    manifest=load('manifest.json');actual={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and p.name!='manifest.json'}
    need(actual==set(manifest['files']), 'exact packaged files')
    for name,item in manifest['files'].items():
        data=(ROOT/name).read_bytes();need(len(data)==item['bytes'] and sha(data)==item['sha256'], 'manifest '+name)
    check_git_objects(repo)
    run=load('run-latest.json');need(run['id']==RUN and run['head_sha']==SOURCE and run['conclusion']=='success', 'terminal run')
    zip_data={};pressure=[]
    for aid,digest in ZIP_HASHES.items():
        data=(ROOT/f'artifact{aid}.zip').read_bytes();need(sha(data)==digest, 'original artifact ZIP bytes')
        custody=load(f'artifact{aid}-custody.json');need(custody['artifact']['id']==aid and custody['local_zip_sha256']==digest, 'artifact identity')
        with zipfile.ZipFile(ROOT/f'artifact{aid}.zip') as z:
            members={i.filename:z.read(i) for i in z.infolist() if not i.is_dir()}
        need(set(members)=={x['name'] for x in custody['members']}, 'complete ZIP member census')
        for m in custody['members']:need(len(members[m['name']])==m['bytes'] and sha(members[m['name']])==m['sha256'], 'ZIP member hash')
        zip_data[aid]=members
        for name,data in members.items():
            if name.endswith('.jsonl'):
                rows=[json.loads(x) for x in data.splitlines()];check_pressure(rows);pressure.append(rows)
    need(len(pressure)==4, 'four build/check/list logs')
    core=zip_data[14090703744];need(core['source-revision.txt'].decode().strip()==SOURCE, 'artifact source')
    listed=[x[:-6] for x in core['selected-tests.log'].decode().splitlines() if x.endswith(': test')]
    lines=core['tests.log'].decode().splitlines();starts=[];results=[];active=False
    for line in lines:
        start=re.match(r'^test (\S+) \.\.\. ',line)
        if start:need(not active,'serial non-overlapping starts');starts.append(start[1]);active=True
        if line in ['ok','FAILED'] and active:results.append(line);active=False
    check_selection(core['requested-tests.txt'].decode().splitlines(),listed,starts,results)
    terminal=[x for x in lines if x.startswith('test result:')]
    need(terminal==['test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 1086 filtered out; finished in 15.52s'], 'actual Rust result')
    snapshots=snapshot_lines(lines);saved=json.loads(gzip.decompress((ROOT/'passive-snapshots.json.gz').read_bytes()))
    need(snapshots==[x['snapshot'] for x in saved['snapshots']], 'complete retained snapshots')
    for jid,digest in RAW_HASHES.items():
        raw=gzip.decompress((ROOT/f'job{jid}.log.gz').read_bytes());need(sha(raw)==digest,'full raw job bytes')
        index=load(f'job{jid}-index.json');job=index['job']
        need(index['raw_sha256']==digest and index['raw_bytes']==len(raw),'raw job index')
        need(job['id']==jid and job['run_id']==RUN and job['head_sha']==SOURCE and job['conclusion']=='success','terminal job identity')
        if jid==132271009934:need(snapshot_lines(raw.decode().splitlines())==snapshots,'raw job and ZIP snapshots agree')
    result=check_snapshots(snapshots);summary=load('analysis-summary.json')
    need(summary['source']==SOURCE and summary['captured_busy_drops']==18 and summary['all_capture_globals_incomplete'], 'summary scope')
    need('original failure cause inconclusive' in summary['classification'], 'original cause remains inconclusive')
    return result, snapshots, pressure, (core['requested-tests.txt'].decode().splitlines(),listed,starts,results)
def self_test(context):
    _,snapshots,pressure,selection=context;passed=[]
    def refuses(name,call):
        try:call()
        except (ValueError,KeyError):passed.append(name);return
        raise ValueError('negative control accepted: '+name)
    changed=copy.deepcopy(snapshots);changed[0]['global_capture_complete']=True
    refuses('false global completeness',lambda:check_snapshots(changed))
    changed=copy.deepcopy(snapshots);changed[0]['dropped_busy']=0
    refuses('missing busy drops',lambda:check_snapshots(changed))
    changed=copy.deepcopy(snapshots)
    next(r for r in changed[-1]['records'] if r['stage']=='lifecycle_strong_actual_return')['data']['full_value']['mode']=99
    refuses('Strong payload mismatch',lambda:check_snapshots(changed))
    changed=copy.deepcopy(snapshots)
    next(r for r in changed[0]['records'] if r['stage']=='metadata_probe_actual_return' and not r['data']['observation']['observation']['result_ok'])['capture_elapsed_us']=0
    refuses('cancel before matching terminal',lambda:check_snapshots(changed))
    changed=copy.deepcopy(pressure[0]);changed[-1]['guard_returncode']=75
    refuses('resource interruption mislabeled success',lambda:check_pressure(changed))
    req,listed,starts,results=copy.deepcopy(selection);listed.pop()
    refuses('missing selected original case',lambda:check_selection(req,listed,starts,results))
    data=bytearray((ROOT/'artifact14090703744.zip').read_bytes());data[-1]^=1
    refuses('changed original artifact',lambda:need(sha(data)==ZIP_HASHES[14090703744],'artifact tamper'))
    return passed
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--self-test',action='store_true');a=p.parse_args()
    context=verify(a.repo);output={'verified':True,'source':SOURCE,'run':RUN,'actual_result':'4 passed /0 failed','logical_checks':context[0],'engine_opened':False,'original_failure_cause':'inconclusive'}
    if a.self_test:output['negative_controls_passed']=self_test(context)
    print(json.dumps(output,indent=2))
if __name__=='__main__':main()
