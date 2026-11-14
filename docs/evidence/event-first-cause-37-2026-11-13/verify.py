#!/usr/bin/env python3
"""Verify retained source/log/frame DATA only; no network, replay or DB open."""
import argparse,copy,gzip,hashlib,json,pathlib,re,subprocess,zipfile
ROOT=pathlib.Path(__file__).resolve().parent
BASE='1ce471aa315b93f58c00ddce5c0577505df472ab'
OLD_RUNTIME='cd3660eafb3ea3c6ea5b15d3c8bd3e33d124e00f'
RUNTIME='0c9602c570083b2c5f26a4d98ac6ba2f9a672fb6'
OLD='521243e597c2d833be840773acac728a85161969'
NEW='6dd0313c7e41a276960c725978927fcfcc7cdd94'
DELIVERY='99f1a05bce08ba0ad5f8a777a844317d6c7a6d2b'
CORE='091a94aac3f7f3ab8deb56710b0ee55c5dacdcc6'
RUNS={43542004108:OLD,43542846413:NEW}
JOBS={132284456735:(43542004108,'8d308fcababe3b60efa890550141bec14911e90b1d06f4a968ece3e5fc974da6'),132284483732:(43542004108,'bfe828ed042d1129fec929060212d73d2ae5c5e53470003f6eab7b7929889045'),132287134837:(43542846413,'4f38a4771e6e6fe55b9443e76f0f70d3d615e9f33d216147044ef1706c4f9ca5'),132287156501:(43542846413,'b4722caf297116a6bc1ce5414ec9c38f0b436b18d0f61439e1dcb2f1dbc7d001')}
ARTIFACTS={14091634702:(43542004108,'5b0d6167875d3699354fdf171d7a979c505a33886446a0883cc2cf4247718fe3'),14091744735:(43542004108,'3dd4f478e88db84a13215f694598df2a1535941a58381768788eb1dfe250d9c4'),14093101399:(43542846413,'37046bf5e8b1b3198775375012feefbade9a40b7843d3a6a33f17d8dfa482aba'),14092079731:(43542846413,'5208027498be3c2adff98d22b6acbbda1df1f8e778a49ba80a225c253f690bb3')}
def need(value,message):
    if not value:raise ValueError(message)
def sha(data):return hashlib.sha256(data).hexdigest()
def load(name):return json.loads((ROOT/name).read_text())
def tree_entries(raw):
    result={};offset=0
    while offset<len(raw):
        space=raw.index(b' ',offset);end=raw.index(b'\0',space)
        result[raw[space+1:end].decode()]=raw[end+1:end+21].hex();offset=end+21
    need(offset==len(raw),'Git tree boundary');return result
def source_proof(repo):
    proof=load('source-proof.json');objects={}
    for oid,kind in proof['object_types'].items():
        data=(ROOT/'git-objects'/(oid+'.bin')).read_bytes()
        need(hashlib.sha1(kind.encode()+b' '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid,'raw Git object hash')
        objects[oid]=data
        if repo:need(subprocess.check_output(['git','-C',str(repo),'cat-file',kind,oid])==data,'repository object bytes')
    for commit,point in proof['commits'].items():
        need(objects[commit].splitlines()[0]==('tree '+point['root']).encode(),'commit root binding')
        need(tree_entries(objects[point['root']])['crates']==point['crates'],'crates binding')
        need(tree_entries(objects[point['crates']])['bicdb-core']==point['core'],'Core binding')
    need(proof['commits'][NEW]['core']==proof['commits'][RUNTIME]['core']==proof['commits'][DELIVERY]['core']==CORE,'corrected/delivery source-only Core equality')
    need(proof['commits'][OLD]['core']==proof['commits'][OLD_RUNTIME]['core'],'old qualifier Core equality')
    paths={}
    for record in proof['paths']:
        oid=proof['commits'][record['commit']]['root']
        for part in record['path'].split('/'):oid=tree_entries(objects[oid])[part]
        need(oid==record['blob'] and proof['object_types'][oid]=='blob','source path membership')
        paths[(record['commit'],record['path'])]=objects[oid]
    preservation=load('first-cause-successor-static-preservation.json')
    for path in preservation['unchanged_source_and_test_files']:
        need(paths[(BASE,path)]==paths[(OLD_RUNTIME,path)]==paths[(RUNTIME,path)],'unchanged original source/test body')
    parent='crates/bicdb-core/src/distribution_transport/evidence/tests.rs'
    need(paths[(RUNTIME,parent)]==paths[(BASE,parent)]+b'\nmod first_cause_tests;\n','original parent bodies exact prefix')
    for path in ['crates/bicdb-core/src/distribution_transport.rs','crates/bicdb-core/src/distribution_transport/evidence.rs']:
        need(paths[(OLD_RUNTIME,path)]==paths[(RUNTIME,path)],'type correction leaves production code unchanged')
    for path in ['.github/workflows/tests.yml','scripts/qualification/guard-build-memory.py']:
        need(paths[(OLD,path)]==paths[(NEW,path)],'identical qualifier workflow and guard')
    need(sha(paths[(NEW,'scripts/qualification/guard-build-memory.py')])=='1fc9b018175b95de49ad59738f37209e76427f05a890aeae24d55bdcb59736c7','reviewed guard source')
    return preservation
def check_pressure(rows,expected):
    need(rows[0]['event']=='started' and rows[-1]['event']=='completed','pressure boundaries')
    need(all(r['event'] in ['started','sample','completed'] for r in rows),'no monitor/resource interruption')
    final=rows[-1];need(final['child_returncode']==final['guard_returncode']==expected,'guard preserves child status')
    need(final['omitted_sample_records']==0 and final['final_group_signal_before_leader_reap'],'guard cleanup/sample scope')
    need(0<sum(r['event']=='sample' for r in rows)<=4096,'bounded sampled pressure')
def check_selection(listed,executed,results):
    expected=load('qualification-static.json')['expected_short_names']
    need(len(listed)==len(set(listed))==37,'37 unique actual names')
    need(sorted(x.rsplit('::',1)[-1] for x in listed)==expected,'fixed union of expected names')
    need(executed==listed and results==['ok']*37,'actual selected/executed37 all passed')
def check_controls(controls):
    priorities=controls['actual_rpc_evidence_first_cause_priority'];need(len(priorities)==2,'two actual priority phases')
    for item,phase,dispatched,nextid,cause in zip(priorities,['invoked','response_frame_before_decode'],[False,True],[1,2],['actual invoked callback failure before dispatch','actual response callback failure before original cancellation override']):
        wanted='Err(RpcEvidenceFailure { may_have_dispatched: '+str(dispatched).lower()+', detail: '+json.dumps(cause)+' })'
        need(item['phase']==phase and item['result']==item['future_fence']==wanted and item['parent_canceled'] and item['next_request_id']==nextid,'exact original cause/uncertainty wins over cancellation')
        if dispatched:
            response=next(r for r in item['records'] if r['phase']=='response_frame_before_decode');raw=bytes(response['raw_frame'])
            need(int.from_bytes(raw[:4],'big')==len(raw)-4,'complete actual frame length header')
            decoded=json.loads(raw[4:]);need(decoded['response']=={'Ok':'Pong'} and decoded['request_id']==1,'synthetic actual TLS Pong, not DB commit')
    concurrent=controls['actual_rpc_evidence_concurrent_first_cause'];need(len(concurrent)==1,'one concurrent control')
    for key,dispatched in [('primary',False),('before_peer',False),('peer',True),('after_peer',True)]:
        expected='Err(RpcEvidenceFailure { may_have_dispatched: '+str(dispatched).lower()+', detail: "actual second-request callback failure before dispatch" })'
        need(concurrent[0][key]==expected,'first cause retained while uncertainty escalates')
    unwind=controls['actual_rpc_evidence_unwind_first_cause'];need(len(unwind)==1,'one unwind control')
    need(unwind[0]['caught_unwind'] and unwind[0]['next_request_id']==1 and unwind[0]['future_fence']=='Err(RpcEvidenceFailure { may_have_dispatched: false, detail: "evidence sink callback unwound; observed dispatch is fenced" })','caught unwind publishes actual future fence')
def verify(repo=None):
    manifest=load('manifest.json');actual={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and p.name!='manifest.json'}
    need(actual==set(manifest['files']),'complete packaged census')
    for name,pin in manifest['files'].items():
        data=(ROOT/name).read_bytes();need(len(data)==pin['bytes'] and sha(data)==pin['sha256'],'manifest '+name)
    preservation=source_proof(repo);archives={};pressures=[];raw_by_job={}
    for jobid,(run,digest) in JOBS.items():
        raw=gzip.decompress((ROOT/str(run)/f'job{jobid}.log.gz').read_bytes());need(sha(raw)==digest,'full lossless raw job log')
        index=load(str(run)+f'/job{jobid}-index.json');job=index['job'];need(index['raw_sha256']==digest and index['raw_bytes']==len(raw),'raw index')
        need(job['head_sha']==RUNS[run] and job['run_id']==run and job['id']==jobid,'job source identity')
        lines=raw.decode().splitlines();checkouts=[]
        for n,line in enumerate(lines[:-1]):
            if 'git log -1 --format=%H' in line:
                m=re.search(r'Z ([0-9a-f]{40})$',lines[n+1]);need(m is not None,'actual checkout line');checkouts.append(m[1])
        need(checkouts==[RUNS[run]],'actual logged checkout matches source')
        if run==43542004108:
            need(job['conclusion']=='failure' and index['summaries']==[] and index['observed_test_start_lines']==[],'old compile failure0executed')
            need(sum('error[E0308]' in x['text'] for x in index['failure_lines'])==2,'two actual E0308 compiler diagnostics')
        else:need(job['conclusion']=='success','corrected terminal job success')
        raw_by_job[jobid]=lines
    for aid,(run,digest) in ARTIFACTS.items():
        path=ROOT/str(run)/f'artifact{aid}.zip';need(sha(path.read_bytes())==digest,'original unchanged ZIP')
        custody=load(str(run)+f'/artifact{aid}-custody.json');need(custody['artifact']['id']==aid and custody['zip_sha256']==digest,'artifact source identity')
        with zipfile.ZipFile(path) as z:members={i.filename:z.read(i) for i in z.infolist() if not i.is_dir()}
        need(set(members)=={m['name'] for m in custody['members']},'complete member census/CRC reads')
        for item in custody['members']:need(sha(members[item['name']])==item['sha256'] and len(members[item['name']])==item['bytes'],'member hash')
        archives[aid]=members
        for name,data in members.items():
            if not name.endswith('.jsonl'):continue
            expected=101 if run==43542004108 and name in ['list-pressure.jsonl','alltargets.jsonl'] else 0
            rows=[json.loads(s) for s in data.splitlines()];check_pressure(rows,expected);pressures.append((rows,expected))
    need(len(pressures)==7,'all seven build/check/list guard logs')
    old=archives[14091634702];need('tests.log' not in old and old['source-revision.txt'].decode().strip()==OLD,'no original runtime log fabricated')
    need(not any(x.endswith(': test') for x in old['selected-tests.log'].decode().splitlines()),'old listing compile failed before selected test list')
    core=archives[14093101399];need(core['source-revision.txt'].decode().strip()==NEW,'corrected artifact source')
    need(core['requested-filters.txt'].decode().splitlines()==['event_client_','actual_rpc_evidence_'],'fixed filters')
    listed=[x[:-6] for x in core['selected-tests.log'].decode().splitlines() if x.endswith(': test')]
    lines=core['tests.log'].decode().splitlines();executed=[];results=[]
    for line in lines:
        m=re.match(r'^test (\S+) \.\.\. (ok|FAILED)$',line)
        if m:executed.append(m[1]);results.append(m[2])
    check_selection(listed,executed,results)
    need([x for x in lines if x.startswith('test result:')]==['test result: ok. 37 passed; 0 failed; 0 ignored; 0 measured; 1075 filtered out; finished in 12.48s'],'actual Rust result/duration')
    old34=preservation['original34_names'];new3=preservation['new_controls']
    need(len(old34)==34 and len(new3)==3 and set(x.rsplit('::',1)[-1] for x in listed)==set(old34+new3),'unchanged34 plus3 union')
    controls={prefix:[json.loads(x.split(prefix+' ',1)[1]) for x in lines if prefix+' ' in x] for prefix in ['actual_rpc_evidence_first_cause_priority','actual_rpc_evidence_concurrent_first_cause','actual_rpc_evidence_unwind_first_cause']};check_controls(controls)
    panics=[x for x in raw_by_job[132287156501] if 'panicked at' in x]
    need(len(panics)==1 and 'actual_rpc_evidence_first_cause_unwind_publishes_cause_before_future_fence' in panics[0],'one expected caught-unwind panic; no failed test inferred')
    old_dispatch=load('old-dispatch-provenance.json');need(old_dispatch['original_pre_POST_request_body_saved'] is False and old_dispatch['new_dispatch_created_for_this_packet'] is False,'missing old request limitation preserved')
    request=load('dispatch-request.json');need(request['qualifier_source']==NEW and request['dispatch_count']==1 and request['post_attempted'] and request['post_returncode']==0 and request['run_id']==43542846413,'corrected original single POST record')
    need(sha((ROOT/'0c960-corrected37-root-original-artifact-verification.json').read_bytes())=='a235f258fd00760e5ace5b8fae281826f092fefb54f9ea399391d3b07eba4c00','Root separate original-artifact receipt')
    return controls,(listed,executed,results),pressures
def self_test(context):
    controls,selection,pressures=context;passed=[]
    def refuses(label,call):
        try:call()
        except (ValueError,KeyError):passed.append(label);return
        raise ValueError('negative control accepted: '+label)
    changed=copy.deepcopy(controls);changed['actual_rpc_evidence_first_cause_priority'][1]['result']='Ok(Pong)'
    refuses('late synthetic success admitted',lambda:check_controls(changed))
    changed=copy.deepcopy(controls);changed['actual_rpc_evidence_concurrent_first_cause'][0]['after_peer']=changed['actual_rpc_evidence_concurrent_first_cause'][0]['before_peer']
    refuses('dispatch uncertainty downgraded',lambda:check_controls(changed))
    changed=copy.deepcopy(controls);changed['actual_rpc_evidence_first_cause_priority'][0]['future_fence']='Err(generic observer failure)'
    refuses('first cause replaced',lambda:check_controls(changed))
    changed=copy.deepcopy(controls);record=next(x for x in changed['actual_rpc_evidence_first_cause_priority'][1]['records'] if x['phase']=='response_frame_before_decode');record['raw_frame'][3]^=1
    refuses('frame length altered',lambda:check_controls(changed))
    listed,executed,results=copy.deepcopy(selection);listed.pop()
    refuses('missing original case',lambda:check_selection(listed,executed,results))
    rows,expected=copy.deepcopy(pressures[0]);rows[-1]['guard_returncode']=75
    refuses('resource interruption labeled compiler outcome',lambda:check_pressure(rows,expected))
    changed=copy.deepcopy(controls);changed['actual_rpc_evidence_unwind_first_cause'][0]['caught_unwind']=False
    refuses('uncaught unwind claimed passed',lambda:check_controls(changed))
    data=bytearray((ROOT/'43542846413/artifact14093101399.zip').read_bytes());data[-1]^=1
    refuses('original ZIP tamper',lambda:need(sha(data)==ARTIFACTS[14093101399][1],'ZIP hash'))
    return passed
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--self-test',action='store_true');a=p.parse_args();context=verify(a.repo)
    result={'verified':True,'runtime_source':RUNTIME,'qualifier':NEW,'corrected_run':43542846413,'actual_tests':'37 passed /0failed /0ignored','duration_seconds':'12.48','old_run':43542004108,'old_result':'2E0308 in each compilation job;0executed','unchanged34_plus3':True,'caught_unwind_panic_count':1,'guard_logs':7,'missing_old_dispatch_body':'explicit limitation','frame_scope':'actual legacy TLS/synthetic test RPCs, not database commits','engine_opened':False}
    if a.self_test:result['negative_controls_passed']=self_test(context)
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
