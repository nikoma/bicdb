#!/usr/bin/env python3
"""Portable log/Git-data verification only; never opens a database."""
import argparse, copy, gzip, hashlib, json, pathlib, re, subprocess

ROOT=pathlib.Path(__file__).resolve().parent
HEAD='c6a081a6026f4efd00d80f392c94b67f2f2f4e12'
CHECKOUT='de15f8f9836e0e93fb2e912ad230437bc975aa6a'
TREE='a4712298110429f1c0d7ce63876b200e42a874b3'
RUN=43529104485
OLD_JOB=132242099377
NEW_JOB=132276661454
OLD_HASH='8af0ca1ea94ef88ae64a0ed4101d907d044072acd20ff8ce2c22d4149da09e54'
NEW_HASH='546058667c030f6e4f6abb427801031816782a9e23c5541541b6d1357858e312'
COMMAND='cargo test --config \'profile.dev.package."*".opt-level=2\' -p bicdb-core --lib --locked -- distribution_native_host:: range_image_native::rf3_fixture:: --test-threads=1'
FOUR=[
 'db::range_image_native::rf3_fixture::native_historical_tls::native_strong_point_tests::serving_observation_scope_tests::normal_serving_record_tests::native_serving_observation_scope_actual_normal_serving_current_damage_refused_at_exit',
 'distribution_native_host::provisional_listener::tests::initial_data_tests::hot_recovery_tests::native_hot_recovery_finite_open_never_starts_process_worker',
 'distribution_native_host::provisional_listener::tests::initial_data_tests::hot_recovery_tests::native_hot_recovery_retained_original_store_replace_restore_revokes_held_reservation',
 'distribution_native_host::provisional_listener::tests::initial_data_tests::native_initial_data_actual_tls_drop_without_stop_ends_inner_owner_and_reopens_same_root',
]
IGNORED='distribution_native_host::provisional_listener::tests::process_tests::native_initial_birth_process_child'
def need(value,message):
    if not value:raise ValueError(message)
def sha(data):return hashlib.sha256(data).hexdigest()
def load(name):return json.loads((ROOT/name).read_text())
def parse_log(raw):
    text=re.sub(r'\x1b\[[0-9;]*m','',raw.decode())
    lines=text.splitlines();tests=[];checkouts=[];summary=[]
    for index,line in enumerate(lines):
        match=re.search(r'(?:^|Z )test (\S+) \.\.\. (.*)',line)
        if match:tests.append({'line':index+1,'test':match[1],'result':match[2]})
        match=re.search(r'(?:^|Z )test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9.]+)s',line)
        if match:summary.append({'status':match[1],'passed':int(match[2]),'failed':int(match[3]),'ignored':int(match[4]),'measured':int(match[5]),'filtered':int(match[6]),'seconds':match[7]})
        if 'git log -1 --format=%H' in line:
            found=re.search(r'Z ([0-9a-f]{40})$',lines[index+1]);need(found is not None,'actual checkout output');checkouts.append(found[1])
    need(len(summary)==1,'one actual Rust summary')
    command=[line.split('Z ',1)[1] for line in lines if re.search(r'Z '+re.escape(COMMAND)+r'$',line)]
    image=[lines[i+1].split('Image: ',1)[1]+'/'+lines[i+2].split('Version: ',1)[1] for i,line in enumerate(lines) if line.endswith('##[group]Runner Image')]
    return {'tests':tests,'summary':summary[0],'checkouts':checkouts,'lines':lines,'command':command,'declared_image':image}
def check_comparison(old,new):
    need(old['checkouts']==new['checkouts']==[CHECKOUT],'same actual checkout')
    need(old['command']==new['command']==[COMMAND],'same actual serial command and flags')
    need(old['declared_image']==new['declared_image']==['ubuntu-24.04/20261111.370.1'],'same declared runner image; not identical load proof')
    need(old['summary']=={'status':'FAILED','passed':208,'failed':4,'ignored':1,'measured':0,'filtered':877,'seconds':'1197.50'},'original failed result')
    need(new['summary']=={'status':'ok','passed':212,'failed':0,'ignored':1,'measured':0,'filtered':877,'seconds':'913.06'},'exact repeat result')
    a=old['tests'];b=new['tests'];need(len(a)==len(b)==213,'213 selected names')
    need([x['test'] for x in a]==[x['test'] for x in b] and len({x['test'] for x in b})==213,'same complete order')
    old_fail=[x['test'] for x in a if x['result']=='FAILED']
    need(sorted(old_fail)==sorted(FOUR),'original four failures')
    need(all(next(x for x in b if x['test']==name)['result']=='ok' for name in FOUR),'all four now passed')
    ignored_old=[x for x in a if x['result'].startswith('ignored,')]
    ignored_new=[x for x in b if x['result'].startswith('ignored,')]
    need(len(ignored_old)==len(ignored_new)==1 and ignored_old[0]['test']==ignored_new[0]['test']==IGNORED,'same ignored child')
    need(ignored_old[0]['result']==ignored_new[0]['result'],'same ignore reason')
    need(sum(x['result']=='ok' for x in a)==208 and sum(x['result']=='ok' for x in b)==212,'actual per-test outcomes')
def check_reused(first,second):
    previous={j['name']:j for j in first['jobs']};fresh=[];reused=[];skipped=[]
    for job in second['jobs']:
        if job['id']==NEW_JOB:
            need(job['run_attempt']==2 and job['head_sha']==HEAD and job['conclusion']=='success','new native job');fresh.append(job);continue
        if job['conclusion']=='skipped':skipped.append(job);continue
        original=previous[job['name']]
        need(job['conclusion']==original['conclusion']=='success','reused passing conclusion')
        need(job['started_at']==original['started_at'] and job['completed_at']==original['completed_at'],'reused original timestamps')
        need(job['started_at']<'2026-11-14T01:16:00Z','copied previous execution time')
        reused.append(job)
    need(len(fresh)==1 and len(reused)>0,'only one actual fresh validation job')
    return {'fresh_native_jobs':1,'reused_other_passes':len(reused),'skipped_not_executed':len(skipped)}
def check_tree(repo):
    proof=load('checkout-tree-proof.json');objects={}
    need(set(proof['commits'])=={HEAD,CHECKOUT},'pinned head and checkout')
    for oid,kind in proof['object_types'].items():
        data=(ROOT/'git-objects'/(oid+'.bin')).read_bytes()
        need(hashlib.sha1(kind.encode()+b' '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid,'raw Git object identity')
        objects[oid]=data
        if repo:need(subprocess.check_output(['git','-C',str(repo),'cat-file',kind,oid])==data,'repository object bytes')
    for commit,binding in proof['commits'].items():
        need(binding['tree']==TREE and objects[commit].splitlines()[0]==('tree '+TREE).encode(),'complete source/checkout tree equality')
    if repo:
        for path,pin in load('source-identity.json')['source_pins'].items():
            data=subprocess.check_output(['git','-C',str(repo),'show',HEAD+':'+path])
            need(sha(data)==pin['sha256'],'repository source pin')
            need(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==pin['blob'],'repository blob identity')
def verify(repo=None):
    manifest=load('manifest.json');paths={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and p.name!='manifest.json'}
    need(paths==set(manifest['files']),'exact portable file census')
    for name,pin in manifest['files'].items():
        data=(ROOT/name).read_bytes();need(len(data)==pin['bytes'] and sha(data)==pin['sha256'],'manifest '+name)
    old_raw=gzip.decompress((ROOT/'original-failed-job.log.gz').read_bytes());new_raw=gzip.decompress((ROOT/'native-job.log.gz').read_bytes())
    need(sha(old_raw)==OLD_HASH and sha(new_raw)==NEW_HASH,'full lossless raw logs')
    old=parse_log(old_raw);new=parse_log(new_raw);check_comparison(old,new);check_tree(repo)
    first=load('attempt1-jobs-original.json');second=load('attempt2-jobs-latest.json');status=check_reused(first,second)
    for name,jid,attempt,outcome in [('original-job-metadata.json',OLD_JOB,1,'failure'),('native-job-latest.json',NEW_JOB,2,'success')]:
        job=load(name);need(job['id']==jid and job['head_sha']==HEAD and job['run_id']==RUN and job['run_attempt']==attempt and job['conclusion']==outcome,'authoritative job identity')
    request=load('single-post-request.json');need(request['source']==HEAD and request['original_run']==RUN and request['dispatch_count']==1,'owner single POST provenance record')
    summary=load('analysis-summary.json');need(summary['original_failure']['causes']=='inconclusive' and summary['source']==HEAD,'original cause not reclassified')
    need(summary['runner']['runner_id']==1000024937 and summary['original_runner']['runner_id']==1000024718,'distinct runner identities')
    need(summary['runtime_RAM_metrics'].startswith('not available'),'no invented RAM measurement')
    need(summary['selection']['same_exact_order_as_original'] and summary['selection']['same_ignored_internal_child'],'comparison summary')
    old_index=load('original-actual-test-order.json')['actual_test_start_lines']
    need([x['test'] for x in old_index]==[x['test'] for x in old['tests']],'original retained name order')
    index=load('native-job-index.json');need(index['raw_sha256']==NEW_HASH and index['raw_bytes']==len(new_raw),'new raw index')
    return old,new,first,second,status
def self_test(context):
    old,new,first,second,_=context;passed=[]
    def refuses(label,call):
        try:call()
        except (ValueError,KeyError):passed.append(label);return
        raise ValueError('negative control accepted: '+label)
    changed=copy.deepcopy(new);changed['tests'][0],changed['tests'][1]=changed['tests'][1],changed['tests'][0]
    refuses('changed test order',lambda:check_comparison(old,changed))
    changed=copy.deepcopy(new);next(x for x in changed['tests'] if x['test']==FOUR[0])['result']='FAILED'
    refuses('old failure mislabeled pass',lambda:check_comparison(old,changed))
    changed=copy.deepcopy(new);next(x for x in changed['tests'] if x['test']==IGNORED)['result']='ok'
    refuses('ignored child counted executed',lambda:check_comparison(old,changed))
    changed=copy.deepcopy(new);changed['checkouts']=[HEAD]
    refuses('run head substituted for checkout',lambda:check_comparison(old,changed))
    changed=copy.deepcopy(new);changed['command']=[COMMAND.replace('--test-threads=1','--test-threads=4')]
    refuses('serial execution flag changed',lambda:check_comparison(old,changed))
    changed=copy.deepcopy(second);next(x for x in changed['jobs'] if x['id']!=NEW_JOB and x['conclusion']=='success')['started_at']='2026-11-14T01:16:19Z'
    refuses('copied check counted fresh',lambda:check_reused(first,changed))
    changed=copy.deepcopy(new);changed['summary']['seconds']='1197.50'
    refuses('wrong repeat duration',lambda:check_comparison(old,changed))
    data=bytearray(gzip.decompress((ROOT/'native-job.log.gz').read_bytes()));data[0]^=1
    refuses('raw log tamper',lambda:need(sha(data)==NEW_HASH,'raw hash'))
    return passed
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--self-test',action='store_true');a=p.parse_args()
    context=verify(a.repo);result={'verified':True,'source':HEAD,'actual_checkout':CHECKOUT,'complete_tree':TREE,'run':RUN,'attempt':2,'actual_new_job':NEW_JOB,'result':context[1]['summary'],'same213_order':True,'all_original_four_now_passed':True,'execution_scope':context[4],'original_causes':'inconclusive','engine_opened':False}
    if a.self_test:result['negative_controls_passed']=self_test(context)
    print(json.dumps(result,indent=2))
if __name__=='__main__':main()
