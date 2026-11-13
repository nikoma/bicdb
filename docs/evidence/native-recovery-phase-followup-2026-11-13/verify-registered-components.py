#!/usr/bin/env python3
"""Reconcile public proposal/reader-fence/Installing component observations.
No DB/WAL replay, native grant recreation, fsync or serving qualification.
"""
import hashlib,json,pathlib,re,zipfile

def require(value,why):
    if not value:raise ValueError(why)

def digest(value):return hashlib.sha256(value).hexdigest()
def encoded(value):return json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()
def native_delta(before,after,pointer,bytes_sha):
    require(all(after.get(k)==v for k,v in before.items()),'existing native inventory changed')
    attempt=pointer['attempt_id'];xid=pointer['native_page_xid']
    added={k:v for k,v in after.items() if k not in before}
    expected={f'registered-remote-image-proposal/{attempt}/current',f'registered-remote-image-proposal/{attempt}/archive/{xid:020d}'}
    require(set(added)==expected and all(v==bytes_sha for v in added.values()),'native CURRENT/archive differs')

def proposal(d):
    require(d['before']==d['after'] and d['no_rows_or_serving'] and d['no_catalogue_installing'],'proposal changed target data or claimed later stage')
    record=d['record_after_first'];pointer=d['pointer'];outcome=d['first_outcome'];p=outcome['proposal']
    require(pointer==p['attempted_pointer'],'returned proposal pointer differs')
    require(pointer['attempt_id']==record['attempt']==outcome['actual_owner'] and outcome['lease_present'],'original actual A differs')
    require(pointer['native_page_xid']==record['native_page_xid']==p['actual_commit_returned_xid'],'actual commit xid differs')
    require(pointer['checksum_sha256']==record['checksum_sha256'],'protected record checksum differs')
    require(digest(bytes(record['intent_bytes']))==record['intent_sha256'],'full protected intent hash differs')
    native_delta(d['native_before'],d['native_after_first'],pointer,p['attempted_bytes_sha256'])
    return {'kind':'protected_proposal','pointer':pointer,'first_outcome':p['outcome'],'exact_error':d['exact_error'],'full_target_state_unchanged':True,
        'native_existing_inventory_unchanged':True,'only_matching_CURRENT_archive_added':True,'native_WAL_commit_is_source_assertion_not_replay':True}

def reader(d):
    require(d['before']==d['after'] and d['catalogue_installing_or_rows'] is False and d['hardware_power_loss'] is False,'reader-fence control scope differs')
    require(len(d['failures'])==3 and all('Input/output error' in e for e in d['failures']),'injected EIO observations differ')
    require(len(d['prepared_outcomes'])==2 and all(o['proposal']['outcome']=='prepared_no_native_xid' and o['proposal']['actual_commit_returned_xid'] is None for o in d['prepared_outcomes']),'unverified preparation relabeled committed')
    p=d['verified_retry_outcome']['proposal'];record=d['committed_record'];pointer=d['pointer']
    require(p['outcome']=='exact_current_archive_verified' and p['attempted_pointer']==pointer and p['actual_commit_returned_xid']==pointer['native_page_xid']==record['native_page_xid'],'exact retry pointer/xid differs')
    require(record['attempt']==pointer['attempt_id'] and record['checksum_sha256']==pointer['checksum_sha256'],'reader-fence protected record differs')
    require(digest(bytes(record['intent_bytes']))==record['intent_sha256'],'reader-fence protected full intent differs')
    native_delta(d['native_before'],d['native_after'],pointer,p['attempted_bytes_sha256'])
    return {'kind':'reader_fence_EIO_retry','pointer':pointer,'injected_errors':d['failures'],'prepared_outcomes_not_committed':2,'full_target_state_unchanged':True,
        'actual_reader_flag_durability_is_Rust_assertion':True,'old_binary_and_physical_replay_not_tested':True}

def installing(d):
    require(d['actual_catalogue_installing'] and d['no_rows_or_serving'] and d['hardware_power_loss'] is False,'Installing scope differs')
    before,after=d['before'],d['after'];require({k:v for k,v in before.items() if k!='raw'}=={k:v for k,v in after.items() if k!='raw'},'target state besides range catalogue changed')
    old=json.loads(bytes(before['raw']));new=json.loads(bytes(after['raw']));cat=new['catalog'];candidate=d['retained_after']['installing'];protected=d['protected_proposal'];pointer=candidate['pointer']
    require(digest(encoded(cat))==new['checksum_sha256']==candidate['candidate_sha256'],'actual raw catalogue/candidate digest differs')
    require(cat==candidate['candidate'] and candidate['verified'],'actual installed catalogue differs from verified candidate')
    original=old['catalog'];require(original['format_version']==15 and cat['format_version']==17,'actual format transition differs')
    require({k:v for k,v in original.items() if k not in ['format_version','image_install_intents']}=={k:v for k,v in cat.items() if k not in ['format_version','image_install_intents']},'preexisting catalogue fields changed')
    require(not original.get('image_install_intents') and len(cat['image_install_intents'])==1,'more than one actual refusal intent installed')
    intent=next(iter(cat['image_install_intents'].values()));require(digest(encoded(intent))==candidate['intent_sha256'],'actual Installing intent digest differs')
    require(intent['registered_proposal']==pointer and pointer['attempt_id']==protected['attempt']==d['retained_before']['actual_owner']==d['retained_after']['actual_owner'],'actual proposal/A binding differs')
    require(pointer['native_page_xid']==protected['native_page_xid'] and pointer['checksum_sha256']==protected['checksum_sha256'],'protected proposal pointer differs')
    raw_intent=bytes(protected['intent_bytes']);require(digest(raw_intent)==protected['intent_sha256'],'protected pre-pointer intent digest differs')
    require({k:v for k,v in intent.items() if k!='registered_proposal'}==json.loads(raw_intent),'Installing intent differs from actual protected source intent')
    require(len(d['errors'])==len(d['failed_outcomes']),'failed attempts lost their exact errors')
    for failed in d['failed_outcomes']:
        f=failed['installing'];require(not f['verified'] and f['candidate']==candidate['candidate'] and f['candidate_sha256']==candidate['candidate_sha256'] and f['intent_sha256']==candidate['intent_sha256'] and f['pointer']==pointer,'failed attempt replaced exact candidate/pointer')
    require(d['retained_before']['proposal']==d['retained_after']['proposal'],'proposal changed during Installing')
    return {'kind':'catalogue_Installing','pointer':pointer,'candidate_sha256':candidate['candidate_sha256'],'intent_sha256':candidate['intent_sha256'],'failed_attempts':len(d['failed_outcomes']),'exact_errors':d['errors'],'same_exact_candidate_across_attempts':True,'full_target_state_except_refusal_unchanged':True,
        'native_after_inventory_not_recorded':True,'hidden_native_inventory_equality_is_Rust_assertion_only':True,'rows_Ready_Serving_cold_replay_not_tested':True}

def verify(path,expected,source):
    require(digest(path.read_bytes())==expected,'ZIP digest differs')
    with zipfile.ZipFile(path) as z:
        require(z.read('source-revision.txt').decode().strip()==source,'source differs')
        logs=re.sub(r'\x1b\[[0-9;]*m','',z.read('tests.log').decode());results=dict(re.findall(r'^test ([^\s]+) \.\.\. (ok|FAILED)$',logs,re.M))
        selected=[l[:-6] for l in re.sub(r'\x1b\[[0-9;]*m','',z.read('selected-tests.log').decode()).splitlines() if l.endswith(': test')]
        require(set(selected)==set(results) and all(v=='ok' for v in results.values()),'actual selected/executed component tests differ')
        records=[];identities={}
        for name in z.namelist():
            if not re.search(r'/registered-(proposal|proposal-reader-fence-retry|installing)-[0-9]+\.json$',name):continue
            prefix=name.rsplit('/',1)[0]+'/';identity=json.loads(z.read(prefix+'execution-identity.json'))
            require(identity['source_commit']==source and identity['actual_rust_test_thread'] in results,'actual fixture source/test identity differs')
            identities[prefix]=identity
            d=json.loads(z.read(name));kind=name.rsplit('/',1)[1]
            result=reader(d) if 'reader-fence' in kind else installing(d) if kind.startswith('registered-installing') else proposal(d)
            records.append({'member':name,'identity':identity,'result':result})
        require({v['actual_rust_test_thread'] for v in identities.values()}==set(results),'missing actual case identity')
    return {'source':source,'projection_sha256':expected,'actual_tests':results,'observations':records,'qualification':False,
        'limits':['No native WAL/DB replay, independent fsync/power-loss proof, old-binary validation, row copy, Ready, Serving, automatic rebalance, capacity or SQL qualification.',
        'fd4 has no explicit native_after field; hidden inventory equality remains a source-pinned Rust assertion, never retrofilled.']}
