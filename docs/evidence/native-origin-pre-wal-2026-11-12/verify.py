#!/usr/bin/env python3
"""Offline original-log/ledger reconciliation; no engine, compiler or network."""
import argparse
import collections
import copy
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile

SOURCE = '721c329de4196aeef96604642c439fe7d8ddb0bf'
ZIP_SHA = '8528db4cb8a55a32eca222a1c695f120370edc8b996db6b3c50b8905e68e66a8'
def need(ok, message):
    if not ok: raise ValueError(message)
def sha(data): return hashlib.sha256(data).hexdigest()
def load(path): return json.loads(path.read_bytes())
def canonical(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
def derive(rows):
    need([r['journal_sequence'] for r in rows] == list(range(1, len(rows)+1)), 'journal sequence gaps')
    kinds = collections.Counter(r['kind'] for r in rows)
    bykind = {k: [r for r in rows if r['kind'] == k] for k in kinds}
    singleton = lambda k: bykind[k][0] if len(bykind[k]) == 1 else None
    identity = singleton('execution_identity'); need(identity['source'] == SOURCE, 'execution source')
    exit_event = singleton('actual_origin_exit'); probe = exit_event['probe']
    need(all(probe[x] is True for x in ('fired','release_callback_returned','release_callback_ok','normal_app_handler_exit','app_span_ended')), 'actual origin prerequisites')
    need(probe['core_result'] == 'commit_outcome_unknown' and probe['unknown_sequence'] is None and probe['unknown_transaction_id'] == probe['actual_transaction_id'] == 11, 'actual first Unknown/transaction')
    need(not probe['panicked'] and not probe['diagnostic_integrity_error'], 'observer/handler failure')
    native = singleton('actual_native_cut'); command = native['actual_command']; old = singleton('old_ack_native_command_correlation')
    need(command['command_id'] == probe['actual_ticket_command_id'] and command['index'] == 2 and command['range_id'] == 3 and command['range_epoch'] == 1 and command['leader_node_id'] == 'n0', 'actual admitted command scope')
    need(native['raw_state'] == 'quorum_committed' and native['native_proof_not_inferred_from_raw_counters'], 'retained CQ classification')
    p = native['origin']; need(p['native_applied_index'] == 1 and p['pending_command_index'] == 2 and p['pending_command_id'] == command['command_id'] and p['materialized_pending_index'] is None and not p['invalidated'], 'actual origin pending cut')
    peers = native['peer_native_materialization']; need([p['node'] for p in peers] == ['n1','n2'], 'actual two peers')
    facts = [p['verified_actual_native'] for p in peers]
    for f in facts:
        need(f['native_R'] == 2 and f['typed_local_B'] == 2 and f['H0'] == old['native_before']['H0'], 'peer actual native/typed B')
        need(f['full_record'] == command['mutations'][0]['record'] and f['old_ack_full_record'] == old['actual_command']['mutations'][0]['record'], 'both full native records')
    need(facts[0]['H'] == facts[1]['H'] and len(facts[0]['H']) == 64, 'peer history equality')
    intents = bykind['logical_intent']; replies = {r['diagnostic_business_id']:r for r in bykind['logical_public_reply']}
    need(len(intents) == len(replies) == 13, 'complete logical API ledger')
    operations = []
    for intent in intents:
        context = intent['context']; lid = context['diagnostic_business_id']; reply = replies[lid]
        need(intent['journal_sequence'] < reply['journal_sequence'], 'intent before receipt')
        if context['operation'] == 'append':
            need(sha(canonical(context['payload'])) == context['payload_sha256'], 'full logical payload hash')
        phases = [r for r in bykind['actual_rpc'] if r['logical_context']['diagnostic_business_id'] == lid]
        need(all(r['logical_context'] == context and intent['journal_sequence'] < r['journal_sequence'] < reply['journal_sequence'] for r in phases), 'RPC/parent context lifetime')
        attempts = {}
        for r in phases:
            a = r['actual']; attempts.setdefault(a['attempt_id'], []).append(a)
        wire=[]; discoveries=collections.Counter()
        for attempt_id, events in attempts.items():
            item={'attempt_id':attempt_id,'operation':events[0]['operation'],'request_id':next((a['request_id'] for a in events if a['request_id'] is not None),None),
                'destination':events[0]['destination'],'generation':next((a['topology_generation'] for a in events if a['topology_generation'] is not None),None),
                'positive_bytes_written':max(a['bytes_written'] for a in events),'may_have_dispatched':any(a['may_have_dispatched'] for a in events),
                'phases':[{'phase':a['phase'],'wall_ns':a['phase_unix_ns'],'elapsed_ns':a['elapsed_ns'],'error_chain':a['error_chain'],
                    'frame_bytes':len(a['raw_frame']) if a['raw_frame'] is not None else None,
                    'frame_sha256':sha(bytes(a['raw_frame'])) if a['raw_frame'] is not None else None} for a in events]}
            frames=[]
            for a in events:
                if a['phase'] not in ('request_frame_before_dispatch','response_frame_before_decode'): continue
                raw=bytes(a['raw_frame']); need(int.from_bytes(raw[:4],'big') == len(raw)-4, 'exact frame boundary')
                body=json.loads(raw[4:]); need(body['request_id'] == a['request_id'], 'request ID binding')
                frames.append({'phase':a['phase'],'body':body})
            if item['operation'] == 'metadata_snapshot_discovery':
                for f in frames:
                    if f['phase'] == 'response_frame_before_decode':
                        b=f['body']; snap=b.get('response',{}).get('Ok',{}).get('BootstrapSnapshot')
                        need(snap is not None, 'discovery response is not successful snapshot')
                        discoveries[b['source_node_id']+':generation='+str(snap['topology']['generation'])]+=1
            else:
                item['frames']=frames;wire.append(item)
        operations.append({'context':context,'intent_sequence':intent['journal_sequence'],'reply':reply,
            'observed_transport_attempts_including_discovery':len(attempts),
            'successful_discovery_replies':dict(discoveries),'application_or_strong_attempts':wire})
    need(operations[0]['reply']['outcome']['classification'] == 'USER_ACK' and operations[1]['reply']['outcome']['classification'] == 'STRONG_PRESENT' and operations[0]['reply']['outcome']['full_payload'] == operations[1]['reply']['outcome']['full_payload'], 'old external ACK full Strong')
    need(operations[1]['reply']['journal_sequence'] < singleton('fault_armed')['journal_sequence'] < exit_event['journal_sequence'] < operations[3]['intent_sequence'], 'actual exit before postfault reads')
    need(all(o['reply']['outcome']['classification'] != 'STRONG_ABSENT' for o in operations), 'successful false absence')
    need(all(o['reply']['outcome']['classification'] != 'USER_ACK' for o in operations[2:]), 'later API ACK classification changed')
    rounds=bykind['health_round'];need(len(rounds)==2 and all(not r['next_user_ack'] and not r['ever_next_user_ack'] and not r['old_exact'] and not r['committed_exact'] and not r['next_exact'] for r in rounds), 'actual failed health gate')
    stopped=singleton('test_terminal_after_owned_stop_attempts');need(stopped['cleanup_errors']==[] and stopped['portable_native_db_custody'] is False and stopped['no_engine_reopen_or_repair'] is True, 'cleanup/custody classification')
    need('REQUIRED autonomous committed-tail repair' in stopped['first_error'], 'earliest final failure')
    return {'identity':identity,'kind_counts':dict(kinds),'old_ack_native_command':old,'actual_first_origin_exit':exit_event,
        'actual_native_cut':native,'logical_operations':operations,'health_rounds':rounds,'cleanup':stopped,
        'limits':{'bootstrap_raw_frame_covered':False,'complete_global_wire_ledger':False,'portable_native_DB_custody':False,'native_engine_replay':False,'physical_qualified':False}}
def verify(root, repo=None):
    manifest=load(root/'MANIFEST.json')
    need({str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts} == set(manifest)|{'MANIFEST.json','README.md','verify.py'}, 'unexpected packet paths')
    for name,item in manifest.items():
        raw=(root/name).read_bytes();need(len(raw)==item['bytes'] and sha(raw)==item['sha256'], 'payload checksum '+name)
    receipt=load(root/'receipt.json');need(receipt['source']==SOURCE and receipt['core']['passed']==1 and receipt['core']['failed']==1 and receipt['classification']=='FAILED_REQUIRED_AUTONOMOUS_RECOVERY', 'receipt result/source')
    raw=(root/'original-artifact.zip').read_bytes();need(len(raw)==211301 and sha(raw)==ZIP_SHA,'original artifact identity')
    with zipfile.ZipFile(root/'original-artifact.zip') as z:
        need(z.testzip() is None and len(z.infolist())==6 and sum(x.file_size for x in z.infolist())<8*1024*1024, 'original archive inventory')
        names=z.namelist();journal_name=next(n for n in names if n.endswith('/journal.jsonl'))
        need(set(names)=={journal_name,journal_name.rsplit('/',1)[0]+'/terminal.json','rustc.txt','selected-tests.log','source-revision.txt','tests.log'},'non-evidence archive member')
        need(z.read('source-revision.txt').decode().strip()==SOURCE,'artifact source')
        need(b'1 passed; 1 failed; 0 ignored; 0 measured; 957 filtered out; finished in 26.04s' in z.read('tests.log'),'actual runtime outcome')
        rows=[json.loads(line) for line in z.read(journal_name).splitlines()]
        actual=derive(rows);need(actual==load(root/'events.json'),'derived ledger/facts differ')
        terminal=json.loads(z.read(journal_name.rsplit('/',1)[0]+'/terminal.json'))
        need(terminal==receipt['actual_terminal'] and not terminal['bounded_autonomous_repair_and_next_ack'] and terminal['health_limit_ns']==20_000_000_000 and terminal['health_elapsed_ns']==15_333_465_020,'terminal receipt/deadline')
    for label in ('core','compile'):
        meta=load(root/'raw'/(label+'-index.json'));data=gzip.decompress((root/'raw'/(label+'-job.log.gz')).read_bytes())
        need(meta['source_revision']==SOURCE and len(data)==meta['raw_bytes'] and sha(data)==meta['raw_sha256'],'raw job/source identity')
        need(meta['job_conclusion']==('failure' if label=='core' else 'success'),'actual job conclusion')
    if repo:
        for item in load(root/'source-files.json')['files']:
            raw=subprocess.check_output(['git','-C',str(repo),'show',SOURCE+':'+item['path']]);need(len(raw)==item['bytes'] and sha(raw)==item['sha256'],'selected exact source '+item['path'])
    print('PASS exact failed baseline: 1PASS/1FAIL; actual first Unknown/release/EXIT; origin R1/Pending2 and two native R2 peers; 13 API receipts/1134 RPC phases; no replay')
def tamper(root):
    cases=['classification','probe','peer_record','api_ack','frame','custody']
    for case in cases:
        with tempfile.TemporaryDirectory(prefix='bicdb-origin-evidence-tamper-') as d:
            target=Path(d)/'packet';shutil.copytree(root,target)
            if case=='classification':
                p=target/'receipt.json';v=load(p);v['classification']='PASSED'
            else:
                p=target/'events.json';v=load(p)
                if case=='probe':v['actual_first_origin_exit']['probe']['unknown_sequence']=0
                elif case=='peer_record':v['actual_native_cut']['peer_native_materialization'][0]['verified_actual_native']['full_record']['metadata']={}
                elif case=='api_ack':v['logical_operations'][2]['reply']['outcome']['classification']='USER_ACK'
                elif case=='frame':v['logical_operations'][2]['application_or_strong_attempts'][0]['frames'][1]['body']['response']={'Ok':{'ApplicationEvent':{'result':'appended','inserted':True}}}
                else:v['limits']['portable_native_DB_custody']=True
            p.write_text(json.dumps(v)+'\n');manifest=load(target/'MANIFEST.json');data=p.read_bytes();manifest[p.name]={'bytes':len(data),'sha256':sha(data)};(target/'MANIFEST.json').write_text(json.dumps(manifest)+'\n')
            try:verify(target)
            except (ValueError,AssertionError):continue
            raise AssertionError('tamper accepted: '+case)
    print('PASS 6 semantic tamper refusals with outer byte manifest refreshed')
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path);p.add_argument('--tamper-tests',action='store_true');a=p.parse_args();root=Path(__file__).resolve().parent
    verify(root,a.repo)
    if a.tamper_tests:tamper(root)
