#!/usr/bin/env python3
"""Offline byte/provenance/semantic verifier; never opens an engine or replays WAL."""
import argparse,collections,gzip,hashlib,json,pathlib,subprocess
HEAD='1d3976f9f8011f3ac7411494aa0dfb432763ce78'
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def require(ok,why):
 if not ok:raise ValueError(why)
def verify(root,repo=None,private=None):
 root=pathlib.Path(root);manifest=load(root/'manifest.json')
 for n,d in manifest.items():
  p=root/n;require(p.is_file() and not p.is_symlink() and sha(p.read_bytes())==d,'portable hash '+n)
 r=load(root/'receipt.json');require(r['source']==HEAD and r['cli_passed']==5 and r['cli_failed']==1 and not r['strict_health_success'],'baseline classification')
 require(not r['engine_replay_or_ack_reconciliation_performed'] and not r['bootstrap_setup_raw_covered'],'scope limitation')
 for s in r['sources']:
  body=(root/s['file']).read_bytes();require(sha(body)==s['sha256'],'source bytes')
  if repo:require(body==subprocess.check_output(['git','-C',str(repo),'show',HEAD+':'+s['path']]),'source leaf differs')
 for stem,key in [('1d3-strict-writer-registry-baseline-job131408621456','cli_raw_sha256'),('1d3-strict-writer-registry-alltargets-job131408563373','checks_raw_sha256')]:
  idx=load(root/'raw'/(stem+'-index.json'));body=gzip.decompress((root/'raw'/(stem+'.log.gz')).read_bytes());require(sha(body)==r[key]==idx['raw_sha256'] and idx['source_revision']==HEAD,'raw source/log')
  if key=='cli_raw_sha256':
   require(b'5 passed; 1 failed' in body and b'old_ack_read=false, fresh_ack=false, fresh_ack_read=false' in body,'intended final failure');cli_raw=body
 ledger=[json.loads(x) for x in (root/'observations/external-ledger.jsonl').read_bytes().splitlines()]
 require(sum(x.get('acknowledged') is True for x in ledger)==5,'five case-correlated external ACKs')
 actual_ledger=[json.loads(line.split('NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ',1)[1]) for line in cli_raw.decode().splitlines() if 'NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ' in line]
 require(actual_ledger==ledger,'primary ledger vs exact raw CI output')
 require(load(root/'observations/lineage-and-configurations.json')==[x for x in ledger if x.get('stage') in ['readonly_retained_writer_binding','actual_before_after_configuration','same_five_owned_children_after_health_gate','strict_registry_addition_terminal']],'derived lineage parity')
 lineage=[x for x in ledger if x.get('stage')=='readonly_retained_writer_binding'];require(len(lineage)==8,'before/after binding records')
 fields=['retained_owner_coordinator_id','persisted_policy_coordinator_id','persisted_policy_registration_generation','persisted_policy_database_open_incarnation','persisted_policy_range_store_open_id','persisted_policy_checksum_sha256']
 for i in range(4):
  before=next(x for x in lineage if x['node']==f'n{i}' and x['phase']=='before_fifth')['reply']['host']['initial_data'];after=next(x for x in lineage if x['node']==f'n{i}' and x['phase']=='after_health_gate')['reply']['host']['initial_data']
  a=before['writer_binding_diagnostic'];b=after['writer_binding_diagnostic'];require(all(a[k]==b[k] for k in fields),'original writer binding changed')
  require(before['writer_policy_current'] and not after['writer_policy_current'],'registry policy status cut')
  for d in [a,b]:require(d['observed_bound_registration_active'] and d['observed_bound_authority_matches_retained_coordinator'] and d['observed_binding_lifetime_live'] and d['current_database_hook_slot_not_relocked'],'diagnostic liveness/scope')
 cfg=next(x for x in ledger if x.get('stage')=='actual_before_after_configuration');a,b=cfg['before'],cfg['after']
 require(a['ranges']==b['ranges'] and b['nodes']['n4']['labels']=={},'unchanged ranges/missing new schema')
 require(all(a['nodes'][f'n{i}']['native_store_birth_v1']==b['nodes'][f'n{i}']['native_store_birth_v1'] for i in range(4)),'original births')
 children=next(x for x in ledger if x.get('stage')=='same_five_owned_children_after_health_gate');require(len(children['children'])==5 and all(c['running'] is True for c in children['children']) and not children['restarted'],'actual five children')
 allrows=[]
 for meta in r['journals']:
  body=gzip.decompress((root/'journals'/(meta['name']+'.jsonl.gz')).read_bytes());rows=[json.loads(x) for x in body.splitlines()]
  require(sha(body)==meta['raw_sha256'] and len(body)==meta['raw_bytes'] and len(rows)==meta['records'],'journal bytes')
  require(rows[0]['source']==HEAD and rows[0]['fixture_root']==meta['fixture_root'],'journal source/case')
  require([x['sink_sequence'] for x in rows]==list(range(1,len(rows)+1)),'sink sequence')
  intents={x['diagnostic_operation_id'] for x in rows if x['kind']=='logical_request_intent'};replies={x['diagnostic_operation_id'] for x in rows if x['kind']=='logical_public_reply'}
  require(intents==replies and rows[-1]['kind']=='fixture_case_finished','complete logical journal')
  seq=collections.defaultdict(list)
  for x in rows:
   if x['kind']=='actual_rpc_phase':
    v=x['actual'];seq[x['observer_instance_id']].append(v['journal_sequence'])
    if v['raw_frame']:
     raw=bytes(v['raw_frame']);require(int.from_bytes(raw[:4],'big')==len(raw)-4,'raw frame length');json.loads(raw[4:])
  require(all(sorted(v)==list(range(1,len(v)+1)) for v in seq.values()),'observer sequence')
  allrows.append(rows)
 strict=allrows[2];calls={x['diagnostic_operation_id']:x['request'] for x in strict if x['kind']=='logical_request_intent' and x['request'].get('phase')=='strict_after_fifth'}
 require(len(calls)==4,'both APIs in two rounds');derived={}
 for lid,request in calls.items():
  req=[];resp=[];snap=[]
  for x in strict:
   if x['kind']!='actual_rpc_phase' or x['logical_context']['diagnostic_operation_id']!=lid:continue
   actual=x['actual']
   if not actual['raw_frame']:continue
   obj=json.loads(bytes(actual['raw_frame'])[4:])
   if actual['operation']=='metadata_snapshot_discovery':
    if 'response' in obj:snap.append(actual['destination'])
   elif 'request' in obj:req.append((actual,obj))
   else:resp.append((actual,obj))
  if request['public_operation']=='get_strong':
   require(len(req)==8 and len(resp)==7,'read request/reply frame count')
   for actual,obj in req:
    q=obj['request']['NativeEventGet'];require(actual['destination']=='n0' and q['range_id']==4 and q['range_epoch']==1 and q['mode']=='strong','actual read scope')
   for actual,obj in resp:
    v=obj['response']['Ok']['NativeEventGet'];e=v['result'];require(e['kind']=='unavailable' and e['code']=={'code':'native_read','stage':'WriterAdmission'} and e['route']=={'generation':4,'range_id':4,'range_epoch':1,'leader':'n0'} and 'current voter registry changed' in e['detail'],'actual read refusal')
  else:
   require(len(req)==5 and len(resp)==5,'write request/reply frame count')
   for actual,obj in req:
    q=obj['request']['ApplicationEvent'];require(actual['destination']=='n2' and q['range_id']==2 and q['range_epoch']==1 and q['operation']['value']==request['value'],'actual write scope/full payload')
   for actual,obj in resp:require(obj['source_node_id']=='n2' and obj['response']['Err']=={'code':'cluster_data_operation_failed','message':'cluster error: native host writer enrollment: current voter registry changed'},'actual write refusal')
  events=[x['actual'] for x in strict if x['kind']=='actual_rpc_phase' and x['logical_context']['diagnostic_operation_id']==lid and x['actual']['operation']!='metadata_snapshot_discovery']
  written=len({x['attempt_id'] for x in events if x['bytes_written']>0});derived[lid]=(len(req),len(resp),len(snap),written)
 require(sorted(derived.values())==sorted([(8,7,34,8),(5,5,20,5),(8,7,35,7),(5,5,20,5)]),'observed dispatch and successful discovery counts')
 declared=load(root/'observations/strict-wire.json')
 for lid,counts in derived.items():
  require((len(declared[lid]['request_frames']),len(declared[lid]['response_frames']),sum(declared[lid]['snapshot_responses'].values()),declared[lid]['positive_bytes_written_attempts'])==counts,'derived wire count parity')
  for entry in declared[lid]['response_frames']:
   actual=next(x for x in strict if x['sink_sequence']==entry['sequence'])['actual'];body=json.loads(bytes(actual['raw_frame'])[4:]);require(entry['response']==body['response'] and entry['source']==body['source_node_id'] and entry['actual_native_source']==body.get('actual_native_source'),'derived exact response parity')
 classification=load(root/'observations/catalogue-classification.json');expected=classification['strict_case_prior_ack'];require(classification['unknown_api_attempts']==2 and not classification['unknown_key_in_captured_raw_catalogues'] and not classification['other_cohort_db_roots_retained'],'unknown/case scope')
 for meta in classification['catalogues']:
  body=(root/'catalogues'/(meta['node']+'.json')).read_bytes();require(sha(body)==meta['sha256'],'raw catalogue bytes');env=json.loads(body);cat=env['catalog'];require(sha(json.dumps(cat,separators=(',',':'),ensure_ascii=False).encode())==env['checksum_sha256'],'catalogue envelope')
  require(classification['unknown_key'].encode() not in body,'unknown key raw inventory changed')
  if meta['node'] in expected['raw_applied_copies']:
   entry=cat['ranges']['4']['entries']['1'];require(entry['state']=='applied' and entry['command']['checksum_sha256']==expected['raw_command_sha256'] and entry['command']['mutations'][0]['record']['metadata']==expected['value'],'raw ACK command/payload')
  else:require(not cat['ranges'],'unassigned/nonparticipant raw case inventory')
 mb=(root/'custody/stopped-manifest.json').read_bytes();m=json.loads(mb);require(sha(mb)==r['db_manifest_sha256'] and m['source_revision']==HEAD and m['complete'] and m['payload_published'] and m['status']=='COPIED_STOPPED_OWNED_ROOTS','stopped custody classification')
 require([x['pid'] for x in m['actual_children_waited']]==[x['pid'] for x in children['children']] and all(x['wait_ok'] for x in m['actual_children_waited']),'actual all-child wait before copy')
 files=[e for e in m['entries'] if e['type']=='regular_file'];require(len(files)==96 and sum(x['copied_bytes'] for x in files)==2275560,'full copied file inventory')
 if private:
  private=pathlib.Path(private)
  for a in r['private_artifacts']:
   p=private/f"artifact{a['id']}.zip";require(p.stat().st_size==a['bytes'] and sha(p.read_bytes())==a['sha256'],'private ZIP parity')
  base=next((private/'extracted').glob('database-failure-*'));require((base/'manifest.json').read_bytes()==mb,'private manifest parity')
  for e in files:
   p=base/'database-snapshot'/e['path'];require(p.is_file() and not p.is_symlink() and p.stat().st_size==e['copied_bytes'] and sha(p.read_bytes())==e['copied_sha256'],'private file parity '+e['path'])
 print('PASS exact strict RED;5PASS/1FAIL;26 pre-dispatch frames,25 positive-write-count attempts,24 complete refusal replies;109 discovery replies;96 stopped files; no engine replay')
 return True
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=pathlib.Path);p.add_argument('--private-dir',type=pathlib.Path);a=p.parse_args();verify(pathlib.Path(__file__).resolve().parent,a.repo,a.private_dir)
