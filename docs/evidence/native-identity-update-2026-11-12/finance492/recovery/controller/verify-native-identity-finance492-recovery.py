"""Independently reconcile source, all cuts, original ACK hashes and audit identity."""
import argparse,copy,hashlib,json,pathlib,time
p=argparse.ArgumentParser();p.add_argument('--vm',action='store_true');p.add_argument('--finance492',action='store_true');a=p.parse_args()
root=pathlib.Path('/root/bicdb-durability-s3');source='3a8484bf16f1653d7e96292992b4900f706b7370';runtime='0031d1976ece1be2d6139ea9972fbf49069b1590';folder=root/'native-identity-recovery-3a8484bf';output=root/('report-native-identity-'+('process-IO-VM' if a.vm else 'process-IO')+'-independent.json')
if a.finance492:
 source='2403deefde68869c15f02b73d40406ffdfb50148';runtime=source;folder=root/'native-identity-finance492-recovery-2403deef';output=root/('report-native-identity-finance492-'+('process-IO-VM' if a.vm else 'process-IO')+'-independent.json')
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
def content_hash(row):return hashlib.sha256(json.dumps(row,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def messages(text,marker):return [json.loads(line.split(marker+' ',1)[1]) for line in text.splitlines() if line.startswith(marker+' ')]
record={'source':source,'production_equivalent':runtime,'status':'verification_failed','recorded_unix':time.time(),'scope':'ActualNativeidentityUPDATE+oneaudit: independentexactcoverage/source+binarySHA/original4seedACKfilehashes+Nativefullrecordhashes/NativeTXwitness/auditpayload+idempotency/threeunchangedopens/ACK-new/no-grant-refusal. Process38+EIO8; optionalactualKVM10. Not old482fixturequalification/fullapplication/physicalHA/capacity.'}
try:
 build=json.loads((root/('native-identity-finance492-probe-custody.json' if a.finance492 else 'report-native-identity-recovery-probe.json')).read_text());assert build['source']==source and build['status']==('probe_built' if a.finance492 else 'focused_passed') and sha(pathlib.Path(build['binary']))==build['binary_sha256']
 if a.finance492:
  progress=json.loads((root/'report-native-identity-finance492-validation.json').read_text());assert progress['source']==source and len(progress['steps'])>=3 and all(x.get('exit')==0 for x in progress['steps'][:3])
 result=json.loads((folder/'result.json').read_text());assert result['source']==source and result['production_equivalent']==runtime and result['status']=='recovery_passed' and result['originals_unchanged'] is True and result['binary_sha256']==build['binary_sha256']
 assert len(result['fixtures'])==len(result['controls'])==2
 fixtures={};expected_hashes={}
 for fixture in result['fixtures']:
  mode=fixture['mode'];assert mode in ['embedded_memory','server_paged'] and mode not in fixtures;fixtures[mode]=fixture
  before=pathlib.Path(fixture['original_ACK']);assert sha(before)==fixture['original_ACK_sha256'];old=json.loads(before.read_text());assert len(old['rows'])==len(fixture['ACKs'])==2
  actual=sorted([{'id':r['id'],'hash':content_hash(r)} for r in old['rows']],key=lambda x:x['id']);assert actual==sorted(fixture['ACKs'],key=lambda x:x['id'])
  row=next(r for r in old['rows'] if r['id']=='2');assert row['vector']==[0.25,0.5,0.75] and row['timestamp']==1700000001 and row['payload']==[0,1,2,255] and row['metadata']['native_ack']=='outside-SQL-schema-must-survive'
  new=copy.deepcopy(old['rows']);changed=next(r for r in new if r['id']=='2');changed['id']='4';changed['metadata']['k']=4;changed['metadata']['b']=2
  expected_hashes[mode]={'complete_old':actual,'complete_new':sorted([{'id':r['id'],'hash':content_hash(r)} for r in new],key=lambda x:x['id'])}
  db=pathlib.Path(fixture['database']);files={str(p.relative_to(db)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in db.rglob('*') if p.is_file() and not p.is_symlink()};assert files==fixture['files']
 def observations(obs,mode,tx,require_new=False,require_old=False,ack=None):
  assert len(obs)==3 and [x['reopen'] for x in obs]==[0,1,2] and len({x['state'] for x in obs})==1
  assert tx['old_id']=='2' and tx['new_id']=='4' and isinstance(tx['native_tx_id'],int) and tx['native_tx_id']>0
  for x in obs:
   assert x['full_records_schema_policy_equal'] is True and x['native_indexes_valid'] is True and x['grantless_write_refused'] is True
   assert x['state'] in ['complete_old','complete_new'] and x['row_hashes']==expected_hashes[mode][x['state']]
   if require_new:assert x['state']=='complete_new'
   if require_old:assert x['state']=='complete_old'
   assert x['row_hashes']==obs[0]['row_hashes'] and x['audit_messages']==obs[0]['audit_messages']
   if x['state']=='complete_old':assert x['audit_count']==0 and x['audit_messages']==[]
   else:
    assert x['audit_count']==len(x['audit_messages'])==1;message=x['audit_messages'][0];payload=message['payload']
    assert payload=={'transaction_id':tx['native_tx_id'],'grant_id':1,'relation':'computed','operation':'update','record_count':1,'record_ids':['2'],'record_ids_truncated':False,'actor':{'actor_id':'identity-recovery-host','roles':['writer'],'scopes':[],'tenant_id':None,'workspace_id':None,'originating_plugin':'source-pinned-recovery-probe','originating_resource':'computed','originating_action':'update','trace_id':'one-authorized-identity-update','deadline_unix_ms':4102444800000},'audit_metadata':{'proof':'one actual authorized UPDATE'},'replacement_record_ids':['4']}
    assert message['idempotency_key']==f'mutation-audit/{tx["native_tx_id"]}/1/1' and message['headers']['actor_id']=='identity-recovery-host'
    if ack:assert ack['update_acknowledged'] is True and ack['native_tx_id']==tx['native_tx_id'] and ack['audit_message_id']==message['message_id']
  return obs[0]['state']
 for c in result['controls']:
  assert c['native_commit_span']==1;ackp=pathlib.Path(c['ack']);assert sha(ackp)==c['ack_sha256'];ack=json.loads(ackp.read_text());assert ack['after_native_sequence']-ack['before_native_sequence']==1;observations(c['observations'],c['mode'],c['tx_started'],True,ack=ack)
 expected=set()
 for mode in fixtures:
  points=['identity.after_staging:1','identity.after_commit:1','identity.after_ack:1','commit.before_broker_publish:1','commit.after_broker_publish:1','transaction_log.write:1','transaction_log.sync:1']
  if mode=='server_paged':points+=['commit.'+s+':1' for s in ['before_decision','after_txlog_durable','after_page_apply','after_marker','after_sync_publish']]
  expected.update((mode,point,variant) for point in points for variant in [False,True])
 seen=set();states={}
 for c in result['process_cases']:
  key=(c['mode'],c['point'],c['volatile_page_and_directory_emulation']);assert key in expected and key not in seen;seen.add(key)
  assert c['passed'] is True and c['cut_exit']==-6 and c['cut_reached'] is True and c['verify_exit']==0
  assert c['acknowledged']==(c['point']=='identity.after_ack:1')
  name=pathlib.Path(c['database']).name.removesuffix('-db');cut=(folder/(name+'-cut.stdout')).read_text();assert messages(cut,'IDENTITY_TX_STARTED')==[c['tx_started']]
  parsed=messages((folder/(name+'-verify.stdout')).read_text(),'IDENTITY_VERIFIED');assert parsed==c['observations']
  ack=json.loads((folder/(name+'-ACK.json')).read_text()) if c['acknowledged'] else None
  state=observations(parsed,c['mode'],c['tx_started'],c['requires_complete_new'],c['point']=='identity.after_staging:1',ack);states[state]=states.get(state,0)+1
 assert seen==expected and len(seen)==38
 expected={(m,s,v) for m in fixtures for s in ['transaction_log.write','transaction_log.sync'] for v in [False,True]};seen=set();io_states={}
 for c in result['io_failure_cases']:
  key=(c['mode'],c['site'],c['persistent']);assert key in expected and key not in seen;seen.add(key)
  f=c['failure_report'];assert c['passed'] is True and c['failure_exit']==c['verify_exit']==0 and c['fault_reached'] is True and f['fault_fired']>0 and f['update_acknowledged'] is False and f['after_failure_write_refused'] is True and f['native_tx_id']==c['tx_started']['native_tx_id'] and f['error'] and f['after_failure_error']
  state=observations(c['observations'],c['mode'],c['tx_started']);io_states[state]=io_states.get(state,0)+1
 assert seen==expected and len(seen)==8
 record.update(process_cases=38,process_states=states,IO_failure_cases=8,IO_failure_states=io_states,fixture_ACKs=4,originals_unchanged=True,process_result_sha256=sha(folder/'result.json'),binary_sha256=build['binary_sha256'])
 if a.vm:
  vmroot=root/('native-identity-finance492-vm-2403deef' if a.finance492 else 'native-identity-vm-3a8484bf');v=json.loads((vmroot/'result.json').read_text());assert v['source']==source and v['production_equivalent']==runtime and v['status']=='vm_matrix_passed' and v['expected_cases']==10 and v['originals_unchanged'] is True
  assert v['binary_sha256']==v['guest_sha256']==build['binary_sha256']==sha(vmroot/'initroot/bin/identity_probe');assert v['kernel_sha256']==sha(root/'vm-reset/vmlinuz') and v['initrd_sha256']==sha(vmroot/'initrd.gz')
  expected={(m,s) for m in fixtures for s in ['transaction_log.write','transaction_log.sync','identity.after_commit','identity.after_ack']}|{('server_paged',s) for s in ['page_log.write','page_log.sync']};seen=set();vm_states={};acked=0
  for c in v['cases']:
   key=(c['mode'],c['site']);assert key in expected and key not in seen;seen.add(key);assert c['passed'] is True and c['hard_kill_exit']==-9 and c['saved_image_unchanged_after_verification'] is True and c['original_ACKs_linked']==2 and c['original_ACK_sha256']==fixtures[c['mode']]['original_ACK_sha256']
   raw=pathlib.Path(c['saved_image']);assert raw.parent.parent==vmroot and raw.name=='crashed.raw' and sha(raw)==c['saved_image_sha256']
   with raw.open('rb') as f:f.seek(1080);assert f.read(2)==b'\x53\xef'
   work=raw.parent;cut=(work/'cut.console').read_text();verified=(work/'verify.console').read_text();w=json.loads((work/'external-witness.json').read_text())
   assert v['guest_sha256'] in cut and v['guest_sha256'] in verified and 'IDENTITY_VM_APPLY_UNEXPECTED_EXIT' not in cut and 'IDENTITY_VM_VERIFY_DONE_0' in verified
   assert messages(cut,'IDENTITY_VM_CUT_READY')==[{'site':c['site']}] and messages(cut,'IDENTITY_TX_STARTED')==[c['tx_started']]
   for field in ['mode','site','owned_qemu_pid','guest_witness','tx_started','acknowledged','ack']:assert w[field]==c[field]
   assert c['acknowledged']==(c['site']=='identity.after_ack');acks=messages(cut,'IDENTITY_VM_ACK');assert len(acks)==int(c['acknowledged'])
   if c['acknowledged']:assert acks==[c['ack']];acked+=1
   parsed=messages(verified,'IDENTITY_VERIFIED');assert parsed==c['observations']
   state=observations(parsed,c['mode'],c['tx_started'],c['site'] in ['identity.after_commit','identity.after_ack'],c['site']=='transaction_log.write',c['ack']);vm_states[state]=vm_states.get(state,0)+1
  assert seen==expected and len(seen)==10 and acked==2;record.update(VM_cases=10,VM_states=vm_states,VM_acknowledged_new=2,VM_result_sha256=sha(vmroot/'result.json'))
 record['status']='verified_passed'
except BaseException as error:record['error']=repr(error)
assert not output.exists();output.write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record));raise SystemExit(0 if record['status']=='verified_passed' else 1)
