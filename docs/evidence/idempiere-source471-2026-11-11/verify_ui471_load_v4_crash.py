"""Independently compare the closed client ACK ledger with both recovered rowsets."""
import collections,copy,hashlib,json,pathlib,sqlite3,subprocess,time
from decimal import Decimal
root=pathlib.Path('/root/.local/state/bicdb/typed-compare');folder=root/'main471-ui-load-v4';out=folder/'independent-crash-verification.json';assert not out.exists()
source='b0e2498842be7b01e355bb8da1cc249ff16be056';binary='e23d7c1a078b7ce6722237f35cfa67a822727d9762c928c808f3ce2767c543c2'
while subprocess.check_output(['systemctl','show','bicdb-main471-ui-load-crash-v4','-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading','deactivating']:time.sleep(2)
coordinator=json.loads((folder/'crash-coordinator.json').read_text());assert coordinator['status']=='two_reopens_captured_independent_verification_pending',coordinator
while subprocess.check_output(['systemctl','show','bicdb-main471-ui-load-v4','-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading','deactivating']:time.sleep(2)
raw=subprocess.check_output(['ssh','-o','BatchMode=yes','root@74.50.69.58','cat /root/.local/state/bicdb/idempiere-reference/main471-ui-4lhe0gou/multiuser-crash-load-v4/result.json']);(folder/'remote-recovery-capture.json').write_bytes(raw);recovery=json.loads(raw);worker=json.loads((folder/'result.json').read_text());assert recovery['status']=='captured_two_reopens';assert recovery['source']==worker['source']==coordinator['source']==source;assert recovery['binary_sha256']==worker['binary_sha256']==binary;assert recovery['prefix']==worker['prefix'];assert recovery['actual_loaded_executable_sha256']==binary and recovery['pidfd_bound'];assert recovery['commit_durability']=='all-logs' and recovery['sync_outbox']=='off' and not recovery['experimental_page_flags_enabled'];assert coordinator['process_crash_performed'] and coordinator['clients_continue_until_signal_acknowledged']
assert all(int(coordinator['acks_before_cut_request'].get(str(i),0))>=20 for i in range(4));assert coordinator['pending_saves_before_cut_request'];assert all(next(x for x in recovery['original_process_shutdown'] if x['role']==role)['returncode']==code for role,code in [('database',-9)])
with sqlite3.connect('file:'+str(folder/'client-acks.sqlite')+'?mode=ro',uri=True) as database:
 assert database.execute('PRAGMA integrity_check').fetchone()[0]=='ok';database.row_factory=sqlite3.Row;attempts={row['marker']:dict(row) for row in database.execute('SELECT * FROM attempts')}
acks={k:v for k,v in attempts.items() if v['ack_time'] is not None};assert acks
assert set(worker['sessions'])=={'0','1','2','3'} and {s['user_id'] for s in worker['sessions'].values()}=={101,102}
for i in range(4):assert sum(a['worker']==i for a in acks.values())>=20

def audit(rows,attempts,acks):
 errors=[];by_marker=collections.defaultdict(list)
 for row in rows:
  if len(row)!=10:errors.append('row width');continue
  by_marker[row[2]].append(row)
 if any(len(v)!=1 for v in by_marker.values()):errors.append('duplicate marker')
 if len({str(row[0]) for row in rows})!=len(rows):errors.append('duplicate order id')
 if len({str(row[1]) for row in rows})!=len(rows):errors.append('duplicate document number')
 missing=set(acks)-set(by_marker)
 if missing:errors.append('acknowledged rows missing')
 if not set(by_marker)<=set(attempts):errors.append('row outside attempted-operation ledger')
 for marker,candidates in by_marker.items():
  if marker not in attempts or len(candidates)!=1:continue
  row=candidates[0];attempt=attempts[marker]
  if int(row[3])!=attempt['user_id'] or int(row[4])!=11 or int(row[5])!=118 or row[6:8]!=['DR','N'] or Decimal(str(row[8]))!=0 or Decimal(str(row[9]))!=0:errors.append('business values differ:'+marker)
  if marker in acks and str(row[1])!=acks[marker]['document_no']:errors.append('ACK document differs:'+marker)
 return dict(errors=errors,missing_acknowledged=sorted(missing),unacknowledged_committed=sorted(set(by_marker)-set(acks)),rows=len(rows))

captures=recovery['captures'];assert len(captures)==2 and [c['reopen'] for c in captures]==[1,2];results=[]
for capture in captures:
 assert capture['process_exit']==0;result=audit(capture['rows'],attempts,acks);assert not result['errors'],result
 assert capture['prior_invoice']==[['100001','CO','Y','Y','33.75','Y']],capture['prior_invoice'];assert capture['prior_payment']==[['400001','CO','Y','Y','33.07','0.68','Y']],capture['prior_payment'];results.append(result)
assert captures[0]['rows']==captures[1]['rows'],'second durable reopen differs'
# These controls operate on copied evidence and never mutate the application.
rows=captures[0]['rows'];first=next(i for i,row in enumerate(rows) if row[2] in acks);bad=copy.deepcopy(rows);bad.pop(first);assert audit(bad,attempts,acks)['errors']
bad=copy.deepcopy(rows);bad[first][1]='wrong-document';assert audit(bad,attempts,acks)['errors']
bad=copy.deepcopy(rows);bad[first][3]='102' if int(bad[first][3])==101 else '101';assert audit(bad,attempts,acks)['errors']
bad=copy.deepcopy(rows);bad.append(copy.deepcopy(bad[first]));assert audit(bad,attempts,acks)['errors']
bad=copy.deepcopy(rows);bad[first][2]=worker['prefix']+'-unrecorded';assert audit(bad,attempts,acks)['errors']
result=dict(status='four_active_user_sessions_process_crash_ack_rows_verified_on_two_reopens',source=source,binary_sha256=binary,attempts=len(attempts),acknowledged=len(acks),per_worker={str(i):sum(v['worker']==i for v in acks.values()) for i in range(4)},captures=results,negative_controls_rejected=['lost_ACK_row','changed_document_number','changed_user_identity','duplicate_row','row_outside_attempt_ledger'],prior_completed_posted_invoice_and_allocated_payment_preserved=True,signal_sent_unix=recovery['signal_sent_unix'],cut_source_process_start_ticks=recovery['process_start_ticks'],actual_pidfd_cut=True,all_logs_fsync=True,sync_outbox='off',experimental_flags_enabled=False,physical_power_loss=False,application_fully_qualified=False,ui_first_use_failures_remain_preserved=True,original_assertions_and_waits_unchanged=True,frontend_idle_synchronization_added=True,inputs={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [folder/'client-acks.sqlite',folder/'result.json',folder/'crash-coordinator.json',folder/'remote-recovery-capture.json',root/'main471-ui-load-v4-worker-shape.json']},finished_unix=time.time())
with out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
print(json.dumps(result,indent=2))
