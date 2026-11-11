"""Cut only the proven private server while all four original workers are active."""
import hashlib,json,os,pathlib,shlex,sqlite3,subprocess,time
root=pathlib.Path('/root/.local/state/bicdb/typed-compare');folder=root/'main471-ui-load-v4';out=folder/'crash-coordinator.json';assert not out.exists();r=dict(status='waiting_prior_useful_run_and_current_readiness',source='b0e2498842be7b01e355bb8da1cc249ff16be056',started_unix=time.time(),physical_power_loss=False,process_crash_performed=False)
def save():
 p=out.with_suffix('.tmp')
 with p.open('w') as f:json.dump(r,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
 p.replace(out)
while not folder.exists():time.sleep(1)
save()
try:
 while True:
  status=subprocess.check_output(['systemctl','show','bicdb-main471-ui-load-v4','-p','ActiveState','--value'],text=True).strip();assert status in ['active','activating','reloading']
  if (folder/'start-coordinator.json').exists():
   start=json.loads((folder/'start-coordinator.json').read_text());assert start['status']=='started_all_four_ready_sessions';r['start_receipt']=start;break
  if (folder/'result.json').exists():
   worker=json.loads((folder/'result.json').read_text());assert not any(v['status']=='error' for v in worker['sessions'].values()),'readiness failed; retain useful producer without sending a signal'
  time.sleep(1)
 r['status']='waiting_four_active_acknowledged_writers';save()
 while True:
  worker=json.loads((folder/'result.json').read_text());assert worker['source']==r['source'];assert set(worker['sessions'])=={'0','1','2','3'};assert {v['user_id'] for v in worker['sessions'].values()}=={101,102};assert not any(v['status']=='error' for v in worker['sessions'].values()),'original worker assertion failed; do not cut or stop useful remaining workers'
  with sqlite3.connect('file:'+str(folder/'client-acks.sqlite')+'?mode=ro',uri=True) as c:
   c.execute('BEGIN');counts=dict(c.execute('SELECT worker,COUNT(*) FROM attempts WHERE ack_time IS NOT NULL GROUP BY worker'));pending=c.execute('SELECT marker,worker FROM attempts WHERE ack_time IS NULL AND error IS NULL').fetchall();c.rollback()
  if all(counts.get(i,0)>=20 for i in range(4)) and pending and all(v['status']=='working' for v in worker['sessions'].values()):break
  assert worker['status'] not in ['sessions_finished','stopped_after_requested_crash'],'producer completed before active-write coverage; do not call a terminal run a crash test';time.sleep(.05)
 r.update(status='launching_verified_cut',prefix=worker['prefix'],acks_before_cut_request=counts,pending_saves_before_cut_request=pending,worker_stages_before_cut={k:v.get('stage') for k,v in worker['sessions'].items()},requested_unix=time.time(),clients_continue_until_signal_acknowledged=True);save()
 command=['systemd-run','--wait','--pipe','--unit=bicdb-main471-ui-load-v4-cut-recovery','--property=MemoryMax=12G','--property=MemoryHigh=10G','--property=MemorySwapMax=0','--property=CPUQuota=200%','--property=LimitCORE=0','/root/bicdb-durability-s3/codex-monitored-v2.sh','/root/bicdb-durability-s3/host-control/venv/bin/python3','/root/bicdb-durability-s3/host-control/cut_and_recover_main471_ui_load_v4.py','--prefix',worker['prefix']]
 process=subprocess.Popen(['ssh','-o','BatchMode=yes','root@74.50.69.58',shlex.join(command)],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);r['remote_observer_pid']=process.pid;save()
 with (folder/'remote-cut-and-recovery.log').open('x') as log:
  for line in process.stdout:
   log.write(line);log.flush()
   try:event=json.loads(line)
   except json.JSONDecodeError:continue
   if event.get('event')=='owned_process_sigkill_sent':
    (folder/'crash-requested').write_text('Actual remote pidfd SIGKILL confirmed; preserve pending-save and ACK outcomes\n');r.update(status='signal_sent_recovery_running',process_crash_performed=True,cut_event=event);save()
  code=process.wait()
 r['remote_exit']=code;save();assert code==0 and r['process_crash_performed'];r.update(status='two_reopens_captured_independent_verification_pending',finished_unix=time.time());save()
except BaseException as error:
 r.update(status='requires_followup',error=repr(error),finished_unix=time.time());save();raise
