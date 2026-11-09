#!/usr/bin/env python3
"""Synthetic >buffer-pool transaction/blob reconciliation, hard QEMU kills.
Only task-owned VM and disks are touched. Not a physical power-loss test,
not a product online-backup test, and not a production deployment.
"""
from pathlib import Path
import hashlib,json,os,signal,stat,subprocess,threading,time
from pgw import Conn,ConnectionLost
BASE=Path(__file__).resolve().parent
assert str(BASE)=='/root/bicdb-durability-s3/vm-reset'
DISK=Path(os.environ['BICDB_VM_DISK']);assert DISK.parent==BASE
assert DISK.name.startswith('large-tx-')
RUN=BASE/('large-tx-campaign-'+str(time.time_ns()));RUN.mkdir()
N=8192;WIDTH=384;POOL=int(os.environ['BICDB_VM_POOL']);BLOB=POOL*2+65536
assert N*WIDTH>POOL
ack=set();observed_committed=set();attempted=set();native={};vm=None;resets=0

def sha(b):return hashlib.sha256(b).hexdigest()
def journal(name,value):
 with (RUN/(name+'.jsonl')).open('a') as f:
  f.write(json.dumps(value,sort_keys=True)+'\n');f.flush();os.fsync(f.fileno())
def payload(g,i):return (f'g{g:02}-i{i:05}-'+'abcdefghijklmnopqrstuvwxyz'*20)[:WIDTH]
def blob(g):return (f'blob-generation-{g}-'+'0123456789abcdef'*((BLOB//16)+1))[:BLOB]
def connect():
 end=time.monotonic()+120;last=None
 while time.monotonic()<end:
  try:
   c=Conn('127.0.0.1',56388,timeout=180);c.query('SELECT 1');return c
  except Exception as e:last=repr(e);time.sleep(.1)
 raise RuntimeError('guest unavailable '+str(last))
def launch(disk=DISK):
 for name in ['qmp.sock','console.sock']:
  p=BASE/name
  if p.exists():assert stat.S_ISSOCK(p.stat().st_mode);p.unlink()
 env=os.environ.copy();env['BICDB_VM_DISK']=str(disk)
 with (RUN/'qemu.log').open('ab') as f:
  return subprocess.Popen(['bash',str(BASE/'launch.sh')],stdout=f,stderr=f,env=env,start_new_session=True)
def kill():
 global vm,resets
 assert vm is not None and vm.poll() is None
 os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10);vm=None;resets+=1
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(DISK),str(RUN/f'crashed-{resets}.raw')],check=True)
def restart():
 global vm
 vm=launch();return connect()
def metrics(c,label):
 report=json.loads(c.query('SELECT bicdb_space_report()').rows[0][0]);journal('pool',{'label':label,'report':report})
 pool=report['paged']['buffer_pool'];assert pool['budget_bytes']==POOL;assert pool['unlogged_steals']==0
 return pool

def prepare(c,g):
 journal('intents',{'generation':g,'rows':N,'row_payload_bytes':N*WIDTH,'blob_bytes':BLOB,'blob_sha256':sha(blob(g).encode())})
 c.query('BEGIN');tid=c.query('SELECT txid_current()').rows[0][0]
 assert tid not in {native[a] for a in ack | observed_committed},('durable native txid reused',tid,native,ack,observed_committed)
 native[g]=tid;journal('native-ids',{'generation':g,'native_transaction_id':tid})
 for start in range(0,N,128):
  values=','.join(f"({g*N+i},{g},{i%4},'{payload(g,i)}')" for i in range(start,min(start+128,N)))
  c.query('INSERT INTO lt_rows VALUES '+values)
 if g==0:c.query(f"INSERT INTO lt_blob VALUES(1,0,'{blob(0)}')")
 else:c.query(f"UPDATE lt_blob SET generation={g},payload='{blob(g)}' WHERE id=1")
 c.query(f"INSERT INTO lt_receipts VALUES({g},'{sha(blob(g).encode())}')")
def commit(c,g):
 attempted.add(g);journal('commit-attempts',{'generation':g,'native_transaction_id':native[g]})
 c.query('COMMIT');ack.add(g);journal('acks',{'generation':g,'native_transaction_id':native[g]})
def reconcile(c,label,expected=None):
 rows=c.query('SELECT id,generation,owner,payload FROM lt_rows ORDER BY id').rows
 receipts={int(g):h for g,h in c.query('SELECT generation,digest FROM lt_receipts ORDER BY generation').rows}
 by={}
 for row in rows:by.setdefault(int(row[1]),[]).append(row)
 assert set(by)==set(receipts),(label,'receipt atomicity')
 assert ack<=set(by),(label,'lost acknowledged generation',ack,set(by))
 assert set(by)<=attempted,(label,'unattempted generation visible')
 durable_native=[native[g] for g in by]
 assert len(durable_native)==len(set(durable_native)),(label,'committed native identity reused')
 observed_committed.update(by)
 for g,found in by.items():
  want=[[str(g*N+i),str(g),str(i%4),payload(g,i)] for i in range(N)]
  assert found==want,(label,'partial or incorrect transaction',g,len(found))
  assert receipts[g]==sha(blob(g).encode())
 for owner in range(4):
  found=c.query(f'SELECT id FROM lt_rows WHERE owner={owner} ORDER BY id').rows
  want=[[row[0]] for row in rows if row[2]==str(owner)]
  assert found==want,(label,'secondary index mismatch',owner)
 b=c.query('SELECT generation,payload FROM lt_blob WHERE id=1').rows
 assert b==[[str(max(by)),blob(max(by))]],(label,'blob/receipt/rows atomicity')
 digest=sha(json.dumps(rows,separators=(',',':')).encode())
 if expected is not None:assert digest==expected,(label,'restore/repeated recovery mismatch')
 journal('recoveries',{'label':label,'generations':sorted(by),'rows':len(rows),'rows_sha256':digest,'blob_sha256':sha(b[0][1].encode())})
 return digest

config={k:os.environ.get(k) for k in ['BICDB_VM_SOURCE','BICDB_VM_BINARY','BICDB_VM_POOL','BICDB_VM_WAL','BICDB_VM_SEGMENT','BICDB_VM_DISK']}
config.update(format=1,guest_server_sha256=sha((BASE/'initroot/bin/bicdb').read_bytes()),row_count_per_tx=N,row_payload_bytes=N*WIDTH,blob_bytes=BLOB,
 dependencies_sha256={name:sha((BASE/name).read_bytes()) for name in ['launch.sh','prepare.py','pgw.py','proxy.c','proxy','initroot/init','initrd.gz','vmlinuz']})
(RUN/'config.json').write_text(json.dumps(config,indent=2)+'\n');(RUN/'harness.py').write_bytes(Path(__file__).read_bytes())
try:
 vm=launch();c=connect()
 c.query('CREATE TABLE lt_rows(id BIGINT PRIMARY KEY,generation INT NOT NULL,owner INT NOT NULL,payload TEXT NOT NULL)')
 c.query('CREATE INDEX lt_owner ON lt_rows(owner)')
 c.query('CREATE TABLE lt_blob(id INT PRIMARY KEY,generation INT NOT NULL,payload TEXT NOT NULL)')
 c.query('CREATE TABLE lt_receipts(generation INT PRIMARY KEY,digest TEXT NOT NULL)')
 before=metrics(c,'before-large-tx');prepare(c,0);commit(c,0);base_digest=reconcile(c,'base-acked')
 after=metrics(c,'after-large-tx');assert after['evictions']>before['evictions'],'no actual eviction pressure observed'
 c.close();kill()
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(DISK),str(RUN/'nonempty-backup.raw')],check=True)
 c=restart();reconcile(c,'base-recovery',base_digest)
 for g,delay in enumerate([.005,.025,.100],start=1):
  prepare(c,g);sent=threading.Event();out=[];original=c._send
  def send(data):
   original(data)
   if data.startswith(b'Q') and data.endswith(b'COMMIT\0'):sent.set()
  c._send=send
  def committing():
   try:commit(c,g);out.append('ack')
   except Exception as e:out.append(repr(e));journal('client-errors',{'generation':g,'error':repr(e)})
  thread=threading.Thread(target=committing);thread.start()
  assert sent.wait(30),'COMMIT was not sent'
  time.sleep(delay);kill();thread.join(timeout=30);assert not thread.is_alive()
  journal('reset-schedule',{'generation':g,'after_commit_send_seconds':delay,'client_outcome':out})
  c=restart();reconcile(c,f'uncertain-{g}');metrics(c,f'recovered-{g}')
 digest=reconcile(c,'final')
 for i in range(2):
  c.close();kill();c=restart();reconcile(c,f'repeat-{i}',digest)
 c.close();kill()
 vm=launch(RUN/'nonempty-backup.raw');c=connect()
 # The restore predates later uncertain transactions and intentionally has only generation zero.
 saved_ack=ack.copy();ack={0};reconcile(c,'nonempty-raw-image-restore',base_digest);ack=saved_ack
 c.close()
 result=dict(format=1,complete=True,source=config['BICDB_VM_SOURCE'],guest_server_sha256=config['guest_server_sha256'],hard_vm_kills=resets,
  acknowledged_generations=sorted(ack),attempted_generations=sorted(attempted),row_payload_bytes_per_tx=N*WIDTH,blob_bytes=BLOB,pool_bytes=POOL,
  actual_evictions_delta=after['evictions']-before['evictions'],nonempty_raw_image_restore=True,physical_power_loss=False,product_online_backup=False)
 (RUN/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
except BaseException as e:
 (RUN/'result.json').write_text(json.dumps({'complete':False,'error':repr(e),'source':config['BICDB_VM_SOURCE']},indent=2)+'\n');raise
finally:
 if vm is not None and vm.poll() is None:os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10)
