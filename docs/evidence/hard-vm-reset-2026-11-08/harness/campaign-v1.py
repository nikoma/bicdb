#!/usr/bin/env python3
"""Independent SQL acknowledgment reconciliation across actual guest resets.

Runs only in the task-owned VM directory, against a dedicated raw disk.
Never resets the host. This is a hard VM reset test, not physical power loss.
"""
from pathlib import Path
import hashlib,json,os,random,signal,socket,subprocess,threading,time
from pgw import Conn,ConnectionLost,PgError
BASE=Path(__file__).resolve().parent
assert str(BASE)=='/root/bicdb-durability-s3/vm-reset'
DISK=Path(os.environ.get("BICDB_VM_DISK", str(BASE/"smoke.raw")))
assert DISK.parent == BASE
PORT=56388
SEED=861250
rng=random.Random(SEED)
RUN=BASE/('campaign-'+str(int(time.time())))
RUN.mkdir()
intent={};ack={};ledger_lock=threading.Lock();failures=[];next_id=0;crash_id=0


def journal(name,record):
 with (RUN/(name+'.jsonl')).open('a') as file:
  file.write(json.dumps(record,sort_keys=True)+'\n');file.flush();os.fsync(file.fileno())


def connect(timeout=60):
 end=time.monotonic()+timeout
 last=None
 while time.monotonic()<end:
  try:
   c=Conn('127.0.0.1',PORT,timeout=3);c.query('SELECT 1');return c
  except Exception as error:last=repr(error);time.sleep(.1)
 raise RuntimeError('guest SQL unavailable: '+str(last))


def qmp(command):
 s=socket.socket(socket.AF_UNIX);s.settimeout(10);s.connect(str(BASE/'qmp.sock'))
 f=s.makefile('rwb',buffering=0);greeting=json.loads(f.readline())
 for execute in ['qmp_capabilities',command]:
  f.write((json.dumps({'execute':execute})+'\n').encode())
  while True:
   result=json.loads(f.readline())
   if 'error' in result:raise RuntimeError(result)
   if 'return' in result:break
 f.close();s.close();return greeting


def launch():
 for name in ['qmp.sock','console.sock']:
  p=BASE/name
  if p.exists():
   import stat
   assert stat.S_ISSOCK(p.stat().st_mode)
   p.unlink()
 with (RUN/'qemu.log').open('ab') as log:
  return subprocess.Popen(['/bin/bash',str(BASE/'launch.sh')],stdout=log,stderr=log,start_new_session=True)


def hard_reset(vm,kind):
 global crash_id
 before=(BASE/'console.log').read_text(errors='replace').count('VM_SERVER_STARTED')
 begin=time.monotonic()
 if kind=='qmp-system-reset':qmp('system_reset')
 else:
  assert vm.poll() is None
  os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10)
  crash_id+=1
  subprocess.run(['cp','--reflink=auto','--sparse=always',str(DISK),str(RUN/f'crashed-{crash_id}.raw')],check=True)
  vm=launch();before=0
 while (BASE/'console.log').read_text(errors='replace').count('VM_SERVER_STARTED')<=before:
  if vm.poll() is not None:raise RuntimeError('VM exited during boot')
  if time.monotonic()-begin>60:raise RuntimeError('VM failed to reboot')
  time.sleep(.1)
 c=connect();return vm,c,round(time.monotonic()-begin,3)


def worker(w,stop):
 global next_id
 try:c=connect()
 except Exception as error:
  journal('client-errors',{'worker':w,'error':repr(error)});return
 while not stop.is_set():
  with ledger_lock:
   next_id+=1;key=next_id
   owner=(w+key)%4
   payload=f'w{w}-r{key}-'+('abcdefghijklmnopqrstuvwxyz'*20)
   row=[str(key),str(owner),payload]
   intent[key]=row
   journal('intent',{'id':key,'row':row})
  committed=False
  try:
   c.query('BEGIN')
   c.query(f"INSERT INTO vm_rows VALUES({key},{w},'{payload}')")
   c.query(f'UPDATE vm_rows SET owner={owner} WHERE id={key}')
   c.query(f"INSERT INTO vm_receipts VALUES({key},'{hashlib.sha256(payload.encode()).hexdigest()}')")
   c.query('COMMIT');committed=True
   with ledger_lock:
    ack[key]=row;journal('ack',{'id':key,'row':row})
  except (ConnectionLost,OSError,TimeoutError) as error:
   journal('client-errors',{'worker':w,'id':key,'commit_acknowledged':committed,'error':repr(error)});return
  except PgError as error:
   journal('sql-errors',{'worker':w,'id':key,'sqlstate':error.code,'error':str(error)})
   try:c.query('ROLLBACK')
   except Exception:return
   if error.code not in ('08007','58030','40001','40P01'):
    failures.append({'kind':'unexpected-sql-error','id':key,'sqlstate':error.code})
   return


def reconcile(c,round_id):
 rows=c.query('SELECT id,owner,payload FROM vm_rows ORDER BY id').rows
 stored={int(row[0]):row for row in rows}
 receipts={int(row[0]):row[1] for row in c.query('SELECT id,digest FROM vm_receipts ORDER BY id').rows}
 problems=[]
 for key,row in ack.items():
  if stored.get(key)!=row:problems.append({'kind':'ack-mismatch','id':key,'expected':row,'actual':stored.get(key)})
 for key,row in stored.items():
  if intent.get(key)!=row:problems.append({'kind':'unattempted-or-wrong-row','id':key,'actual':row})
  if receipts.get(key)!=hashlib.sha256(row[2].encode()).hexdigest():problems.append({'kind':'atomic-receipt-mismatch','id':key})
 if set(receipts)!=set(stored):problems.append({'kind':'atomic-table-keyset-mismatch'})
 for owner in range(4):
  expected=sorted(key for key,row in stored.items() if row[1]==str(owner))
  found=[int(row[0]) for row in c.query(f'SELECT id FROM vm_rows WHERE owner={owner} ORDER BY id').rows]
  if found!=expected:problems.append({'kind':'secondary-index-mismatch','owner':owner,'expected':expected,'actual':found})
 # Verify uniqueness on a connection separate from normal mutation loops.
 if stored:
  key=next(iter(stored))
  try:
   c.query(f"INSERT INTO vm_rows VALUES({key},0,'must-not-commit')")
   problems.append({'kind':'uniqueness-not-enforced','id':key})
  except PgError as error:
   if error.code!='23505':problems.append({'kind':'wrong-unique-error','sqlstate':error.code})
 result=dict(round=round_id,acknowledged=len(ack),attempted=len(intent),stored=len(stored),
             stored_rows_sha256=hashlib.sha256(json.dumps(rows).encode()).hexdigest(),problems=problems)
 journal('recoveries',result);failures.extend(problems)
 return result


vm=None
try:
 vm=launch();c=connect()
 c.query('CREATE TABLE vm_rows(id BIGINT PRIMARY KEY, owner INT NOT NULL, payload TEXT NOT NULL)')
 c.query('CREATE INDEX vm_rows_owner ON vm_rows(owner)')
 c.query('CREATE TABLE vm_receipts(id BIGINT PRIMARY KEY, digest TEXT NOT NULL)')
 # Independent restore point: kill only this guest, copy its now-static raw disk,
 # restart from it, and later prove the initial schema/empty row set restores.
 os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10)
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(DISK),str(RUN/'initial-backup.raw')],check=True)
 vm=launch();c=connect();assert c.query('SELECT count(*) FROM vm_rows').rows==[['0']]
 for round_id in range(12):
  stop=threading.Event();threads=[threading.Thread(target=worker,args=(w,stop)) for w in range(4)]
  for thread in threads:thread.start()
  time.sleep(rng.uniform(.25,.85));stop.set()
  kind='kill-qemu-and-reboot'
  vm,c,reboot_s=hard_reset(vm,kind)
  for thread in threads:thread.join(timeout=5)
  assert all(not thread.is_alive() for thread in threads),'client failed to stop'
  result=reconcile(c,round_id)
  journal('resets',dict(round=round_id,kind=kind,reboot_to_sql_s=reboot_s))
  print(json.dumps(result),flush=True)
  if failures:break
 # Repeated recovery must stabilize without further writes.
 if not failures:
  expected=reconcile(c,'final-before-reopen')['stored_rows_sha256']
  for rep in range(2):
   vm,c,_=hard_reset(vm,'kill-qemu-and-reboot')
   assert reconcile(c,'repeat-'+str(rep))['stored_rows_sha256']==expected
 # Restore the independently retained initial disk into another task-owned file.
 os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10);vm=None
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(RUN/'initial-backup.raw'),str(RUN/'restored.raw')],check=True)
 env=os.environ.copy();env['BICDB_VM_DISK']=str(RUN/'restored.raw')
 with (RUN/'restore-qemu.log').open('ab') as log:
  vm=subprocess.Popen(['/bin/bash',str(BASE/'launch.sh')],stdout=log,stderr=log,env=env,start_new_session=True)
 c=connect();restored_rows=c.query('SELECT count(*) FROM vm_rows').rows
 assert restored_rows==[['0']],restored_rows
 result=dict(format=1,seed=SEED,source=os.environ.get('BICDB_VM_SOURCE','unknown'),
             experimental=bool(os.environ.get('BICDB_VM_EXPERIMENTAL')),
             physical_power_loss=False,hard_vm_reset=True,guest_ram_mb=4096,
             server_binary_sha256=hashlib.sha256((BASE/'initroot/bin/bicdb').read_bytes()).hexdigest(),
             acknowledged=len(ack),attempted=len(intent),failures=failures,
             initial_backup_restore_verified=True,complete=not failures)
 (RUN/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
except BaseException as error:
 result=dict(format=1,complete=False,source=os.environ.get('BICDB_VM_SOURCE','unknown'),acknowledged=len(ack),attempted=len(intent),error=repr(error),failures=failures)
 (RUN/'result.json').write_text(json.dumps(result,indent=2)+'\n')
 raise
finally:
 if vm is not None and vm.poll() is None:os.kill(vm.pid,signal.SIGKILL);vm.wait(timeout=10)
