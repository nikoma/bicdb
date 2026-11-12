"""Discard only owned guest RAM around an authorized Native identity UPDATE."""
import fcntl,hashlib,json,os,pathlib,re,shutil,signal,subprocess,sys,time
root=pathlib.Path('/root/bicdb-durability-s3');shared=root/'vm-reset';source='2403deefde68869c15f02b73d40406ffdfb50148';runtime='2403deefde68869c15f02b73d40406ffdfb50148'
smoke='--smoke' in sys.argv;tag='native-identity-finance492-vm-2403deef'+('-smoke' if smoke else '')
out=root/tag;out.mkdir(exist_ok=False)
record={'source':source,'production_equivalent':runtime,'status':'waiting_for_build','started_unix':time.time(),'scope':'ActualdisposableKVMguestRAMloss forexactversionedfinance492candidateCore-onlyNativeidentityUPDATE; no privateSQLpartition/generatedcolumncode. ExistingpreIOtxlog/pageWAL hooks; postcommit andafterindependentfsyncedACK. Twooriginalcurrent-sourceACKs perMode includeNativevector/timestamp/payload/unknownmetadata. Requireexactold/newfullrecords+schema+policy, onecorrectNativeTX-boundaudit/no duplicates,3stableguestopens/indexes/grantlessrefusals. No volatileemulation/newspend, notphysicalhostpowercut/HA/fullapplication.','cases':[]}
def save():
 p=out/'result.json';p.write_text(json.dumps(record,indent=2)+'\n')
def sha(p):return hashlib.file_digest(pathlib.Path(p).open('rb'),'sha256').hexdigest()
def fsynced_json(p,value):
 with p.open('x') as f:json.dump(value,f);f.write('\n');f.flush();os.fsync(f.fileno())
 fd=os.open(p.parent,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def wait_unit(unit):
 while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(3)
def hashes(p):return {str(f.relative_to(p)):{'bytes':f.stat().st_size,'sha256':sha(f)} for f in p.rglob('*') if f.is_file() and not f.is_symlink()}
def copy_disk(a,b):subprocess.run(['cp','--reflink=auto','--sparse=always',str(a),str(b)],check=True)
save();guest=None
try:
 progress=json.loads((root/'report-native-identity-finance492-validation.json').read_text());assert progress['source']==source and len(progress['steps'])>=3 and all(x.get('exit')==0 for x in progress['steps'][:3])
 build=json.loads((root/'native-identity-finance492-probe-custody.json').read_text());assert build['source']==source and build['status']=='probe_built',build
 binary=pathlib.Path(build['binary']);assert sha(binary)==build['binary_sha256']
 # Process controller already exited successfully in this serial pipeline.
 recovery=json.loads((root/'native-identity-finance492-recovery-2403deef/result.json').read_text());assert recovery['source']==source and recovery['status']=='recovery_passed' and len(recovery['controls'])==2 and len(recovery['process_cases'])==38 and len(recovery['io_failure_cases'])==8
 lock=(shared/'final-qualification.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 for proc in pathlib.Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:argv=(proc/'cmdline').read_bytes().split(b'\0')
  except (FileNotFoundError,ProcessLookupError,PermissionError):continue
  assert not (argv and b'qemu-system' in pathlib.Path(os.fsdecode(argv[0])).name.encode()),'another VM is active'
 assert pathlib.Path('/dev/kvm').exists()
 q=shared/'qemu';qemu=q/'usr/bin/qemu-system-x86_64';kernel=shared/'vmlinuz';initroot=out/'initroot'
 for d in ['bin','sbin','proc','sys','dev','run','data','tmp','etc']:(initroot/d).mkdir(parents=True,exist_ok=True)
 shutil.copy2('/usr/bin/busybox',initroot/'bin/busybox')
 for n in ['sh','mount','mkdir','sleep','cat','echo','sha256sum']:(initroot/'bin'/n).symlink_to('busybox')
 for executable,target in [(binary,'bin/identity_probe'),(pathlib.Path('/usr/sbin/e2fsck'),'sbin/e2fsck')]:
  shutil.copy2(executable,initroot/target)
  for lib in re.findall(r'(/[^\s()]+)',subprocess.check_output(['ldd',str(executable)],text=True)):
   dest=initroot/lib.lstrip('/');dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(lib,dest)
 assert sha(initroot/'bin/identity_probe')==build['binary_sha256']
 init='''#!/bin/sh
export PATH=/bin:/sbin MALLOC_ARENA_MAX=2
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /run
for arg in $(cat /proc/cmdline); do
 case "$arg" in
  task_action=*) action=${arg#*=};;
  task_mode=*) mode=${arg#*=};;
  task_case=*) testcase=${arg#*=};;
  task_site=*) site=${arg#*=};;
  task_new=*) require_new=${arg#*=};;
  task_tx=*) expected_tx=${arg#*=};;
 esac
done
sha256sum /bin/identity_probe
/sbin/e2fsck -p /dev/vda
checked=$?
if [ "$checked" -gt 1 ]; then echo IDENTITY_VM_FSCK_FAILED_$checked; while true; do sleep 1; done; fi
mount -t ext4 /dev/vda /data || { echo IDENTITY_VM_MOUNT_FAILED; while true; do sleep 1; done; }
echo IDENTITY_VM_MOUNTED
if [ "$action" = apply ]; then
 if [ "$site" = identity.after_ack ]; then export IDENTITY_VM_PAUSE_AFTER_ACK=1; elif [ "$site" = identity.after_commit ]; then export IDENTITY_VM_PAUSE_AFTER_COMMIT=1; else export IDENTITY_VM_PAUSE_IO="$site:1"; fi
 /bin/identity_probe "$mode" apply /data/db /data/update-ACK.json
 result=$?
 echo IDENTITY_VM_APPLY_UNEXPECTED_EXIT_$result
else
 if [ "$require_new" = 1 ]; then export IDENTITY_REQUIRE_NEW=1; fi
 export IDENTITY_EXPECTED_TX_ID="$expected_tx"
 if [ -f /data/update-ACK.json ]; then export IDENTITY_ACK=/data/update-ACK.json; fi
 /bin/identity_probe "$mode" verify /data/db /data/original-ACK.json
 result=$?
 echo IDENTITY_VM_VERIFY_DONE_$result
fi
while true; do sleep 1; done
'''
 (initroot/'init').write_text(init);(initroot/'init').chmod(0o755)
 archive=out/'initrd.gz'
 with archive.open('wb') as dest:
  a=subprocess.Popen(['find','.','-print0'],cwd=initroot,stdout=subprocess.PIPE)
  b=subprocess.Popen(['cpio','--null','-o','--format=newc'],cwd=initroot,stdin=a.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE);a.stdout.close()
  c=subprocess.Popen(['gzip','-1'],stdin=b.stdout,stdout=dest);b.stdout.close()
  assert c.wait()==0;err=b.communicate()[1];assert b.returncode==0,err;assert a.wait()==0
 record.update(status='running',binary=str(binary),binary_sha256=build['binary_sha256'],guest_sha256=sha(initroot/'bin/identity_probe'),kernel_sha256=sha(kernel),initrd_sha256=sha(archive));save()
 env={**os.environ,'LD_LIBRARY_PATH':str(q/'usr/lib/x86_64-linux-gnu')}
 def boot(disk,log,mode,testcase,site,action,require_new=False,txid=0):
  assert not log.exists()
  cmd=[str(qemu),'-L',str(q/'usr/share/qemu'),'-name',tag,'-nodefaults','-no-user-config','-machine','q35,accel=kvm','-cpu','host','-m','2048','-smp','1','-display','none','-serial','file:'+str(log),'-monitor','none','-bios',str(q/'usr/share/seabios/bios-256k.bin'),'-kernel',str(kernel),'-initrd',str(archive),'-append',f'console=ttyS0 panic=-1 quiet task_action={action} task_mode={mode} task_case={testcase} task_site={site} task_new={int(require_new)} task_tx={txid}','-drive',f'file={disk},format=raw,if=virtio,cache=writeback,aio=threads']
  return subprocess.Popen(cmd,env=env,stdout=(out/'qemu.stdout').open('ab'),stderr=(out/'qemu.stderr').open('ab'))
 def observe(proc,log,marker):
  deadline=time.monotonic()+90
  while time.monotonic()<deadline:
   text=log.read_text(errors='replace') if log.exists() else ''
   assert proc.poll() is None,('QEMU exited before witness',proc.returncode,text[-2000:])
   if any(marker in line and line.endswith('\n') for line in text.splitlines(keepends=True)):return text
   assert not any(x in text for x in ['IDENTITY_VM_FSCK_FAILED','IDENTITY_VM_MOUNT_FAILED','IDENTITY_VM_APPLY_UNEXPECTED_EXIT','Kernel panic']),text[-4000:]
   time.sleep(.025)
  raise TimeoutError('owned guest did not reach '+marker)
 def kill_owned(proc):
  assert proc.poll() is None
  proc.kill();assert proc.wait(timeout=15)==-signal.SIGKILL
 recovery_root=root/'native-identity-finance492-recovery-2403deef'
 chosen=recovery['fixtures']
 if smoke:chosen=[x for x in chosen if x['mode']=='server_paged']
 def messages(text,marker):return [json.loads(line.split(marker+' ',1)[1]) for line in text.splitlines() if line.startswith(marker+' ')]
 for fixture in chosen:
  mode=fixture['mode'];db=pathlib.Path(fixture['database']);before=pathlib.Path(fixture['original_ACK']);assert hashes(db)==fixture['files'] and sha(before)==fixture['original_ACK_sha256']
  original=json.loads(before.read_text());assert len(original['rows'])==len(fixture['ACKs'])==2
  for acknowledged in fixture['ACKs']:
   row=next(x for x in original['rows'] if x['id']==acknowledged['id']);assert row['id']==acknowledged['id'] and hashlib.sha256(json.dumps(row,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()==acknowledged['hash']
  control=next(x for x in recovery['controls'] if x['mode']==mode);assert control['native_commit_span']==1
  sites=['transaction_log.write','transaction_log.sync','identity.after_commit','identity.after_ack']
  if mode=='server_paged':sites+=['page_log.write','page_log.sync']
  if smoke:sites=['identity.after_commit','identity.after_ack']
  for site in sites:
   name=mode+'-'+site.replace('.','_');work=out/name;work.mkdir();stage=work/'stage';stage.mkdir();shutil.copytree(db,stage/'db');shutil.copy2(before,stage/'original-ACK.json')
   disk=work/'working.raw'
   with disk.open('xb') as f:f.truncate(512*1024**2)
   subprocess.run(['mkfs.ext4','-q','-F','-d',str(stage),str(disk)],check=True)
   cut_log=work/'cut.console';guest=boot(disk,cut_log,mode,'signed_update',site,'apply');text=observe(guest,cut_log,'IDENTITY_VM_CUT_READY ')
   assert record['guest_sha256'] in text
   started=messages(text,'IDENTITY_TX_STARTED');assert len(started)==1;started=started[0]
   ready=messages(text,'IDENTITY_VM_CUT_READY');assert ready==[{'site':site}]
   acknowledgments=messages(text,'IDENTITY_VM_ACK');expected_ack=site=='identity.after_ack';assert len(acknowledgments)==int(expected_ack)
   if expected_ack:assert acknowledgments[0]['update_acknowledged'] is True and acknowledgments[0]['native_tx_id']==started['native_tx_id'] and acknowledgments[0]['after_native_sequence']-acknowledgments[0]['before_native_sequence']==1
   witness={'mode':mode,'site':site,'owned_qemu_pid':guest.pid,'guest_witness':ready[0],'tx_started':started,'acknowledged':expected_ack,'ack':acknowledgments[0] if expected_ack else None,'observed_unix':time.time()};fsynced_json(work/'external-witness.json',witness)
   kill_owned(guest);guest=None;saved=work/'crashed.raw';copy_disk(disk,saved);saved_hash=sha(saved);verify_disk=work/'verify.raw';copy_disk(saved,verify_disk)
   require_new=site in ['identity.after_commit','identity.after_ack'];verify_log=work/'verify.console';guest=boot(verify_disk,verify_log,mode,'signed_update',site,'verify',require_new,started['native_tx_id']);verified=observe(guest,verify_log,'IDENTITY_VM_VERIFY_DONE_');assert 'IDENTITY_VM_VERIFY_DONE_0' in verified,verified[-6000:]
   observations=messages(verified,'IDENTITY_VERIFIED');assert len(observations)==3 and [x['reopen'] for x in observations]==[0,1,2] and len({x['state'] for x in observations})==1
   for x in observations:assert x['full_records_schema_policy_equal'] and x['native_indexes_valid'] and x['grantless_write_refused'] and x['audit_count']==(1 if x['state']=='complete_new' else 0)
   assert all(x['row_hashes']==observations[0]['row_hashes'] and x['audit_messages']==observations[0]['audit_messages'] for x in observations)
   if require_new:assert all(x['state']=='complete_new' for x in observations)
   if site=='transaction_log.write':assert all(x['state']=='complete_old' for x in observations)
   if expected_ack:assert observations[0]['audit_messages'][0]['message_id']==acknowledgments[0]['audit_message_id']
   kill_owned(guest);guest=None;assert sha(saved)==saved_hash
   item={**witness,'passed':True,'hard_kill_exit':-signal.SIGKILL,'requires_complete_new':require_new,'observations':observations,'original_ACKs_linked':2,'saved_image':str(saved),'saved_image_sha256':saved_hash,'saved_image_unchanged_after_verification':True,'original_ACK_sha256':sha(before)};record['cases'].append(item);save()
   disk.unlink();verify_disk.unlink();shutil.rmtree(stage)
  assert hashes(db)==fixture['files']
 expected=2 if smoke else 10;assert len(record['cases'])==expected
 record.update(status='vm_smoke_passed' if smoke else 'vm_matrix_passed',expected_cases=expected,originals_unchanged=True,finished_unix=time.time());save()
except BaseException as error:
 record.update(status='vm_qualification_failed',error=repr(error),finished_unix=time.time());save()
 if guest is not None and guest.poll() is None:guest.kill();guest.wait(timeout=15)
 raise
print(json.dumps({k:v for k,v in record.items() if k!='cases'}))
