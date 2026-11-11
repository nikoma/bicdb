"""Only run after explicit driver/root readiness; one exact n4 VM fault."""
import argparse,datetime,hashlib,json,os,shlex,subprocess,time,uuid
from pathlib import Path
p=Path('/tmp/bicdb-azure-run');s=json.loads((p/'state.private.json').read_text());e=p/'vm-fault';control=p/'vm-fault-control.private.json';ready=e/'ready-for-poweroff.json'
parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true',required=True);parser.parse_args()
def az(args,timeout=180):return subprocess.run(['az',*args,'--subscription',s['subscription_id']],capture_output=True,text=True,timeout=timeout)
def ssh(command,timeout=120):return subprocess.run([str(p/'bin/ssh'),s['experiment_id']+'-n4',command],capture_output=True,text=True,timeout=timeout)
def write():
 temporary=control.with_suffix('.tmp')
 with temporary.open('w') as f:json.dump(record,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
 os.replace(temporary,control)
 fd=os.open(p,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def note(kind,**values):record['events'].append({'kind':kind,'unix_ns':time.time_ns(),**values});write()
def view():
 r=az(['vm','get-instance-view','-g',s['resource_group'],'-n','n4','--query','instanceView.statuses[].code','-o','json']);assert r.returncode==0,'instance-view failed'
 values=json.loads(r.stdout);note('instance_view',statuses=values);return values
assert ready.exists() and not control.exists(),'readiness missing or duplicate fault'
r=json.loads(ready.read_text());assert r['leader']=='n4';root=Path(r['run_root']);db=Path(r['member_root'])
assert root.parent==Path('/srv/bicdb-data') and root.name.startswith('bicdb-scale-') and root.name.endswith('-n4') and db==root/'db','unsafe run/db scope'
vm=az(['vm','show','-g',s['resource_group'],'-n','n4','-o','json']);assert vm.returncode==0;vm=json.loads(vm.stdout)
expected=s['resource_group_id']+'/providers/Microsoft.Compute/virtualMachines/n4'
assert vm['id'].lower()==expected.lower() and vm.get('tags',{}).get('purpose')=='bicdb-scale' and vm.get('tags',{}).get('experiment_id')==s['experiment_id'],'VM identity guard refused'
binary_check=ssh('sha256sum '+shlex.quote(str(root/'bicdb')));assert binary_check.returncode==0 and binary_check.stdout.split()[0]==r['binary_sha256'],'victim binary identity changed'
boot=ssh('cat /proc/sys/kernel/random/boot_id');assert boot.returncode==0;before=str(uuid.UUID(boot.stdout.strip()))
record={'attempt_id':r['attempt_id'],'fault_mechanism':'azure_vm_stop_skip_shutdown','physical_host_power_loss':False,'guest_boot_id_before':before,'selected_range_id':r['range_id'],'binary_sha256':r['binary_sha256'],'events':[],'request_started_unix_ns':time.time_ns()};write()
result=az(['vm','stop','-g',s['resource_group'],'-n','n4','--skip-shutdown','--no-wait']);(e/'stop.private.stderr').write_text(result.stderr);note('azure_stop_request_returned',exit_code=result.returncode);assert result.returncode==0,'Azure stop request failed'
for _ in range(90):
 if 'PowerState/stopped' in view():record['power_off_confirmed_unix_ns']=time.time_ns();write();break
 time.sleep(5)
else:raise RuntimeError('VM power-off was not confirmed')
deadline=time.monotonic()+180
while time.monotonic()<deadline:
 stable=e/'stable-recovery.json'
 if stable.exists():
  try:stable=json.loads(stable.read_text())
  except json.JSONDecodeError:time.sleep(1);continue
  assert stable['range_id']==r['range_id'] and stable['attempt_id']==r['attempt_id'];record['stable_acks_while_off_observed']=True;note('five_affected_range_acks_while_off_observed');break
 time.sleep(2)
else:record['stable_acks_while_off_observed']=False;note('while_off_availability_gate_failed_after_180_seconds')
record['start_requested_unix_ns']=time.time_ns();write();result=az(['vm','start','-g',s['resource_group'],'-n','n4','--no-wait']);note('azure_start_request_returned',exit_code=result.returncode);assert result.returncode==0
for _ in range(90):
 if 'PowerState/running' in view():record['guest_running_confirmed_unix_ns']=time.time_ns();write();break
 time.sleep(5)
else:raise RuntimeError('VM restart was not confirmed')
for _ in range(60):
 boot=ssh('cloud-init status --wait >/dev/null && cat /proc/sys/kernel/random/boot_id && findmnt -no FSTYPE /srv/bicdb-data',timeout=45)
 if boot.returncode==0:break
 time.sleep(3)
else:raise RuntimeError('guest SSH/mount did not recover')
new_boot=str(uuid.UUID(boot.stdout.splitlines()[0]));assert new_boot!=before and boot.stdout.splitlines()[1]=='ext4';record['guest_boot_id_after']=new_boot;note('guest_reboot_and_data_mount_verified')
archive=root/'cold-after-guest-reboot-before-engine-open.tgz'
# Avoid token replacement in strings/identifiers: use positional prefix variables.
code="root_path="+repr(str(root))+"\ndb_path="+repr(str(db))+"\narchive_path="+repr(str(archive))+"\n"+"""import pathlib,subprocess,hashlib,json,os
root=pathlib.Path(root_path);db=pathlib.Path(db_path);archive=pathlib.Path(archive_path)
for proc in pathlib.Path('/proc').iterdir():
 if proc.name.isdigit():
  try:args=(proc/'cmdline').read_bytes().split(bytes([0]))
  except (OSError,PermissionError):continue
  assert not(args and pathlib.Path(os.fsdecode(args[0])).name=='bicdb' and os.fsencode(db) in args),'BicDB already opened victim DB'
assert db.is_dir() and not db.is_symlink() and not archive.exists()
subprocess.run(['tar','-czf',str(archive),'-C',str(root),'db'],check=True)
with archive.open('rb') as f:d=hashlib.file_digest(f,'sha256').hexdigest()
print(json.dumps({'sha256':d,'bytes':archive.stat().st_size,'classification':'database-files-after-filesystem-journal-replay-before-BicDB-open-not-raw-disk'}))
"""
result=ssh('python3 -c '+shlex.quote(code),timeout=300);(e/'archive.private.stderr').write_text(result.stderr);assert result.returncode==0,'cold DB archive failed'
remote=json.loads(result.stdout);local=e/'victim-db-after-guest-reboot.private.tgz'
scp=subprocess.run([str(p/'bin/scp'),s['experiment_id']+'-n4:'+str(archive),str(local)],capture_output=True,text=True,timeout=300);assert scp.returncode==0,'cold DB export failed'
with local.open('rb') as f:d=hashlib.file_digest(f,'sha256').hexdigest()
assert d==remote['sha256'];record['cold_database_archive']={**remote,'controller_sha256':d,'exported':True};note('cold_database_archive_exported_verified')
record['bicdb_restart_ready']=True;record['bicdb_restart_ready_unix_ns']=time.time_ns();write();print('VM fault/restart/cold-archive protocol complete; driver may restart BicDB')
