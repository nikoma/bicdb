"""Explicit parent-triggered teardown, scoped to one exact tagged experiment group."""
import argparse,collections,datetime,hashlib,json,subprocess,time
from pathlib import Path
p=Path('/tmp/bicdb-azure-run');s=json.loads((p/'state.private.json').read_text());rg=s['resource_group'];sid=s['subscription_id'];expected=s['resource_group_id']
parser=argparse.ArgumentParser();parser.add_argument('--request-delete',action='store_true',required=True);parser.parse_args()
assert rg==s['experiment_id'] and expected.lower()==f'/subscriptions/{sid}/resourceGroups/{rg}'.lower()
def az(args,timeout=120):return subprocess.run(['az',*args,'--subscription',sid],capture_output=True,text=True,timeout=timeout)
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
group=az(['group','show','-n',rg,'-o','json']);assert group.returncode==0,'group lookup failed'
g=json.loads(group.stdout);assert g['id'].lower()==expected.lower() and g.get('tags',{}).get('purpose')=='bicdb-scale' and g.get('tags',{}).get('experiment_id')==s['experiment_id'],'identity guard refused'
resources=az(['resource','list','-g',rg,'-o','json']);assert resources.returncode==0,'resource snapshot failed'
r=json.loads(resources.stdout)
query='[].{node:name,provisioning_state:provisioningState,power_state:powerState,size:hardwareProfile.vmSize,image:storageProfile.imageReference,os_disk:{size_gb:storageProfile.osDisk.diskSizeGb,caching:storageProfile.osDisk.caching,sku:storageProfile.osDisk.managedDisk.storageAccountType},data_disks:storageProfile.dataDisks[].{size_gb:diskSizeGb,caching:caching,sku:managedDisk.storageAccountType}}'
vms=az(['vm','list','-g',rg,'--show-details','--query',query,'-o','json'])
assert vms.returncode==0,'final VM state export failed'
(p/'teardown-final-vms.json').write_text(json.dumps(json.loads(vms.stdout),indent=2)+'\n')
receipt={'group_identity_sha256':hashlib.sha256(expected.encode()).hexdigest(),'requested_at_utc':stamp(),'identity_guards_passed':True,'resource_types_before':dict(collections.Counter(x['type'] for x in r)),'resource_count_before':len(r),'deleted_group_confirmed_absent':False,'workload_evidence_export':'parent authorized deletion after export'}
(p/'deletion-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
result=az(['group','delete','-n',rg,'--yes','--no-wait']);(p/'delete.private.stderr').write_text(result.stderr);assert result.returncode==0,'delete request failed; scheduled watchdog remains active'
receipt['delete_request_exit']=result.returncode
for attempt in range(120):
 result=az(['group','exists','-n',rg,'-o','tsv'])
 if result.returncode==0 and result.stdout.strip()=='false':
  receipt['deleted_group_confirmed_absent']=True;receipt['confirmed_absent_at_utc']=stamp();break
 time.sleep(5)
(p/'deletion-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert receipt['deleted_group_confirmed_absent'],'Azure did not confirm group absence; watchdog retained'
# Only dismantle this exact run's deadline guard after Azure confirms deletion.
for unit in [s['cleanup_timer'],s['cleanup_service']]:
 assert unit.startswith(s['experiment_id']+'-cleanup-persistent')
 file=Path('/etc/systemd/system')/unit
 assert file.exists()
 subprocess.run(['systemctl','disable','--now',unit],check=False,capture_output=True)
 file.unlink()
subprocess.run(['systemctl','daemon-reload'],check=True)
receipt['owned_cleanup_units_removed']=True;(p/'deletion-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
