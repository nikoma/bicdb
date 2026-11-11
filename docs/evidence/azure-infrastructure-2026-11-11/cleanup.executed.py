import json,subprocess,time,sys
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'state.private.json').read_text());rg=state['resource_group'];experiment=state['experiment_id'];subscription=state['subscription_id'];expected=state['resource_group_id']
assert rg.startswith('bicdb-scale-') and rg==experiment and expected.lower()==f'/subscriptions/{subscription}/resourceGroups/{rg}'.lower()
log=open(base/'cleanup.private.log','a',buffering=1)
def az(args):return subprocess.run(['az',*args,'--subscription',subscription],capture_output=True,text=True,timeout=120)
try:
 r=az(['group','show','--name',rg,'-o','json'])
 if r.returncode:
  exists=az(['group','exists','--name',rg,'-o','tsv'])
  if exists.returncode==0 and exists.stdout.strip()=='false':log.write('group_absent\n');sys.exit(0)
  log.write('lookup_failed_retry\n');sys.exit(1)
 group=json.loads(r.stdout)
 if group['id'].lower()!=expected.lower() or group.get('tags',{}).get('experiment_id')!=experiment or group.get('tags',{}).get('purpose')!='bicdb-scale':log.write('identity_guard_refused\n');sys.exit(2)
 log.write('deletion_requested\n')
 r=az(['group','delete','--name',rg,'--yes','--no-wait'])
 log.write('delete_exit='+str(r.returncode)+'\n')
 # Exit nonzero until Azure confirms group missing. systemd retries independent of workload.
 time.sleep(15)
 exists=az(['group','exists','--name',rg,'-o','tsv'])
 sys.exit(0 if exists.returncode==0 and exists.stdout.strip()=='false' else 1)
except (subprocess.TimeoutExpired,ValueError,KeyError) as error:
 log.write('transient_exception='+type(error).__name__+'\n');sys.exit(1)
