"""Verify portable records without manufacturing live or production evidence."""
import gzip,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
for e in json.loads((ROOT/'provenance.json').read_text())['exports']:
 p=ROOT/e['export_artifact'];assert p.parent==ROOT
 assert hashlib.sha256(p.read_bytes()).hexdigest()==e['export_sha256'],p.name
def load(name):
 data=(ROOT/name).read_bytes();return json.loads(gzip.decompress(data) if name.endswith('.gz') else data)
p=load('probe-report.json');f=p['fault_control'];off=f['power_off_confirmed_unix_ns'];start=f['start_requested_unix_ns']
assert p['complete'] and p['verification_passed'] and p['qualification_status']=='not_qualified'
assert p['physical_host_power_loss'] is False
assert p['source_commit']=='b05385d4b033edf0db03f3eccf4bdd86f7c8eddb'
assert p['binary_sha256']=='15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028'
strict=[c for c in p['calls'] if c['acknowledged'] and c['started_unix_ns']>=off and c['completed_unix_ns']<start]
assert len(strict)==5 and [c['sequence'] for c in strict]==list(range(20,25))
assert load('strict-off-window.json')['calls']==strict
rows=load('targeted-ledger.json.gz');attempts=[x for x in rows if x['kind']=='attempt'];acks=[x for x in rows if x['kind']=='ack']
assert len(attempts)==41 and len(acks)==38 and sum(x['kind']=='uncertain' for x in rows)==3
assert len({x['operation_id'] for x in attempts})==41
byseq={x['value']['sequence']:x for x in attempts}
for c in strict:
 a=byseq[c['sequence']];matched=[x for x in rows if x['operation_id']==a['operation_id']]
 ack=next(x for x in matched if x['kind']=='ack')
 assert sum(x['kind']=='attempt' for x in matched)==1
 assert a['unix_ns']>=off and ack['unix_ns']<start and ack['inserted'] is True
 assert a['payload_sha256']==ack['payload_sha256']
assert p['targeted_ack_verification']=={'checked_acknowledged_operations':38,'failures':[],'verification_passed':True}
assert p['earlier_external_ack_verification']=={'checked_acknowledged_operations':107,'failures':[],'verification_passed':True}
assert sum(x['checked'] for x in p['earlier_native_verification'])==1281
assert all(x['verification_passed'] and x['unverified']==0 for x in p['earlier_native_verification'])
assert f['guest_boot_id_before_sha256']!=f['guest_boot_id_after_sha256']
assert f['cold_database_archive']['sha256']==f['cold_database_archive']['controller_sha256']=='45fd44506366d1302bec01d236221f306c7c6ca6e3cf099109bb7905a5d1f233'
assert f['cold_database_archive']['bytes']==1363864
for i in range(4):
 a=load(f'n{i}-continuity-earlier.json');b=load(f'n{i}-continuity-later.json')
 assert a['after_first_guest_start_request'] and b['after_first_guest_start_request']
 assert a['pid']==b['pid'] and a['process_start_ticks']==b['process_start_ticks'] and a['cpu_ticks']<b['cpu_ticks']
 assert b['listener_pid_matches'] and b['port9444_listening'] and b['kernel_oom']['matching_lines']==[]
 assert load(f'n{i}-status-window.json')['recorded_times_inside_strict_off_window']
v2=load('v2-launch.json');entry=next(x for x in v2['files'] if x['name']=='vm-fault-probe.attempt-2.frozen.py')
assert hashlib.sha256((ROOT/'executed-probe-v2.py').read_bytes()).hexdigest()==entry['sha256']
assert hashlib.sha256((ROOT/'executed-infra-helper.py').read_bytes()).hexdigest()==load('infra-helper-provenance.json')['sha256']
print(json.dumps({'verified':True,'fresh_acknowledgments_wholly_inside_confirmed_off_window':5,'targeted_acknowledged_rechecked':38,'earlier_external_rechecked':107,'earlier_native_rechecked':1281,'production_qualified':False}))
