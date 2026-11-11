"""Verify the initial smoke's sanitized exports and their internal consistency."""
import gzip,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
provenance=json.loads((ROOT/'provenance.json').read_text())
for record in provenance['exports']:
 path=ROOT/record['export_artifact']
 assert path.parent==ROOT
 assert hashlib.sha256(path.read_bytes()).hexdigest()==record['export_sha256']
def load(name):
 data=(ROOT/name).read_bytes()
 return json.loads(gzip.decompress(data) if name.endswith('.gz') else data)
report=load('growth-report.json')
assert report['complete'] and report['qualification_status']=='not_qualified'
assert report['source_commit']=='b05385d4b033edf0db03f3eccf4bdd86f7c8eddb'
assert report['binary_sha256']=='15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028'
assert report['physical_failure_domains_verified'] is False
assert report['fsync'] is True and report['replication_factor']==3 and report['ranges']==64
assert sum(r['acknowledged_writes'] for r in report['results'])==1281
assert sum(r['failed_operations'] for r in report['results'])==6
for r,v in zip(report['results'],report['post_recovery_verification']):
 assert r['phase']==v['phase'] and v['verification_passed'] and v['unverified']==0
 assert v['checked']==r['acknowledged_writes']
 assert len(load(r['phase']+'-ledger.json.gz')['keys'])==r['acknowledged_writes']
external=load('external-report.json');verification=load('external-verification.json')
assert external['acknowledged_operations']==external['attempted_operations']==107
assert external['uncertain_operations']==0 and external['fsync_per_attempt_and_ack']
assert verification=={'checked_acknowledged_operations':107,'failures':[],'verification_passed':True}
rows=load('external-ledger.json.gz');attempts={x['operation_id']:x for x in rows if x['kind']=='attempt'};acks=[x for x in rows if x['kind']=='ack']
assert len(attempts)==len(acks)==107
assert all(x['payload_sha256']==attempts[x['operation_id']]['payload_sha256'] for x in acks)
first=load('topology-5-1794373382138058536.json')
assert len(first['completed_relocations_to_n4'])==38
assert sum(x['snapshot_bytes_copied'] for x in first['completed_relocations_to_n4'])==537266
assert all(first['metrics'][k]==0 for k in ['active_relocations','failed_relocations','unavailable_ranges','under_replicated_ranges'])
proof=load('node-five-user-data-proof.json')
assert len(proof['sampled_user_records'])==5 and all(x['payload_matches'] for x in proof['sampled_user_records'])
for x in proof['sampled_user_records']:
 assert all(x[key]['targets'][0]['leader_node']=='n4' for key in ['route_before','route_after'])
print(json.dumps({'verified':True,'native_acknowledged_checked_after_process_fault':1281,'external_acknowledged_checked_after_whole_cluster_restart':107,'production_qualified':False,'capacity_gain_demonstrated':False}))
