#!/usr/bin/env python3
"""Offline evidence verifier. Optional raw ZIPs are read as bytes, never engine-opened."""
import argparse,collections,hashlib,importlib.util,json,pathlib,re,zipfile
HERE=pathlib.Path(__file__).resolve().parent
SOURCE='b6398fb075eed7c93a71ab25a52fad3f936421e5'
def sha(data):return hashlib.sha256(data).hexdigest()
def verify(record,raw=None):
 p=HERE/record['key'];proj=json.loads((p/'projection-manifest.json').read_text());summary=json.loads((p/'capture-reconciliation.json').read_text());github=json.loads((p/'github-run.json').read_text())
 assert github['headSha']==record['source']==SOURCE and github['databaseId']==record['run'] and github['conclusion']==record['workflow_conclusion']
 assert next(j for j in github['jobs'] if j['databaseId']==record['core_job'])['conclusion']==record['workflow_conclusion']
 assert next(j for j in github['jobs'] if j['name']=='Community-only workspace compiles, tests included')['conclusion']=='success'
 env=json.loads((p/'environment-request.json').read_text());assert env['source_revision']==SOURCE and env['compared_environment']['focused_dependencies_opt_level']==2 and not env['compared_environment']['same_environment_claim']
 assert env['parent_budget_ms_unchanged']==5000 and env['rust_test_threads_unchanged']==4
 access=json.loads((p/'custody-access-scope.json').read_text());assert access['repository']['private'] is False and not access['production_credentials_claimed']
 assert proj['original_zip_sha256']==summary['raw_sha256']==record['raw_sha256'] and proj['original_archive_unchanged']
 assert sha((p/'logical-evidence.zip').read_bytes())==proj['projection_zip_sha256']==record['projection_sha256']
 spec=importlib.util.spec_from_file_location('project_'+record['key'],p/'project-logical.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 assert sha((p/'project-logical.py').read_bytes())==proj['projector_sha256']
 expected={m['path']:m for m in proj['included_members']};captured={c['case']:c for c in summary['cases']};app_count=0;failed=[]
 with zipfile.ZipFile(p/'logical-evidence.zip') as z:
  names=z.namelist();assert len(names)==len(set(names)) and set(names)==set(expected)
  for name in names:
   assert module.allowed(name) and '/custody/' not in name and not name.endswith('/custody-manifest.json')
   data=z.read(name);assert sha(data)==expected[name]['sha256'] and len(data)==expected[name]['bytes'] and not module.PRIVATE_PEM.search(data)
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  selected=[s[:-6] for s in z.read('selected-tests.log').decode().splitlines() if s.endswith(': test')];log=z.read('tests.log').decode();outcomes=dict(re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',log,re.M))
  assert len(selected)==len(set(selected))==len(outcomes)==record['selected'] and set(selected)==set(outcomes)
  assert collections.Counter(outcomes.values())==({ 'ok':record['passed'],'FAILED':record['failed']} if record['failed'] else {'ok':record['passed']})
  assert not any(n.endswith('registered-native-base-unforced-timeout-continuation.json') for n in names)
  for identity in [n for n in names if n.endswith('/execution-identity.json')]:
   root=identity.rsplit('/',1)[0]+'/'
   def read(name):return json.loads(z.read(root+name))
   i=read('execution-identity.json');case=i['source_case'];assert i['source_commit']==SOURCE and i['package_version']=='1.0.500-beta'
   status=outcomes[i['actual_rust_test_thread']];classification=captured[case].get('test_classification',captured[case].get('classification'));assert classification==('passed' if status=='ok' else 'failed')
   cleanup=read('cleanup.json');assert cleanup['errors']==[] and read('evidence-errors.json')['errors']==[] and (cleanup['primary_failure_preserved'] is None)==(status=='ok')
   app=read('ack-ledger.json');assert len(app)==2;app_count+=2
   for ack in app:
    assert ack['operation']=='ClusterEventClient.append' and ack['outcome']=='returned' and ack['actual_api_error'] is None
    assert sha(json.dumps(ack['value'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())==ack['payload_json_sha256']
   if status=='FAILED':
    assert record['key']=='remote51' and 'metadata observation request changed or expired' in cleanup['primary_failure_preserved'];failed.append(case)
    if case=='TargetNoDomain':assert root+'no-domain-1-point.json' in names
   if case in ('TargetNativeBaseAbortSyncFailed','TargetNativeBaseCommitSyncUnknown'):
    report=read('registered-native-base-abort-recovery-outcome.json');before,first,after=[report[k] for k in ('before','after_first_failure','after_fresh_request')]
    base=first['image']['value']['native_base'];assert base==after['image']['value']['native_base'] and base['native_attempts_issued']==1 and base['retired']==[]
    assert report['actual_io_fault']['fired']==1 and report['actual_io_fault']['fault_disarmed_before_fresh_request']
    for snap in (before,first,after):assert snap['memory_custody']==before['memory_custody']
    for snap in (first,after):assert snap['owner']['diagnostic_error']==report['first_health_status']['core_recovery_error']
    assert 'WriteFailed' in report['fresh_error_debug']
   if case=='TargetNativeBaseAbortBudget':
    report=read('registered-native-base-abort-recovery-outcome.json');assert len(report['actual_aborted_requests'])==9
    a,b=[report[k]['image']['value']['native_base'] for k in ('before_exhausted_request','after_exhausted_request')];assert a==b and a['native_attempts_issued']==9 and len(a['retired'])==8
   if 'RoleExpiry' in case:
    report=read('registered-native-base-real-role-expiry-outcome.json');assert report['retry_exact_error'] is None
    a,b=[report[k]['image']['value']['native_base'] for k in ('after_first_expiry','after_fresh_request')]
    assert b['verified'] and b['actual_commit_returned']==b['receipt']['native_page_xid']
    if case.endswith('AfterCommit'):assert b['receipt']['native_page_xid']==report['issued_native_xid'] and b['retired']==[]
    else:
     assert b['receipt']['native_page_xid']>report['issued_native_xid'] and len(b['retired'])==1
     assert b['retired'][0]['receipt']==a['receipt'] and b['retired'][0]['canonical_bytes']==a['attempted_canonical_bytes']
   if case=='TargetCatalogueFloorDirSyncRetry':
    report=read('registered-catalogue-floor-before-assertions.json');faults=[read(f'registered-catalogue-floor-directory-error-{i}.json') for i in (1,2)]
    assert [f['actual_fault_count'] for f in faults]==[1,2]
    assert report['first_progress']==report['second_progress'] and report['catalogue_floor_adopted'] and not report['serving']
    assert all(f['owner']['catalogue_floor']['candidate']==report['new_catalogue'] and f['observed']['raw']==report['after']['raw'] for f in faults)
 assert app_count==record['app_returned_event_operations']
 if record['key']=='remote51':assert len(failed)==7 and 'TargetNoDomain' in failed
 raw_result=False
 if raw is not None:
  assert sha(raw.read_bytes())==record['raw_sha256'] and raw.stat().st_size==record['raw_bytes'];files=size=0
  with zipfile.ZipFile(raw) as archive:
   members=proj['included_members']+proj['omitted_members'];assert set(archive.namelist())=={m['path'] for m in members}
   for m in members:
    data=archive.read(m['path']);assert len(data)==m['bytes'] and sha(data)==m['sha256']
   for n in archive.namelist():
    if not n.endswith('/custody-manifest.json'):continue
    custody=json.loads(archive.read(n));root=n.rsplit('/',1)[0]+'/'
    assert custody['status']=='observed_stable_with_live_actual_owners' and custody['engine_owners_alive'] and custody['all_five_metadata_service_db_native_journal_guards_held']
    assert not custody['engine_reopened'] and not custody['listeners_drained'] and not custody['atomic_powerloss_snapshot_claimed']
    count=0
    for m in custody['files']:
     data=archive.read(root+'custody/'+m['path']);assert len(data)==m['bytes'] and sha(data)==m['sha256'];files+=1;count+=len(data)
    assert count==custody['total_bytes'];size+=count
  assert files==record['custody']['custody_files_verified'] and size==record['custody']['custody_bytes_verified'];raw_result={'files':files,'bytes':size}
 return {'run':record['run'],'selected':record['selected'],'passed':record['passed'],'failed':record['failed'],'APP_returned_event_operations':app_count,'raw_custody_verified':raw_result,'unforced_continuation':'not tested','production_qualified':False}
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--raw-ten',type=pathlib.Path);parser.add_argument('--raw-remote51',type=pathlib.Path);args=parser.parse_args();sums=json.loads((HERE/'SHA256SUMS.json').read_text())
 actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='SHA256SUMS.json' and '__pycache__' not in p.parts};assert actual==set(sums)
 for p,digest in sums.items():assert sha((HERE/p).read_bytes())==digest,p
 records=json.loads((HERE/'runs.json').read_text())['runs'];print(json.dumps({'verification':'passed','runs':[verify(r,args.raw_ten if r['key']=='ten' else args.raw_remote51) for r in records]},sort_keys=True))
if __name__=='__main__':main()
