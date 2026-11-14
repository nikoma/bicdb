#!/usr/bin/env python3
"""Check this failed baseline's retained public evidence; never open a database."""
import argparse,copy,gzip,hashlib,json,pathlib,re,runpy,subprocess,sys,zipfile
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Run without Python -O; assertions required')
SOURCE='34dded55efef6ed989cdf7d141d5751d4f2a2391'
def sha(b):return hashlib.sha256(b).hexdigest()
def check_results(d):
 assert d['source']==SOURCE and len(d['cases'])==2
 cases={c['source_case']:c for c in d['cases']};a=cases['TargetNativeBaseRoleExpiryBeforePut'];b=cases['TargetNativeBaseRoleExpiryAfterCommit']
 assert a['classification']=='failed'and a['native_tx_status']=='Aborted'and not a['base_or_archive_present']and a['retry_progress']is None
 assert a['retry_exact_error']=='cluster error: native target image: Registered immutable native-base publication missing'
 assert b['classification']=='passed'and b['native_tx_status']=='Committed'and b['fresh_authority_same_xid_reconciled']and b['retry_exact_error']is None
 for c in cases.values():
  assert c['issued_native_xid']==21 and c['same_original_A']and c['actual_role_expired']and c['original_parent_stayed_live']
  assert c['role_remaining_us_after_wait']<0 and c['parent_remaining_us_after_wait']>3500000 and c['app_returned_event_operations']==2
  assert c['actual_source_frames']==18 and c['old_native_records_unchanged']and c['selected_ack_full_payload_before_after_retry']
 return cases

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--original',type=pathlib.Path,help='PRIVATE original ZIP for optional192-file custody hash reproduction');p.add_argument('--self-test',action='store_true');a=p.parse_args();root=pathlib.Path(__file__).resolve().parent
 manifest=json.loads((root/'manifest.json').read_text());assert manifest['source_revision']==SOURCE and manifest['run']==43502925624
 assert not any(f.is_symlink()for f in root.rglob('*'))
 assert {str(f.relative_to(root))for f in root.rglob('*')if f.is_file()}==set(manifest['files'])|{'manifest.json'}
 for n,v in manifest['files'].items():
  name=pathlib.PurePosixPath(n);assert not name.is_absolute()and '..'not in name.parts
  b=(root/n).read_bytes();assert len(b)==v['bytes']and sha(b)==v['sha256']
 actual=json.loads((root/'provenance/run-terminal.json').read_text());assert actual['headSha']==SOURCE and actual['databaseId']==43502925624 and actual['status']=='completed'and actual['conclusion']=='failure'
 for name in manifest['job_indices']:
  j=json.loads((root/name).read_text());assert j['source_revision']==SOURCE and j['run_id']==43502925624 and j['job_status']=='completed'and j['authoritative_source_verified']
  assert j['job_conclusion']==('failure'if j['job_id']==132153142118 else 'success')
  compressed=(root/'jobs'/j['compressed_file']).read_bytes();assert sha(compressed)==j['compressed_sha256'];raw=gzip.decompress(compressed)
  assert len(raw)==j['raw_bytes']and sha(raw)==j['raw_sha256']and b'PRIVATE KEY-----'not in raw
  if j['job_id']==132153142118:
   assert len(j['test_observations'])==2 and sorted(v['result']for v in j['test_observations'])==['FAILED','ok']
   assert b'native_expiry_tests.rs:301:5'in raw and b'1 passed; 1 failed;'in raw
  else:
   assert any(s['name']=='cargo check --workspace --all-targets'and s['conclusion']=='success'for s in j['steps'])
   assert any('Workflow consumer'in s['name']and s['conclusion']=='success'for s in j['steps'])
 projection=json.loads((root/'artifacts/projection-manifest.json').read_text());assert projection['source_commit']==SOURCE and projection['projection_zip_sha256']==sha((root/'artifacts/public-logical.zip').read_bytes())
 assert len(projection['included_members'])==30 and len(projection['omitted_members'])==194 and not projection['projection_fixture_db_custody_present']and projection['original_archive_unchanged']
 with zipfile.ZipFile(root/'artifacts/public-logical.zip')as z:
  assert len(z.namelist())==len(set(z.namelist()))==30
  assert z.read('source-revision.txt').decode().strip()==SOURCE
  selected=re.findall(r'^(\S+): test$',z.read('selected-tests.log').decode(),re.M);assert len(selected)==len(set(selected))==2
  outcomes=re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',z.read('tests.log').decode(),re.M);assert {n for n,_ in outcomes}==set(selected)
  for m in projection['included_members']:
   assert '/custody/'not in m['path']and not m['path'].endswith('/custody-manifest.json')
   b=z.read(m['path']);assert len(b)==m['bytes']and sha(b)==m['sha256']and b'PRIVATE KEY-----'not in b
  for m in projection['omitted_members']:assert m['path']not in z.namelist()and re.fullmatch('[a-f0-9]{64}',m['sha256'])
  independent=json.loads((root/'provenance/independent-verification.json').read_text());assert independent['source']==SOURCE and independent['run']==43502925624 and not independent['engine_opened']
  for c in independent['cases']:
   name=next(n for n in z.namelist()if n.endswith('/execution-identity.json')and json.loads(z.read(n))['source_case']==c['case']);prefix=name.rsplit('/',1)[0]+'/'
   assert sha(z.read(prefix+'registered-native-base-real-role-expiry-outcome.json'))==c['outcome_sha256']
 d=runpy.run_path(str(root/'verify_cases.py'))['verify'](root/'artifacts/public-logical.zip',projection,a.original);cases=check_results(d)
 source=json.loads((root/'source-index.json').read_text());assert source['source_revision']==SOURCE and source['failed_acceptance']['line']==301
 if a.repo:
  for n,v in source['source_pins'].items():
   assert sha(subprocess.check_output(['git','show',SOURCE+':'+n],cwd=a.repo))==v['sha256']
   assert subprocess.check_output(['git','rev-parse',SOURCE+':'+n],cwd=a.repo,text=True).strip()==v['git_blob']
 negative=[]
 if a.self_test:
  for kind in ['false_abort_resume_pass','changed_original_A']:
   changed=copy.deepcopy(d)
   c=next(c for c in changed['cases']if c['source_case']=='TargetNativeBaseRoleExpiryBeforePut')
   if kind=='false_abort_resume_pass':c['classification']='passed'
   else:c['same_original_A']=False
   try:check_results(changed)
   except AssertionError:negative.append(kind+'_refused')
   else:raise AssertionError('changed evidence assertion accepted')
 print(json.dumps({'source':SOURCE,'run':43502925624,'baseline':{'passed':1,'failed':1,'suite_seconds':18.06,'duration_is_RTO':False},'cases':cases,'source_APP_API_ledger':{'returned_ACKs':4,'failed':0,'uncertain':0,'separately_recorded_caller_retries':0,'internal_wire_retry_count':None,'logical_collection':'items','physical_collection':'__bic_cluster_events'},'internal_publication_ledger':{'initial_requests':2,'initial_timeout_replies':2,'known_aborted_native_outcomes':1,'known_committed_native_outcomes':1,'fresh_request_continuations':2,'continuations_returned':1,'continuations_failed':1,'new_APP_commits':0,'separate_from_source_ACKs':True},'private_custody_hashes_recomputed_this_invocation':bool(a.original),'custody_files':192,'self_test_controls':negative,'database_opened':False,'production_qualified':False},indent=2))
if __name__=='__main__':main()
