#!/usr/bin/env python3
"""Verify this three-run evidence packet without opening any database."""
import argparse,copy,gzip,hashlib,json,pathlib,re,runpy,subprocess,sys,tempfile,zipfile
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Run without Python -O; assertions are required')
SOURCE='e1ecfeab71d3f1e104530e9d6aa3bf37971abd65'
def sha(b):return hashlib.sha256(b).hexdigest()
def inventory(root):
 manifest=json.loads((root/'manifest.json').read_text())
 assert manifest['source_revision']==SOURCE and manifest['source_package_version']=='1.0.494-beta'
 assert {v['run_id']for v in manifest['runs']}=={43492849838,43493986492,43493938281}
 assert not manifest['production_qualified']and not manifest['deployed']
 assert not any(f.is_symlink()for f in root.rglob('*'))
 assert {str(f.relative_to(root))for f in root.rglob('*')if f.is_file()}==set(manifest['files'])|{'manifest.json'}
 for name,value in manifest['files'].items():
  p=pathlib.PurePosixPath(name);assert not p.is_absolute()and '..'not in p.parts
  b=(root/name).read_bytes();assert len(b)==value['bytes']and sha(b)==value['sha256'],name
 return manifest
def projection(root,label):
 d=json.loads((root/f'artifacts/{label}-projection-manifest.json').read_text());p=root/f'artifacts/{label}-public.zip'
 assert d['source_commit']==SOURCE and sha(p.read_bytes())==d['projection_zip_sha256']
 assert not d['projection_fixture_db_custody_present']and d['original_archive_unchanged']
 with zipfile.ZipFile(p)as z:
  assert len(z.namelist())==len(set(z.namelist()))==len(d['included_members'])
  for v in d['included_members']:
   assert v['path']in z.namelist()and '/custody/'not in v['path']and not v['path'].endswith('/custody-manifest.json')
   b=z.read(v['path']);assert len(b)==v['bytes']and sha(b)==v['sha256']and b'PRIVATE KEY-----'not in b
  for v in d['omitted_members']:assert v['path']not in z.namelist()and re.fullmatch('[a-f0-9]{64}',v['sha256'])
  assert z.read('source-revision.txt').decode().strip()==SOURCE
 return p

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=pathlib.Path);p.add_argument('--self-test',action='store_true');p.add_argument('--cold-original',type=pathlib.Path,help='Optional PRIVATE original ZIP: stream custody hashes and WAL CRC/payload only; no engine open')
 args=p.parse_args();root=pathlib.Path(__file__).resolve().parent;manifest=inventory(root)
 for run in manifest['runs']:
  m=json.loads((root/run['metadata']).read_text());assert m['id']==run['run_id']and m['head_sha']==SOURCE and m['status']=='completed'and m['conclusion']==run['conclusion']
  for index in run['job_indices']:
   j=json.loads((root/index).read_text());assert j['source_revision']==SOURCE and j['run_id']==run['run_id']and j['job_status']=='completed'
   expected='failure'if run['kind']=='catalogue4'and j['job_id']==run['core_job']else 'success'
   assert j['job_conclusion']==expected
   if 'authoritative_source_verified'in j:assert j['authoritative_source_verified']
   compressed=(root/'jobs'/j['compressed_file']).read_bytes();assert sha(compressed)==j['compressed_sha256']
   raw=gzip.decompress(compressed);assert len(raw)==j['raw_bytes']and sha(raw)==j['raw_sha256']and b'PRIVATE KEY-----'not in raw
   if j['job_id']==run['core_job']:
    assert len(j['test_observations'])==run['passed']+run['failed']
    assert sum(v['result']=='ok'for v in j['test_observations'])==run['passed']
    assert sum(v['result']=='FAILED'for v in j['test_observations'])==run['failed']
    assert any(f"{run['passed']} passed; {run['failed']} failed;"in s for s in j['test_summaries'])
   else:
    assert any(s['name']=='cargo check --workspace --all-targets'and s['conclusion']=='success'for s in j['steps'])
    assert any('Workflow consumer'in s['name']and s['conclusion']=='success'for s in j['steps'])
 coldpath=projection(root,'cold7');rowpath=projection(root,'rows5');catpath=projection(root,'catalogue4')
 app_calls=[]
 for archive in (coldpath,rowpath,catpath):
  with zipfile.ZipFile(archive)as z:
   for name in z.namelist():
    if not name.endswith('/ack-ledger.json'):continue
    calls=json.loads(z.read(name));assert len(calls)==2
    for call in calls:
     assert call['operation']=='ClusterEventClient.append'and call['collection']=='items'
     assert call['outcome']=='returned'and call['actual_api_inserted']and call['actual_api_error']is None and not call['effect_unknown_on_error']
    app_calls.extend(calls)
 assert len(app_calls)==20
 cold_module=runpy.run_path(str(root/'verify_cold.py'));cold=cold_module['verify'](coldpath,args.cold_original)
 rows=runpy.run_path(str(root/'verify_rows.py'))['report']
 cat_module=runpy.run_path(str(root/'verify_catalogue.py'));cat=cat_module['verify'](catpath)
 assert cold['source_APP']['acknowledged']+rows['source_APP_operations']['acknowledged']+cat['source_APP_returned_ACKs']==20
 assert len(rows['same_open_wal_retention_controls'])==3 and len(rows['durable_index_controls'])==2
 independent=json.loads((root/'provenance/root-e1ec-cold-seven-independent-verification.json').read_text())
 assert independent['source_revision']==SOURCE and independent['private_original_verification']['files_independently_hashed']==20 and independent['private_original_verification']['wal_frames_crc_checked']==3 and not independent['private_original_verification']['database_opened']
 # This is a retained private-verification receipt, not a claim that the
 # public projection itself contains the omitted page/key/WAL bytes.
 source=json.loads((root/'source-index.json').read_text());assert source['source_revision']==SOURCE
 if args.repo:
  assert subprocess.check_output(['git','rev-parse',SOURCE+'^{tree}'],cwd=args.repo,text=True).strip()==source['source_tree']
  for n,h in source['paths_sha256'].items():assert sha(subprocess.check_output(['git','show',SOURCE+':'+n],cwd=args.repo))==h,n
 issues=json.loads((root/'issues.json').read_text());assert len(issues['failed_cases'])==2 and all(v['native_base_outcome']=='inconclusive'and v['named_fault_control']=='not tested'for v in issues['failed_cases'])
 negative=[]
 if args.self_test:
  # Corrupt semantic evidence with a new internally valid ZIP, bypassing
  # outer archive checks to exercise the actual Catalogue ledger verifier.
  with zipfile.ZipFile(catpath)as z:original={n:z.read(n)for n in z.namelist()}
  with tempfile.TemporaryDirectory(prefix='bicdb-e1ec-evidence-negative-')as tmp:
   for kind in ['bad_payload_hash','invented_post_error_outcome']:
    changed=dict(original)
    if kind=='bad_payload_hash':
     key=next(n for n in changed if n.endswith('/ack-ledger.json'));a=json.loads(changed[key]);a[0]['payload_json_sha256']='0'*64;changed[key]=json.dumps(a).encode()
    else:
     key=next(n for n in changed if n.endswith('/execution-identity.json')and json.loads(changed[n])['source_case']=='TargetCatalogueFloorCanceled')
     changed[key.rsplit('/',1)[0]+'/registered-catalogue-floor-fabricated.json']=b'{}'
    target=pathlib.Path(tmp)/(kind+'.zip')
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED)as z:
     for n,b in changed.items():z.writestr(n,b)
    try:cat_module['verify'](target)
    except AssertionError:negative.append(kind+'_refused')
    else:raise AssertionError('corrupted semantic evidence accepted: '+kind)
 report={'classification':'passed_portable_verification_of_mixed_run_results','source':SOURCE,'source_package_version':'1.0.494-beta','focused_tests':{'passed':14,'failed':2},'cold7':cold,'rows5':rows,'catalogue4':cat,'source_APP_API_calls':{'returned_acknowledgments':20,'failed':0,'uncertain':0,'separately_recorded_caller_retries':0,'internal_wire_retry_count':None,'logical_collection':'items','physical_record_collection':'__bic_cluster_events','ordinary_SQL_or_failover_RPO_claim':False},'internal_target_outcomes':{'row_apply_unknown_retained_fenced':1,'cold_killed_before_reply_observed_committed_WAL':1,'catalogue_native_base_setup_outcomes_inconclusive':2,'distinct_from_source_APP_ledger':True},'cold_private_receipt_rechecked_from_original_in_this_invocation':bool(args.cold_original),'self_test_controls':negative,'database_opened':False,'native_grant_reconstructed':False,'production_qualified':False,'deployed':False}
 print(json.dumps(report,indent=2))
if __name__=='__main__':main()
