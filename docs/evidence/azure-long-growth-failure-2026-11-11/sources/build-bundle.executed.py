import collections,gzip,hashlib,json,pathlib,shutil,tarfile,time
P=pathlib.Path('/tmp/bicdb-azure-run');B=P/'long-comparison';D=B/'post-failure-diagnosis';E=pathlib.Path('/tmp/bicdb-azure-long-evidence/docs/evidence/azure-long-growth-failure-2026-11-11');assert (D/'summary.json').exists();assert (D/'diagnostic-databases.private/receipt.json').exists()
def digest(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
redactions=[]
for i,h in enumerate(json.loads((P/'inventory.private.json').read_text())['hosts']):
 for field in ['address','ssh']:
  value=h.get(field)
  if value:
   redactions.append((value,'<node-'+str(i)+'-'+field+'>'))
   if field=='address' and ':' in value:redactions.append((value.rsplit(':',1)[0],'<node-'+str(i)+'-address>'))
redactions.sort(key=lambda x:-len(x[0]))
derived=[]
def public_json(source,name,extra=None):
 text=pathlib.Path(source).read_text()
 for original,replacement in redactions:text=text.replace(original,replacement)
 value=json.loads(text)
 if extra:value.update(extra)
 (E/name).write_text(json.dumps(value,indent=2)+'\n');derived.append({'artifact':name,'source_sha256':digest(source),'derivation':'JSON formatting; known inventory addresses/SSH aliases redacted; scope annotation if present'})
public_json(D/'summary.json','diagnosis-summary.json',{'new_writes_field_scope':'no new application/event writes; metadata/recovery/relocation activity continues','ledger_checks_completed_field_scope':'number of command attempts finished, not number of successful ledger verifications'})
public_json(D/'original-unverified-point-reads.private.json','diagnosis-point-reads.json')
public_json(D/'all-ack-ledger-checks.private.json','diagnosis-ledger-checks.json')
public_json(D/'convergence-observations.json','diagnosis-convergence-observations.json')
public_json(D/'diagnostic-databases.private/receipt.json','post-diagnosis-cold-export-receipt.json')
C=B/'raw-baseline-sample-classification'
for source,name in [('launch.json','raw-classification-client-settings.json'),('summary.json','raw-classification-original-summary.json'),('results.private.json','raw-classification-results.json'),('validity.json','raw-classification-validity.json')]:public_json(C/source,name)
shutil.copy2(P/'classify-baseline-samples.executed.py',E/'sources/raw-classification.executed.py');shutil.copy2(B/'baseline-sample-classification-launch.json',E/'raw-classification-launch.json');shutil.copy2(B/'unverified-key-set-comparison.json',E/'unverified-key-set-comparison.json')
latest=json.loads((D/'latest-topology.private.json').read_text());failed=[v for v in latest['topology']['relocations'].values() if v['phase']=='failed'];(E/'diagnosis-boundary-status.json').write_text(json.dumps({'source_sha256':digest(D/'latest-topology.private.json'),'metrics':latest['metrics'],'metadata_consensus':latest['metadata_consensus'],'failed_relocations':failed,'scope':'last n0 local status in bounded300s observation; not final or continuously observed cluster liveness'},indent=2)+'\n')
final=[]
for i in range(5):
 archive=D/f'diagnostic-databases.private/node-{i}.tar.gz'
 with tarfile.open(archive) as a:
  topology_bytes=a.extractfile('db/cluster-topology.json').read();envelope=json.loads(topology_bytes);t=envelope['topology'];moves=list(t['relocations'].values());fail=[v for v in moves if v['phase']=='failed']
  for v in fail:
   if v.get('last_error'):
    for original,replacement in redactions:v['last_error']=v['last_error'].replace(original,replacement)
  final.append({'node':i,'archive_sha256':digest(archive),'topology_file_sha256':hashlib.sha256(topology_bytes).hexdigest(),'topology_checksum_sha256':envelope['checksum_sha256'],'topology_generation':t['generation'],'replica_count':sum(len(v['replicas']) for v in t['ranges'].values()),'range_leader_counts':dict(collections.Counter(v.get('leader') for v in t['ranges'].values())),'relocation_phases':dict(collections.Counter(v['phase'] for v in moves)),'failed_relocations':fail})
(E/'final-persisted-topology-summaries.json').write_text(json.dumps({'scope':'five separately stopped post-diagnosis persisted topology files; not live status or five copies of each record','nodes':final},indent=2)+'\n')
summary=json.loads((D/'summary.json').read_text());checks=json.loads((D/'all-ack-ledger-checks.private.json').read_text());passed=True
for row,count in zip(checks,[3804,1223]):
 response=row.get('response',{});result=json.loads(response['stdout']) if response.get('code')==0 else None;passed&=bool(result and result.get('checked')==count and result.get('unverified')==0 and result.get('verification_passed'))
passed&=len(checks)==2
claims={'format':1,'runtime_version':'1.0.461-beta','source_commit':'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb','native_trial_passed':False,'production_qualified':False,'capacity_gain_demonstrated':False,'completed_before_after_comparison':False,'all_known_acks_verified_after_diagnostic_restart':bool(passed),'known_acknowledgments':5027,'original_unverified_keys':2,'original_unverified_keys_matched_from_member_contexts':sum(v['matches_expected'] for v in json.loads((D/'original-unverified-point-reads.private.json').read_text())),'diagnostic_policy_reconverged_within_300_seconds':summary['recovery_converged'],'new_application_event_writes_during_diagnosis':False,'physical_failure_domains_verified':False,'native_failure_records':{'failed_operation_count':404,'retained_error_samples':20,'all_failed_attempts_logged':False},'diagnostic_known_acks_matched':4898,'diagnostic_combined_predicate_unverified':129,'cleanup_confounded_classification_used_for_availability_claims':False,'unexecuted_native_phases':['steady-5','failure-5','recovered-5','final post-recovery all-ledger verification']}
(E/'claims.json').write_text(json.dumps(claims,indent=2)+'\n')
private=[]
for label,path in [('original-failed-growth-cold',B/'diagnostic-databases.private'),('post-diagnostic-cold',D/'diagnostic-databases.private')]:
 for row in json.loads((path/'receipt.json').read_text()):private.append({'class':label,'node':row['node'],'bytes':row['archive_bytes'],'sha256':row['sha256'],'remote_sha256':row['remote_sha256'],'public_contents':False,'classification':'contains task-generated database identity material; private archive retained'})
(E/'private-artifact-digests.json').write_text(json.dumps(private,indent=2)+'\n')
shutil.copy2(P/'export-long-post-recovery.executed.py',E/'sources/export-post-recovery.executed.py');shutil.copy2(B/'post-recovery-export-launch.json',E/'post-recovery-export-launch.json');shutil.copy2(pathlib.Path(__file__),E/'sources/build-bundle.executed.py')
shutil.copy2(B/'build-bundle-launch.json',E/'build-bundle-launch.json')
(E/'derivation.json').write_text(json.dumps({'format':1,'created_unix_ns':time.time_ns(),'builder_sha256':digest(__file__),'transformed_artifacts':derived,'private_data_not_included':True},indent=2)+'\n')
p=E/'README.md';text=p.read_text();text+='\nThe diagnostic policy gate did not converge within 300 seconds. Both original IDs subsequently matched the expected complete payload through all five routed member contexts (10 matching reads). '
if passed:text+='Both full ledger checks later verified all 5,027 known acknowledgments after this diagnostic restart; this recovery observation does not erase the failed growth verification or qualify production durability.\n'
else:text+='Full-ledger verification remained incomplete or unsuccessful under its declared command bounds; see the exact command responses/errors. A timeout is not a structured missing-key count and cannot establish permanent loss or successful reconciliation.\n'
text+='\nThe growth diagnostic check returned a structured failure: all 1,223 ACKs checked, 1,205 matched, 18 unverified, all 18 IDs retained, after 214.847 seconds. Neither full-ledger command timed out. Combined, 4,898 exact matches and 129 unverified predicate failures were observed across disjoint ledgers; this is not a count of lost records. The later complete 18-ID growth list has no overlap with the original two failed IDs.\n\nOne additional raw classification attempt was invalid for engine-availability analysis: the final completion check was missed before launch, and growth verification had already ended 12.950243 seconds earlier. All 20 fresh Get calls occurred within the existing helper owned-cleanup window. Its raw ten matches/ten connection-refused results and erroneous concurrent-intent annotation are retained, but all calls are explicitly cleanup-confounded and contribute no availability or commit-count claim. No additional restart or retry followed. Exact per-node SIGINT times were not recorded; the conservative cleanup window is bounded by the second ledger completion and final cleanup summary.\n\nSeparate [read-only original cold-catalog reconciliation](../azure-cold-catalog-reconciliation-2026-11-11/README.md) found exact payloads in retained Applied commands at/below resolved watermarks for all 5,027 ACK identities. That command/catalog evidence does not establish row visibility, cryptographic quorum evidence, physical-host independence, or a passing live verification.\n'
p.write_text(text)
manifest={'format':1,'files':{str(p.relative_to(E)):digest(p) for p in sorted(E.rglob('*')) if p.is_file() and p.name!='manifest.json'}};(E/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');print(json.dumps({'bundle':str(E),'files':len(manifest['files']),'all_known_acks_verified_after_diagnostic_restart':bool(passed)}))
