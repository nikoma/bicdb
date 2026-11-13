#!/usr/bin/env python3
"""Bounded retained logical-catalogue inspection; never opens key material."""
import pathlib,json,zipfile,hashlib,collections,argparse,gzip
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('original_archive',type=pathlib.Path)
parser.add_argument('--cohort-report',type=pathlib.Path,required=True)
parser.add_argument('--output',type=pathlib.Path,required=True)
args=parser.parse_args()
archive=args.original_archive
assert archive.stat().st_size<1024*1024*1024
original_sha='5864bd49abbea6b377e035e0529e0b896e5e94c8cba6505b875090a6b9f187ad'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==original_sha
report_bytes=args.cohort_report.read_bytes()
r=json.loads(gzip.decompress(report_bytes) if args.cohort_report.suffix=='.gz' else report_bytes)
assert r['source_commit']=='7f2c5f0abc4b2a65700971e41ec362c5da0fbb03'
out={'source_commit':r['source_commit'],'run':43469307523,'core_job':132036734162,'all_target_job':132036684467,
    'actual_test_results':r['actual_test_results'],'original_zip_sha256':original_sha,'overlap':r['overlap'],'cases':[],
    'not_the_historical_6d_first_app_failure':True,'no_cold_replay_or_hardware_qualification':True,
    'counterevidence':'Independent Rootfd4 three-case Installing cohort43469675906/Core132038089731 passed3/0; concurrency alone is not an established cause.'}
with zipfile.ZipFile(archive) as z:
 for c in r['cases']:
  prefix=c['directory'];case={'source_case':c['actual_identity']['source_case'],'actual_identity':c['actual_identity'],'actual_test_result':c['actual_test_result'],
    'phase':c['terminal']['phase'],'primary_error':c['terminal']['primary_error'],'counts':c['counts'],'recorded_actual_closes':[],
    'fresh_round_role_own_context_ids_and_timestamps_recorded':False,'original_round_context_id_deadline_timestamp_recorded':False}
  case['pre_app_state']=c['all_actual_snapshot_observations']['first-app-pre-append.json']
  for op in c['operations']:
   if op['classification']!='uncertain':continue
   obs=[]
   for n in c['all_actual_snapshot_observations']['first-app-after-append-2.json']['nodes']:
    p=next((p for p in n['native_observations'] or [] if p['range']==op['context']['range']),None)
    cfg=op['context']['configuration'];rng=next(t for t in cfg['ranges'].values() if t['id']==op['context']['range'])
    voter=any(p['node_id']==n['node'] and p['role']=='voter' for p in rng['replicas'])
    if not voter:continue
    obs.append({'node':n['node'],'slot':n['actual_service_store_open'],'native_observation':p,
      'row_payload':'not_observed','row_absence_observed':False,'native_command_body':'not_retained',
      'outcome':'native_applied_index_observed' if p else 'DB_read_guard_unavailable_at_snapshot'})
   case['uncertain_write']={'logical_id':op['logical_id'],'record_id':op['context']['record_id'],'expected_payload':op['context']['value'],
      'expected_payload_sha256':op['context']['payload_json_sha256'],'range':op['context']['range'],'epoch':op['context']['range_epoch'],
      'topology_generation':op['context']['topology_generation'],'leader':op['context']['expected_route_leader'],
      'expected_leader_address':op['context']['expected_route_address'],'actual_api_error':op['actual_reply']['actual_api_error'],
      'api_duration_ns':op['api_duration_ns'],'actual_event_rpc_attempts':sum(a['phases'][0]['operation']=='event_append' for a in op['all_rpc_attempts']),
      'actual_discovery_attempts':sum(a['phases'][0]['operation']=='metadata_snapshot_discovery' for a in op['all_rpc_attempts']),
      'replica_observations':obs,'actual_full_record_reconciled':False,'live_Strong_read_recorded':False,
      'DB_custody_present':any(n.startswith(prefix+'custody/') for n in z.namelist()),'custody_error':c['terminal']['custody_error'],
      'classification':'externally uncertain; native progress is not a full payload reconciliation; no rollback or lost-ACK claim'}
  for member in z.namelist():
   if not(member.startswith(prefix+'custody/') and member.endswith('/cluster-range-write-log.json')):continue
   assert z.getinfo(member).file_size<1024*1024
   raw=z.read(member);cat=json.loads(raw)['catalog']
   for range_id,fact in cat.get('native_joint_handoffs',{}).items():
    case['recorded_actual_closes'].append({'member':member,'member_sha256':hashlib.sha256(raw).hexdigest(),'member_bytes':len(raw),
      'node':cat['node_id'],'range':range_id,'native_joint_handoff':fact,'actual_promise':cat.get('handoff_promises',{}).get(range_id)})
  if case['recorded_actual_closes']:
   case['failure_location']={'file':'remote_source_tests.rs','line':862,'operation':'round.record_retained_closed_actual(closed)',
      'individual_close_returned_at_line':861,'predicate':'NativeJointOldRound.metadata.authority.require_live first, before checking returned observation',
      'source_proven_expiry':'original validated immutable request ID cannot change; MetadataReadObservation.require_live_request has only request mismatch or Instant>=deadline',
      'no_fresh_role_or_OWN_expiry_inferred':True}
   case['retained_native_closes_cover_all_three_old_members']=set(f['node'] for f in case['recorded_actual_closes'])=={'n1','n2','n3'}
  out['cases'].append(case)
args.output.write_text(json.dumps(out,indent=2,sort_keys=True)+'\n')
print(json.dumps({'cases':[(c['source_case'],c['counts'],len(c['recorded_actual_closes'])) for c in out['cases']],
 'max_observed_API_overlap':r['overlap']['maximum_distinct_fixtures_with_overlapping_recorded_api_intervals'],
 'uncertain_replica_rows_reconciled':False,'qualification':False}))
