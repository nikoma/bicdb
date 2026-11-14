#!/usr/bin/env python3
"""Portable retained-evidence checks. Optional raw ZIP/source checks stay read-only."""
import argparse,copy,gzip,hashlib,json,pathlib,runpy,subprocess
HERE=pathlib.Path(__file__).resolve().parent
SOURCE='03454e72cab0dea71b4783153fabf6716d9784fd'
BASE='68b30c1bbe59b59345ec5a4d2f12354ae60d38ea'
HAPPY='initial_data_tests::native_initial_data_actual_cli_four_process_first_ack_and_strong_full_value'
FAILED='replica_receiver_tests::native_registered_replica_receiver_actual_cli_four_data_then_fifth_same_owner_no_data_grant'
REGISTRY='writer_registry_refresh_baseline::native_metadata_registry_addition_actual_cli_requires_healthy_existing_read_and_fresh_ack'
def require(value,message):
 if not value:raise ValueError(message)
def sha(b):return hashlib.sha256(b).hexdigest()
def read(name):return json.loads((HERE/name).read_text())
def verify_projection(r):
 require(r['source']==SOURCE and r['product_source_identical_to']==BASE,'source')
 require(r['run']==43527079466 and r['focused_job']==132235428483 and r['artifact']==14086103789,'run identity')
 require(r['artifact_sha256']=='1934f0a43ac6ad8eda96fbbb478f88bccf91d3a92f5570fa84d31b2d9132e2af','artifact digest')
 require(r['result']==[{'passed':5,'failed':1,'seconds':45.39}],'failed target retained')
 cases={x['test']:x['outcome'] for x in r['case_results']};require(len(cases)==6 and cases[HAPPY]=='ok' and cases[FAILED]=='FAILED' and cases[REGISTRY]=='ok','case results')
 require(len(r['artifact_members'])==31 and sum(x['bytes'] for x in r['artifact_members'])==147718211,'artifact inventory')
 require(len(r['journals'])==2,'two actual client journals')
 js={j['test']:j for j in r['journals']};require(set(js)=={HAPPY,REGISTRY},'failed case has no client journal')
 h=js[HAPPY]; require(h['case_finished'] and h['records']==424,'happy journal complete')
 ops=h['operations'];classes=[x['reply']['classification'] for x in ops]
 require(classes.count('USER_ACK')==3 and classes.count('STRONG_SUCCESS')==5,'happy ACK/read counts')
 require(classes.count('NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN')==1 and classes.count('NO_SUCCESSFUL_READ_NO_ABSENCE')==1,'expected negative uncertainty retained')
 first=next(x for x in ops if x['request'].get('key')=='external-first-ack' and x['request']['public_operation']=='append')
 require(first['reply']=={'classification':'USER_ACK','inserted':True},'actual first positive ACK')
 require(len(first['attempts'])==1,'no positive APP retries')
 a=first['attempts'][0];require(a['operation']=='event_append' and a['destination']=='n0','first destination')
 request=next(x for x in a['phases'] if x['phase']=='request_frame_before_dispatch')
 require(request['topology_generation']==1 and request['request_id']==93 and request['event_route']=={'range_id':4,'range_epoch':1},'actual first wire route')
 require(request['event_request']['value']==first['request']['value'] and request['event_request']['key']=='external-first-ack','actual first wire payload')
 response=next(x for x in a['phases'] if x['phase']=='response_frame_before_decode')
 require(response['event_response']=={'result':'appended','inserted':True},'actual first wire ACK')
 final=a['phases'][-1];require(final['phase']=='validated_transport_result_ok' and final['elapsed_ns']==534631326,'first transport completion')
 for op in ops:
  for attempt in op['attempts']:
   for p in attempt['phases']:
    if 'frame' in p:require(p['frame']['complete'],'complete retained frame')
  if op['request'].get('key')=='external-first-ack' and op['request']['public_operation']=='get_strong':
   require(op['reply']['presence']=='present' and op['reply']['value']==first['request']['value'],'exact full Strong')
 for key,value in [('external-json-null',None),('external-empty-object',{})]:
  out=next(x['reply'] for x in ops if x['request'].get('key')==key and x['request']['public_operation']=='get_strong')
  require(out=={'classification':'STRONG_SUCCESS','presence':'present','value':value},'present null/empty distinguished')
 absent=next(x['reply'] for x in ops if x['request'].get('key')=='genuine-unwritten-key');require(absent=={'classification':'STRONG_SUCCESS','presence':'absent'},'genuine absence')
 negative=next(x for x in ops if x['request'].get('key')=='before-initial-data-quorum'); counts={op:sum(a['operation']==op for a in negative['attempts']) for op in ['event_append','metadata_snapshot_discovery']};require(counts=={'event_append':5,'metadata_snapshot_discovery':16},'negative APP retries/discovery')
 pending=next(x for x in ops if x['request'].get('key')=='not-written-before-open');require(sum(a['operation']=='native_event_get' for a in pending['attempts'])==15 and sum(a['operation']=='metadata_snapshot_discovery' for a in pending['attempts'])==56,'pre-open Strong retries')
 g=js[REGISTRY];require(g['case_finished'] and g['records']==54,'registry journal complete')
 classes=[x['reply']['classification'] for x in g['operations']];require(classes.count('USER_ACK')==2 and classes.count('STRONG_SUCCESS')==3 and classes.count('ACTUAL_RECEIVER_REFUSAL')==3,'registry outcomes')
 primary={x['logical_operation_id']:x for x in r['external_ledger_projection'] if x.get('acknowledged') is True}
 require(len(primary)==5,'five separately journaled primary APP ACKs')
 for j in r['journals']:
  for op in j['operations']:
   if op['reply']['classification']=='USER_ACK':require(op['id'] in primary,'primary ACK precedes supplemental proof')
 f=r['stopped_failure_diagnostic_projection'];require(len(f)==1 and f[0]['test']==FAILED,'different failed prerequisite')
 f=f[0];require(f['pending_command_projection']=={'version':1,'sequence':1,'action':'prepare','budget_ms':5000} and f['copied_file_digests_verified']==21 and len(f['empty_child_output_members'])==8,'actual prepare and copied-file digests');require(f['capture_status']=='captured_allowlist_after_actual_wait' and not f['refused_allowlisted_paths'],'stopped diagnostic capture')
 require(len(f['all_children_waited'])==4 and all(x['before_kill']=={'exited':False} and x['wait_ok'] and x['exit']['signal']==9 for x in f['all_children_waited']),'actual alive/kill/wait')
 require(len(f['statuses'])==4 and all(x['sequence']==0 and x['phase']=='provisional' and x['metadata'] is None and x['data_serving'] is False for x in f['statuses']),'no completed prepare or DATA receipt')
 require({x['node'] for x in f['metadata_json_observations']}=={'n0','n1','n2'},'observed partial JSON prefix')
 require(all(x['term']==0 and x['commit_index']==0 and x['log_entries']==0 and x['topology_generation']==1 and x['envelope_checksum_matches'] for x in f['metadata_json_observations']),'metadata JSON scope')
 require('db3/native-initial-metadata/cluster-metadata-consensus.json' in f['missing_allowlisted_paths'],'missing fourth JSON retained without native absence claim')
 return {'cases':6,'returned_APP_ACKs':5,'successful_Strong':8,'expected_negative_APP_unknown':1,'expected_negative_Strong':1,'receiver_refusals':3,'failed_case_APP_or_Strong':'not reached'}
def verify_packet(repo=None):
 manifest=read('packet-manifest.json');actual={str(p.relative_to(HERE)) for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name!='packet-manifest.json'}
 require(actual==set(manifest['files']),'packet membership')
 for name,v in manifest['files'].items():
  b=(HERE/name).read_bytes();require(len(b)==v['bytes'] and sha(b)==v['sha256'],'packet file '+name)
 index=read('source-index.json');require(index['product_source']==BASE and index['diagnostic_source']==SOURCE,'source index')
 for pin in index['pins']:
  b=(HERE/pin['retained']).read_bytes();require(sha(b)==pin['sha256'] and len(b)==pin['bytes'],'source SHA256 '+pin['path'])
  require(hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==pin['git_blob'],'source Git blob '+pin['path'])
  if repo:
   actual=subprocess.check_output(['git','-C',str(repo),'show',pin['revision']+':'+pin['path']]);require(actual==b,'actual repository source '+pin['path'])
 if repo:
  changed=subprocess.check_output(['git','-C',str(repo),'diff','--name-only',BASE,SOURCE],text=True).splitlines();require(changed==['.github/workflows/tests.yml'],'workflow-only qualifier')
 original=read('provenance/original-failure-forensics.json');require(original['classification']['returned_positive_app_acks']==0 and original['original_job']==132191138917,'original failure separate')
 require(original['classification']['root_cause'].startswith('INCONCLUSIVE'),'original cause not filled by later pass')
 job=read('provenance/focused-job.json');require(job['head_sha']==SOURCE and job['conclusion']=='failure','actual focused failed job')
 jobs=read('provenance/jobs.json')['jobs'];require(next(j for j in jobs if j['id']==132235387195)['conclusion']=='success','separate community result')
 raw=gzip.decompress((HERE/'logs/focused-cli-job132235428483.log.gz').read_bytes()).decode();require('finished in 45.39s' in raw and 'operator prepare failed: Error: native metadata action timed out' in raw,'actual focused log')
 retry=read('provenance/foreign-broad-retry-terminal-job.json');require(retry['conclusion']=='cancelled' and retry['head_sha']==BASE,'cancelled retry separate')
 return verify_projection(read('logical-projection.json'))
def self_test():
 r=read('logical-projection.json');tested=0
 mutations=[lambda x:x['result'][0].update(failed=0),lambda x:x['journals'][0].update(case_finished=False),lambda x:x['stopped_failure_diagnostic_projection'][0]['statuses'][0].update(data_serving=True),lambda x:x.update(source=BASE),lambda x:x['journals'][0]['operations'][3]['reply'].update(classification='NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN')]
 for mutate in mutations:
  c=copy.deepcopy(r);mutate(c)
  try:verify_projection(c)
  except (ValueError,KeyError,StopIteration):tested+=1
  else:raise ValueError('negative mutation was accepted')
 return tested
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--self-test',action='store_true');p.add_argument('--repo',type=pathlib.Path);p.add_argument('--artifact',type=pathlib.Path);a=p.parse_args();summary=verify_packet(a.repo)
 if a.self_test:summary['negative_controls']=self_test()
 if a.artifact:
  project=runpy.run_path(str(HERE/'project_artifact.py'))['project'];require(project(a.artifact)==read('logical-projection.json'),'exact ZIP reprojection');summary['raw_artifact_reproduced']=True
 print(json.dumps({'verified':True,'scope':'retained diagnostic evidence; no engine open/recovery or qualification','summary':summary},sort_keys=True))
