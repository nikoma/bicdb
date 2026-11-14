#!/usr/bin/env python3
"""Read bounded uploaded diagnostic records only; never open a DB or issue RPC."""
import argparse,collections,hashlib,json,pathlib,re,zipfile
SOURCE='03454e72cab0dea71b4783153fabf6716d9784fd'
def sha(b):return hashlib.sha256(b).hexdigest()
def frame(b):
 if b is None:return None
 b=bytes(b); r={'bytes':len(b),'sha256':sha(b),'complete':len(b)>=4 and len(b)==4+int.from_bytes(b[:4],'big')}
 if r['complete']:r['decoded']=json.loads(b[4:])
 return r
def project(path):
 with zipfile.ZipFile(path) as z:
  infos=[i for i in z.infolist() if not i.is_dir()]; assert len(infos)<=1024
  assert sum(i.file_size for i in infos)<=512*1024*1024
  assert len(infos)==len({i.filename for i in infos})
  files={}; manifest=[]
  for i in infos:
   p=pathlib.PurePosixPath(i.filename); assert not p.is_absolute() and '..' not in p.parts
   assert i.file_size<=160*1024*1024
   h=hashlib.sha256()
   with z.open(i) as f:
    while b:=f.read(1024*1024):h.update(b)
   manifest.append({'member':i.filename,'bytes':i.file_size,'sha256':h.hexdigest()})
   if i.filename.endswith(('journal.jsonl','.status','.command','/manifest.json','cluster-metadata-consensus.json')) or i.filename in ['tests.log','source-revision.txt','rustc.txt','selected-tests.log']:
    assert i.file_size<=64*1024*1024;files[i.filename]=z.read(i)
 assert files['source-revision.txt'].decode().strip()==SOURCE
 log=files['tests.log'].decode(); selected=[l for l in files['selected-tests.log'].decode().splitlines() if l.endswith(': test')];assert len(selected)==6
 cases=[{'test':n,'outcome':result} for n,result in re.findall(r'^test (\S+) \.\.\. (ok|FAILED)$',log,re.M)]
 results=[{'passed':int(a),'failed':int(b),'seconds':float(c)} for a,b,c in re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed;[^\n]*finished in ([\d.]+)s',log)]
 journals=[]
 for name,raw in sorted(files.items()):
  if not name.endswith('/journal.jsonl'):continue
  assert raw.endswith(b'\n'); rows=[json.loads(l) for l in raw.splitlines()]
  assert [r['sink_sequence'] for r in rows]==list(range(1,len(rows)+1))
  assert rows[0]['kind']=='journal_started' and rows[0]['source']==SOURCE
  intents={}; replies={}; attempts={}
  for r in rows:
   if r['kind']=='logical_request_intent':intents[r['diagnostic_operation_id']]=r
   elif r['kind']=='logical_public_reply':replies[r['diagnostic_operation_id']]=r
   elif r['kind']=='actual_rpc_phase':
    a=r['actual']; k=(r['observer_instance_id'],a['attempt_id']); ctx=r['logical_context'];identity=ctx['diagnostic_operation_id'] if ctx else None
    attempt=attempts.setdefault(k,{'attempt_id':a['attempt_id'],'observer_instance_id':r['observer_instance_id'],'logical_operation_id':identity,'operation':a['operation'],'destination':a['destination'],'phases':[]})
    assert attempt['logical_operation_id']==identity
    phase={k:a[k] for k in ['phase','phase_unix_ns','elapsed_ns','topology_generation','address','request_id','bytes_written','may_have_dispatched','error_chain']};phase['sink_sequence']=r['sink_sequence'];f=frame(a['raw_frame'])
    if f is not None:
     phase['frame']={k:v for k,v in f.items() if k!='decoded'}
     decoded=f.get('decoded',{}); request=decoded.get('request',{})
     if 'ApplicationEvent' in request:
      app=request['ApplicationEvent'];phase['event_route']={k:app[k] for k in ['range_id','range_epoch']};phase['event_request']=app['operation']
     if 'NativeEventGet' in request:
      get=request['NativeEventGet'];phase['event_route']={k:get[k] for k in ['mode','range_id','range_epoch','budget_ms']};phase['event_request']={k:get[k] for k in ['namespace','key']}
     response=decoded.get('response',{})
     if 'Error' in response:phase['actual_response_error']=response['Error']
     ok=response.get('Ok',{})
     if 'ApplicationEvent' in ok:phase['event_response']=ok['ApplicationEvent']
     if 'NativeEventGet' in ok:
      get=ok['NativeEventGet']['result'];phase['event_response']={k:v for k,v in get.items() if k!='audit'}
      if 'audit' in get:phase['read_audit']={k:v for k,v in get['audit'].items() if k in ['range_id','range_epoch','range_index','target_local_visibility_db_sequence','snapshot_sequence','final_metadata_generation','final_metadata_term','final_metadata_read_index']}
    attempt['phases'].append(phase)
  ops=[]
  for identity,r in intents.items():
   q=r['request']; reply=replies.get(identity);out=reply['outcome'] if reply else None
   request={k:q[k] for k in ['public_operation','namespace','key','value','payload_sha256','phase','request_timeout_ms'] if k in q}
   if out and out['classification']=='ACTUAL_AUTHENTICATED_BOOTSTRAP_DATA_ONLY':out={'classification':out['classification'],'bootstrap_raw_frame_covered':False,'topology_generation':out['snapshot']['topology']['generation']}
   ops.append({'id':identity,'request':request,'reply':out,'reply_any_actual_rpc_may_have_dispatched':reply.get('any_actual_rpc_may_have_dispatched') if reply else None,'attempts':[a for a in attempts.values() if a['logical_operation_id']==identity]})
  assert len(intents)==len(replies)
  finish=[r for r in rows if r['kind']=='fixture_case_finished']; assert len(finish)==1
  journals.append({'member':name,'sha256':sha(raw),'records':len(rows),'test':rows[0]['test'],'case_finished':True,'operations':ops,'unscoped_attempts':[a for a in attempts.values() if a['logical_operation_id'] is None]})
 failures=[]
 for name,raw in sorted(files.items()):
  if '/failure-diagnostics/' not in '/'+name or not name.endswith('/manifest.json'):continue
  m=json.loads(raw); prefix=name.rsplit('/',1)[0]; statuses=[]; metadata=[]
  by_name={i['member']:i for i in manifest}
  for copied in m['files']:
   actual=by_name[prefix+'/'+copied['path']]; assert actual['bytes']==copied['bytes'] and actual['sha256']==copied['sha256']
  command=json.loads(files[prefix+'/files/node0.command']); command={k:command[k] for k in ['version','sequence','action','budget_ms']}
  empty_outputs=[i['member'] for i in manifest if i['member'].startswith(prefix+'/files/node') and i['member'].endswith(('.stdout','.stderr')) and i['bytes']==0]
  for n,b in files.items():
   if not n.startswith(prefix+'/files/'):continue
   if n.endswith('.status'):
    s=json.loads(b);statuses.append({'node_file':n.split('/')[-1],'sequence':s['sequence'],'process_id':s['process_id'],'phase':s['host']['phase'],'metadata':s['host']['metadata'],'data_serving':s['data_serving'],'error':s['error']})
   if n.endswith('cluster-metadata-consensus.json'):
    v=json.loads(b); state=v['state']; assert sha(json.dumps(state,separators=(',',':'),ensure_ascii=False).encode())==v['checksum_sha256']; metadata.append({'member':n,'sha256':sha(b),'node':state['node_id'],'term':state['current_term'],'commit_index':state['commit_index'],'log_entries':len(state['log']),'topology_generation':state['committed_topology']['generation'],'envelope_checksum_matches':True})
  failures.append({'test':m['test'],'capture_status':m['status'],'manifest_member':name,'manifest_sha256':sha(raw),'all_children_waited':m['actual_children_waited'],'pending_command_projection':command,'copied_file_digests_verified':len(m['files']),'empty_child_output_members':empty_outputs,'statuses':statuses,'metadata_json_observations':metadata,'missing_allowlisted_paths':m['missing_allowlisted_paths'],'refused_allowlisted_paths':m['refused_allowlisted_paths'],'no_raw_custody_published_in_this_projection':True})
 external=[]
 for line in log.splitlines():
  marker='NATIVE_INITIAL_DATA_EXTERNAL_LEDGER '
  if marker in line:
   v=json.loads(line.split(marker,1)[1]); allowed={'logical_operation_id','stage','key','value','payload_sha256','acknowledged','inserted','error','present','success','round','phase','externally_acknowledged','data_move','fifth_data_stage','execution_identity'}
   if 'logical_operation_id' in v or v.get('stage') in ['four_initial_owners','four_initial_data_complete','strict_registry_addition_terminal']:
    item={k:v[k] for k in allowed if k in v}
    if 'execution_identity' in v:item['execution_identity']=v['execution_identity']
    external.append(item)
 return {'format':1,'source':SOURCE,'product_source_identical_to':'68b30c1bbe59b59345ec5a4d2f12354ae60d38ea','run':43527079466,'focused_job':132235428483,'artifact':14086103789,'artifact_sha256':sha(pathlib.Path(path).read_bytes()),'artifact_members':sorted(manifest,key=lambda x:x['member']),'selected_tests':selected,'case_results':cases,'result':results,'rustc':files['rustc.txt'].decode(),'journals':journals,'stopped_failure_diagnostic_projection':failures,'external_ledger_projection':external,'limits':['Whitelisted diagnostic DATA only; no raw RPC frames, stopped custody files, DB/WAL files, keys or certificates published.','Actual client traffic only: host/peer traffic and bootstrap raw frames are not covered.','Projection verifies retained observations, not source-code authority, disk recovery, power-loss durability or original failure cause.']}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('artifact');p.add_argument('--output',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.write_text(json.dumps(project(a.artifact),indent=2)+'\n');print(sha(out.read_bytes()))
