"""Offline retained-state positions; no engine open, replay, or VM access."""
import collections,hashlib,json,pathlib,time
P=pathlib.Path;ROOT=P('/tmp/bicdb-azure-repro/experiment/light-30cf5a05bde64ced88bd576e8b9950ea/prestop-forensics');nodes=[]
def compact(x):return json.dumps(x,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
def sha(x):return hashlib.sha256(x).hexdigest()
for i in range(4):
 folder=ROOT/f'n{i}';raw=(folder/'db__cluster-range-write-log.json').read_bytes();envelope=json.loads(raw);catalog=envelope['catalog'];top=json.loads((folder/'db__cluster-topology.json').read_text())['topology'];ranges=[];errors=[];states=collections.Counter();checked=0
 for rid,log in catalog['ranges'].items():
  entries=log['entries'];maximum={};local_states=collections.Counter();tail=[]
  for index,entry in entries.items():
   command=entry['command'];state=entry['state'];states[state]+=1;local_states[state]+=1;maximum[state]=max(maximum.get(state,0),int(index));checked+=1
   if int(index)!=command['index']or int(rid)!=command['range_id']:errors.append({'range_id':rid,'index':index,'error':'command_index_or_range_mismatch'})
   payload={k:v for k,v in command.items()if k!='checksum_sha256'}
   if sha(compact(payload))!=command['checksum_sha256']:errors.append({'range_id':rid,'index':index,'error':'command_checksum_mismatch'})
   if int(index)>log['resolved_through']:tail.append({'index':int(index),'command_id':command['command_id'],'command_epoch':command['range_epoch'],'state':state,'command_checksum_sha256':command['checksum_sha256']})
  route=top['ranges'].get(rid,{})
  ranges.append({'range_id':int(rid),**{k:log[k]for k in ['current_epoch','last_index','resolved_through','compacted_through']},'retained_state_counts':dict(local_states),'highest_retained_index_by_state':maximum,'unresolved_tail':tail,'independently_copied_route_epoch':route.get('epoch'),'independently_copied_route_leader':route.get('leader'),'independently_copied_route_replicas':route.get('replicas'),'separate_signed_certificate_objects_present':False})
 nodes.append({'node':i,'raw_catalog_sha256':sha(raw),'catalog_checksum_valid':sha(compact(catalog))==envelope['checksum_sha256'],'command_checksums_checked':checked,'validation_errors':errors,'retained_state_counts':dict(states),'topology_generation':top['generation'],'ranges':ranges})
result={'helper_sha256':sha(P(__file__).read_bytes()),'completed_unix_ns':time.time_ns(),'scope':'independent live atomic-file captures; not a coherent quorum snapshot, row-read, or signed-certificate verification','retained_command_epoch_is_not_routing_epoch':True,'nodes':nodes}
(ROOT/'all-range-position-analysis.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'nodes':len(nodes),'all_catalog_checksums_valid':all(n['catalog_checksum_valid']for n in nodes),'command_checksums_checked':sum(n['command_checksums_checked']for n in nodes),'validation_errors':sum(len(n['validation_errors'])for n in nodes)}))
