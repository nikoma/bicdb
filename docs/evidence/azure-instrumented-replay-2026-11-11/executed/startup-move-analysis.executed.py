"""Offline sampled startup moves; never opens engine or accesses VMs."""
import hashlib,json,pathlib,tarfile
P=pathlib.Path;base=P('/tmp/bicdb-azure-repro/experiment');prior=base/'8186dd18b83f4195b1319a85a1ee3498';root=base/'light-30cf5a05bde64ced88bd576e8b9950ea';result=[]
for i in range(4):
 with tarfile.open(prior/f'n{i}-cold-final.private.tgz')as archive:
  raw=archive.extractfile('db/cluster-topology.json').read();old=json.loads(raw)['topology']
 observations=[]
 for path in (root/'snapshots').glob('*.json'):
  for node in json.loads(path.read_text())['nodes']:
   if node['node']!=i or 'status'not in node:continue
   status=node['status'];observations.append({'controller_call_started_unix_ns':node['started_unix_ns'],'controller_call_finished_unix_ns':node['finished_unix_ns'],'sample_file':path.name,'topology_generation':status['topology']['generation'],'metadata_consensus':status['metadata_consensus'],'metrics':status['metrics'],'moves':status['topology']['relocations']})
 observations.sort(key=lambda row:(row['controller_call_started_unix_ns'],row['controller_call_finished_unix_ns']))
 ever_active={move_id for row in observations for move_id,move in row['moves'].items()if move['phase']not in ['completed','failed']};changed=ever_active|{move_id for row in observations for move_id,move in row['moves'].items()if move_id not in old['relocations']or move!=old['relocations'][move_id]}
 tracks=[]
 for move_id in sorted(changed,key=int):
  last=None;states=[]
  for row in observations:
   move=row['moves'].get(move_id)
   if move is None or move==last:continue
   states.append({k:row[k]for k in ['controller_call_started_unix_ns','controller_call_finished_unix_ns','sample_file','topology_generation']}|{'move':move});last=move
  tracks.append({'move_id':int(move_id),'present_in_original_cold_topology':move_id in old['relocations'],'original_cold_move':old['relocations'].get(move_id),'sampled_states':states})
 result.append({'node':i,'original_cold_topology_sha256':hashlib.sha256(raw).hexdigest(),'original_cold_generation':old['generation'],'sample_count':len(observations),'maximum_sampled_active_relocations':max((r['metrics']['active_relocations']for r in observations),default=0),'active_ever_or_changed_move_ids':len(changed),'moves':tracks,'metadata_samples':[{k:r[k]for k in ['controller_call_started_unix_ns','controller_call_finished_unix_ns','sample_file','topology_generation','metadata_consensus','metrics']}for r in observations]})
out={'method':'offline original cold topology plus all light snapshot files; controller observation intervals can overlap; member timestamps uncalibrated; no exact transition time/causality claim','fifth_node_join_attempted':False,'new_engine_feature_implemented':False,'nodes':result};p=root/'startup-move-history-analysis.json';p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'nodes':[{k:r[k]for k in ['node','sample_count','maximum_sampled_active_relocations','active_ever_or_changed_move_ids']}for r in result]}))
