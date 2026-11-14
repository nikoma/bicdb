#!/usr/bin/env python3
"""Portable evidence verification; no Git/network/engine opens. Not qualification."""
import argparse,gzip,hashlib,importlib.util,json,re,shutil,tempfile,zipfile,sys
sys.dont_write_bytecode=True
from pathlib import Path
RAW_SHA='241738e911657e0ed0e6d975f5ddf14ff10076f40e222699f859bf945a1181d0'
ACTOR_SHA='259db67190a3ab2bebba942cdd6012c3fb21b81c7de0c37de72fbeee3b225711'
ISOLATED_SHA='b99d5a60110a4df91e184b39b0eeaf9c377dd8837bdf95026c10f867820f6740'
HEAD='88ffe19f40ffd4ee83fb63a062795380b385bb33';RUNTIME='5a85900f571c389ff97e278a0b9555048bdeadec'
def require(ok,detail):
 if not ok:raise ValueError(detail)
def sha(data):return hashlib.sha256(data).hexdigest()
def load(path):return json.loads(path.read_bytes())
def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as f:
  for b in iter(lambda:f.read(65536),b''):h.update(b)
 return h.hexdigest()
def oid(kind,data):return hashlib.sha1(kind.encode()+b' '+str(len(data)).encode()+b'\0'+data).hexdigest()
def tree(root,ident):
 data=(root/'proofs/git/trees'/f'{ident}.bin').read_bytes();require(oid('tree',data)==ident,'Git tree identity')
 result={};at=0
 while at<len(data):
  space=data.index(b' ',at);nul=data.index(b'\0',space+1);require(nul+21<=len(data),'Git tree framing')
  name=data[space+1:nul].decode();require(name not in result,'Duplicate tree name');result[name]=(data[at:space].decode(),data[nul+1:nul+21].hex());at=nul+21
 return result
def commit(root,ident):
 data=(root/'proofs/git/commits'/f'{ident}.txt').read_bytes();require(oid('commit',data)==ident,'Git commit identity');return re.match(rb'tree ([0-9a-f]{40})\n',data)[1].decode()
def member(root,ident,path):
 parts=Path(path).parts;require(not Path(path).is_absolute()and'..'not in parts,'Unsafe source path')
 for i,p in enumerate(parts):
  entries=tree(root,ident);require(p in entries,'Source membership missing: '+path);mode,ident=entries[p]
  if i+1<len(parts):require(mode in['40000','040000'],'Source directory mode')
 return mode,ident

def verify(root,original=None):
 manifest=load(root/'manifest.json');expected={x['path']:x for x in manifest['files']}
 actual=set()
 for p in root.rglob('*'):
  require(not p.is_symlink(),'Packet symlink')
  if p.is_file():actual.add(p.relative_to(root).as_posix())
 require(actual==set(expected)|{'manifest.json'},'Packet file set')
 for name,e in expected.items():
  require(not Path(name).is_absolute()and'..'not in Path(name).parts,'Manifest path')
  p=root/name;require(p.stat().st_size==e['bytes']and digest(p)==e['sha256'],'Packet hash/size: '+name)
 require(digest(root/'actor-correlation-original.json')==ACTOR_SHA,'Original actor report changed')
 require(digest(root/'isolated-source-classification-original.json')==ISOLATED_SHA,'Original source classification changed')
 h=hashlib.sha256();total=lines=maxline=0;checkouts=[];pending=False;private_markers=0
 with gzip.open(root/'raw-core-job.log.gz','rb')as f:
  while True:
   line=f.readline(262145)
   if not line:break
   require(len(line)<=262144,'Bounded raw line');h.update(line);total+=len(line);lines+=1;maxline=max(maxline,len(line));require(total<=32*1024*1024,'Bounded raw log')
   private_markers+=len(re.findall(rb'-----BEGIN (?:(?:RSA|EC|OPENSSH|ENCRYPTED|DSA) )?PRIVATE KEY-----',line))
   if b'git log -1 --format=%H'in line:pending=True
   elif pending:
    m=re.search(rb'\b([0-9a-f]{40})\s*$',line)
    if m:checkouts.append(m[1].decode());pending=False
 require(h.hexdigest()==RAW_SHA and total==22693709 and lines==65927 and maxline==171493,'Original full raw log identity/framing')
 require(checkouts==[HEAD],'Actual checkout from log');require(private_markers==0,'Literal PEM private block in published raw log')
 run=load(root/'run-terminal.json');job=load(root/'job-terminal.json');obs=load(root/'run-observation.json')
 require(run['id']==43544810818 and run['head_sha']==HEAD and run['conclusion']=='failure','Original failed run')
 require(job['id']==132293554417 and job['run_id']==run['id']and job['conclusion']=='failure','Original failed job')
 require(obs['actual_checkout_from_raw_log']==HEAD and obs['core_runtime_source']==RUNTIME,'No source rewrite')
 require(digest(root/'original-pressure-artifact.zip')=='7676855d20070a02076c7a00b9d916c199a3f254e261fb7dc3fbb0e0fc3505d4','Original pressure ZIP identity')
 with zipfile.ZipFile(root/'original-pressure-artifact.zip')as pressure:
  require(set(pressure.namelist())=={'alltargets.jsonl','consumer.jsonl'}and sum(x.file_size for x in pressure.infolist())==32249 and pressure.testzip()is None,'Original pressure contents/CRC')
 source=load(root/'source-index.json');ht=commit(root,HEAD);rt=commit(root,RUNTIME)
 require(source['head_tree']==ht and source['runtime_tree']==rt and ht!=rt,'Workflow/runtime complete tree distinction')
 require(member(root,ht,'crates/bicdb-core')[1]==member(root,rt,'crates/bicdb-core')[1]==source['core_tree']=='0472ff801146f2cdf51a6db769c0354315eb5b04','Exact entire Core tree')
 for pin in source['source_pins']:
  raw=(root/'sources'/pin['path']).read_bytes();require(len(raw)==pin['bytes']and sha(raw)==pin['sha256'],'Source copy bytes')
  require(oid('blob',raw)==pin['blob']==member(root,ht,pin['path'])[1],'Git source member')
  if pin['same_runtime_blob']:require(member(root,rt,pin['path'])[1]==pin['blob'],'Runtime source match')
 # Raw whitelisted projection, with complete omission inventory but NO DB bytes.
 spec=importlib.util.spec_from_file_location('project',root/'project.py');project=importlib.util.module_from_spec(spec);spec.loader.exec_module(project)
 projection=load(root/'projection-manifest.json');require(projection['original_zip']['sha256']==project.ORIGINAL_SHA and projection['original_zip']['files']==9361,'Original ZIP reference')
 pe=projection['projection_zip'];require(digest(root/'logical-projection.zip')==pe['sha256'],'Projection ZIP identity')
 included={x['path']:x for x in projection['included']};omitted={x['path']:x for x in projection['omitted']}
 require(not(set(included)&set(omitted))and len(included)+len(omitted)==9361,'Complete original inventory partition')
 require(len(included)==1075 and len(omitted)==8286,'Projection scope counts')
 require(sum(Path(n).name=='mesh_signing_key.json'for n in omitted)==410,'Omitted signing-key filename count')
 require(all(project.allowed(n)for n in included),'Explicit projection allowlist')
 require(all(n.split('/')[2]=='custody'for n in omitted),'Entire custody omitted')
 require(sum(x['bytes']for x in projection['included']+projection['omitted'])==379438111,'Original uncompressed inventory')
 with zipfile.ZipFile(root/'logical-projection.zip')as z:
  require(len(z.namelist())==len(set(z.namelist()))and set(z.namelist())==set(included),'Projection exact file set')
  for n,e in included.items():
   require(z.getinfo(n).file_size==e['bytes']<=32*1024*1024,'Projection individual bound');require(sha(z.read(n))==e['sha256'],'Projection member hash: '+n)
  listing=z.read('selected-tests.log').decode();names=[l[:-6]for l in listing.splitlines()if l.endswith(': test')];require(len(names)==len(set(names))==82,'Actual compiler listing82')
  text=z.read('tests.log').decode();summary=re.findall(r'test result: FAILED\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9.]+)s',text)
  require(summary==[('70','12','0','0','1088','941.30')],'Original70/12 result')
  tail=text.rsplit('failures:\n',1)[1].split('test result:',1)[0];failed=[l.strip()for l in tail.splitlines()if l.strip().startswith('distribution_native_host::')]
  require(len(failed)==12 and set(failed)<=set(names),'Actual final failure names')
  require(names==obs['compiler_listed_names']and failed==obs['failed_names'],'Saved actual inventory')
  roots=sorted({n.split('/')[1]for n in z.namelist()if n.startswith('fault-state/')});require(len(roots)==82,'Actual82 fixture roots')
  case_data={};acks=uncertain=0
  for name in roots:
   prefix='fault-state/'+name;identity=json.loads(z.read(prefix+'/execution-identity.json'));terminal=json.loads(z.read(prefix+'/terminal.json'));ledger=json.loads(z.read(prefix+'/ack-ledger.json'))
   require(identity['source_commit']==HEAD and identity['actual_rust_test_thread']in names,'Actual source/test identity')
   require(len(ledger)==2,'Original source APP intents')
   for item in ledger:
    require(item['operation']=='ClusterEventClient.append','Source APP operation');require(item['outcome']=='returned'and item['actual_api_inserted']is True and item['effect_unknown_on_error']is False,'Original ACK not uncertain/failed')
    require(sha(json.dumps(item['value'],separators=(',',':'),ensure_ascii=False,sort_keys=True).encode())==item['payload_json_sha256'],'Full expected payload hash')
    acks+=1;uncertain+=item['effect_unknown_on_error']is True
   case_data[prefix]=(identity,terminal,ledger)
  require(acks==164 and uncertain==0,'ACK count across82 independent fixtures')
  require(len({v[0]['actual_rust_test_thread']for v in case_data.values()})==82,'Unique executed fixtures')
  saved=load(root/'case-observations.json');require(len(saved['records'])==82,'Saved observations count')
  for row in saved['records']:
   ident,term,ledger=case_data[row['case_root']];require(row['test']==ident['actual_rust_test_thread']and row['source_case']==ident['source_case']and row['phase']==term['phase']and row['primary_error']==term['primary_error'],'Case observations raw relation')
   require(row['result']==('FAILED'if row['test']in failed else'PASSED')and row['source_app_acks']==2,'Case result not promoted')
  actor=load(root/'actor-correlation-original.json');metadata=load(root/'derived-mstore-points.json');points={x['case_root']:x['points']for x in metadata['records']}
  require(len(actor['records'])==len(points)==12,'Twelve failure correlation records')
  hot=domain=0
  hot_source=(root/'sources'/('crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests.rs')).read_bytes()
  predicate=(root/'sources/crates/bicdb-core/src/distribution_consensus.rs').read_bytes()
  require(b'let actor = clients[donor].local_metadata_read_participant();'in hot_source,'Original actor construction source')
  require(b'(range.leader != initiator.id && metadata_leader != &initiator.id)'in predicate,'Original production predicate source')
  for rec in actor['records']:
   prefix=rec['case_root'];ident,term,ledger=case_data[prefix]
   require(ident['source_case']==rec['case']and term['primary_error']==rec['primary_error']and term['phase']==rec['phase_at_panic'],'Failure case relation')
   for suffix,key in [('execution-identity.json','execution_identity'),('remote-old-ledger.json','remote_old_ledger'),('source-frame-ledger.json','source_frame_ledger')]:require(sha(z.read(prefix+'/'+suffix))==rec['report_hashes'][key],'Correlation input bytes')
   olds=json.loads(z.read(prefix+'/remote-old-ledger.json'));collect=[x for x in olds if x['operation']=='remote_old_fresh_all_closed_collection'];require(len(collect)==1 and collect[0]['actual_donor']==rec['actual_donor_from_collector'],'Actual donor from ledger')
   for node,key in [('n0','copied_MStore_node0'),('n4','copied_MStore_node4')]:
    point=points[prefix][node];require(point['derived_point']==rec[key],'Derived MStore/correlation fields')
    require(point['original_member']in omitted and point['original_sha256']==omitted[point['original_member']]['sha256'],'Omitted MStore provenance')
   require(points[prefix]['n0']['original_sha256']==rec['report_hashes']['copied_MStore_node0'],'Original source MStore input hash')
   frames=json.loads(z.read(prefix+'/source-frame-ledger.json'));actual_frames=[]
   for x in frames:
    if x['operation']!='source_image_frame':continue
    mf=x.get('actual_reply',{}).get('payload',{}).get('Manifest',{})
    actual_frames.append({'operation':x['intent']['operation'],'outcome':x['outcome'],'source':x['source'],'target':x['target'],'manifest_source':mf.get('source_id'),'manifest_A':mf.get('target_attempt'),'original_parent_live':x['original_parent_live_at_reply'],'original_own_live':x['original_own_live_at_reply']})
   require(actual_frames==rec['actual_frames'],'Actual source frame correlation')
   require(term['reports'][0]['current_range_leader']==rec['range_leader'],'Recorded selected range leader')
   if rec['case'].startswith('TargetHot'):
    hot+=1;donor=rec['actual_donor_from_collector'];leader=rec['range_leader'];metadata_leader=points[prefix]['n0']['derived_point']['leader_id']
    require(donor!=leader and donor!=metadata_leader and metadata_leader=='n0','Source-derived actor violates recorded initiator predicate')
    require('metadata recovery-role identity, purpose or committed configuration changed'in rec['primary_error'],'Actual witness rejection text')
    require(len(actual_frames)==2 and actual_frames[0]['operation']=='Manifest'and'History'in actual_frames[1]['operation'],'Failure before later native/image/business gates')
    wire=[json.loads(z.read(n))for n in sorted(z.namelist())if n.startswith(prefix+'/original-sdk-rpc/')]
    require(len(wire)==8 and[r['journal_sequence']for r in wire]==list(range(1,9))and all(r['operation']=='event_append'for r in wire),'SDK observer covers APP only, not witness RPC')
   else:
    domain+=1;require(rec['case']=='TargetNoDomainMemberChanged','Twelfth distinct case')
    require(points[prefix]['n4']['derived_point']['current_term']==2 and term['reports'][0]['scope']['metadata_term']==1,'Earlier fence1/copy targetterm2')
    require('actual metadata authority version changed/dirty; retained native outcome is not a current grant'in rec['primary_error'],'Actual stronger version refusal')
    require(prefix+'/no-domain-1-member.json'not in z.namelist(),'Original missing post-assert NoDomain evidence remains missing')
  require((hot,domain)==(11,1),'Failure families kept separate')
 # Optional private original rederivation: byte-only ZIP reads, no engine.
 if original:
  with tempfile.TemporaryDirectory(prefix='native82-reprojection-')as tmp:
   generated=Path(tmp)/'projection.zip';fresh=project.build(original,generated)
   require(all(fresh[k]==projection[k]for k in ['original_zip','included','omitted','scope']),'Original archive -> exact public member/omission hashes')
   require(all(fresh['projection_zip'][k]==projection['projection_zip'][k]for k in ['files','uncompressed_bytes','raw_database_custody_present']),'Original projection content scope')
   require(project.derive_metadata(original,[x['case_root']for x in actor['records']])==metadata,'Original omitted MStore JSON -> exact derived subset')
 return {'packet_integrity':'PASS','native_result':'FAILED70/12','actual_listed':82,'correlated_hot_fixture_failures':11,'distinct_NoDomain_matcher_failure':1,'source_APP_ACKs_across_independent_fixtures':164,'source_APP_uncertain':0,'original_private_archive_rederived':bool(original),'engine_opened':False,'physical_or_production_qualification':False}

def refresh(root):
 manifest={'files':[]}
 for p in sorted(root.rglob('*')):
  if p.is_file()and p!=root/'manifest.json':manifest['files'].append({'path':p.relative_to(root).as_posix(),'bytes':p.stat().st_size,'sha256':digest(p)})
 (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
def tamper(root):
 controls=[]
 for mode in ['log_hash','derived_metadata_semantic','source_membership']:
  with tempfile.TemporaryDirectory(prefix='native82-tamper-')as tmp:
   copy=Path(tmp)/'packet';shutil.copytree(root,copy)
   if mode=='log_hash':
    p=copy/'raw-core-job.log.gz';b=bytearray(p.read_bytes());b[-1]^=1;p.write_bytes(b)
   elif mode=='derived_metadata_semantic':
    p=copy/'derived-mstore-points.json';v=load(p);v['records'][0]['points']['n0']['derived_point']['leader_id']='n2';p.write_text(json.dumps(v));refresh(copy)
   else:
    p=copy/'sources/crates/bicdb-core/src/distribution_consensus.rs';p.write_bytes(p.read_bytes().replace(b'range.leader != initiator.id',b'range.leader == initiator.id',1))
    index=load(copy/'source-index.json')
    entry=next(x for x in index['source_pins']if x['path']=='crates/bicdb-core/src/distribution_consensus.rs');entry.update(bytes=p.stat().st_size,sha256=digest(p),blob=oid('blob',p.read_bytes()))
    (copy/'source-index.json').write_text(json.dumps(index));refresh(copy)
   try:verify(copy)
   except (ValueError,KeyError,AssertionError,gzip.BadGzipFile)as e:controls.append({'control':mode,'rejected':True,'reason':str(e)})
   else:raise ValueError('Tamper not rejected: '+mode)
 return controls
if __name__=='__main__':
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--root',type=Path,default=Path(__file__).resolve().parent);ap.add_argument('--original-zip',type=Path);ap.add_argument('--tamper-controls',action='store_true');args=ap.parse_args()
 result=verify(args.root,args.original_zip)
 if args.tamper_controls:result['tamper_controls']=tamper(args.root)
 print(json.dumps(result,indent=2))
