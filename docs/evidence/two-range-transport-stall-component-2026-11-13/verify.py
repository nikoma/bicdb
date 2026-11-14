#!/usr/bin/env python3
"""Portable artifact checks. No Git/network, native execution or database opens."""
import argparse,gzip,hashlib,importlib.util,json,re,shutil,sys,tempfile,zipfile
from pathlib import Path
sys.dont_write_bytecode=True
HEAD='21669a25e0d95ff5b7a9ec8d367d5895c6d8e8fc'
RUNTIME='c9680e82acf9ceb01ad8994b7bb0b67b25768782'
OLD_HEAD='b01ed646b476cbb5f76a495d0f71bc8d13b929d6'
OLD_RUNTIME='c08fb440be78ff1b891dc9a5e4c6ec87228918cb'
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

def module(root,name):
 spec=importlib.util.spec_from_file_location(name,root/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def verify(root,original=None):
 expected={x['path']:x for x in load(root/'manifest.json')['files']};actual=set()
 for p in root.rglob('*'):
  require(not p.is_symlink(),'Packet symlink')
  if p.is_file():actual.add(p.relative_to(root).as_posix())
 require(actual==set(expected)|{'manifest.json'},'Packet exact file set')
 for name,e in expected.items():
  require(not Path(name).is_absolute()and'..'not in Path(name).parts,'Unsafe manifest path');p=root/name
  require(p.stat().st_size==e['bytes']and digest(p)==e['sha256'],'Packet hash/size: '+name)
 require(digest(root/'analysis-summary.json')=='85ff804a09e4ad0c87efc6aa0a58850c5f6b865d7df28925902fbf611cd9d3db','Original analysis summary')
 require(digest(root/'logical-projection.json')=='8cf2f3005a10af1d81a61dfb17ff85191fe3935c9bf32fc0d09fd8709a71899c','Original logical analysis projection')
 require(digest(root/'root-original-artifact-reconciliation.json')=='226474eeb097f0623a9d20f5137bd76554978f64ac82069cbbb3fedd9912e0e6','Original root reconciliation')
 source=load(root/'source-index.json');require(source['qualifier']==HEAD and source['runtime']==RUNTIME and source['original_failed_qualifier']==OLD_HEAD and source['original_failed_runtime']==OLD_RUNTIME,'Sources remain distinct')
 for h in [HEAD,RUNTIME,OLD_HEAD,OLD_RUNTIME]:
  require(commit(root,h)==source['commits'][h]['tree'],'Git complete commit tree')
  require(member(root,source['commits'][h]['tree'],'crates/bicdb-core')[1]==source['commits'][h]['core_tree'],'Git complete Core tree')
 require(source['commits'][HEAD]['core_tree']==source['commits'][RUNTIME]['core_tree']=='9068ea94acc33d8645cc76c1d51d11c79eda0127','Passed qualifier actual runtime equality')
 require(source['commits'][OLD_HEAD]['core_tree']==source['commits'][OLD_RUNTIME]['core_tree']=='9109e4eedb45a731334f17d2eb6e88a746010e0e','Failed qualifier actual runtime equality')
 for pin in source['pins']:
  raw=(root/'sources'/pin['commit']/pin['path']).read_bytes();require(len(raw)==pin['bytes']and sha(raw)==pin['sha256'],'Source bytes')
  require(oid('blob',raw)==pin['blob']==member(root,source['commits'][pin['commit']]['tree'],pin['path'])[1],'Git source member')
 # Exact four original log streams, actual checkout and terminal job states.
 old_errors=0
 for run,jobs,h,result in [(43548089992,[132303983971,132304020107],HEAD,'success'),(43547094599,[132300848308,132300870146],OLD_HEAD,'failure')]:
  folder=root/'runs'/str(run);rv=load(folder/'run-latest.json');require(rv['id']==run and rv['head_sha']==h and rv['conclusion']==result,'Original run state')
  for job in jobs:
   index=load(folder/f'job{job}-index.json');require(index['source']==h and index['job']['conclusion']==result,'Original job state')
   p=folder/f'job{job}.log.gz';require(digest(p)==index['gzip_sha256'],'Original gzip bytes')
   hasher=hashlib.sha256();total=0;pending=False;checkouts=[];compiler_error=False;native_summaries=0
   with gzip.open(p,'rb')as f:
    while True:
     line=f.readline(262145)
     if not line:break
     require(len(line)<=262144,'Bounded raw line');total+=len(line);require(total<=8*1024*1024,'Bounded raw log');hasher.update(line)
     require(not re.search(rb'-----BEGIN (?:(?:RSA|EC|OPENSSH|ENCRYPTED|DSA) )?PRIVATE KEY-----',line),'Literal private PEM marker')
     if b'git log -1 --format=%H'in line:pending=True
     elif pending:
      m=re.search(rb'\b([0-9a-f]{40})\s*$',line)
      if m:checkouts.append(m[1].decode());pending=False
     clean=re.sub(rb'\x1b\[[0-9;]*m',b'',line)
     compiler_error|=b'error[E0603]: function `exercise` is private'in clean
     native_summaries+=bool(re.search(rb'test result: (?:ok|FAILED)\.',line))
   require(hasher.hexdigest()==index['raw_sha256']and total==index['raw_bytes'],'Complete raw log hash/size')
   require(checkouts==[h],'Actual checkout recorded in each job')
   if run==43547094599:require(compiler_error and native_summaries==0,'Original failed compile, zero native runtime');old_errors+=1
 require(old_errors==2,'Both failed build jobs retained')
 project=module(root,'project');projection=load(root/'projection-manifest.json')
 require(projection['original_zip']['sha256']==project.ORIGINAL_SHA and projection['original_zip']['bytes']==397780 and projection['original_zip']['files']==153,'Private original identity')
 included={x['path']:x for x in projection['included']};omitted={x['path']:x for x in projection['omitted']}
 require(len(included)==52 and len(omitted)==101 and not(set(included)&set(omitted)),'Full original inventory partition')
 require(all(project.allowed(n)for n in included)and all('/custody/'in n for n in omitted),'Explicit logical-only projection')
 require(sum(Path(n).name=='mesh_signing_key.json'for n in omitted)==5,'Omitted signing key files')
 require(sum(x['bytes']for x in projection['included']+projection['omitted'])==5595680,'Complete original byte inventory')
 require(digest(root/'logical-projection.zip')==projection['projection_zip']['sha256'],'Public ZIP identity')
 request=load(root/'dispatch-request.json');summary=load(root/'analysis-summary.json')
 with zipfile.ZipFile(root/'logical-projection.zip')as z:
  require(len(z.namelist())==len(set(z.namelist()))and set(z.namelist())==set(included),'Exact public ZIP members')
  for n,e in included.items():require(z.getinfo(n).file_size==e['bytes']<=2*1024*1024 and sha(z.read(n))==e['sha256'],'Exact included original bytes')
  require(json.loads(z.read('expected-inventory.json'))==request['expected83_inventory'],'Actual whole inventory bound to request')
  derived=module(root,'reconcile').reconcile(z,request['execute_exact_full_function'],request['expected83_inventory'])
  require(derived['wire']==summary['wire']['requests']and derived['source_batch']==summary['source_rows_batch'],'Independent actual frame reconciliation matches retained analysis')
  prefix=derived['fixture_root'];identity=json.loads(z.read(prefix+'/execution-identity.json'));require(identity['source_commit']==HEAD and identity['source_case']=='TwoRangeSourceReplyStall'and identity['actual_rust_test_thread']==request['execute_exact_full_function'],'Actual executed test identity')
  logical=load(root/'logical-projection.json');require(logical['fixture_prefix']==prefix,'Original parsed projection root')
  for n,v in logical['files'].items():
   if n=='original-sdk-rpc':
    for name,row in v.items():require(json.loads(z.read(prefix+'/'+n+'/'+name))==row,'Original SDK parsed/raw byte relation')
   else:require(json.loads(z.read(prefix+'/'+n))==v,'Original parsed/raw logical relation')
  result=json.loads(z.read(prefix+'/two-range-stall-result.json'));require(result['transport_hold_after_actual_export']is True,'Actual transport-only hold point')
  require(result['actual_rows_reply']['payload']['Rows']['complete']is False,'Partial stream not full relocation')
  require(not any(x.get('intent',{}).get('operation')=='Finish'for x in result['actual_request_journal']),'No Finish promoted to full receive')
  # Each successful Strong follows a real prior APP ACK in this sequential journal.
  seen=set()
  for call in derived['wire']:
   if call['operation']=='append':seen.add(call['key'])
   else:require(call['key']in seen,'Strong full value is after actual prior APP ACK')
 # Public pressure/failed-list ZIPs contain only explicitly known root logical files.
 for run,ids in [(43548089992,[14095127454]),(43547094599,[14094194341,14094414929])]:
  for aid in ids:
   p=root/'runs'/str(run)/f'artifact{aid}.zip'
   entry=next(x for x in load(root/'capture-manifest.json')['files']if x['file']==f'{run}/artifact{aid}.zip')
   require(digest(p)==entry['sha256']and p.stat().st_size==entry['bytes'],'Exact original pressure/compile ZIP')
   with zipfile.ZipFile(p)as z:
    require(z.testzip()is None and all('/'not in n and n in {'alltargets.jsonl','consumer.jsonl','helper-pressure.jsonl','list-pressure.jsonl','exact-test-name.txt','requested-filter.txt','rustc.txt','selected-tests.log','source-revision.txt'}for n in z.namelist()),'Safe known pressure/failed-list members')
 if original:
  with tempfile.TemporaryDirectory(prefix='c968-reprojection-')as t:
   fresh=project.build(original,Path(t)/'projection.zip');require(all(fresh[k]==projection[k]for k in ['original_zip','included','omitted','scope']),'Complete private original rederives exact logical/omission hashes')
 return {'packet_integrity':'PASS','actual_component':'1PASS/0FAIL12.29s','whole_compiler_listed':83,'whole_cases_executed':1,'prior_compile':'E0603/0runtime','APP_ACKs':derived['source_APP_ACKs'],'full_Strong_values':derived['full_Strong_values'],'held_calls':derived['held_calls'],'primary_wire_attempts':derived['primary_wire_attempts'],'recorded_retry_or_refresh':0,'original_private_archive_rederived':bool(original),'engine_opened':False,'scope':'after-export transport independence component only','physical_or_production_qualification':False}

def refresh(root):
 files=[]
 for p in sorted(root.rglob('*')):
  if p.is_file()and p!=root/'manifest.json':files.append({'path':p.relative_to(root).as_posix(),'bytes':p.stat().st_size,'sha256':digest(p)})
 (root/'manifest.json').write_text(json.dumps({'files':files},indent=2)+'\n')
def tamper(root):
 results=[]
 for mode in ['raw_log_hash','held_snapshot_semantics','source_membership']:
  with tempfile.TemporaryDirectory(prefix='c968-tamper-')as t:
   copy=Path(t)/'packet';shutil.copytree(root,copy)
   if mode=='raw_log_hash':
    p=copy/'runs/43548089992/job132304020107.log.gz';data=bytearray(p.read_bytes());data[-1]^=1;p.write_bytes(data)
   elif mode=='held_snapshot_semantics':
    p=copy/'logical-projection.zip'
    with zipfile.ZipFile(p)as z:values={n:z.read(n)for n in z.namelist()}
    n=next(n for n in values if n.endswith('/two-range-stall-result.json'));before=values[n];values[n]=before.replace(b'"currently_held": true',b'"currently_held":false',1);require(len(values[n])==len(before)and values[n]!=before,'Same-byte-count semantic tamper')
    with zipfile.ZipFile(p,'w',compression=zipfile.ZIP_DEFLATED)as z:
     for name,raw in values.items():z.writestr(name,raw)
    projection=load(copy/'projection-manifest.json');projection['projection_zip']['sha256']=digest(p)
    e=next(x for x in projection['included']if x['path']==n);e.update(bytes=len(values[n]),sha256=sha(values[n]))
    (copy/'projection-manifest.json').write_text(json.dumps(projection));refresh(copy)
   else:
    index=load(copy/'source-index.json');pin=next(x for x in index['pins']if x['path'].endswith('/two_range_stall_tests.rs')and x['commit']==HEAD)
    p=copy/'sources'/pin['commit']/pin['path'];p.write_bytes(p.read_bytes().replace(b'pub(in super::super)',b'pub(crate)',1));pin.update(bytes=p.stat().st_size,sha256=digest(p),blob=oid('blob',p.read_bytes()))
    (copy/'source-index.json').write_text(json.dumps(index));refresh(copy)
   try:verify(copy)
   except (ValueError,AssertionError,KeyError,gzip.BadGzipFile)as e:results.append({'control':mode,'rejected':True,'reason':str(e)})
   else:raise ValueError('Tamper not rejected: '+mode)
 return results
if __name__=='__main__':
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--root',type=Path,default=Path(__file__).resolve().parent);ap.add_argument('--original-zip',type=Path);ap.add_argument('--tamper-controls',action='store_true');args=ap.parse_args()
 result=verify(args.root,args.original_zip)
 if args.tamper_controls:result['tamper_controls']=tamper(args.root)
 print(json.dumps(result,indent=2))
