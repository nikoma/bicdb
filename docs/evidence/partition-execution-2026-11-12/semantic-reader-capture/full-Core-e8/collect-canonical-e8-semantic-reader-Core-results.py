"""Count the final parent-harness result per Cargo target, excluding child summaries."""
import hashlib,json,pathlib,re
root=pathlib.Path('/root/bicdb-durability-s3');results=[]
for label in ['semantic-reader-current-core-broad-v2']:
 receipt=json.loads((root/('report-'+label+'.json')).read_text());assert receipt['status']=='focused_passed' and all(s['exit']==0 for s in receipt['steps'])
 log=root/('tc-step-'+label+'.log');scope=None;current=None;targets=[]
 raw=log.read_text();decoder=json.JSONDecoder();pieces=[];cursor=0
 for event in re.finditer(r'\{"timestamp":',raw):
  if event.start()<cursor:continue
  try:value,end=decoder.raw_decode(raw,event.start())
  except json.JSONDecodeError:continue
  if isinstance(value,dict) and {'timestamp','event','severity','fields'}.issubset(value):
   pieces.append(raw[cursor:event.start()]);cursor=end
 pieces.append(raw[cursor:]);normalized=''.join(pieces)
 normalized=re.sub(r'memory_available_kib=[^\n]*\n','',normalized)
 for line in normalized.splitlines():
  m=re.match(r'\s+Running (?:unittests )?(.+?) \((/root/[^)]+/debug/deps/[^)]+)\)',line)
  if m:
   if current is not None:targets.append(current)
   for package in ['bicdb_core','bicdb_sql','bicdb_pgwire']:
    if pathlib.Path(m[2]).name.startswith(package+'-'):scope=package
   assert scope,line
   current={'package':scope,'target':m[1],'binary':m[2],'observed_summaries':[]}
  m=re.search(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored',line)
  if m:
   assert current is not None,line
   current['observed_summaries'].append({'status':m[1],**{k:int(v) for k,v in zip(['passed','failed','ignored'],m.groups()[1:])}})
 if current is not None:targets.append(current)
 counts={}
 for t in targets:
  assert t['observed_summaries'],(label,t); t['parent_result']=t['observed_summaries'][-1];assert t['parent_result']['status']=='ok' and t['parent_result']['failed']==0
  c=counts.setdefault(t['package'],{'Cargo_targets':0,'passed':0,'failed':0,'ignored':0,'child_summaries_excluded':0});c['Cargo_targets']+=1;c['child_summaries_excluded']+=len(t['observed_summaries'])-1
  for key in ['passed','failed','ignored']:c[key]+=t['parent_result'][key]
 results.append({'label':label,'source':receipt['source'],'status':'canonical_parent_targets_passed','log_sha256':hashlib.file_digest(log.open('rb'),'sha256').hexdigest(),'counts':counts,'targets':targets})
out=root/'report-semantic-reader-e8-current-Core-canonical-parent-results.json';assert not out.exists();out.write_text(json.dumps({'status':'verified_passed','scope':'TelemetryJSONandmemorymonitorinterleaving removedonlyfromderivedparse, rawlogs retained. Onlyfinalparentharness resultperCargoRunning target; nestedchildsummaries separatelyretained, notdoublecounted. ExactcompletedSourceScope, no latercodequalification.','runs':results},indent=2)+'\n')
print(json.dumps([{k:v for k,v in x.items() if k!='targets'} for x in results]))
