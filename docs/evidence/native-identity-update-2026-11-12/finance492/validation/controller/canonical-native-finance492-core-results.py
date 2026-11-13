import json,pathlib,re,hashlib
root=pathlib.Path('/root/bicdb-durability-s3');label='native-identity-finance492-validation';receipt=json.loads((root/('report-'+label+'.json')).read_text());assert receipt['source']=='2403deefde68869c15f02b73d40406ffdfb50148' and receipt['status']=='focused_passed' and len(receipt['steps'])==4 and all(x['exit']==0 for x in receipt['steps'])
log=root/('tc-step-'+label+'.log');raw=log.read_text();decoder=json.JSONDecoder();pieces=[];cursor=0
for event in re.finditer(r'\{"timestamp":',raw):
 if event.start()<cursor:continue
 try:value,end=decoder.raw_decode(raw,event.start())
 except json.JSONDecodeError:continue
 if isinstance(value,dict) and {'timestamp','event','severity','fields'}.issubset(value):pieces.append(raw[cursor:event.start()]);cursor=end
pieces.append(raw[cursor:]);normalized=re.sub(r'memory_available_kib=[^\n]*\n','',''.join(pieces));current=None;started=False;targets=[]
for line in normalized.splitlines():
 m=re.match(r'\s+Running (?:unittests )?(.+?) \((/root/[^)]+/debug/deps/[^)]+)\)',line)
 if m:
  if pathlib.Path(m[2]).name.startswith('bicdb_core-'):started=True
  if not started:continue
  if current:targets.append(current)
  current={'target':m[1],'binary':m[2],'summaries':[]}
 m=re.search(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored',line)
 if m and current:current['summaries'].append({'status':m[1],**{k:int(v) for k,v in zip(['passed','failed','ignored'],m.groups()[1:])}})
if current:targets.append(current)
counts={'Cargo_targets':len(targets),'passed':0,'failed':0,'ignored':0,'child_summaries_excluded':0}
for t in targets:
 assert t['summaries'],t;t['parent']=t['summaries'][-1];assert t['parent']['status']=='ok' and t['parent']['failed']==0;counts['child_summaries_excluded']+=len(t['summaries'])-1
 for key in ['passed','failed','ignored']:counts[key]+=t['parent'][key]
record={'source':receipt['source'],'version':'1.0.492-beta','status':'canonical_full_Core_passed','scope':'FinalparentCargo-targetsummaries only, excludechildsummaries. Exactfinance main490/issuer/semanticreader492candidate; notprivateSQLcomposition. Telemetryremovedonlyfromderivedparser/rawlogpreserved. AllfullCorelib+integrationtargetscommandexit0. FinalcurrentSQLauthority/version3checks andrequiredCIallSQL/wire/workspace separate.','counts':counts,'focused_issuer_and_grants_included_in_full_Core':14,'log_sha256':hashlib.file_digest(log.open('rb'),'sha256').hexdigest(),'targets':targets};out=root/'report-native-identity-finance492-canonical-Core.json';assert not out.exists();out.write_text(json.dumps(record,indent=2)+'\n');print(json.dumps({k:v for k,v in record.items() if k!='targets'}))
