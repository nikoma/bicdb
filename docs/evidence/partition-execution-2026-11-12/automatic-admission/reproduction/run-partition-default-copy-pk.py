import pathlib,json,subprocess,time,hashlib,shutil,os
root=pathlib.Path('/root/bicdb-durability-s3');label='partition-copy-pk-probes';unit='bicdb-'+label
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
build=json.loads((root/('report-'+label+'.json')).read_text());assert build['status']=='focused_passed',build
binary=pathlib.Path(build['binary']);assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==build['binary_sha256']
old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());out=root/'partition-default-copy-pk-8ed4a67b';out.mkdir(exist_ok=False)
record={'status':'running','source':build['source'],'binary':str(binary),'binary_sha256':build['binary_sha256'],'old_source':fixtures['source'],'old_binary_sha256':fixtures['binary_sha256'],'started_unix':time.time(),'cases':[]}
def save():(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
def verify(case):
 source=pathlib.Path(case['directory'])/'db';actual={str(p.relative_to(source)) for p in source.rglob('*') if p.is_file() and not p.is_symlink()};assert actual==set(case['files']),str(source)
 for name,info in case['files'].items():
  path=source/name;assert path.stat().st_size==info['bytes'] and hashlib.file_digest(path.open('rb'),'sha256').hexdigest()==info['sha256'],str(path)
def hashes(path):return {str(p.relative_to(path)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in path.rglob('*') if p.is_file() and not p.is_symlink()}
for case in fixtures['cases']:
 verify(case);name=case['mode']+'-'+case['name'];dest=out/name;shutil.copytree(pathlib.Path(case['directory'])/'db',dest)
 result=subprocess.run([str(binary),case['mode'],case['name'],str(dest)],text=True,capture_output=True);(out/(name+'.stdout')).write_text(result.stdout);(out/(name+'.stderr')).write_text(result.stderr);record['cases'].append({'mode':case['mode'],'case':case['name'],'database_copy':str(dest),'exit':result.returncode,'stdout':name+'.stdout','stderr':name+'.stderr'});verify(case);save()
for case in [c for c in fixtures['cases'] if c['name']=='parent_heap']:
 name=case['mode']+'-old-reader';dest=out/name;shutil.copytree(pathlib.Path(case['directory'])/'db',dest)
 candidate=subprocess.run([str(binary),case['mode'],'reader_fence',str(dest)],text=True,capture_output=True);assert candidate.returncode==0,candidate.stderr
 before=hashes(dest);old_reader=subprocess.run([fixtures['binary'],'sql',str(dest),'SELECT k,payload FROM p ORDER BY k','--json'],text=True,capture_output=True);after=hashes(dest)
 (out/(name+'.stdout')).write_text(old_reader.stdout);(out/(name+'.stderr')).write_text(old_reader.stderr)
 record['cases'].append({'mode':case['mode'],'case':'old_reader_fence','candidate_exit':candidate.returncode,'old_reader_exit':old_reader.returncode,'old_reader_accepted':old_reader.returncode==0,'byte_identity_preserved':before==after,'changed_files':sorted(k for k in set(before)|set(after) if before.get(k)!=after.get(k)),'stdout':name+'.stdout','stderr':name+'.stderr'});verify(case);save()
record.update(status='baseline_requirements_failed' if any(c.get('exit',0)!=0 or c.get('old_reader_accepted',False) or (c.get('case')=='old_reader_fence' and not c.get('byte_identity_preserved',False)) for c in record['cases']) else 'all_requirements_passed',finished_unix=time.time(),originals_unchanged=True,old_acknowledged_rows=sum(1 for _ in (old/'ack-ledger.jsonl').open()),scope='Automatic SQL default admission: unchanged original6old482row/policy/boundrequirements plus2realoldreaderrefusal/byteidentitycases, bothstores,14ACKoriginalshashesuntouched. IndependentofexplicitAPI/focused/native/serverstartup/broad/application/HAqualification.');save();print(json.dumps(record))

raise SystemExit(0 if record['status']=='all_requirements_passed' else 1)
