import pathlib,json,subprocess,time,hashlib,shutil,sys
root=pathlib.Path('/root/bicdb-durability-s3');label='partition-admission-trigger-fix-probes';unit='bicdb-'+label
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
build=json.loads((root/('report-'+label+'.json')).read_text());assert build['status']=='focused_passed';binary=pathlib.Path(build['binary']);assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==build['binary_sha256']
old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());out=root/'partition-explicit-admission-trigger-fix-08cbbfc0';out.mkdir(exist_ok=False)
record={'status':'running','source':build['source'],'binary':str(binary),'binary_sha256':build['binary_sha256'],'started_unix':time.time(),'cases':[]}
def save():(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
def hashes(path):return {str(p.relative_to(path)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in path.rglob('*') if p.is_file() and not p.is_symlink()}
def verify(c):
 source=pathlib.Path(c['directory'])/'db';assert hashes(source)=={name:info['sha256'] for name,info in c['files'].items()},str(source)
for mode in ['embedded_memory','server_paged']:
 for case,original in [('apply_parent_heap','parent_heap'),('apply_bad_child_bound','bad_child_bound'),('apply_policy_keep_stored','independent_child_policy'),('apply_policy_preserve_historical','independent_child_policy')]:
  c=next(c for c in fixtures['cases'] if c['mode']==mode and c['name']==original);verify(c);name=mode+'-'+case;dest=out/name;shutil.copytree(pathlib.Path(c['directory'])/'db',dest)
  r=subprocess.run([str(binary),mode,case,str(dest)],text=True,capture_output=True);(out/(name+'.stdout')).write_text(r.stdout);(out/(name+'.stderr')).write_text(r.stderr)
  item={'mode':mode,'case':case,'exit':r.returncode,'stdout':name+'.stdout','stderr':name+'.stderr'}
  if r.returncode==0:
   before=hashes(dest);reader=subprocess.run([fixtures['binary'],'sql',str(dest),'SELECT k,payload FROM p ORDER BY k','--json'],text=True,capture_output=True);after=hashes(dest);(out/(name+'.old-reader.stderr')).write_text(reader.stderr);(out/(name+'.old-reader.stdout')).write_text(reader.stdout)
   item.update(old_reader_exit=reader.returncode,old_reader_refused=reader.returncode!=0,byte_identity_preserved=before==after,changed_files=sorted(k for k in set(before)|set(after) if before.get(k)!=after.get(k)))
  verify(c);record['cases'].append(item);save()
passed=all(c['exit']==0 and c.get('old_reader_refused') and c.get('byte_identity_preserved') for c in record['cases'])
record.update(status='explicit_apply_passed' if passed else 'explicit_apply_failed',finished_unix=time.time(),originals_unchanged=True,old_acknowledged_rows=14,scope='ExplicitoperatorAPIonly:8genuineold482fixturecopies, bothmodes,parentACKmigration+upsert/indexchecks, reviewedbadchildrelocation,bothreviewedpolicychoices+rawarchives,idempotentplanreceiptswithoutnewcommitsimmediatelyandafter2reopens,actualold482readerrefusal+byteidentity. Automaticentry/startup code nowimplemented; itsqualification is recordedseparately. Fullfeature/broad/application/HAqualification remainsopen.');save();print(json.dumps(record));sys.exit(0 if passed else 1)
