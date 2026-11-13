import pathlib,json,subprocess,time,hashlib,shutil
root=pathlib.Path('/root/bicdb-durability-s3');label='partition-upgrade-inspector-build';unit='bicdb-'+label
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
build=json.loads((root/('report-'+label+'.json')).read_text());assert build['status']=='focused_passed',build
binary=pathlib.Path(build['binary']);assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==build['binary_sha256']
old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());out=root/'partition-upgrade-inspection-08f42204';out.mkdir(exist_ok=False)
record={'source':build['source'],'binary':str(binary),'binary_sha256':build['binary_sha256'],'status':'running','started_unix':time.time(),'cases':[]}
def verify(c):
 source=pathlib.Path(c['directory'])/'db'
 assert {str(p.relative_to(source)) for p in source.rglob('*') if p.is_file() and not p.is_symlink()}==set(c['files'])
 for name,info in c['files'].items():
  p=source/name;assert p.stat().st_size==info['bytes'] and hashlib.file_digest(p.open('rb'),'sha256').hexdigest()==info['sha256'],str(p)
for c in fixtures['cases']:
 verify(c);name=c['mode']+'-'+c['name'];dest=out/name;shutil.copytree(pathlib.Path(c['directory'])/'db',dest)
 r=subprocess.run([str(binary),c['mode'],'inspect',str(dest)],text=True,capture_output=True);(out/(name+'.stdout')).write_text(r.stdout);(out/(name+'.stderr')).write_text(r.stderr);assert r.returncode==0,(name,r.stderr)
 reports=[json.loads(line) for line in r.stdout.splitlines() if line.startswith('{')];assert len(reports)==3 and reports[0]==reports[1]==reports[2],(name,reports)
 report=reports[0];assert report['legacy_relations']==['high','low','p'],report
 if c['name']=='parent_heap':assert report['parent_heap_rows']==2 and report['acknowledged_rows_examined']==4 and not report['invalid_rows'] and not report['policy_decisions_required'],report
 elif c['name']=='bad_child_bound':
  assert report['acknowledged_rows_examined']==1 and len(report['invalid_rows'])==1 and not report['policy_decisions_required'],report
  assert report['invalid_rows'][0]['source']=='low' and report['invalid_rows'][0]['suggested_destination']=='high',report
 else:
  assert report['acknowledged_rows_examined']==2 and not report['invalid_rows'] and len(report['policy_decisions_required'])==2,report
  high=next(p for p in report['policy_decisions_required'] if p['relation']=='high');assert any(p['name']=='authored_child' for p in high['stored']['policies']) and all(p['name']!='authored_child' for p in high['historical_effective']['policies']),report
 record['cases'].append({'mode':c['mode'],'case':c['name'],'inspection':report,'same_after_two_reopens':True});verify(c);(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
record.update(status='inspection_passed',finished_unix=time.time(),originals_unchanged=True,scope='Sixactualold482fixtures/copies; stablefingerprintsacross2reopens; realparentheapcounts/boundviolationdestination/independentrawpolicyconflicts. This does not implement or qualify applying the upgrade.');(out/'result.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps({'status':record['status'],'cases':len(record['cases']),'source':record['source']}))
