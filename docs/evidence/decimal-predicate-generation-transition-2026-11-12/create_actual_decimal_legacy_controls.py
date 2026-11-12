import pathlib,json,subprocess,hashlib,time
base=pathlib.Path('/root/bicdb-durability-s3');proof='5e0fa30b60ef8640e161b41ec2d882df61cc9d1d';receipt=json.loads((base/'report-decimal-legacy-fixture-creator-5e0fa30b.json').read_text());binary=pathlib.Path(receipt['binary']);assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==receipt['binary_sha256']=='bc25d0ca9a63e2fb6ed394d259f9bf591ef00aeee79ab00264cdffe4d680837f'
out=base/'diagnostics/actual-decimal-legacy-controls-20261112';out.mkdir(exist_ok=False);report={'status':'running','creator_source':proof,'production_core':'f43583da4c34209e810c5e6a8f5925831b2949a1','binary_sha256':receipt['binary_sha256'],'started_unix':time.time(),'cases':{}}
def save():(out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
save()
for case in ['embedded','duplicate','multiple']:
 root=out/(case+'-db');r=subprocess.run([str(binary),str(root),case],capture_output=True,text=True);(out/(case+'.stdout')).write_text(r.stdout);(out/(case+'.stderr')).write_text(r.stderr);assert r.returncode==0,r.stderr[-3000:];result=json.loads(r.stdout.strip().splitlines()[-1]);assert result['status']=='actual_old_store_closed';assert all(x['valid'] for x in result['verification']);files={}
 for p in sorted(root.rglob('*')):
  if not p.is_file():continue
  if p.relative_to(root).as_posix()=='mesh_signing_key.json':continue
  files[p.relative_to(root).as_posix()]={'bytes':p.stat().st_size,'sha256':hashlib.file_digest(p.open('rb'),'sha256').hexdigest()}
 fixture={'kind':'actual unmodified Native state created and closed by old Core','creator_source':proof,'production_core':report['production_core'],'binary_sha256':receipt['binary_sha256'],'case':case,'excluded_unrelated_secret':'mesh_signing_key.json; original retained only in private closed S3 fixture','observation':result,'files':files};(out/(case+'-FIXTURE.json')).write_text(json.dumps(fixture,indent=2)+'\n');report['cases'][case]={'database':str(root),'fixture':str(out/(case+'-FIXTURE.json')),'observation':result};save()
report.update(status='actual_old_controls_created_and_closed',finished_unix=time.time());save();print(json.dumps(report))
