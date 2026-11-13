import pathlib,json,subprocess,hashlib,gzip,base64
root=pathlib.Path('/root/bicdb-partition-runtime/docs/evidence/partition-execution-2026-11-12/legacy-upgrade')
root.mkdir(exist_ok=True)
remote_script=r'''import pathlib,json,base64
root=pathlib.Path('/root/bicdb-durability-s3')
directories=['partition-old-store-fixtures-482-20261112','partition-old-store-baseline-1f6a25ed','partition-upgrade-inspection-08f42204','partition-upgrade-apply-first-eccbe714','partition-upgrade-api-e45a5118','partition-upgrade-resume-f5ee9eec','partition-upgrade-resume-fixture-fixed']
files=[]
for name in directories:
 p=root/name
 if not p.exists():continue
 for f in sorted(p.iterdir()):
  if f.is_file() and (f.suffix in ['.json','.jsonl','.stdout','.stderr']):files.append(f)
for label in ['partition-reader-fence-b715e546','partition-reader-fence-retry-3b0289b2','partition-upgrade-inspector-build','partition-upgrade-inspector-bound-build','partition-upgrade-apply-first-build','partition-upgrade-api-preflight-build-e45a5118','partition-upgrade-resume-build-f5ee9eec','partition-upgrade-resume-fixture-fixed']:
 for f in [root/('report-'+label+'.json'),root/('tc-step-'+label+'.log')]:
  if f.exists():files.append(f)
for name in ['partition-upgrade-resume-f5ee9eec.log','partition-upgrade-resume-fixture-fixed-runtime.log']:
 f=root/name
 if f.exists():files.append(f)
print(json.dumps({str(f.relative_to(root)):base64.b64encode(f.read_bytes()).decode() for f in files}))
'''
data=json.loads(subprocess.check_output(['ssh','root@74.50.69.58','python3','-'],input=remote_script.encode()))
manifest={}
for name,encoded in data.items():
 raw=base64.b64decode(encoded);rel=pathlib.PurePosixPath(name)
 assert not rel.is_absolute() and '..' not in rel.parts
 p=root/rel;p.parent.mkdir(parents=True,exist_ok=True)
 if p.suffix in ['.log','.stdout','.stderr']:
  p=p.with_name(p.name+'.gz');p.write_bytes(gzip.compress(raw,mtime=0));assert gzip.decompress(p.read_bytes())==raw
 else:p.write_bytes(raw)
 manifest[name]={'stored_as':str(p.relative_to(root)),'raw_bytes':len(raw),'raw_sha256':hashlib.sha256(raw).hexdigest()}
(root/'raw-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({'files':len(manifest),'raw_bytes':sum(v['raw_bytes'] for v in manifest.values()),'stored_bytes':sum((root/v['stored_as']).stat().st_size for v in manifest.values())}))
