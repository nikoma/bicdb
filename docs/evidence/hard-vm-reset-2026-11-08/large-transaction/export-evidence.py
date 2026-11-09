from pathlib import Path
import gzip,hashlib,json,tarfile
BASE=Path('/root/bicdb-durability-s3/vm-reset');OUT=BASE/'portable-large-vm-evidence';OUT.mkdir(exist_ok=False)
for name in ['large-tx-campaign-1794202766372217291','large-tx-campaign-1794202882515055041']:
 src=BASE/name;dest=OUT/name;dest.mkdir()
 for filename in ['config.json','harness.py','result.json']:
  (dest/filename).write_bytes((src/filename).read_bytes())
 for filename in ['intents.jsonl','native-ids.jsonl','acks.jsonl','commit-attempts.jsonl','client-errors.jsonl','recoveries.jsonl','reset-schedule.jsonl','pool.jsonl']:
  p=src/filename
  if p.exists():(dest/(filename+'.gz')).write_bytes(gzip.compress(p.read_bytes(),mtime=0))
 if (src/'independent-verifier').exists():
  verify=dest/'independent-verifier';verify.mkdir()
  for p in (src/'independent-verifier').glob('*.json'):(verify/p.name).write_bytes(p.read_bytes())
  ids={int(v['generation']):str(v['native_transaction_id']) for v in map(json.loads,(src/'native-ids.jsonl').read_text().splitlines())}
  seen=set();observed=set()
  for line in (src/'recoveries.jsonl').read_text().splitlines():
   v=json.loads(line)
   if v['label']=='nonempty-raw-image-restore':continue
   current=set(v['generations']);assert observed<=current
   for g in current-observed:
    assert ids[g] not in seen,(g,ids,seen)
    seen.add(ids[g])
   observed.update(current)
  (dest/'durable-native-id-audit.json').write_text(json.dumps({'complete':True,'scope':'Acknowledged and uncertain transactions proven committed by recovered rows and receipts; aborted/absent uncertain allocations may reuse IDs.','committed_generations':sorted(observed),'committed_native_ids':{g:ids[g] for g in sorted(observed)},'no_reuse':True},indent=2)+'\n')
manifest={str(p.relative_to(OUT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(OUT.rglob('*')) if p.is_file()}
(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
with tarfile.open(BASE/'portable-large-vm-evidence.tar.gz','w:gz') as tar:
 for p in sorted(OUT.rglob('*')):
  if p.is_file():tar.add(p,arcname=str(p.relative_to(OUT)),recursive=False)
print(len(manifest),'portablefiles')
