#!/usr/bin/env python3
"""Audit external ledgers, deterministic expected data hashes, and cut reports."""
from pathlib import Path
import gzip,hashlib,json
ROOT=Path(__file__).resolve().parent

def lines(run,name):
 p=run/(name+'.jsonl.gz')
 return [json.loads(line) for line in gzip.decompress(p.read_bytes()).decode().splitlines()] if p.exists() else []
def digest(value):return hashlib.sha256(value).hexdigest()
def main():
 manifest=json.loads((ROOT/'runs/manifest.json').read_text())
 for name,h in manifest.items():
  p=ROOT/'runs'/name;assert p.resolve().is_relative_to((ROOT/'runs').resolve())
  assert digest(p.read_bytes())==h,name
 run=ROOT/'runs/large-tx-campaign-1794202882515055041'
 result=json.loads((run/'result.json').read_text());config=json.loads((run/'config.json').read_text())
 assert result['complete'] and result['hard_vm_kills']==7
 assert result['row_payload_bytes_per_tx']>result['pool_bytes']
 assert result['blob_bytes']>result['pool_bytes']
 native={v['generation']:v['native_transaction_id'] for v in lines(run,'native-ids')}
 ack={v['generation'] for v in lines(run,'acks')};attempts={v['generation'] for v in lines(run,'commit-attempts')}
 assert ack==set(result['acknowledged_generations'])=={0}
 assert attempts==set(result['attempted_generations'])=={0,1,2,3}
 # All four observed IDs happen to be distinct in this run. Absent/uncommitted
 # allocations are not promised a durable reservation across a crash.
 assert len(native)==len(set(native.values()))
 recoveries=lines(run,'recoveries');durable=set()
 for v in recoveries:
  generations=set(v['generations'])
  if v['label']=='nonempty-raw-image-restore':assert generations=={0}
  else:assert ack<=generations and durable<=generations;durable.update(generations)
  expected=[]
  for g in sorted(generations):
   for i in range(8192):
    text=(f'g{g:02}-i{i:05}-'+'abcdefghijklmnopqrstuvwxyz'*20)[:384]
    expected.append([str(g*8192+i),str(g),str(i%4),text])
  assert len(expected)==v['rows']
  assert digest(json.dumps(expected,separators=(',',':')).encode())==v['rows_sha256']
  blob=(f'blob-generation-{max(generations)}-'+'0123456789abcdef'*((result['blob_bytes']//16)+1))[:result['blob_bytes']]
  assert digest(blob.encode())==v['blob_sha256']
 assert durable=={0,2,3}
 assert {'repeat-0','repeat-1','nonempty-raw-image-restore'}<={v['label'] for v in recoveries}
 pools=lines(run,'pool')
 for v in pools:
  p=v['report']['paged']['buffer_pool'];assert p['budget_bytes']==result['pool_bytes'] and p['unlogged_steals']==0
 before=next(v['report']['paged']['buffer_pool'] for v in pools if v['label']=='before-large-tx')
 after=next(v['report']['paged']['buffer_pool'] for v in pools if v['label']=='after-large-tx')
 assert after['evictions']-before['evictions']==result['actual_evictions_delta']==14845
 cuts=json.loads((run/'independent-verifier/manifest.json').read_text());assert len(cuts['cuts'])==7
 for cut in cuts['cuts']:
  assert cut['exit_code']==0 and cut['extracted_files_unchanged'] and cut['original_size_mtime_unchanged']
  p=run/'independent-verifier'/(cut['cut']+'.json');assert digest(p.read_bytes())==cut['report_sha256']
  report=json.loads(p.read_text());assert report['complete'] and report['verdict']=='clean'
 failed=json.loads((ROOT/'runs/large-tx-campaign-1794202766372217291/result.json').read_text())
 assert not failed['complete'] and 'native txid reused' in failed['error']
 print(json.dumps({'complete':True,'source':config['BICDB_VM_SOURCE'],'acknowledged_rows':8192,'recovered_rows':24576,'independent_clean_cuts':7,'actual_evictions':14845}))
if __name__=='__main__':main()
