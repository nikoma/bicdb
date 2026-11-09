#!/usr/bin/env python3
"""Audit all allowlisted timing records; no outliers or timing thresholds."""
from pathlib import Path
import hashlib,json,math,statistics
ROOT=Path(__file__).resolve().parent

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def output(path,key):
 for line in path.read_text().splitlines():
  try:r=json.loads(line)
  except json.JSONDecodeError:continue
  if key in r:return r
 raise AssertionError(path)
def main():
 for name,h in json.loads((ROOT/'manifest.json').read_text()).items():
  path=ROOT/name;assert path.resolve().is_relative_to(ROOT) and sha(path)==h,name
 config=json.loads((ROOT/'configuration.json').read_text());builds=json.loads((ROOT/'artifact-manifests.json').read_text());summary=json.loads((ROOT/'summary.json').read_text());assert config['mode']=='all-logs' and config['storage_mode']=='server_paged' and config['fsync'] and not config['sync_outbox'];assert config['pool_bytes']==268435456 and config['repeats']==3 and config['seconds_per_run']==15;assert sha(ROOT/'runner.py')==config['runner_sha256']
 for side in ['main','candidate']:
  assert builds[side]['source']==config[side+'_source']
  assert builds[side]['benchmark_source_sha256']==config['example_sha256']
  assert builds[side]['experimental_feature'] is None
 rows=[];order=0
 for w in [1,16,64]:
  for t in range(3):
   for side in (['main','candidate'] if t%2==0 else ['candidate','main']):
    name=f'w{w}-t{t}-{side}';r=json.loads((ROOT/'raw'/(name+'.json')).read_text());assert r['order']==order;order+=1;assert r['side']==side and r['writers']==w and r['trial']==t and r['exit']==-6;assert r['persisted_format']['storage_mode']=='server_paged';assert r['bench']['commits']>0
    actual=output(ROOT/'raw'/(name+'.stdout'),'commits')
    for key,value in actual.items():assert r['bench'][key]==value,(name,key)
    assert len(r['recoveries'])==2
    for n,rec in enumerate(r['recoveries'],1):
     assert rec['exit']==0 and rec['bench']['rows']==r['bench']['commits']+w
     assert output(ROOT/'raw'/(name+f'-recover{n}.stdout'),'recovery_open_ms')==rec['bench']
    assert all(s['memory_available_kib']>=10*1024*1024 for s in r['samples'])
    rows.append(r)
 for g in summary['groups']:
  w=g['writers'];ratios=[]
  for side in ['main','candidate']:
   selected=[r for r in rows if r['writers']==w and r['side']==side]
   tps=[r['bench']['commits_per_second'] for r in selected]
   # Order is alternating, so compare membership for descriptive values.
   expected=g['sides'][side]['transactions_per_second'];assert sorted(tps)==sorted(expected['values']);assert statistics.median(tps)==expected['median'] and min(tps)==expected['min'] and max(tps)==expected['max']
  for t in range(3):
   p={r['side']:r['bench']['commits_per_second'] for r in rows if r['writers']==w and r['trial']==t};ratios.append(p['candidate']/p['main'])
  assert ratios==g['paired_throughput_ratios'];assert math.isclose(math.exp(sum(map(math.log,ratios))/3),g['paired_ratio_geometric_mean'])
 assert summary['runs']==len(rows)==18 and summary['successful_recoveries']==36 and summary['complete']
 print(json.dumps({'complete':True,'timing_runs':len(rows),'row_count_recoveries':36,'sources':summary['source'],'interpretation':summary['interpretation']}))
if __name__=='__main__':main()
