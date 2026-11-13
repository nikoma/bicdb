#!/usr/bin/env python3
"""Offline byte and bounded actual-result checks; no engine or network."""
import gzip, hashlib, json, pathlib, re
root=pathlib.Path(__file__).resolve().parent
manifest=json.loads((root/'MANIFEST.json').read_text())
for item in manifest['files']:
 p=root/item['path'];data=p.read_bytes()
 assert len(data)==item['bytes'] and hashlib.sha256(data).hexdigest()==item['sha256'],item['path']
r=json.loads((root/'results.json').read_text())
assert (r['serving']['passed'],r['serving']['failed'],r['cli']['passed'],r['cli']['failed'])==(9,0,6,0)
assert r['serving']['source']=='ca8383ef36190c6c69cc8853cd51dd7803c7cc52'
assert r['cli']['source']=='5cec7687196fe6b113805485672e8730c8295a56'
for label in ['serving-core','serving-compile','cli-core','cli-compile']:
 raw=gzip.decompress((root/(label+'.log.gz')).read_bytes())
 idx=json.loads((root/(label+'-index.json')).read_text())
 assert len(raw)==idx['raw_bytes'] and hashlib.sha256(raw).hexdigest()==idx['raw_sha256']
 assert idx['job_conclusion']=='success'
 if label.endswith('-core'):
  text=raw.decode();expected='9 passed; 0 failed;' if label.startswith('serving') else '6 passed; 0 failed;'
  assert 'test result: ok. '+expected in text
  if label.startswith('serving'):
   assert 'ACTUAL_SERVING_SCHEMA_REFUSAL reached=true exact_schema_error=true parent_live_at_semantic_refusal=true' in text
   assert len(r['serving']['tests'])==len(set(r['serving']['tests']))==9
summaries=json.loads((root/'cli-journal-summary.json').read_text())
for item,summary in zip(r['cli']['journals'],summaries,strict=True):
 raw=gzip.decompress((root/item['file']).read_bytes());rows=[json.loads(l) for l in raw.splitlines()]
 assert len(raw)==item['raw_bytes'] and hashlib.sha256(raw).hexdigest()==item['raw_sha256']
 assert len(rows)==item['records'] and [x['sink_sequence'] for x in rows]==list(range(1,len(rows)+1))
 assert rows[0]['source']==r['cli']['source'] and rows[-1]['kind']=='fixture_case_finished'
 assert not rows[-1]['trace_fenced']
 actual=[x['actual'] for x in rows if x['kind']=='actual_rpc_phase']
 assert len(set(x['attempt_id'] for x in actual))==summary['attempt_count']
 assert sum(x['raw_frame'] is not None for x in actual)==summary['wire_frames']
 assert sum(x.get('outcome',{}).get('classification')=='USER_ACK' for x in rows)==[3,1,2][summaries.index(summary)]
assert [s['attempt_count'] for s in summaries]==[127,6,8]
assert [s['wire_frames'] for s in summaries]==[253,12,16]
logical=summaries[0]['logical']
assert any(x['classification']=='NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN' for x in logical)
assert any(x['key']=='external-json-null' and x['reply'].get('presence')=='present' and 'value' in x['reply'] and x['reply']['value'] is None for x in logical)
assert any(x['key']=='genuine-unwritten-key' and x['reply'].get('presence')=='absent' for x in logical)
assert r['prior_failed_history']['fourcase_fail']==1 and r['prior_failed_history']['fourcase_pass']==3
assert not r['qualified'] and not r['deployed'] and r['fifth_data_move']=='NOT_ATTEMPTED'
assert not r['source_engine_opened_or_replayed_for_receipt']
print('PASS manifest bytes, exact9/6 source-specific results, three complete actual journals, null/absence and failed-gate distinctions')
