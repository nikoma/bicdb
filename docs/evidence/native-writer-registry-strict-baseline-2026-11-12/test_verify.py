#!/usr/bin/env python3
"""Rehashed semantic tampering must still fail, never an engine test."""
import gzip,contextlib,hashlib,importlib.util,io,json,pathlib,shutil,tempfile
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('strict_verifier',HERE/'verify.py');v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def save(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def seal(p):save(p/'manifest.json',{str(f.relative_to(p)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(p.rglob('*')) if f.is_file() and f.name!='manifest.json' and '__pycache__' not in f.parts})
def classification(p):
 r=json.loads((p/'receipt.json').read_text());r['cli_failed']=0;save(p/'receipt.json',r)
def response(p):
 r=json.loads((p/'observations/strict-wire.json').read_text());next(iter(r.values()))['response_frames'][0]['response']={'Ok':{'invented':'healthy'}};save(p/'observations/strict-wire.json',r)
def payload(p):
 f=p/'catalogues/db0.json';r=json.loads(f.read_text());r['catalog']['ranges']['4']['entries']['1']['command']['mutations'][0]['record']['metadata']={'wrong':'payload'}
 r['checksum_sha256']=hashlib.sha256(json.dumps(r['catalog'],separators=(',',':')).encode()).hexdigest();save(f,r)
 c=json.loads((p/'observations/catalogue-classification.json').read_text());c['catalogues'][0]['sha256']=hashlib.sha256(f.read_bytes()).hexdigest();save(p/'observations/catalogue-classification.json',c)
def binding(p):
 f=p/'observations/external-ledger.jsonl';r=[json.loads(x) for x in f.read_text().splitlines()]
 a=next(x for x in r if x.get('stage')=='readonly_retained_writer_binding' and x['phase']=='after_health_gate');a['reply']['host']['initial_data']['writer_binding_diagnostic']['persisted_policy_registration_generation']='fake replacement'
 f.write_text(''.join(json.dumps(x)+'\n' for x in r))
def error_snapshot(p):
 f=p/'journals/strict-registry.jsonl.gz';rows=[json.loads(x) for x in gzip.decompress(f.read_bytes()).splitlines()]
 row=next(x for x in rows if x['kind']=='actual_rpc_phase' and x['logical_context']['request'].get('phase')=='strict_after_fifth' and x['actual']['operation']=='metadata_snapshot_discovery' and x['actual']['phase']=='response_frame_before_decode')
 body=json.loads(bytes(row['actual']['raw_frame'])[4:]);body['response']={'Err':{'code':'failure','message':'not a successful snapshot'}};encoded=json.dumps(body,separators=(',',':')).encode();row['actual']['raw_frame']=list(len(encoded).to_bytes(4,'big')+encoded)
 raw=b''.join((json.dumps(x,separators=(',',':'))+'\n').encode() for x in rows);f.write_bytes(gzip.compress(raw,mtime=0))
 r=json.loads((p/'receipt.json').read_text());r['journals'][2]['raw_sha256']=hashlib.sha256(raw).hexdigest();r['journals'][2]['raw_bytes']=len(raw);save(p/'receipt.json',r)
for mutation in [classification,response,payload,binding,error_snapshot]:
 with tempfile.TemporaryDirectory(prefix='strict-registry-tamper-') as td:
  p=pathlib.Path(td)/'evidence';shutil.copytree(HERE,p,ignore=shutil.ignore_patterns('__pycache__'));mutation(p);seal(p)
  try:
   with contextlib.redirect_stdout(io.StringIO()):v.verify(p)
  except (ValueError,KeyError,AssertionError):print('PASS rejected '+mutation.__name__)
  else:raise AssertionError('accepted semantic tamper: '+mutation.__name__)
