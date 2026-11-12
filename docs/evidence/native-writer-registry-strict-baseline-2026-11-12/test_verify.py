#!/usr/bin/env python3
"""Rehashed semantic tampering must still fail, never an engine test."""
import contextlib,hashlib,importlib.util,io,json,pathlib,shutil,tempfile
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
for mutation in [classification,response,payload,binding]:
 with tempfile.TemporaryDirectory(prefix='strict-registry-tamper-') as td:
  p=pathlib.Path(td)/'evidence';shutil.copytree(HERE,p,ignore=shutil.ignore_patterns('__pycache__'));mutation(p);seal(p)
  try:
   with contextlib.redirect_stdout(io.StringIO()):v.verify(p)
  except (ValueError,KeyError,AssertionError):print('PASS rejected '+mutation.__name__)
  else:raise AssertionError('accepted semantic tamper: '+mutation.__name__)
