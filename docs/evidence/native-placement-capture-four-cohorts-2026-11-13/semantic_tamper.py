#!/usr/bin/env python3
"""Portable semantic negatives: update altered hashes, retain verification rules."""
import copy,gzip,json,pathlib,shutil,tempfile
import verify

def seal(root):
 files=[{'path':str(p.relative_to(root)),'bytes':p.stat().st_size,'sha256':verify.sha(p.read_bytes())} for p in sorted(root.rglob('*')) if p.is_file() and p.name!='MANIFEST.json' and '__pycache__' not in p.parts]
 (root/'MANIFEST.json').write_text(json.dumps({'version':1,'files':files},indent=2)+'\n')
def raw_change(root,tag,change):
 d=root/tag;b=gzip.decompress((d/'core.log.gz').read_bytes()).decode();out=[]
 for line in b.splitlines(keepends=True):
  if 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in line:
   i=line.index('{');t=json.loads(line[i:]);change(t);line=line[:i]+json.dumps(t,separators=(',',':'))+'\n'
  out.append(line)
 raw=''.join(out).encode();(d/'core.log.gz').write_bytes(gzip.compress(raw,mtime=0))
 jobs=verify.load(d/'jobs.json')
 for j in jobs:
  if j['label']=='core':j['raw_sha256']=verify.sha(raw);j['raw_bytes']=len(raw)
 (d/'jobs.json').write_text(json.dumps(jobs,indent=2)+'\n')
def edit_json(root,path,change):
 p=root/path;v=verify.load(p);change(v);p.write_text(json.dumps(v,indent=2)+'\n')
def run():
 mutations=[
 ('drop-actual-source-ACK',lambda r:raw_change(r,'751',lambda t:t['ack_ledger'].pop())),
 ('null-payload-as-absence',lambda r:raw_change(r,'751',lambda t:t['ack_ledger'][0]['value']['nested'].__setitem__(0,'absent'))),
 ('retroactive-80a-success',lambda r:raw_change(r,'80a',lambda t:t.__setitem__('required_baseline_succeeded',True))),
 ('wrong-80a-refusal',lambda r:raw_change(r,'80a',lambda t:[x.__setitem__('actual_error','native writer absent') for x in t['reports'][:2] if not x['actual_reply_ok']])),
 ('invented-target-native-zero',lambda r:raw_change(r,'5de',lambda t:t['reports'][0]['after']['points']['1'].__setitem__('native_applied_index',0))),
 ('lost-original-role',lambda r:raw_change(r,'5de',lambda t:t['reports'][0].__setitem__('original_role_remaining_ms',0))),
 ('planner-retires-initiator',lambda r:raw_change(r,'80a',lambda t:[m.__setitem__('source','n2') for m in (t.get('actual_automatic_plan') or {}).get('replica_moves',[])])),
 ('forge-serving-permission',lambda r:raw_change(r,'5de',lambda t:t['reports'][0].__setitem__('serving_granted',True))),
 ('wrong-executed-source',lambda r:edit_json(r,pathlib.Path('212/workflow.json'),lambda v:v.__setitem__('head_sha',verify.EXPECTED['751']['source']))),
 ('false-custody-replay',lambda r:edit_json(r,pathlib.Path('5de/custody-receipt.json'),lambda v:v.__setitem__('engine_opened_or_replayed',True))),
 ('false-whole-workflow-pass',lambda r:edit_json(r,pathlib.Path('80a/workflow.json'),lambda v:v.__setitem__('conclusion','success'))),
 ]
 for name,mutation in mutations:
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp)/'packet';shutil.copytree(verify.ROOT,root,ignore=shutil.ignore_patterns('__pycache__'))
   mutation(root);seal(root)
   try:verify.verify(root)
   except (AssertionError,KeyError,StopIteration,ValueError,TypeError):print('PASS reject',name)
   else:raise AssertionError('accepted semantic alteration: '+name)
if __name__=='__main__':run()
