#!/usr/bin/env python3
"""Rehash changed packets before semantic checks; no engine, network or build."""
import gzip,json,pathlib,shutil,tempfile
import verify
ROOT=pathlib.Path(__file__).resolve().parent
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def rehash(p):
    m=json.loads((p/'MANIFEST.json').read_bytes())
    for x in m['files']:
        b=(p/x['path']).read_bytes();x.update(bytes=len(b),sha256=verify.sha(b))
    save(p/'MANIFEST.json',m)
def edit_terminal(p,fn):
    raw=gzip.decompress((p/'core.log.gz').read_bytes());lines=raw.decode().splitlines(keepends=True)
    stored=json.loads(gzip.decompress((p/'derived.json.gz').read_bytes()))
    done=False
    for i,l in enumerate(lines):
        if not done and 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in l:
            t=json.loads(l[l.index('{'):]);old=json.loads(json.dumps(t));fn(t)
            next(f for f in stored['fixtures'] if f['terminal']==old)['terminal']=t
            lines[i]=l[:l.index('{')]+json.dumps(t,separators=(',',':'))+'\n';done=True
    assert done
    raw=''.join(lines).encode();b=gzip.compress(raw,mtime=0);(p/'core.log.gz').write_bytes(b)
    idx=json.loads((p/'core-index.json').read_bytes());idx.update(raw_bytes=len(raw),raw_sha256=verify.sha(raw),compressed_sha256=verify.sha(b));save(p/'core-index.json',idx)
    (p/'derived.json.gz').write_bytes(gzip.compress(json.dumps(stored).encode(),mtime=0))
def missing_ack(p):edit_terminal(p,lambda t:t['ack_ledger'].pop())
def null_lost(p):edit_terminal(p,lambda t:t['ack_ledger'][0]['actual_full_record']['metadata']['nested'].__setitem__(0,'absent'))
def invented_zero(p):edit_terminal(p,lambda t:t['reports'][0]['actual_success_observation']['native'].__setitem__('native_applied_index',0))
def row_grant(p):edit_terminal(p,lambda t:t['reports'][0].__setitem__('rows_copied',True))
def false_afterstate(p):
    x=json.loads(gzip.decompress((p/'controls.json.gz').read_bytes()));next(c for c in x if c['case']=='Foreign')['control_result']={'invented_afterstate':True};(p/'controls.json.gz').write_bytes(gzip.compress(json.dumps(x).encode(),mtime=0))
def wrong_source(p):
    b=(p/'core.log.gz').read_bytes();raw=gzip.decompress(b);lines=raw.decode().splitlines(keepends=True)
    i=next(i for i,l in enumerate(lines) if '[command]/usr/bin/git log -1 --format=%H' in l)
    lines[i+1]=lines[i+1].replace(verify.SOURCE,'0'*40)
    raw=''.join(lines).encode();b=gzip.compress(raw,mtime=0);(p/'core.log.gz').write_bytes(b)
    x=json.loads((p/'core-index.json').read_bytes());x.update(raw_bytes=len(raw),raw_sha256=verify.sha(raw),compressed_sha256=verify.sha(b));save(p/'core-index.json',x)
def false_qualification(p):
    x=json.loads((p/'results.json').read_bytes());x['physical_qualification']=True;save(p/'results.json',x)
def missing_custody(p):
    x=json.loads((p/'custody-receipt.json').read_bytes());x['fixtures'].pop();save(p/'custody-receipt.json',x)
def run():
    verify.verify(ROOT)
    cases=[('missing-original-ACK',missing_ack),('null-as-absence',null_lost),('UNKNOWN-to-native-zero',invented_zero),('invented-row-transfer',row_grant),('fabricated-foreign-afterstate',false_afterstate),('wrong-executed-checkout',wrong_source),('false-physical-qualification',false_qualification),('missing-custody-fixture',missing_custody)]
    for name,fn in cases:
        with tempfile.TemporaryDirectory(prefix='da5-semantic-') as d:
            p=pathlib.Path(d)/'packet';shutil.copytree(ROOT,p,ignore=shutil.ignore_patterns('__pycache__'));fn(p);rehash(p)
            for x in json.loads((p/'MANIFEST.json').read_bytes())['files']:
                b=(p/x['path']).read_bytes();assert len(b)==x['bytes'] and verify.sha(b)==x['sha256']
            try:verify.verify(p)
            except AssertionError:print('PASS SEMANTIC',name)
            else:raise AssertionError('semantic mutation accepted: '+name)
    print('PASS8 rehashed semantic controls; distinct from corrupted-byte failures')
if __name__=='__main__':run()
