#!/usr/bin/env python3
"""Rehashed semantic negative controls, separate from file corruption."""
import gzip,hashlib,json,pathlib,shutil,tempfile
import verify
from derive_profile import derive
ROOT=pathlib.Path(__file__).resolve().parent

def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def rehash(p):
    m=json.loads((p/'MANIFEST.json').read_text())
    for x in m['files']:
        b=(p/x['path']).read_bytes();x.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
    save(p/'MANIFEST.json',m)
def result(p,fn):
    r=json.loads((p/'results.json').read_text());fn(r);save(p/'results.json',r)
def profile(p):
    r=json.loads(gzip.decompress((p/'profile.json.gz').read_bytes()));r['groups'][0]['events'][0]['effective_left_us']+=1
    (p/'profile.json.gz').write_bytes(gzip.compress(json.dumps(r).encode(),mtime=0))
def missing_event(p):
    raw=gzip.decompress((p/'core.log.gz').read_bytes());lines=raw.decode().splitlines(keepends=True)
    i=next(i for i,l in enumerate(lines) if 'NATIVE_FENCE_TRACE ' in l and 'stage=record-counted ' in l)
    del lines[i];raw=''.join(lines).encode();compressed=gzip.compress(raw,mtime=0);(p/'core.log.gz').write_bytes(compressed)
    idx=json.loads((p/'core-index.json').read_text());idx.update(raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest(),gzip_bytes=len(compressed),gzip_sha256=hashlib.sha256(compressed).hexdigest());save(p/'core-index.json',idx)
    # Recompute the entire derived report too; rejection must be semantic.
    (p/'profile.json.gz').write_bytes(gzip.compress(json.dumps(derive(raw)).encode(),mtime=0))
def run():
    verify.verify(ROOT)
    cases=[('workflow-washed-green',lambda p:result(p,lambda r:r.update(workflow_conclusion='success'))),
        ('altered-Core-count',lambda p:result(p,lambda r:r['core'].update(fail=1))),
        ('invented-slack',profile),('missing-record-event-with-rederived-profile',missing_event),
        ('old-cause-declared-fixed',lambda p:result(p,lambda r:r.update(old_cause_resolved=True))),
        ('invented-native-custody',lambda p:result(p,lambda r:r['artifact'].update(native_database_custody_retained=True)))]
    for name,fn in cases:
        with tempfile.TemporaryDirectory(prefix='fence-evidence-semantic-') as d:
            p=pathlib.Path(d)/'packet';shutil.copytree(ROOT,p,ignore=shutil.ignore_patterns('__pycache__'));fn(p);rehash(p)
            for x in json.loads((p/'MANIFEST.json').read_text())['files']:
                b=(p/x['path']).read_bytes();assert len(b)==x['bytes'] and hashlib.sha256(b).hexdigest()==x['sha256']
            try:verify.verify(p)
            except AssertionError:print('PASS SEMANTIC',name)
            else:raise AssertionError('accepted semantic mutation: '+name)
    print('PASS6 rehashed semantic controls; no engine/network')
if __name__=='__main__':run()
