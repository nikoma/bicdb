#!/usr/bin/env python3
"""Repair packet/log/derived hashes before semantic rejection; no engine/network."""
import gzip,hashlib,json,pathlib,shutil,tempfile
import verify
ROOT=pathlib.Path(__file__).resolve().parent

def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def rehash(p):
    m=json.loads((p/'MANIFEST.json').read_text())
    for x in m['files']:
        b=(p/x['path']).read_bytes();x.update(bytes=len(b),sha256=verify.sha(b))
    save(p/'MANIFEST.json',m)
def change_terminal(p,fn):
    compressed=(p/'core.log.gz').read_bytes();raw=gzip.decompress(compressed);actual=verify.terminal(raw);fn(actual)
    lines=raw.decode().splitlines(keepends=True)
    for i,line in enumerate(lines):
        if 'ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE ' in line:
            lines[i]=line[:line.index('{')]+json.dumps(actual,separators=(',',':'))+'\n'
    raw=''.join(lines).encode();compressed=gzip.compress(raw,mtime=0);(p/'core.log.gz').write_bytes(compressed)
    idx=json.loads((p/'core-index.json').read_text());idx.update(raw_bytes=len(raw),raw_sha256=verify.sha(raw),compressed_sha256=verify.sha(compressed));save(p/'core-index.json',idx)
    save(p/'ack-ledger.json',actual['ack_ledger']);save(p/'fence-ledger.json',actual['reports'])
    r=json.loads((p/'results.json').read_text());r['actual_required_fence_successes']=sum(x['actual_reply_ok'] for x in actual['reports']);save(p/'results.json',r)
def false_fence(actual):
    actual['reports'][0]['actual_reply_ok']=True
    actual['reports'][0]['actual_error']=None
    actual['reports'][0]['actual_success_observation']={'invented':True}
def zero_floor(actual):
    for report in actual['reports']:
        for state in ['before','after']:
            for point in report[state]['points'].values():point['native_applied_index']=0
def bad_ack(actual):
    ack=actual['ack_ledger'][0];ack['value']['nested'][2]['full']='wrong payload';ack['actual_full_record']['metadata']=ack['value']
    ack['payload_json_sha256']=verify.sha(json.dumps(ack['value'],sort_keys=True,separators=(',',':')).encode())
def expired_role(actual):actual['reports'][0]['original_role_remaining_ms']=0
def foreign_scope(actual):actual['reports'][0]['scope']['initiator_node_id']='n2'
def missing_ack(actual):actual['ack_ledger'].pop()
def timeout_error(actual):actual['reports'][0]['actual_error']='canceling statement due to query timeout'
def replay(p):
    r=json.loads((p/'results.json').read_text());r['engine_opened_or_replayed']=True;save(p/'results.json',r)
def run():
    verify.verify(ROOT)
    cases=[('false-fence-success',lambda p:change_terminal(p,false_fence)),('UNKNOWN-made-native-zero',lambda p:change_terminal(p,zero_floor)),
        ('full-ACK-payload-altered-with-newhash',lambda p:change_terminal(p,bad_ack)),('live-role-rewritten-expired',lambda p:change_terminal(p,expired_role)),
        ('foreign-initiator-scope',lambda p:change_terminal(p,foreign_scope)),('missing-original-ACK',lambda p:change_terminal(p,missing_ack)),
        ('NoWriter-relabelled-timeout',lambda p:change_terminal(p,timeout_error)),('invented-engine-replay',replay)]
    for name,fn in cases:
        with tempfile.TemporaryDirectory(prefix='fifth-failure-semantic-') as d:
            p=pathlib.Path(d)/'packet';shutil.copytree(ROOT,p,ignore=shutil.ignore_patterns('__pycache__'));fn(p);rehash(p)
            for x in json.loads((p/'MANIFEST.json').read_text())['files']:
                b=(p/x['path']).read_bytes();assert len(b)==x['bytes'] and verify.sha(b)==x['sha256']
            try:verify.verify(p)
            except AssertionError:print('PASS SEMANTIC',name)
            else:raise AssertionError('semantic mutation accepted: '+name)
    print('PASS8 rehashed semantic controls (distinct from byte corruption)')
if __name__=='__main__':run()
