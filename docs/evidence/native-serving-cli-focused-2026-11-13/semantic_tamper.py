#!/usr/bin/env python3
"""Portable negative controls. Semantic cases repair all affected byte hashes and derived summaries."""
import copy
import gzip
import hashlib
import json
import pathlib
import shutil
import tempfile
import verify

ROOT=pathlib.Path(__file__).resolve().parent


def save(path,value):
    path.write_text(json.dumps(value,indent=2)+'\n')


def rehash(root):
    manifest=json.loads((root/'MANIFEST.json').read_text())
    for item in manifest['files']:
        data=(root/item['path']).read_bytes();item['bytes']=len(data);item['sha256']=verify.sha(data)
    save(root/'MANIFEST.json',manifest)


def change_journal(root,index,mutate):
    result=json.loads((root/'results.json').read_text());item=result['cli']['journals'][index]
    rows=[json.loads(x) for x in gzip.decompress((root/item['file']).read_bytes()).splitlines()]
    mutate(rows)
    counters={}
    for sequence,row in enumerate(rows,1):
        row['sink_sequence']=sequence
        if row['kind']=='actual_rpc_phase':
            observer=row['observer_instance_id'];counters[observer]=counters.get(observer,0)+1
            row['actual']['journal_sequence']=counters[observer]
    raw=b''.join((json.dumps(x,separators=(',',':'))+'\n').encode() for x in rows)
    (root/item['file']).write_bytes(gzip.compress(raw,mtime=0))
    item.update(raw_bytes=len(raw),raw_sha256=verify.sha(raw),records=len(rows));save(root/'results.json',result)
    summaries=json.loads((root/'cli-journal-summary.json').read_text());summaries[index]=verify.projection(rows,item)
    save(root/'cli-journal-summary.json',summaries)


def unknown_id(rows):
    return next(x['diagnostic_operation_id'] for x in rows if x['kind']=='logical_public_reply' and x['outcome']['classification']=='NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN')


def drop_unknown(rows):
    ident=unknown_id(rows)
    rows[:]=[x for x in rows if x.get('diagnostic_operation_id')!=ident and x.get('logical_context',{}).get('diagnostic_operation_id')!=ident]


def retro_ack(rows):
    ident=unknown_id(rows)
    next(x for x in rows if x['kind']=='logical_public_reply' and x['diagnostic_operation_id']==ident)['outcome']={'classification':'USER_ACK','inserted':True}


def null_as_absence(rows):
    ident=next(x['diagnostic_operation_id'] for x in rows if x['kind']=='logical_request_intent' and x['request'].get('key')=='external-json-null' and x['request']['public_operation']=='get_strong')
    next(x for x in rows if x['kind']=='logical_public_reply' and x['diagnostic_operation_id']==ident)['outcome']={'classification':'STRONG_SUCCESS','presence':'absent'}
    for row in rows:
        if row['kind']=='actual_rpc_phase' and row['logical_context']['diagnostic_operation_id']==ident and row['actual']['phase']=='response_frame_before_decode':
            envelope=verify.frame(row['actual']['raw_frame']);envelope['response']['Ok']['NativeEventGet']['result']['value']={'presence':'absent'}
            data=json.dumps(envelope,separators=(',',':')).encode();row['actual']['raw_frame']=list(len(data).to_bytes(4,'big')+data)


def missing_attempt(rows):
    attempt=next(x['actual']['attempt_id'] for x in rows if x['kind']=='actual_rpc_phase' and x['actual']['operation']=='metadata_snapshot_discovery')
    rows[:]=[x for x in rows if x.get('actual',{}).get('attempt_id')!=attempt]


def boolean_as_number(rows):
    ident=next(x['diagnostic_operation_id'] for x in rows if x['kind']=='logical_request_intent' and x['request'].get('key')=='external-first-ack' and x['request']['public_operation']=='get_strong')
    reply=next(x for x in rows if x['kind']=='logical_public_reply' and x['diagnostic_operation_id']==ident)
    reply['outcome']['value']['full']['array'][1]=1
    for row in rows:
        if row['kind']=='actual_rpc_phase' and row['logical_context']['diagnostic_operation_id']==ident and row['actual']['phase']=='response_frame_before_decode':
            envelope=verify.frame(row['actual']['raw_frame']);envelope['response']['Ok']['NativeEventGet']['result']['value']['value']['full']['array'][1]=1
            data=json.dumps(envelope,separators=(',',':')).encode();row['actual']['raw_frame']=list(len(data).to_bytes(4,'big')+data)


def wrong_route(rows):
    row=next(x for x in rows if x['kind']=='actual_rpc_phase' and x['actual']['phase']=='request_frame_before_dispatch')
    row['actual']['address']='127.0.0.1:1'


def wrong_response_id(rows):
    row=next(x for x in rows if x['kind']=='actual_rpc_phase' and x['actual']['phase']=='response_frame_before_decode')
    envelope=verify.frame(row['actual']['raw_frame']);envelope['request_id']+=1
    data=json.dumps(envelope,separators=(',',':')).encode();row['actual']['raw_frame']=list(len(data).to_bytes(4,'big')+data)


def truncated_error(rows):
    row=next(x for x in rows if x['kind']=='actual_rpc_phase' and x['actual']['phase']=='validated_transport_result_error')
    row['actual']['error_chain']=['generic unavailable']


def wrong_source(root):
    path=root/'cli-core.log.gz';raw=gzip.decompress(path.read_bytes())
    # Change the ACTUAL standalone checkout line, not an unrelated mention in a command.
    lines=raw.decode().splitlines(keepends=True);found=0
    for i,line in enumerate(lines):
        if verify.plain_lines(line)==[verify.CLI_SOURCE]:
            lines[i]=line.replace(verify.CLI_SOURCE,'0'*40);found+=1
    assert found==1
    raw=''.join(lines).encode();path.write_bytes(gzip.compress(raw,mtime=0))
    idx=json.loads((root/'cli-core-index.json').read_text());idx.update(raw_bytes=len(raw),raw_sha256=verify.sha(raw),gzip_bytes=len(path.read_bytes()),gzip_sha256=verify.sha(path.read_bytes()));save(root/'cli-core-index.json',idx)


def false_qualification(root):
    r=json.loads((root/'results.json').read_text());r['qualified']=True;save(root/'results.json',r)


def wrong_executed_name(root):
    r=json.loads((root/'results.json').read_text());r['cli']['executed_tests'][0]='invented::passing_test';save(root/'results.json',r)


def run():
    verify.verify(ROOT)
    cases=[
        ('dropped-unknown',lambda p:change_journal(p,0,drop_unknown)),
        ('retroactive-ACK',lambda p:change_journal(p,0,retro_ack)),
        ('null-as-absence-even-matching-wire',lambda p:change_journal(p,0,null_as_absence)),
        ('missing-whole-attempt',lambda p:change_journal(p,0,missing_attempt)),
        ('wrong-executed-source-line',wrong_source),
        ('false-qualification',false_qualification),
        ('full-value-boolean-as-number',lambda p:change_journal(p,0,boolean_as_number)),
        ('wrong-actual-route',lambda p:change_journal(p,0,wrong_route)),
        ('wrong-response-request-ID',lambda p:change_journal(p,0,wrong_response_id)),
        ('truncated-original-error',lambda p:change_journal(p,0,truncated_error)),
        ('invented-executed-test-name',wrong_executed_name),
    ]
    for name,mutate in cases:
        with tempfile.TemporaryDirectory(prefix='native-semantic-negative-') as temp:
            p=pathlib.Path(temp)/'packet';shutil.copytree(ROOT,p,ignore=shutil.ignore_patterns('__pycache__'))
            mutate(p);rehash(p)
            # Byte validation must PASS; a semantic assertion must reject.
            for item in json.loads((p/'MANIFEST.json').read_text())['files']:
                data=(p/item['path']).read_bytes();assert len(data)==item['bytes'] and verify.sha(data)==item['sha256']
            try:verify.verify(p)
            except AssertionError as error:print('PASS SEMANTIC',name,':',str(error) or 'raw result/provenance mismatch')
            else:raise AssertionError('semantic tamper accepted: '+name)
    print('PASS11 semantic controls; all packet/derived hashes recomputed; separate from byte-corruption tests')


if __name__=='__main__':run()
