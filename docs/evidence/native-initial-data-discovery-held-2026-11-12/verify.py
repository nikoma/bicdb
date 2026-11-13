#!/usr/bin/env python3
"""Read-only evidence/source reconstruction. Never opens a BicDB database."""
import argparse
import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import zipfile

SOURCE = '05979463ef312fc1128a42903f908de0189c2a63'
BASE = 'b9394418bdf9d87864c2c12d6fe81bb7d4039d53'
RAW_SHA = '752e11ffbf51c5e332d520029211750eec35769ed13c1f5e43ae597c6563d6e8'
ZIP_SHA = '92835f330a6f30206ed723bf913ce892fac8d658c1b0bafb2bca4d45b65b7b91'
BUSY = 'native bootstrap service busy'
REFUSAL = 'initial native DATA refuses generic snapshot/import/repair/cleanup and unsupported operations'
ROOT = Path(__file__).resolve().parent

class Invalid(ValueError):
    pass

def need(condition, message):
    if not condition:
        raise Invalid(message)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def load(path):
    return json.loads(Path(path).read_text())

def lines(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]

def envelope(record):
    data = bytes(record['raw_frame'])
    need(len(data) >= 4 and int.from_bytes(data[:4], 'big') == len(data) - 4,
         'incomplete or changed length-framed RPC')
    return json.loads(data[4:])

def payloads(root=ROOT):
    audits = list((root/'artifact/fault-state').glob('*/audit.jsonl'))
    rpcs = list((root/'artifact/fault-state').glob('*/rpc.jsonl'))
    need(len(audits) == len(rpcs) == 1, 'expected one exact observed fixture')
    return lines(audits[0]), lines(rpcs[0])

def semantics(audit, records):
    phases = {r['phase']: r for r in audit}
    need(phases['begin']['source_sha'] == SOURCE, 'source changed')
    need(phases['begin']['members'] == phases['begin']['rf'] == 3, 'fixture cohort changed')
    need(phases['held_metadata_acquisition_result']['ok'] is True and
         phases['held_post_metadata_acquisition_result']['ok'] is True,
         'real held-MQ prerequisite did not succeed')
    first = phases['held_metadata_qq']
    second = phases['held_metadata_qq_after_refresh']
    need(first['all_actual_service_latches_held'] == 3, 'all actual services were not held')
    need(first['context'] != second['context'] and first['business'] != second['business'],
         'distinct actual HostMQ requests/contexts required')
    for mq in [first, second]:
        need(mq['metadata_leader'] == 'n0' and mq['term'] == mq['read_index'] == 1,
             'retained metadata point changed')
    topology = first['committed_topology']
    need(topology['generation'] == 1 and len(topology['nodes']) == 3,
         'actual committed topology changed')
    refresh = phases['held_public_refresh']
    need(refresh['ok'] is False and 'no event cluster member is reachable' in
         ' '.join(refresh['error_chain']), 'original refresh failure missing')
    held = records[refresh['rpc_record_start']:refresh['rpc_record_end']]
    requests = [r for r in held if r['phase'] == 'request_frame_before_dispatch']
    need(len(requests) == 6, 'exact six held requests required')
    seen = set()
    for request in requests:
        sent = envelope(request)
        dest = request['destination']
        need((dest, sent['request']) not in seen, 'duplicate held route')
        seen.add((dest, sent['request']))
        need(request['address'] == topology['nodes'][dest]['address'] and
             request['topology_generation'] == 1 and
             sent['destination_node_id'] == dest and sent['caller_node_id'] == 'n0' and
             sent['protocol_version'] == 28 and sent['request_id'] == request['request_id'],
             'actual dispatched route/frame identity changed')
        responses = [r for r in held if r['attempt_id'] == request['attempt_id'] and
                     r['phase'] == 'response_frame_before_decode']
        need(len(responses) == 1, 'exact full response missing')
        reply = envelope(responses[0])
        need(reply['source_node_id'] == dest and reply['request_id'] == sent['request_id'] and
             reply['cluster_id'] == sent['cluster_id'] and reply['protocol_version'] == 28 and
             reply['actual_native_source'] == topology['nodes'][dest]['native_store_birth_v1'],
             'actual returned identity/birth changed')
        error = reply['response']['Err']
        need(error['code'] == 'cluster_data_operation_failed', 'original RPC error code changed')
        expected = BUSY if sent['request'] == 'FetchBootstrapSnapshot' else REFUSAL
        need(expected in error['message'], 'exact bootstrap/fallback refusal missing')
    need(seen == {(n, op) for n in ['n0', 'n1', 'n2']
                  for op in ['FetchBootstrapSnapshot', 'FetchTopology']}, 'missing member/fallback')
    healthy = phases['healthy_after_release']
    need(healthy['business_acks'] == 2 and healthy['both_exact_strong_values_verified'] is True and
         healthy['topology_unchanged'] is True and healthy['before'] == healthy['after'],
         'release/retained-value/topology prerequisite changed')
    need(all(s['receiver_listening'] and s['writer_policy_current'] and s['dispatcher_published']
             for s in healthy['after']), 'healthy actual owners were not observed')
    need(phases['acceptance']['owners_stopped'] is True and
         phases['acceptance']['held_discovery_should_succeed'] is False,
         'final positive RED predicate not reached after stop')
    app = []
    strong = []
    for request in [r for r in records if r['phase'] == 'request_frame_before_dispatch']:
        reply = next((r for r in records if r['attempt_id'] == request['attempt_id'] and
                      r['phase'] == 'response_frame_before_decode'), None)
        if reply is None:
            continue
        sent, returned = envelope(request), envelope(reply)
        if request['operation'] == 'event_append':
            need(returned['response']['Ok']['ApplicationEvent'] ==
                 {'result': 'appended', 'inserted': True}, 'actual APP ACK missing')
            app.append((request['destination'], sent['request']['ApplicationEvent']))
        elif request['operation'] == 'native_event_get':
            strong.append((request['destination'], sent['request']['NativeEventGet'],
                           returned['response']['Ok']['NativeEventGet']))
    need(len(app) == 2 and [r[0] for r in app] == ['n0', 'n1'], 'two actual cross-leader ACKs required')
    need([r[1]['range_id'] for r in app] == [3, 1], 'actual selected ranges changed')
    need(app[1][1]['operation']['value'] is None, 'original JSON-null payload changed')
    need(len(strong) == 3, 'all original Strong replies required')
    first_value = phases['business_ack1']['value']
    for i, (node, request, response) in enumerate(strong):
        need(request['business_request_id'] == response['business_request_id'] and
             request['challenge'] == response['challenge'], 'Strong request echo changed')
        result = response['result']
        expected = None if i == 1 else first_value
        need(result['kind'] == 'value' and result['value'] ==
             {'presence': 'present', 'value': expected}, 'present null/value became absence')
        point = result['audit']
        need(point['range_index'] == 1 and result['route']['leader'] == node and
             point['target_local_visibility_db_sequence'] == (2 if i == 1 else 1),
             'actual R/local B/route changed')
    need(strong[0][2]['result']['audit']['logical_history_sha256'] ==
         strong[2][2]['result']['audit']['logical_history_sha256'], 'original ACK history changed')

def integrity(root=ROOT):
    manifest = load(root/'MANIFEST.json')
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()
              and p.name != 'MANIFEST.json' and '__pycache__' not in p.parts}
    need(actual == set(manifest['files']), 'package files missing or unexpectedly added')
    for name, expected in manifest['files'].items():
        data = (root/name).read_bytes()
        need(len(data) == expected['bytes'] and sha(data) == expected['sha256'], 'file changed: '+name)
    need(sha((root/'raw/059-artifact13971398486.zip').read_bytes()) == ZIP_SHA, 'original ZIP changed')
    with zipfile.ZipFile(root/'raw/059-artifact13971398486.zip') as archive:
        names = {i.filename for i in archive.infolist() if not i.is_dir()}
        unpacked = {p.relative_to(root/'artifact').as_posix() for p in (root/'artifact').rglob('*') if p.is_file()}
        need(names == unpacked and len(names) == 6, 'not every original artifact file retained')
        for name in names:
            need(archive.read(name) == (root/'artifact'/name).read_bytes(), 'extraction changed: '+name)
    log = gzip.decompress((root/'raw/059-core-job.log.gz').read_bytes())
    need(sha(log) == RAW_SHA and b'0 passed; 1 failed; 0 ignored;' in log and
         b'finished in 5.07s' in log and b'discovery_latch_baseline.rs:326:5' in log,
         'exact executed final failure changed')
    need((root/'artifact/source-revision.txt').read_text().strip() == SOURCE, 'artifact source mismatch')
    summary = load(root/'observations.json')
    need(summary['run_id'] == 43278532377 and summary['job_id'] == 131393398627 and
         summary['source'] == SOURCE and summary['native_database_or_tls_roots_archived'] is False,
         'source/job/custody scope changed')
    audit, records = payloads(root)
    semantics(audit, records)
    return len(manifest['files'])

def reconstruct(repo, root=ROOT):
    description = load(root/'source-files.json')
    need(description['public_base'] == BASE and description['tested_source'] == SOURCE,
         'reconstruction source/base changed')
    def git(*args, **kwargs):
        return subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True, **kwargs).stdout
    with tempfile.TemporaryDirectory(prefix='bicdb-discovery-index-') as directory:
        env = dict(os.environ, GIT_INDEX_FILE=str(Path(directory)/'index'))
        git('read-tree', BASE, env=env)
        patch = gzip.decompress((root/'source.patch.gz').read_bytes())
        need(sha(patch) == description['patch_raw_sha256'], 'original source patch changed')
        git('apply', '--cached', '--binary', '-', env=env, input=patch)
        actual = {}
        for row in git('ls-files', '--stage', '-z', '--', *description['scope_roots'], env=env).split(b'\0'):
            if not row:
                continue
            meta, path = row.split(b'\t', 1)
            mode, oid, stage = meta.decode().split()
            need(stage == '0', 'unmerged reconstructed index')
            actual[path.decode()] = (mode, oid)
        need(set(actual) == {x['path'] for x in description['files']}, 'reconstructed file set changed')
        rows = description['files']
        blobs = git('cat-file', '--batch', input=('\n'.join(actual[r['path']][1] for r in rows)+'\n').encode())
        stream = io.BytesIO(blobs)
        for row in rows:
            mode, oid = actual[row['path']]
            header = stream.readline().decode().split()
            need(len(header) == 3 and header[:2] == [oid, 'blob'], 'git blob response changed')
            data = stream.read(int(header[2]))
            need(stream.read(1) == b'\n' and mode == row['mode'] and oid == row['git_blob'] and
                 len(data) == row['bytes'] and sha(data) == row['sha256'],
                 'reconstructed source changed: '+row['path'])
    return len(description['files'])

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path)
    args = parser.parse_args()
    count = integrity()
    rebuilt = reconstruct(args.repo) if args.repo else None
    print(json.dumps({'package_files': count, 'semantic_witness': 'PASS_EXPECTED_RED',
                      'reconstructed_source_files': rebuilt, 'source': SOURCE,
                      'scope': 'same-process actual TLS component discovery; no DB opens or new tests'}))

if __name__ == '__main__':
    main()
