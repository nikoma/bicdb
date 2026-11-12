#!/usr/bin/env python3
"""Offline byte/frame/correlation checks only. Never engine/proof constructors."""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile
ROOT = Path(__file__).resolve().parent
SOURCE = 'ca7d481467c40cf6ac324ceb8c0c7e943d4d1cfc'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path)
    parser.add_argument('--private-zip', type=Path)
    args = parser.parse_args()
    manifest = json.loads((ROOT / 'manifest.json').read_text())
    for item in manifest['files']:
        path = Path(item['path'])
        require(not path.is_absolute() and '..' not in path.parts, 'unsafe path')
        data = (ROOT / path).read_bytes()
        require(len(data) == item['bytes'] and sha(data) == item['sha256'], 'byte mismatch: ' + str(path))
    receipt = json.loads((ROOT / 'receipt.json').read_text())
    require(receipt['source_revision'] == SOURCE, 'wrong source')
    require((receipt['cli']['passed'], receipt['cli']['failed']) == (5, 0), 'CLI counts')
    require((receipt['observer_controls']['passed'], receipt['observer_controls']['failed']) == (9, 0), 'controls counts')
    require(receipt['postjoin']['classification'] == 'OBSERVED_WRITER_ADMISSION_UNAVAILABLE_NOT_HEALTHY_CONTINUITY', 'continuity overclaim')
    require(not receipt['postjoin']['postjoin_write_attempted'] and
            not receipt['artifact']['full_database_or_wal_or_range_catalogue_retained'], 'write/custody overclaim')
    cli_raw = None
    for index_path in (ROOT / 'raw').glob('*-index.json'):
        index = json.loads(index_path.read_text())
        require(index['source_revision'] == SOURCE and index['authoritative_source_verified'], 'capture source')
        compressed = (ROOT / 'raw' / index['compressed_file']).read_bytes()
        raw = gzip.decompress(compressed)
        require(sha(compressed) == index['compressed_sha256'] and len(raw) == index['raw_bytes'] and
                sha(raw) == index['raw_sha256'], 'raw log mismatch')
        if index['job_id'] == receipt['cli']['job']:
            cli_raw = raw.decode()
    require(cli_raw is not None, 'CLI raw missing')
    ledger = [json.loads(line.split('NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ', 1)[1])
              for line in cli_raw.splitlines() if 'NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ' in line]
    require(ledger == [json.loads(line) for line in (ROOT / 'observations/external-ledger-from-stdout.jsonl').read_text().splitlines()], 'external ledger splice')
    require(len(ledger) == 34 and all(row['execution_identity'] == receipt['executable']
            for row in ledger if 'execution_identity' in row), 'external ledger identity')
    require(receipt['postjoin']['typed_unavailable_responses'] == 8 and
            receipt['postjoin']['successful_bootstrap_snapshots'] == 34 and
            receipt['postjoin']['transport_error_records'] == 0, 'summary count overclaim')
    receiver = None
    journal_bytes = {}
    for info in receipt['journals']:
        compressed = (ROOT / info['bundle_path']).read_bytes()
        raw = gzip.decompress(compressed)
        require(sha(compressed) == info['compressed_sha256'] and sha(raw) == info['sha256'] and len(raw) == info['bytes'], 'journal bytes')
        journal_bytes[info['path']] = raw
        records = [json.loads(line) for line in raw.splitlines()]
        require([r['sink_sequence'] for r in records] == list(range(1, len(records) + 1)), 'sink gap')
        require(records[0]['source'] == SOURCE and records[-1]['kind'] == 'fixture_case_finished', 'journal boundary')
        require(records[0]['bootstrap_raw_frame_covered'] is False, 'bootstrap raw overclaim')
        require(not any(r['kind'] == 'logical_call_interrupted' for r in records), 'interrupted trace')
        intents = {r['diagnostic_operation_id'] for r in records if r['kind'] == 'logical_request_intent'}
        replies = {r['diagnostic_operation_id'] for r in records if r['kind'] == 'logical_public_reply'}
        require(intents == replies, 'unpaired logical call')
        sequences = defaultdict(list)
        for row in records:
            if row['kind'] != 'actual_rpc_phase':
                continue
            require(row['logical_context']['diagnostic_operation_id'] in intents, 'unattributed phase')
            actual = row['actual']
            sequences[row['observer_instance_id']].append(actual['journal_sequence'])
            if actual['raw_frame'] is not None:
                frame = bytes(actual['raw_frame'])
                require(int.from_bytes(frame[:4], 'big') == len(frame) - 4, 'frame length')
                json.loads(frame[4:])
        require(all(sorted(ids) == list(range(1, len(ids) + 1)) for ids in sequences.values()), 'observer gap')
        if 'replica_receiver_tests' in records[0]['test']:
            receiver = records
    require(receiver is not None, 'receiver missing')
    logical = next(r for r in receiver if r['kind'] == 'logical_request_intent' and r['request'].get('phase') == 'same_gateway_after_fifth')
    phases = [r for r in receiver if r['kind'] == 'actual_rpc_phase' and
              r['logical_context']['diagnostic_operation_id'] == logical['diagnostic_operation_id']]
    typed, snapshots = [], []
    for row in phases:
        actual = row['actual']
        require(not actual['error_chain'], 'postjoin transport error exists')
        if actual['phase'] != 'response_frame_before_decode':
            continue
        envelope = json.loads(bytes(actual['raw_frame'])[4:])
        body = envelope['response'].get('Ok', {})
        if 'NativeEventGet' in body:
            typed.append((row, envelope, body['NativeEventGet']))
        if 'BootstrapSnapshot' in body:
            snapshots.append((envelope['source_node_id'], body['BootstrapSnapshot']['topology']))
    require(len(typed) == 8 and len(snapshots) == 34, 'observed response counts')
    require(Counter(node for node, _ in snapshots) == Counter(n0=7, n1=7, n2=7, n3=7, n4=6), 'snapshot sources')
    require(len({r['business_request_id'] for _, _, r in typed}) == 1 and
            len({r['challenge'] for _, _, r in typed}) == 8, 'business/challenge binding')
    topo = json.loads((ROOT / 'observations/actual-topology-before-after.json').read_text())
    require(topo['before']['ranges'] == topo['after']['ranges'], 'data assignments changed')
    require(set(topo['before']['nodes']) == {'n0', 'n1', 'n2', 'n3'} and
            set(topo['after']['nodes']) == {'n0', 'n1', 'n2', 'n3', 'n4'}, 'addition changed')
    for node in topo['before']['nodes']:
        require(topo['before']['nodes'][node]['native_store_birth_v1'] == topo['after']['nodes'][node]['native_store_birth_v1'], 'old birth changed')
    for _, envelope, reply in typed:
        result = reply['result']
        require(envelope['source_node_id'] == 'n2' and result['kind'] == 'unavailable' and
                result['code'] == {'code': 'native_read', 'stage': 'WriterAdmission'} and
                result['detail'] == receipt['postjoin']['detail'] and
                result['route'] == {'generation': 4, 'range_id': 2, 'range_epoch': 1, 'leader': 'n2'}, 'typed refusal differs')
        birth = topo['after']['nodes']['n2']['native_store_birth_v1']
        require(all(birth.get(k) == v for k, v in envelope['actual_native_source'].items()), 'actual native source birth differs')
    require(all(t['generation'] == 4 and t['ranges'] == topo['after']['ranges'] for _, t in snapshots), 'snapshot domain')
    focused = json.loads((ROOT / 'observations/postjoin-typed-responses.json').read_text())
    require([r['actual_envelope'] for r in focused['typed_responses']] == [e for _, e, _ in typed], 'focused envelope splice')
    if args.repo:
        for item in receipt['source_files']:
            raw = subprocess.check_output(['git', '-C', str(args.repo), 'show', SOURCE + ':' + item['repository_path']])
            require(sha(raw) == item['sha256'] and raw == (ROOT / item['bundle_path']).read_bytes(), 'source excerpt')
    if args.private_zip:
        with args.private_zip.open('rb') as stream:
            require(hashlib.file_digest(stream, 'sha256').hexdigest() == receipt['artifact']['zip_sha256'], 'ZIP hash')
        require(args.private_zip.stat().st_size == receipt['artifact']['zip_bytes'], 'ZIP size')
        with zipfile.ZipFile(args.private_zip) as archive:
            require(archive.read('source-revision.txt').decode().strip() == SOURCE, 'ZIP source')
            for name, raw in journal_bytes.items():
                require(archive.read(name) == raw, 'original journal parity')
    print('PASS: ca7 5/0 + controls 9/0; 8 writer refusals +34 reachable snapshots; continuity remains OPEN.')


if __name__ == '__main__':
    main()
