#!/usr/bin/env python3
"""Offline byte/semantic checks; no engine, network or runtime authority."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parent
SOURCE = '3ee40c19b44affa0a481a66b278ded3af817e910'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
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
        require(len(data) == item['bytes'] and sha(data) == item['sha256'],
                'byte mismatch: ' + str(path))
    receipt = json.loads((ROOT / 'receipt.json').read_text())
    require(receipt['source_revision'] == SOURCE, 'source changed')
    require(receipt['status'] == 'CLI_COHORT_FAILED_RECEIVER_CASE_PASSED', 'status')
    require((receipt['cli']['passed'], receipt['cli']['failed']) == (4, 1), 'CLI count')
    require((receipt['core']['passed'], receipt['core']['failed']) == (3, 0), 'Core count')
    require(receipt['ledger']['json_null'] == 'NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN',
            'null outcome overclaim')
    require(receipt['ledger']['receiver_after_fifth'] ==
            'QUERY_TIMEOUT_UNAVAILABLE_NOT_HEALTHY_CONTINUITY', 'postjoin overclaim')
    require(not receipt['custody']['full_database_or_page_wal_retained'] and
            not receipt['custody']['range_write_catalogue_retained'], 'custody overclaim')
    logs = {}
    for kind, job in [('core', 131337645351), ('cli', 131337870637)]:
        index = json.loads((ROOT / f'raw/3ee-registered-receiver-{kind}-job{job}-index.json').read_text())
        require(index['source_revision'] == SOURCE and index['authoritative_source_verified'],
                'capture source differs')
        compressed = (ROOT / 'raw' / index['compressed_file']).read_bytes()
        require(sha(compressed) == index['compressed_sha256'], 'compressed log mismatch')
        data = gzip.decompress(compressed)
        require(len(data) == index['raw_bytes'] and sha(data) == index['raw_sha256'] ==
                receipt[kind]['raw_sha256'], 'raw log mismatch')
        logs[kind] = data.decode()
    require('3 passed; 0 failed;' in logs['core'], 'Core runtime summary missing')
    require('4 passed; 1 failed;' in logs['cli'], 'CLI failure summary missing')
    ledger_lines = [line.split('NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ', 1)[1]
                    for line in logs['cli'].splitlines()
                    if 'NATIVE_INITIAL_DATA_EXTERNAL_LEDGER ' in line]
    require('\n'.join(ledger_lines) + '\n' ==
            (ROOT / 'observations/ledger-from-cli-stdout.jsonl').read_text(), 'ledger splice')
    records = [json.loads(line) for line in ledger_lines]
    require(len(records) == 28, 'ledger count')
    require(records[0]['execution_identity'] == receipt['executable'], 'binary identity')
    require(receipt['executable']['source'] == SOURCE, 'runtime source absent')
    split = next(i for i, row in enumerate(records) if row['stage'] == 'initial_four')
    require(split == 15 and len(records[split:]) == 13, 'case ledger boundaries')
    saved = (ROOT / 'diagnostics/files/initial-data-external-ledger.jsonl').read_text()
    require([json.loads(row) for row in saved.splitlines()] == records[:split], 'saved ledger differs')
    replies = [row for row in records[:split] if row['stage'] == 'reply']
    require([row['acknowledged'] for row in replies] == [False, True, False], 'ACK classification')
    require(replies[-1]['key'] == 'external-json-null' and replies[-1]['value'] is None,
            'null failure changed')
    receiver = records[split:]
    require(receiver[-1]['stage'] == 'replica_receiver_prerequisite_complete' and
            not receiver[-1]['fifth_data_serving'], 'receiver overclaim')
    require(receiver[-2]['stage'] == 'strong_after_fifth_retryable_unavailable',
            'healthy postjoin claim')
    original = (ROOT / 'diagnostics/manifest.json').read_bytes()
    require(sha(original) == receipt['custody']['manifest_sha256'], 'original manifest')
    custody = json.loads(original)
    require(custody['source_revision'] == SOURCE and custody['test'] ==
            receipt['cli']['failed_test'], 'custody case/source')
    require(len(custody['files']) == 26 and custody['total_bytes'] == 58126,
            'custody count')
    require(all(row['wait_ok'] and row['before_kill'] == {'exited': False}
                for row in custody['actual_children_waited']), 'child exit classification')
    for item in custody['files']:
        data = (ROOT / 'diagnostics' / item['path']).read_bytes()
        require(len(data) == item['bytes'] and sha(data) == item['sha256'], 'original file parity')
    if args.repo:
        for item in receipt['source_files']:
            data = subprocess.check_output(['git', '-C', str(args.repo), 'show',
                                            SOURCE + ':' + item['repository_path']])
            require(sha(data) == item['sha256'] and data ==
                    (ROOT / item['bundle_path']).read_bytes(), 'exact source excerpt mismatch')
    if args.private_zip:
        with args.private_zip.open('rb') as stream:
            require(hashlib.file_digest(stream, 'sha256').hexdigest() ==
                    receipt['custody']['zip_sha256'], 'private ZIP hash')
        require(args.private_zip.stat().st_size == receipt['custody']['zip_bytes'], 'private ZIP size')
        with zipfile.ZipFile(args.private_zip) as archive:
            name = next(name for name in archive.namelist() if name.endswith('/manifest.json'))
            require(archive.read(name) == original, 'private manifest parity')
            base = name.rsplit('/', 1)[0] + '/'
            for item in custody['files']:
                require(archive.read(base + item['path']) ==
                        (ROOT / 'diagnostics' / item['path']).read_bytes(), 'private file parity')
            src = receipt['custody']['archived_tracked_source']
            with archive.open(src['artifact_path']) as stream:
                require(hashlib.file_digest(stream, 'sha256').hexdigest() == src['sha256'],
                        'tracked source archive mismatch')
    print('PASS: preserved CLI 4/1, Core 3/0; null outcome unknown; receiver-only prerequisite.')


if __name__ == '__main__':
    main()
