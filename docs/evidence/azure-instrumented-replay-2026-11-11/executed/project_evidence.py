#!/usr/bin/env python3
"""Project complete private event traces into bounded, public review artifacts.

Every input journal record is retained. Payload bytes and raw response bodies are
represented by hashes; all operation/RPC timings and complete transformed errors
remain. This does not produce an equivalent raw wire capture or HA certificate.
"""
import argparse
import collections
import gzip
import hashlib
import importlib.util
import json
import pathlib


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def project(value, replacements):
    if isinstance(value, str):
        if '-----BEGIN ' in value and 'PRIVATE KEY-----' in value:
            raise ValueError('refusing to project private key material')
        for source, target in sorted(replacements.items(), key=lambda pair: -len(pair[0])):
            value = value.replace(source, target)
        return value
    if isinstance(value, list):
        return [project(item, replacements) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key == 'response_body_raw':
            result['raw_response_body_omitted'] = True
            result['raw_response_body_bytes'] = len(item.encode())
            continue
        if key in ('history', 'applied_rebalance_plans'):
            result[key + '_omitted_canonical_sha256'] = sha(item)
            result[key + '_omitted_count'] = len(item)
            continue
        if key == 'payload' and isinstance(item, str):
            result['payload_bytes_omitted'] = len(item.encode())
            result['payload_bytes_sha256'] = hashlib.sha256(item.encode()).hexdigest()
            continue
        if key == 'value':
            result['original_value_canonical_sha256'] = sha(item)
            result['original_value_is_null'] = item is None
        result[key] = project(item, replacements)
    return result


def write_gzip(path, rows):
    with path.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0, filename='') as output:
            for row in rows:
                output.write(canonical(row) + b'\n')


def collect(journal, output, reader, replacements):
    counts = collections.Counter()
    latencies = collections.defaultdict(list)
    logical_outcomes = collections.Counter()
    source_sha = hashlib.sha256()
    with journal.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            source_sha.update(block)

    def rows():
        for row in reader(journal):
            counts[row['kind']] += 1
            if row['kind'] == 'operation_terminal':
                logical_outcomes[row['outcome']] += 1
                latencies[row['operation'] + ':' + row['outcome']].append(row['logical_elapsed_ns'])
            result = project(row, replacements)
            result['raw_record_sha256'] = result.pop('record_sha256')
            result['raw_previous_sha256'] = result.pop('previous_sha256')
            result['projection_is_raw_wire_capture'] = False
            yield result
    write_gzip(output, rows())
    return {'raw_journal_sha256': source_sha.hexdigest(), 'raw_journal_bytes': journal.stat().st_size,
            'raw_hash_chain_validated': True, 'projected_records': sum(counts.values()),
            'record_counts': dict(counts), 'terminal_outcomes': dict(logical_outcomes),
            'raw_latency_sample_counts': {key: len(values) for key, values in latencies.items()},
            'projection_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
            'projection_is_raw_wire_capture': False, 'strong_read_proof': False,
            'qualification_status': 'not_qualified'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--frozen-client', type=pathlib.Path, required=True)
    parser.add_argument('--expected-client-sha256', required=True)
    parser.add_argument('--replacement-map', type=pathlib.Path, required=True)
    args = parser.parse_args()
    if hashlib.sha256(args.frozen_client.read_bytes()).hexdigest() != args.expected_client_sha256:
        parser.error('frozen source digest differs')
    args.output.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location('frozen_reader', args.frozen_client)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    replacements = json.loads(args.replacement_map.read_text())
    result = collect(args.journal, args.output/'records.jsonl.gz', module.read_journal, replacements)
    (args.output/'projection.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
