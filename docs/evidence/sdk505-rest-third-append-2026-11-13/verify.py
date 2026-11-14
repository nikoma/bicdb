#!/usr/bin/env python3
"""Portable original SDK505 failure verification; no Git/network/BicDB opens."""
import argparse
import gzip
import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path
CLASSIFICATION_SHA = '7fe8d72d0570490aa82f55ced402e6bb6f41d84edd63c20003f04563b6a45816'
RAW_SHA = 'e4b08eb8746494e23f63a64e358a27d2facc1e45a8b4826012a45b8aa2f94bfa'
RAW_BYTES = 85809455
PRIVATE_BLOCK = re.compile(b'-----BEGIN (?:(?:RSA|EC|OPENSSH|ENCRYPTED|DSA) )?PRIVATE KEY-----')

def require(condition, detail):
    if not condition:
        raise ValueError(detail)

def digest(data):
    return hashlib.sha256(data).hexdigest()

def read_json(path):
    return json.loads(path.read_bytes())

def git_object_id(kind, data):
    return hashlib.sha1(kind.encode() + b' ' + str(len(data)).encode() + b'\x00' + data).hexdigest()

def commit_tree(root, commit):
    raw = (root / 'proofs/git/commits' / (commit + '.txt')).read_bytes()
    require(git_object_id('commit', raw) == commit, 'Git commit object identity: ' + commit)
    match = re.match(b'tree ([0-9a-f]{40})\\n', raw)
    require(match is not None, 'Git commit tree missing')
    return match[1].decode()

def tree_entries(root, sha):
    raw = (root / 'proofs/git/trees' / (sha + '.bin')).read_bytes()
    require(git_object_id('tree', raw) == sha, 'Git tree object identity: ' + sha)
    entries = {}
    offset = 0
    while offset < len(raw):
        space = raw.index(b' ', offset)
        nul = raw.index(b'\x00', space + 1)
        require(nul + 21 <= len(raw), 'Truncated Git tree entry')
        name = raw[space + 1:nul].decode()
        require(name not in entries, 'Duplicate Git tree member')
        entries[name] = (raw[offset:space].decode(), raw[nul + 1:nul + 21].hex())
        offset = nul + 21
    return entries

def member_blob(root, tree, path):
    parts = Path(path).parts
    require(parts and all((p not in ('.', '..') for p in parts)), 'Unsafe source path')
    for index, part in enumerate(parts):
        entries = tree_entries(root, tree)
        require(part in entries, 'Missing Git source membership: ' + path)
        mode, tree = entries[part]
        if index + 1 < len(parts):
            require(mode in ('40000', '040000'), 'Expected Git directory')
        else:
            require(mode in ('100644', '100755'), 'Expected ordinary source blob')
    return tree

def file_digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(65536), b''):
            value.update(chunk)
    return value.hexdigest()

def verify(root):
    manifest = read_json(root / 'manifest.json')
    expected = {v['path']: v for v in manifest['files']}
    actual = set()
    for path in root.rglob('*'):
        require(not path.is_symlink(), 'Symlink in packet')
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    require(actual == set(expected) | {'manifest.json'}, 'Manifest file set differs')
    for name, entry in expected.items():
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'Unsafe manifest path')
        path = root / name
        require(path.stat().st_size == entry['bytes'] and file_digest(path) == entry['sha256'], 'Hash/size mismatch: ' + name)
    raw_class = (root / 'classification-original.json').read_bytes()
    require(digest(raw_class) == CLASSIFICATION_SHA, 'Original classification changed')
    report = json.loads(raw_class)
    recorded = read_json(root / 'ledger-original.json')
    boundary = read_json(root / 'raw-boundaries.json')
    names = {r['test'] for r in report['test_results']}
    checksum = hashlib.sha256()
    total = 0
    line_count = 0
    max_line = 0
    records = []
    record_positions = []
    tests = []
    summaries = []
    checkouts = []
    pending = False
    literal_hits = 0
    with gzip.open(root / 'raw-rest-job.log.gz', 'rb') as stream:
        while True:
            line = stream.readline(65537)
            if not line:
                break
            require(len(line) <= 65536, 'Original log line exceeds bounded parser limit')
            checksum.update(line)
            total += len(line)
            line_count += 1
            max_line = max(max_line, len(line))
            require(total <= 128 * 1024 * 1024, 'Original log exceeds bounded parser limit')
            literal_hits += len(PRIVATE_BLOCK.findall(line))
            text = line.decode()
            if 'git log -1 --format=%H' in text:
                pending = True
            elif pending:
                found = re.search('\\b([0-9a-f]{40})\\s*$', text)
                if found:
                    checkouts.append(found[1])
                    pending = False
            marker = 'NATIVE_INITIAL_DATA_EXTERNAL_LEDGER '
            if marker in text:
                value = json.loads(text.split(marker, 1)[1])
                records.append(value)
                record_positions.append({'line': line_count, 'stage': value.get('stage'), 'logical_operation_id': value.get('logical_operation_id'), 'key': value.get('key')})
            found = re.search('test (\\S+) \\.\\.\\. (ok|FAILED|ignored)', text)
            if found and found[1] in names:
                tests.append({'test': found[1], 'result': found[2], 'raw_line': line_count})
            found = re.search('test result: FAILED\\. (\\d+) passed; (\\d+) failed; (\\d+) ignored; (\\d+) measured; (\\d+) filtered out; finished in ([0-9.]+)s', text)
            if found:
                summaries.append(found.groups())
            for field in ['failed_stdout_header', 'panic', 'target_summary']:
                if line_count == boundary[field]['line']:
                    require(text.rstrip() == boundary[field]['text'], 'Raw source/failure boundary differs')
    require(total == RAW_BYTES == report['raw_log']['uncompressed_bytes'] and checksum.hexdigest() == RAW_SHA == report['raw_log']['uncompressed_sha256'], 'Original raw log identity mismatch')
    require(line_count == boundary['full_raw_lines'] and max_line == boundary['maximum_raw_line_bytes'], 'Original raw log framing differs')
    require(records == recorded, 'Retained ledger differs from original raw log')
    require(len(records) == 18 and record_positions == boundary['ledger_record_raw_line_positions'], 'Original ledger count/positions differ')
    require(tests == report['test_results'] == boundary['test_results'], 'Original six test outcomes differ')
    require(len(summaries) == 1, 'Expected one failed target summary in stopped REST job')
    summary = summaries[0]
    require(tuple(map(int, summary[:5])) == (5, 1, 0, 0, 0) and float(summary[5]) == 52.18, 'Original failed5/1 target summary changed')
    require(sum((t['result'] == 'ok' for t in tests)) == 5 and sum((t['result'] == 'FAILED' for t in tests)) == 1, 'Original failure was promoted')
    identity = read_json(root / 'source-and-binary-identity.json')
    require(checkouts == [identity['actual_checkout_sha']], 'Actual checkout not derived from raw log')
    head_tree = commit_tree(root, identity['run_head_sha'])
    checkout_tree = commit_tree(root, identity['actual_checkout_sha'])
    require(identity['run_head_sha'] != identity['actual_checkout_sha'], 'Different checkout/head SHAs collapsed')
    require(head_tree == checkout_tree == identity['tree'], 'Complete checkout/head trees differ')
    for pin in report['source_pins']:
        source = (root / 'sources' / pin['path']).read_bytes()
        require(len(source) == pin['bytes'] and digest(source) == pin['sha256'], 'Source copy differs')
        blob = git_object_id('blob', source)
        require(blob == pin['blob'] == member_blob(root, head_tree, pin['path']) == member_blob(root, checkout_tree, pin['path']), 'Source membership proof differs')
    binary = records[0]['execution_identity']
    require(binary['source'] == 'UNAVAILABLE_RUNTIME_SOURCE' and binary['source_available'] is False, 'Original runtime source was retrofilled')
    require(identity['source_runtime_env_as_observed'] == 'UNAVAILABLE_RUNTIME_SOURCE' and identity['source_env_not_retrofilled'] is True, 'Unavailable runtime source changed')
    require(identity['binary_identity'] == binary == report['binary_execution_identity_as_observed'], 'Measured binary identity differs')
    require(binary['binary_sha256'] == '67468c83b7145ac299e1e20cf01b4aa31167520988b7d8d9ea9464c8fc39b09f' and binary['binary_bytes'] == 774154024, 'Original binary measurement differs')
    replies = [v for v in records if v.get('logical_operation_id') and v.get('stage') == 'reply' and v.get('key', '').startswith('external-')]
    require(len(replies) == 3, 'Three positive append replies required')
    acks = [v for v in replies if v.get('acknowledged') is True]
    uncertain = [v for v in replies if v.get('acknowledged') is False]
    reads = [v for v in records if v.get('logical_operation_id') and v.get('stage') == 'strong_reply']
    require(len(acks) == len(reads) == 2 and len(uncertain) == 1, 'Original2ACK/2Strong/third uncertainty changed')
    for reply in replies:
        attempt = [v for v in records if v.get('logical_operation_id') == reply['logical_operation_id'] and v.get('stage') == 'attempt']
        require(len(attempt) == 1 and attempt[0]['namespace'] == 'items' and (attempt[0]['key'] == reply['key']) and (attempt[0]['value'] == reply['value']), 'Positive append intent/reply differs')
        payload = json.dumps(reply['value'], separators=(',', ':'), ensure_ascii=False).encode()
        require(digest(payload) == reply['payload_sha256'] == attempt[0]['payload_sha256'], 'Expected payload hash differs')
    for read in reads:
        ack = next((v for v in acks if v['key'] == read['key']))
        require(read['present'] is True and read['value'] == ack['value'], 'Strong full value/present-null distinction changed')
        require(records.index(ack) < records.index(read), 'Strong reply does not follow recorded ACK')
    third = uncertain[0]
    require(third['key'] == 'external-empty-object' and third['value'] == {} and (third['inserted'] is None) and ('query timeout' in third['error']), 'Third uncertain append changed')
    require(third['logical_operation_id'] == '01a1d7a9-7fda-7e02-bc20-f7cecc2e95d9', 'Original uncertain operation ID differs')
    require(not any((r['key'] == third['key'] for r in reads)), 'Invented post-uncertain Strong reconciliation')
    negative = [v for v in records if v.get('key') == 'before-initial-data-quorum' and v.get('stage') == 'reply']
    require(len(negative) == 1 and negative[0]['acknowledged'] is False, 'Expected negative preopen control changed')
    job, run = (read_json(root / 'job-terminal.json'), read_json(root / 'run-observation.json'))
    require(job['id'] == report['job'] and job['run_id'] == run['id'] == report['run'], 'Run/job identity differs')
    require(job['status'] == 'completed' and job['conclusion'] == 'failure', 'Original REST job failure changed')
    require(job['head_sha'] == run['head_sha'] == identity['run_head_sha'], 'Recorded API source head differs')
    require(run['repository']['private'] is False, 'Public repository metadata differs')
    for name in expected:
        if name.endswith('.gz'):
            continue
        with (root / name).open('rb') as stream:
            carry = b''
            for chunk in iter(lambda: stream.read(65536), b''):
                part = carry + chunk
                literal_hits += len(PRIVATE_BLOCK.findall(part))
                carry = part[-128:]
    require(literal_hits == 0, 'Recognized private key block marker found')
    return {'status': 'passed', 'scope': 'evidence integrity; native CLI target remainsFAILED', 'native_cli_passed': 5, 'native_cli_failed': 1, 'native_cli_ignored': 0, 'target_seconds_not_RTO': 52.18, 'original_ledger_records': len(records), 'positive_append_ACKs': len(acks), 'Strong_full_present_replies': len(reads), 'uncertain_third_appends': len(uncertain), 'expected_negative_preopen_separate': True, 'runtime_source': 'UNAVAILABLE_RUNTIME_SOURCE', 'binary_hash_basis': 'fixture measurement in original log; binary file not retained', 'actual_checkout': identity['actual_checkout_sha'], 'verified_equal_tree': head_tree, 'source_pin_count': len(report['source_pins']), 'literal_private_block_matches': literal_hits, 'does_not_guarantee_secret_absence': True, 'database_opened': False, 'physical_or_production_qualification': False}

def update_manifest_entry(root, name):
    manifest = read_json(root / 'manifest.json')
    for item in manifest['files']:
        if item['path'] == name:
            item.update(bytes=(root / name).stat().st_size, sha256=file_digest(root / name))
    (root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    result = verify(root)
    if args.self_test:
        passed = []
        for mode in ['metadata_hash', 'ledger_semantics', 'missing_source_semantics']:
            with tempfile.TemporaryDirectory(prefix='bicdb-sdk505-tamper-') as tmp:
                copy = Path(tmp) / 'packet'
                shutil.copytree(root, copy)
                if mode == 'metadata_hash':
                    p = copy / 'job-terminal.json'
                    p.write_bytes(p.read_bytes() + b'\n')
                    expected = 'Hash/size mismatch: job-terminal.json'
                elif mode == 'ledger_semantics':
                    p = copy / 'ledger-original.json'
                    value = read_json(p)
                    value[11]['acknowledged'] = False
                    p.write_text(json.dumps(value))
                    update_manifest_entry(copy, p.name)
                    expected = 'Retained ledger differs from original raw log'
                else:
                    p = copy / 'source-and-binary-identity.json'
                    value = read_json(p)
                    value['source_runtime_env_as_observed'] = value['run_head_sha']
                    p.write_text(json.dumps(value))
                    update_manifest_entry(copy, p.name)
                    expected = 'Unavailable runtime source changed'
                try:
                    verify(copy)
                except ValueError as error:
                    require(expected in str(error), 'Unexpected tamper rejection: ' + str(error))
                else:
                    raise ValueError('Mutation not detected: ' + mode)
            passed.append(mode)
        result['tamper_controls_passed'] = passed
    print(json.dumps(result, indent=2))
if __name__ == '__main__':
    main()
