#!/usr/bin/env python3
"""Verify retained test custody and reported outputs, not engine recovery."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import zipfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def relative(value):
    path = PurePosixPath(value)
    require(not path.is_absolute() and '..' not in path.parts, 'unsafe archive path')
    return path


def verify(directory, expected_source=None):
    summary = json.loads((directory / 'summary.json').read_text())
    source = expected_source or summary['source_sha']
    require(source == summary['source_sha'], 'unexpected source revision')
    archive = directory / summary['archive']
    require(sha(archive.read_bytes()) == summary['archive_sha256'], 'archive digest mismatch')
    with zipfile.ZipFile(archive) as bundle:
        names = bundle.namelist()
        require(len(names) == len(set(names)) and len(names) <= 8192, 'duplicate/excess entries')
        require(sum(x.file_size for x in bundle.infolist()) <= 256 * 1024 * 1024,
                'uncompressed archive exceeds bound')
        for name in names:
            relative(name)
        require(bundle.testzip() is None, 'archive CRC mismatch')
        manifests = [n for n in names if n.endswith('/stopped-original/manifest.json')]
        require(len(manifests) == 1, 'expected one stopped-original manifest')
        name = manifests[0]
        prefix = PurePosixPath(name).parent
        encoded = bundle.read(name)
        manifest = json.loads(encoded)
        require(sha(encoded) == summary['manifest_sha256'], 'manifest digest mismatch')
        require(bundle.read(str(prefix / 'manifest.sha256')).decode().split()[0]
                == summary['manifest_sha256'], 'manifest sidecar mismatch')
        require(manifest['source_sha'] == source, 'manifest source mismatch')
        require(manifest['state'] == 'FULLY_STOPPED_ORIGINAL_ONLY'
                and manifest['private_tls_excluded']
                and not manifest['live_case_images_copied'], 'unexpected custody scope')
        expected, total = {name, str(prefix / 'manifest.sha256')}, 0
        for item in manifest['files']:
            path = relative(item['relative_path'])
            require(path.suffix.lower() not in {'.key', '.pem', '.pfx', '.p12', '.crt', '.csr'},
                    'credential-like path in stopped custody')
            member = str(prefix / path)
            require(member not in expected, 'duplicate manifest entry')
            data = bundle.read(member)
            require(len(data) == item['bytes'] and sha(data) == item['sha256'],
                    'stopped file bytes/digest mismatch: ' + str(path))
            expected.add(member)
            total += len(data)
        actual = {n for n in names if n.startswith(str(prefix) + '/') and not n.endswith('/')}
        require(actual == expected, 'stopped file set mismatch')
        require(len(manifest['files']) == summary['file_count'] == manifest['file_count']
                and total == summary['total_bytes'] == manifest['total_bytes'],
                'stopped custody totals mismatch')
        catalogue_bytes = bundle.read(str(prefix / 'data/n1/cluster-range-write-log.json'))
        require(sha(catalogue_bytes) == summary['removed_catalogue_sha256'],
                'removed catalogue differs from retained original')
        envelope = json.loads(catalogue_bytes)
        catalogue = envelope['catalog']
        compact = json.dumps(catalogue, separators=(',', ':'), ensure_ascii=False).encode()
        require(sha(compact) == envelope['checksum_sha256'], 'original catalogue checksum mismatch')
        require(catalogue['node_id'] == 'n1', 'wrong retained catalogue actor')
        entry = catalogue['ranges'][str(summary['range_id'])]['entries']['1']
        require(entry['state'] == 'quorum_committed'
                and entry['command']['checksum_sha256'] == summary['command_sha256'],
                'retained original does not contain expected CQ1 command')
        reports = []
        for name in sorted(n for n in names if '/reports/' in n and n.endswith('.json')):
            outer = json.loads(bundle.read(name))
            require(outer['source_sha'] == source and outer['source_verified'],
                    'report source mismatch')
            reports.append(outer['report'])
        require(len(reports) == summary['report_count'], 'report count mismatch')
        verdicts = {}
        for report in reports:
            if report.get('classification') in {summary['intact_result'], summary['damaged_result']}:
                require(report['classification'] not in verdicts, 'duplicate final verdict')
                verdicts[report['classification']] = report
        require(len(verdicts) == 2, 'missing paired final verdicts')
        for classification, index, presence in [
                (summary['intact_result'], 1, 'present'),
                (summary['damaged_result'], 0, 'absent')]:
            report = verdicts[classification]
            reply = report['typed_reply']
            result, audit = reply['result'], reply['result']['audit']
            require(report['record_id'] == summary['record_id']
                    and report['key'] == summary['key']
                    and report['actual_prepared_cut'] == index, 'wrong record/prepared cut')
            require(report['ack_ledger'][0]['command_sha256'] == summary['command_sha256']
                    and report['ack_ledger'][0]['value_sha256'] == summary['value_sha256'],
                    'different acknowledged operation')
            require(result['kind'] == 'value' and result['value']['presence'] == presence,
                    'verdict is not the expected successful value/absence')
            require(result['route'] == {
                'generation': summary['topology_generation'], 'leader': summary['recovery_leader'],
                'range_epoch': summary['recovery_epoch'], 'range_id': summary['range_id']},
                'unexpected route')
            require(audit['range_index'] == index and audit['range_epoch'] == summary['recovery_epoch']
                    and audit['final_metadata_term'] == summary['metadata_term']
                    and audit['final_metadata_read_index'] == summary['metadata_read_index']
                    and audit['final_metadata_generation'] == summary['topology_generation']
                    and audit['first_metadata_context_id'] != audit['final_metadata_context_id']
                    and audit['business_request_id'] == reply['business_request_id']
                    and audit['challenge'] == reply['challenge'], 'unexpected read audit')
            if presence == 'present':
                value = json.dumps(result['value']['value'], sort_keys=True,
                                   separators=(',', ':'), ensure_ascii=False).encode()
                require(sha(value) == summary['value_sha256'], 'intact payload differs')
            else:
                require(report['deleted_file_sha256'] == summary['removed_catalogue_sha256']
                        and report['discovered_command_sha256'] is None
                        and audit['logical_history_sha256'] == audit['logical_genesis_sha256'],
                        'damaged case is not the documented clean-zero reconstruction')
    return {'verification': 'passed', 'classification': summary['classification'],
            'source_sha': source, 'stopped_files_verified': summary['file_count'],
            'reports_verified': summary['report_count'],
            'scope': 'Custody/checksum and recorded paired-output verification only; no engine replay, '
                     'signature verification, physical loss claim or production qualification.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--expected-source')
    args = parser.parse_args()
    print(json.dumps(verify(args.directory, args.expected_source), indent=2))


if __name__ == '__main__':
    main()
